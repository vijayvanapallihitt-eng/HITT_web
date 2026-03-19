"""
Company Evaluation Worker.

Runs after document ingestion.  For each company that has embedded chunks in
ChromaDB but no evaluation yet, this worker:

  1. Queries the vector DB for revenue / financial evidence
  2. Queries the vector DB for headcount / employee evidence
  3. Sends the top chunks to OpenAI to extract structured data
  4. Stores the evaluation (revenue estimate, headcount, confidence) in
     the ``company_evaluations`` Postgres table

Usage:
    python worker_evaluate.py                       # default settings
    python worker_evaluate.py --batch 20 --poll 30  # custom batch / poll
    python worker_evaluate.py --once                 # single pass, then exit
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(dotenv_path=ROOT / ".env", override=False)

from broker.embeddings.factory import get_openai_api_key
from broker.models import CompanyEvaluationRecord
from broker.storage.chroma_store import get_or_create_collection
from broker.storage.postgres import (
    connect_postgres,
    count_results_pending_evaluation,
    ensure_pipeline_schema,
    select_results_pending_evaluation,
    upsert_company_evaluation,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [EVALUATE] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
CHROMA_DIR = str(ROOT / "runtime" / "chroma" / "chroma_smoke_db")
COLLECTION_NAME = "construction_docs_openai1536_live"
EMBEDDING_MODEL = "text-embedding-3-small"
TOP_K = 8        # chunks to retrieve per semantic query
MAX_COMPANY_CHUNKS = 50  # max chunks to pull for a single company


# ---------------------------------------------------------------------------
# Vector search helpers
# ---------------------------------------------------------------------------

def embed_query(text: str, client) -> list[float]:
    """Embed a single query string using OpenAI."""
    resp = client.embeddings.create(model=EMBEDDING_MODEL, input=[text])
    return resp.data[0].embedding


def get_all_company_chunks(collection, company: str, limit: int = MAX_COMPANY_CHUNKS) -> list[dict]:
    """Pull ALL chunks for a company from ChromaDB (metadata filter, no embedding needed)."""
    try:
        result = collection.get(
            where={"company": company},
            limit=limit,
            include=["documents", "metadatas"],
        )
    except Exception:
        return []

    ids = result.get("ids") or []
    docs = result.get("documents") or []
    metas = result.get("metadatas") or []

    chunks = []
    for cid, doc, meta in zip(ids, docs, metas):
        chunks.append({
            "id": cid,
            "text": doc[:1500],
            "company": (meta or {}).get("company", ""),
            "url": (meta or {}).get("url_fetched", ""),
            "title": (meta or {}).get("page_title", ""),
            "source_type": (meta or {}).get("source_type", ""),
            "distance": None,
        })
    return chunks


def search_chunks(collection, client, query: str, company: str, k: int = TOP_K) -> list[dict]:
    """Query ChromaDB for chunks relevant to a company + topic (semantic search)."""
    emb = embed_query(query, client)

    where = {"company": company}
    try:
        result = collection.query(
            query_embeddings=[emb],
            n_results=k,
            where=where,
            include=["documents", "metadatas", "distances"],
        )
    except Exception:
        # Fallback: query without company filter
        result = collection.query(
            query_embeddings=[emb],
            n_results=k,
            include=["documents", "metadatas", "distances"],
        )

    ids = (result.get("ids") or [[]])[0]
    docs = (result.get("documents") or [[]])[0]
    metas = (result.get("metadatas") or [[]])[0]
    dists = (result.get("distances") or [[]])[0]

    chunks = []
    for i, (cid, doc, meta, dist) in enumerate(zip(ids, docs, metas, dists)):
        chunks.append({
            "id": cid,
            "text": doc[:1500],
            "company": (meta or {}).get("company", ""),
            "url": (meta or {}).get("url_fetched", ""),
            "title": (meta or {}).get("page_title", ""),
            "source_type": (meta or {}).get("source_type", ""),
            "distance": dist,
        })
    return chunks


def gather_all_evidence(collection, openai_client, company: str) -> list[dict]:
    """Gather ALL available chunks for a company — metadata lookup + semantic boost."""
    # 1. Pull every chunk tagged with this company name
    all_chunks = get_all_company_chunks(collection, company)
    seen_ids = {c["id"] for c in all_chunks}

    # 2. Semantic search to catch any chunks not tagged with the exact company name
    for query in [
        f"{company} revenue earnings annual sales financial",
        f"{company} employees headcount team size staff workforce",
    ]:
        semantic = search_chunks(collection, openai_client, query, company, k=TOP_K)
        for c in semantic:
            if c["id"] not in seen_ids:
                seen_ids.add(c["id"])
                all_chunks.append(c)

    return all_chunks


# ---------------------------------------------------------------------------
# OpenAI extraction
# ---------------------------------------------------------------------------

EXTRACTION_PROMPT = """You are a business intelligence analyst.  You are given ALL available
source articles and web pages about the company "{company}".  Your job is to find every
possible piece of financial and staffing data.

Extract the following:

1. **estimated_revenue** — Annual revenue or most recent revenue figure mentioned.
   Use the exact figure if stated (e.g. "$12M", "$500K/year", "~$2 billion").
   If multiple figures appear, use the most recent or most authoritative one.
   If not mentioned, say "Unknown".

2. **revenue_confidence** — One of: high, medium, low, none.
   "high" = an explicit figure is cited with a credible source.
   "medium" = a range or indirect estimate (e.g. employee-based inference, ranking lists).
   "low" = only vague language. "none" = no evidence at all.

3. **estimated_headcount** — Number of employees / team size.
   Use the exact number or range if stated (e.g. "50", "200-500", "~1000").
   If not mentioned, say "Unknown".

4. **headcount_confidence** — One of: high, medium, low, none.

5. **evidence_summary** — A thorough 3-6 sentence summary of ALL revenue-related and
   staffing-related facts you found across every article.  Include:
   - Every revenue figure, financial milestone, contract value, or project dollar amount
   - Every headcount, team size, or staffing reference
   - Growth indicators (e.g. "doubled revenue", "expanded to 200 employees")
   - Awards, rankings, or market position that imply size
   IMPORTANT: Reference each fact to its source article title and URL.
   For example: 'According to "ABC Inc. Annual Report" (https://example.com/report), revenue was $12M.'
   If the company website was crawled, cite it as well.

Respond ONLY with valid JSON (no markdown, no code fences):
{{"estimated_revenue": "...", "revenue_confidence": "...", "estimated_headcount": "...", "headcount_confidence": "...", "evidence_summary": "..."}}

--- SOURCE ARTICLES ({num_articles} total) ---
{chunks}
"""


def build_chunks_text(chunks: list[dict]) -> str:
    parts = []
    for i, c in enumerate(chunks, 1):
        title = c.get("title") or "Untitled"
        url = c.get("url") or "no URL"
        parts.append(
            f'--- Article {i}: "{title}" ---\n'
            f"URL: {url}\n"
            f"{c['text']}\n"
        )
    return "\n".join(parts)


def extract_evaluation(client, company: str, chunks: list[dict]) -> dict:
    """Call OpenAI to extract revenue + headcount from chunk evidence."""
    chunks_text = build_chunks_text(chunks)
    prompt = EXTRACTION_PROMPT.format(
        company=company, chunks=chunks_text, num_articles=len(chunks),
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.1,
            messages=[
                {"role": "system", "content": "You extract structured business data from text. Respond only with valid JSON."},
                {"role": "user", "content": prompt},
            ],
            max_tokens=800,
        )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[:-3]
        return json.loads(raw)
    except json.JSONDecodeError:
        log.warning("  Failed to parse OpenAI response as JSON for %s", company)
        return {
            "estimated_revenue": "Unknown",
            "revenue_confidence": "none",
            "estimated_headcount": "Unknown",
            "headcount_confidence": "none",
            "evidence_summary": f"OpenAI response was not valid JSON: {raw[:200]}",
        }
    except Exception as exc:
        log.error("  OpenAI API error for %s: %s", company, exc)
        return {
            "estimated_revenue": "Unknown",
            "revenue_confidence": "none",
            "estimated_headcount": "Unknown",
            "headcount_confidence": "none",
            "evidence_summary": f"API error: {exc}",
        }


# ---------------------------------------------------------------------------
# Evaluation cycle
# ---------------------------------------------------------------------------

def evaluate_batch(collection, openai_client, batch_size: int) -> int:
    """Evaluate one batch of companies. Returns number evaluated."""
    with connect_postgres(autocommit=False) as conn:
        rows = select_results_pending_evaluation(conn, batch_size)
        if not rows:
            return 0

        log.info("Evaluating %d companies...", len(rows))
        evaluated = 0

        for row in rows:
            company = row["company"]
            result_id = row["result_id"]
            log.info("  Evaluating: %s (result_id=%d)", company, result_id)

            # Gather ALL chunks for this company (metadata + semantic)
            combined = gather_all_evidence(collection, openai_client, company)

            if not combined:
                log.info("    No chunks found, skipping")
                # Still record that we tried, so we don't re-evaluate
                record = CompanyEvaluationRecord(
                    result_id=result_id,
                    company=company,
                    estimated_revenue="Unknown",
                    revenue_confidence="none",
                    estimated_headcount="Unknown",
                    headcount_confidence="none",
                    evidence_summary="No document chunks found for this company.",
                    chunks_used=0,
                )
                upsert_company_evaluation(conn, record)
                conn.commit()
                evaluated += 1
                continue

            # Extract structured data via OpenAI
            extracted = extract_evaluation(openai_client, company, combined)

            record = CompanyEvaluationRecord(
                result_id=result_id,
                company=company,
                estimated_revenue=extracted.get("estimated_revenue", "Unknown"),
                revenue_confidence=extracted.get("revenue_confidence", "none"),
                estimated_headcount=extracted.get("estimated_headcount", "Unknown"),
                headcount_confidence=extracted.get("headcount_confidence", "none"),
                evidence_summary=extracted.get("evidence_summary", ""),
                chunks_used=len(combined),
            )
            upsert_company_evaluation(conn, record)
            conn.commit()
            evaluated += 1

            log.info(
                "    Revenue: %s (%s) | Headcount: %s (%s) | Chunks: %d",
                record.estimated_revenue,
                record.revenue_confidence,
                record.estimated_headcount,
                record.headcount_confidence,
                record.chunks_used,
            )

        return evaluated


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Company evaluation worker")
    p.add_argument("--batch", type=int, default=10, help="Companies per cycle (default: 10)")
    p.add_argument("--poll", type=int, default=30, help="Seconds between cycles (default: 30)")
    p.add_argument("--once", action="store_true", help="Run one cycle and exit")
    p.add_argument("--chroma-dir", default=CHROMA_DIR, help="ChromaDB persist directory")
    p.add_argument("--collection", default=COLLECTION_NAME, help="ChromaDB collection name")
    return p


def main() -> None:
    args = build_parser().parse_args()

    # Ensure the company_evaluations table exists
    ensure_pipeline_schema()

    # Check OpenAI key
    api_key = get_openai_api_key()
    if not api_key:
        log.error("OPENAI_API_KEY not set. Cannot run evaluations.")
        sys.exit(1)

    from openai import OpenAI
    openai_client = OpenAI(api_key=api_key)

    # Connect to ChromaDB
    collection = get_or_create_collection(args.chroma_dir, args.collection)
    vec_count = collection.count()
    log.info("=" * 55)
    log.info("  Company Evaluation Worker")
    log.info("  ChromaDB: %s (%d vectors)", args.collection, vec_count)
    log.info("  Batch: %d | Poll: %ds", args.batch, args.poll)
    log.info("=" * 55)

    if vec_count == 0:
        log.warning("ChromaDB collection is empty — nothing to evaluate yet.")
        if args.once:
            return

    while True:
        try:
            n = evaluate_batch(collection, openai_client, args.batch)
            if n:
                log.info("Evaluated %d companies this cycle.", n)

                # Check remaining
                with connect_postgres(autocommit=True) as conn:
                    remaining = count_results_pending_evaluation(conn)
                log.info("Remaining to evaluate: %d", remaining)
            else:
                log.info("No companies pending evaluation. Sleeping %ds...", args.poll)

        except KeyboardInterrupt:
            log.info("Interrupted — exiting.")
            break
        except Exception as exc:
            log.error("Error in evaluation cycle: %s", exc, exc_info=True)

        if args.once:
            break

        time.sleep(args.poll)

    log.info("Evaluation worker stopped.")


if __name__ == "__main__":
    main()
