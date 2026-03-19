"""
Unified pipeline — discover, fetch, chunk, embed in one pass per company.

Instead of three separate polling workers that hand off via DB state,
this module processes each company through the full pipeline:

  1. News link discovery  (Google News search → link_candidates)
  2. Website spidering    (crawl4ai → link_candidates + pre-fetched documents)
  3. Fetch remaining      (HTTP fetch for news URLs → documents)
  4. Chunk & embed        (split text → document_chunks + ChromaDB upsert)

All within a single DB transaction per company, so there's no polling gap.
"""

from __future__ import annotations

import hashlib
import logging
import random
import re
import time
from dataclasses import dataclass, field
from typing import Any, Callable

import psycopg2
import psycopg2.errors
import requests

from broker.documents.chunking import chunk_text
from broker.documents.fetch import fetch_url_text
from broker.documents.url_resolver import canonicalize_url, resolve_google_news_url
from broker.embeddings.factory import get_embedder, load_env_from_file
from broker.enrichment.link_discovery import discover_company_links
from broker.enrichment.proxies import ProxyPool
from broker.models import DocumentChunkRecord, DocumentRecord, LinkCandidateRecord
from broker.storage.chroma_store import get_or_create_collection, upsert_chunks
from broker.storage.postgres import (
    connect_postgres,
    ensure_pipeline_schema,
    insert_document,
    insert_document_chunk,
    mark_document_chunks_embedded,
    update_document_fetch_status,
    upsert_link_candidate,
)

logger = logging.getLogger(__name__)


# ── helpers ──────────────────────────────────────────────────────────────────


def _sha1(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()


def _make_chroma_id(chunk_row_id: int) -> str:
    return f"document_chunk:{chunk_row_id}"


# Relevance filter (same logic as document_ingest.py)
_GENERIC_WORDS = {
    "construction", "contractor", "contracting", "builders", "building",
    "homes", "home", "group", "company", "companies", "corp", "corporation",
    "inc", "llc", "ltd", "services", "service", "general", "custom",
    "design", "build", "remodeling", "renovation", "renovations",
    "residential", "commercial", "industrial", "roofing", "concrete",
    "plumbing", "electrical", "hvac", "painting", "landscaping",
    "the", "and", "new", "york", "city", "nyc", "texas",
}


def _company_relevant(company: str, text: str, title: str) -> bool:
    if not company or not text:
        return False
    tokens = re.findall(r"[A-Za-z]{3,}", company)
    if not tokens:
        return True
    haystack = (title + " " + text[:8000]).lower()
    distinctive = [t for t in tokens if t.lower() not in _GENERIC_WORDS]
    if distinctive:
        return any(tok.lower() in haystack for tok in distinctive)
    return company.lower().strip() in haystack


# ── per-company stats ────────────────────────────────────────────────────────


@dataclass
class CompanyPipelineResult:
    """Outcome of processing one company through the full pipeline."""
    result_id: int | str = 0
    company: str = ""
    news_links: int = 0
    website_pages: int = 0
    documents_fetched: int = 0
    documents_ok: int = 0
    documents_skipped: int = 0
    chunks_written: int = 0
    chunks_embedded: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


# ── configuration ────────────────────────────────────────────────────────────


@dataclass
class UnifiedPipelineConfig:
    """All tunables for a unified pipeline run."""
    dsn: str = ""

    # news discovery
    news_top: int = 10
    max_search_retries: int = 2
    delay_min: float = 1.5
    delay_max: float = 3.5

    # website spidering
    spider_timeout: int = 20

    # document fetch
    fetch_timeout: int = 20
    max_chars: int = 25_000

    # chunking
    chunk_words: int = 220
    overlap_words: int = 50

    # embedding
    embedding_backend: str = "openai"
    embedding_model: str = "text-embedding-3-small"
    simple_dim: int = 384
    env_file: str = ""

    # chroma
    persist_dir: str = ""
    collection_name: str = "construction_docs_openai1536_live"

    # batching
    batch_size: int = 10

    def effective_embedding_name(self) -> str:
        if self.embedding_backend == "simple":
            return f"simple:{self.simple_dim}"
        if self.embedding_backend == "chroma-default":
            return "chroma-default"
        return self.embedding_model


# ── SQL: select companies pending full pipeline ──────────────────────────────


def select_results_pending_pipeline(conn, batch_size: int) -> list[dict[str, Any]]:
    """Select results that still need news discovery OR website spidering.

    This is the unified entrypoint — it picks companies that haven't been
    fully processed yet (missing news OR website link_candidates).
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            r.id,
            r.data->>'title' AS company,
            COALESCE(r.data->>'web_site', '') AS web_site,
            NOT EXISTS (
                SELECT 1 FROM link_candidates lc
                WHERE lc.result_id = r.id AND lc.source_type = 'news'
            ) AS needs_news,
            NOT EXISTS (
                SELECT 1 FROM link_candidates lc
                WHERE lc.result_id = r.id AND lc.source_type = 'website'
            ) AS needs_website
        FROM results r
        WHERE COALESCE(r.data->>'title', '') <> ''
          AND (
              NOT EXISTS (
                  SELECT 1 FROM link_candidates lc
                  WHERE lc.result_id = r.id AND lc.source_type = 'news'
              )
              OR (
                  COALESCE(r.data->>'web_site', '') <> ''
                  AND NOT EXISTS (
                      SELECT 1 FROM link_candidates lc
                      WHERE lc.result_id = r.id AND lc.source_type = 'website'
                  )
              )
          )
        ORDER BY r.id
        LIMIT %s
        """,
        (batch_size,),
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "result_id": int(row[0]),
            "company": str(row[1] or "").strip(),
            "web_site": str(row[2] or "").strip(),
            "needs_news": bool(row[3]),
            "needs_website": bool(row[4]),
        }
        for row in rows
    ]


def count_results_pending_pipeline(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM results r
        WHERE COALESCE(r.data->>'title', '') <> ''
          AND (
              NOT EXISTS (
                  SELECT 1 FROM link_candidates lc
                  WHERE lc.result_id = r.id AND lc.source_type = 'news'
              )
              OR (
                  COALESCE(r.data->>'web_site', '') <> ''
                  AND NOT EXISTS (
                      SELECT 1 FROM link_candidates lc
                      WHERE lc.result_id = r.id AND lc.source_type = 'website'
                  )
              )
          )
        """
    )
    count = int(cur.fetchone()[0] or 0)
    cur.close()
    return count


# ── step 1: news link discovery ──────────────────────────────────────────────


def _discover_news(
    conn,
    result_id: int,
    company: str,
    session: requests.Session,
    proxy_pool: ProxyPool | None,
    cfg: UnifiedPipelineConfig,
) -> list[int]:
    """Search Google News and write link_candidates. Returns list of lc IDs."""
    discovery = discover_company_links(
        company=company,
        session=session,
        proxy_pool=proxy_pool,
        news_top=cfg.news_top,
        max_retries=cfg.max_search_retries,
        delay_min=cfg.delay_min,
        delay_max=cfg.delay_max,
    )

    lc_ids: list[int] = []

    if discovery["news_status"] in ("ok", "no_results"):
        articles = discovery["news_articles"]
        if articles:
            for item in articles:
                lc_id = upsert_link_candidate(
                    conn,
                    LinkCandidateRecord(
                        result_id=result_id,
                        source_type="news",
                        query_text=discovery["news_query"],
                        url_discovered=str(item.get("url") or "").strip(),
                        title_discovered=str(item.get("title") or "").strip(),
                        discovery_status="ok",
                    ),
                )
                lc_ids.append(lc_id)
        else:
            # Record a no_results sentinel so we don't retry
            upsert_link_candidate(
                conn,
                LinkCandidateRecord(
                    result_id=result_id,
                    source_type="news",
                    query_text=discovery["news_query"],
                    url_discovered="",
                    title_discovered="",
                    discovery_status="no_results",
                ),
            )

    logger.info(
        "  [news] %s → %s (%d links)",
        company, discovery["news_status"], len(lc_ids),
    )
    return lc_ids


# ── step 2: website spidering ───────────────────────────────────────────────


def _spider_website(
    conn,
    result_id: int,
    company: str,
    web_site: str,
    cfg: UnifiedPipelineConfig,
) -> tuple[list[int], list[int]]:
    """Spider company website with crawl4ai. Returns (lc_ids, doc_ids)."""
    from broker.documents.website_spider import crawl_company_website

    url = web_site.strip()
    if not url:
        return [], []
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    try:
        pages = crawl_company_website(url, timeout=cfg.spider_timeout)
    except Exception as exc:
        logger.warning("  [spider] crawl failed for %s: %s", company, exc)
        pages = []

    lc_ids: list[int] = []
    doc_ids: list[int] = []

    if pages:
        for page in pages:
            lc_id = upsert_link_candidate(
                conn,
                LinkCandidateRecord(
                    result_id=result_id,
                    source_type="website",
                    query_text=f"company website: {company}",
                    url_discovered=page["url"],
                    title_discovered=page.get("title", "")[:500] or f"{company} — Website",
                    discovery_status="ok",
                ),
            )
            lc_ids.append(lc_id)

            # Store pre-fetched text immediately
            text = page.get("text", "")
            doc_id = insert_document(
                conn,
                DocumentRecord(
                    link_candidate_id=lc_id,
                    url_fetched=page["url"],
                    page_title=page.get("title", "")[:500] or "",
                    fetch_status="ok",
                    http_status=200,
                    text_hash=_sha1(text),
                    raw_text=text,
                ),
            )
            doc_ids.append(doc_id)
    else:
        # Fallback: seed homepage URL
        lc_id = upsert_link_candidate(
            conn,
            LinkCandidateRecord(
                result_id=result_id,
                source_type="website",
                query_text=f"company website: {company}",
                url_discovered=url,
                title_discovered=f"{company} — Official Website",
                discovery_status="ok",
            ),
        )
        lc_ids.append(lc_id)

    logger.info(
        "  [spider] %s → %d pages (%d pre-fetched)",
        company, len(lc_ids), len(doc_ids),
    )
    return lc_ids, doc_ids


# ── step 3: fetch unfetched link_candidates ──────────────────────────────────


def _fetch_documents(
    conn,
    lc_ids: list[int],
    company: str,
    session: requests.Session,
    cfg: UnifiedPipelineConfig,
) -> list[tuple[int, int, str]]:
    """Fetch pages for link_candidates that don't have a document yet.

    Returns list of (lc_id, doc_id, fetch_status) tuples.
    """
    if not lc_ids:
        return []

    # Find which lc_ids still need a document
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(lc_ids))
    cur.execute(
        f"""
        SELECT lc.id, lc.url_discovered, lc.source_type, lc.title_discovered
        FROM link_candidates lc
        LEFT JOIN documents d ON d.link_candidate_id = lc.id
        WHERE lc.id IN ({placeholders})
          AND d.id IS NULL
          AND lc.discovery_status = 'ok'
          AND COALESCE(lc.url_discovered, '') <> ''
        """,
        lc_ids,
    )
    pending = cur.fetchall()
    cur.close()

    results: list[tuple[int, int, str]] = []

    for lc_id, url_discovered, source_type, title_discovered in pending:
        # Resolve Google News redirect URLs
        target_url = url_discovered
        if source_type == "news":
            try:
                target_url = resolve_google_news_url(session, url_discovered, timeout=cfg.fetch_timeout)
            except Exception:
                pass
        target_url = canonicalize_url(target_url)

        fetched = fetch_url_text(
            session=session,
            url=target_url,
            timeout=cfg.fetch_timeout,
            max_chars=cfg.max_chars,
        )

        # Classify
        if fetched.get("ok"):
            page_title = str(fetched.get("title") or "").strip()
            page_text = str(fetched.get("text") or "")
            if _company_relevant(company, page_text, page_title):
                fetch_status = "ok"
            else:
                fetch_status = "irrelevant"
                logger.info("    [fetch] irrelevant for %s: %s", company, target_url[:100])
        else:
            error = str(fetched.get("error") or "").lower()
            if "unsupported content-type" in error:
                fetch_status = "unsupported_content_type"
            elif "empty extracted text" in error:
                fetch_status = "empty_text"
            elif fetched.get("http_status") is not None:
                fetch_status = "http_error"
            else:
                fetch_status = "request_error"

        doc_id = insert_document(
            conn,
            DocumentRecord(
                link_candidate_id=lc_id,
                url_fetched=str(fetched.get("url") or target_url),
                page_title=(
                    str(fetched.get("title") or "").strip()
                    or str(title_discovered or "").strip()
                ),
                fetch_status=fetch_status,
                http_status=fetched.get("http_status"),
                text_hash=_sha1(fetched.get("text", "")) if fetched.get("ok") else "",
                raw_text=fetched.get("text", "") if (fetched.get("ok") and fetch_status == "ok") else "",
            ),
        )
        results.append((lc_id, doc_id, fetch_status))

    logger.info(
        "  [fetch] %s → %d/%d fetched (%d ok)",
        company, len(results), len(lc_ids),
        sum(1 for _, _, s in results if s == "ok"),
    )
    return results


# ── step 4: chunk & embed ────────────────────────────────────────────────────


def _chunk_and_embed(
    conn,
    doc_ids: list[int],
    company: str,
    result_id: int | str,
    embed_fn: Callable[[list[str]], list[list[float]]],
    collection,
    embedding_name: str,
    cfg: UnifiedPipelineConfig,
) -> tuple[int, int]:
    """Chunk text from documents and embed into ChromaDB.

    Returns (chunks_written, chunks_embedded).
    """
    if not doc_ids:
        return 0, 0

    # Load documents that need chunking
    cur = conn.cursor()
    placeholders = ",".join(["%s"] * len(doc_ids))
    cur.execute(
        f"""
        SELECT d.id, d.raw_text, d.url_fetched, d.page_title,
               lc.result_id, lc.source_type, lc.id AS lc_id, lc.url_discovered
        FROM documents d
        JOIN link_candidates lc ON lc.id = d.link_candidate_id
        WHERE d.id IN ({placeholders})
          AND d.fetch_status = 'ok'
          AND NOT EXISTS (
              SELECT 1 FROM document_chunks dc WHERE dc.document_id = d.id
          )
        """,
        doc_ids,
    )
    rows = cur.fetchall()
    cur.close()

    total_chunks = 0
    total_embedded = 0

    for doc_id, raw_text, url_fetched, page_title, res_id, source_type, lc_id, url_discovered in rows:
        text = raw_text or ""
        chunks = chunk_text(text, chunk_words=cfg.chunk_words, overlap_words=cfg.overlap_words)

        if not chunks:
            update_document_fetch_status(conn, doc_id, "ok_no_chunks")
            continue

        chunk_row_ids: list[int] = []
        chunk_docs: list[str] = []
        chunk_metas: list[dict] = []

        for chunk_index, chunk in enumerate(chunks):
            chunk_row_id = insert_document_chunk(
                conn,
                DocumentChunkRecord(
                    document_id=doc_id,
                    chunk_index=chunk_index,
                    chunk_text=chunk,
                    chunk_hash=_sha1(chunk),
                    embedding_model=embedding_name,
                    embedded_at=None,
                ),
            )
            chunk_row_ids.append(chunk_row_id)
            chunk_docs.append(chunk)
            chunk_metas.append({
                "result_id": res_id,
                "company": company,
                "source_type": source_type,
                "link_candidate_id": lc_id,
                "document_id": doc_id,
                "chunk_index": chunk_index,
                "url_fetched": url_fetched,
                "page_title": page_title,
            })

        # Embed and upsert to ChromaDB
        embeddings = embed_fn(chunk_docs)
        upsert_chunks(
            collection=collection,
            ids=[_make_chroma_id(cid) for cid in chunk_row_ids],
            documents=chunk_docs,
            metadatas=chunk_metas,
            embeddings=embeddings,
        )
        mark_document_chunks_embedded(conn, chunk_ids=chunk_row_ids, embedding_model=embedding_name)

        total_chunks += len(chunk_row_ids)
        total_embedded += len(chunk_row_ids)

    logger.info(
        "  [embed] %s → %d chunks embedded from %d documents",
        company, total_embedded, len(rows),
    )
    return total_chunks, total_embedded


# ── main entry: process one company ──────────────────────────────────────────


def process_company(
    conn,
    row: dict[str, Any],
    session: requests.Session,
    proxy_pool: ProxyPool | None,
    embed_fn: Callable[[list[str]], list[list[float]]],
    collection,
    cfg: UnifiedPipelineConfig,
) -> CompanyPipelineResult:
    """Run the full pipeline for a single company: discover → fetch → chunk → embed.

    The caller should handle commit/rollback on the connection.
    """
    result_id = row["result_id"]
    company = row["company"]
    web_site = row.get("web_site", "")
    needs_news = row.get("needs_news", True)
    needs_website = row.get("needs_website", True)
    embedding_name = cfg.effective_embedding_name()

    res = CompanyPipelineResult(result_id=result_id, company=company)

    all_lc_ids: list[int] = []
    prefetched_doc_ids: list[int] = []

    # Step 1: News discovery
    if needs_news:
        try:
            news_lc_ids = _discover_news(conn, result_id, company, session, proxy_pool, cfg)
            res.news_links = len(news_lc_ids)
            all_lc_ids.extend(news_lc_ids)
        except Exception as exc:
            res.errors.append(f"news: {exc}")
            logger.error("  [news] error for %s: %s", company, exc)

    # Step 2: Website spidering
    if needs_website and web_site:
        try:
            website_lc_ids, website_doc_ids = _spider_website(
                conn, result_id, company, web_site, cfg,
            )
            res.website_pages = len(website_lc_ids)
            all_lc_ids.extend(website_lc_ids)
            prefetched_doc_ids.extend(website_doc_ids)
        except Exception as exc:
            res.errors.append(f"spider: {exc}")
            logger.error("  [spider] error for %s: %s", company, exc)

    # Step 3: Fetch documents for link_candidates that weren't pre-fetched
    try:
        fetch_results = _fetch_documents(conn, all_lc_ids, company, session, cfg)
        res.documents_fetched = len(fetch_results)
        res.documents_ok = sum(1 for _, _, s in fetch_results if s == "ok")
        res.documents_skipped = sum(1 for _, _, s in fetch_results if s != "ok")

        # Collect doc_ids that were successfully fetched
        fetched_doc_ids = [doc_id for _, doc_id, status in fetch_results if status == "ok"]
    except Exception as exc:
        res.errors.append(f"fetch: {exc}")
        logger.error("  [fetch] error for %s: %s", company, exc)
        fetched_doc_ids = []

    # Step 4: Chunk & embed all documents (pre-fetched from spider + freshly fetched)
    all_doc_ids = prefetched_doc_ids + fetched_doc_ids
    if all_doc_ids:
        try:
            chunks_written, chunks_embedded = _chunk_and_embed(
                conn, all_doc_ids, company, result_id,
                embed_fn, collection, embedding_name, cfg,
            )
            res.chunks_written = chunks_written
            res.chunks_embedded = chunks_embedded
        except Exception as exc:
            res.errors.append(f"embed: {exc}")
            logger.error("  [embed] error for %s: %s", company, exc)

    return res


# ── batch runner ─────────────────────────────────────────────────────────────


@dataclass
class BatchResult:
    """Aggregate stats from processing a batch of companies."""
    companies_selected: int = 0
    companies_processed: int = 0
    companies_errors: int = 0
    total_news_links: int = 0
    total_website_pages: int = 0
    total_docs_fetched: int = 0
    total_docs_ok: int = 0
    total_chunks: int = 0
    total_embedded: int = 0
    remaining: int = 0
    company_results: list[CompanyPipelineResult] = field(default_factory=list)


def run_pipeline_batch(
    cfg: UnifiedPipelineConfig,
    proxy_pool: ProxyPool | None = None,
    shutdown_flag: Callable[[], bool] | None = None,
) -> BatchResult:
    """Process one batch of companies through the full pipeline.

    Returns a BatchResult with aggregate stats.
    """
    load_env_from_file(cfg.env_file)
    embed_fn = get_embedder(cfg.embedding_backend, cfg.embedding_model, cfg.simple_dim)
    collection = get_or_create_collection(cfg.persist_dir, cfg.collection_name)
    embedding_name = cfg.effective_embedding_name()

    batch = BatchResult()

    # Build HTTP session
    session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    })

    dsn = cfg.dsn or None
    conn = psycopg2.connect(dsn) if dsn else psycopg2.connect()
    conn.autocommit = False

    try:
        rows = select_results_pending_pipeline(conn, cfg.batch_size)
        batch.companies_selected = len(rows)

        if not rows:
            batch.remaining = count_results_pending_pipeline(conn)
            return batch

        for row in rows:
            if shutdown_flag and shutdown_flag():
                break

            company = row["company"]
            logger.info("━━━ Processing: %s (result_id=%s) ━━━", company, row["result_id"])

            try:
                result = process_company(
                    conn, row, session, proxy_pool,
                    embed_fn, collection, cfg,
                )
                conn.commit()

                batch.companies_processed += 1
                batch.total_news_links += result.news_links
                batch.total_website_pages += result.website_pages
                batch.total_docs_fetched += result.documents_fetched
                batch.total_docs_ok += result.documents_ok
                batch.total_chunks += result.chunks_written
                batch.total_embedded += result.chunks_embedded
                batch.company_results.append(result)

                if result.errors:
                    batch.companies_errors += 1

                logger.info(
                    "  ✓ %s — news=%d website=%d fetched=%d chunks=%d",
                    company, result.news_links, result.website_pages,
                    result.documents_ok, result.chunks_embedded,
                )

            except psycopg2.errors.ForeignKeyViolation:
                conn.rollback()
                logger.info("  result_id=%s removed by dedup; skipping", row["result_id"])
            except Exception as exc:
                conn.rollback()
                batch.companies_errors += 1
                logger.error("  ✗ %s — %s", company, exc)

            # Polite delay between companies
            time.sleep(random.uniform(cfg.delay_min, cfg.delay_max))

        batch.remaining = count_results_pending_pipeline(conn)

    except Exception:
        conn.rollback()
        raise
    finally:
        session.close()
        conn.close()

    return batch
