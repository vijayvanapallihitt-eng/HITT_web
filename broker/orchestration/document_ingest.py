from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import requests

from broker.documents.chunking import chunk_text
from broker.documents.fetch import fetch_url_text
from broker.documents.url_resolver import canonicalize_url, resolve_google_news_url
from broker.embeddings.factory import get_embedder, load_env_from_file
from broker.models import DocumentChunkRecord, DocumentRecord
from broker.orchestration.status import now_iso, save_json
from broker.storage.chroma_store import get_or_create_collection, upsert_chunks
from broker.storage.postgres import (
    connect_postgres,
    count_documents_pending_chunking,
    count_link_candidates_pending_fetch,
    insert_document,
    insert_document_chunk,
    mark_document_chunks_embedded,
    select_documents_pending_chunking,
    select_link_candidates_pending_fetch,
    update_document_fetch_status,
)

import logging
import re

logger = logging.getLogger(__name__)


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)


def sha1_hex(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()


def make_chroma_chunk_id(chunk_row_id: int) -> str:
    return f"document_chunk:{chunk_row_id}"


def sanitize_id(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_-]+", "-", value).strip("-").lower() or "unknown"


def write_debug_extract(
    debug_dir: Path,
    source_idx: int,
    item: dict,
    fetched_doc: dict,
    chunks_count: int,
    preview_chars: int,
) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    source_tag = sanitize_id(item.get("source_type", "unknown"))
    stem = f"{source_idx:02d}_{source_tag}"

    text = fetched_doc.get("text", "") or ""
    text_path = debug_dir / f"{stem}.txt"
    text_path.write_text(text, encoding="utf-8")

    record = {
        "source_idx": source_idx,
        "source_type": item.get("source_type", ""),
        "url_discovered": item.get("url_discovered", ""),
        "url_fetched": fetched_doc.get("url", ""),
        "page_title": fetched_doc.get("title", ""),
        "text_chars": len(text),
        "chunks_count": chunks_count,
        "preview": text[:preview_chars],
        "text_file": text_path.name,
    }
    with (debug_dir / "debug_extracts.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


def effective_embedding_name(args) -> str:
    if args.embedding_backend == "simple":
        return f"simple:{args.simple_dim}"
    if args.embedding_backend == "chroma-default":
        return "chroma-default"
    return args.embedding_model


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    return session


def classify_fetch_status(result: dict) -> str:
    if result.get("ok"):
        return "ok"
    error = str(result.get("error") or "").lower()
    if "unsupported content-type" in error:
        return "unsupported_content_type"
    if "empty extracted text" in error:
        return "empty_text"
    if result.get("http_status") is not None:
        return "http_error"
    return "request_error"

# Words that are too generic to count as evidence of relevance
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
    """Return True if the fetched page appears relevant to the company.

    Requires at least one *distinctive* word from the company name (i.e. not a
    generic industry term like "Construction" or "Homes") to appear in the
    page text/title.  If the company name only has generic words, falls back to
    checking if the full company name appears as-is.
    """
    if not company or not text:
        return False
    # Strip punctuation, split into tokens, keep words >= 3 chars
    tokens = re.findall(r"[A-Za-z]{3,}", company)
    if not tokens:
        return True

    haystack = (title + " " + text[:8000]).lower()

    # Separate distinctive vs generic tokens
    distinctive = [t for t in tokens if t.lower() not in _GENERIC_WORDS]
    generic = [t for t in tokens if t.lower() in _GENERIC_WORDS]

    if distinctive:
        # At least one distinctive word must appear
        return any(tok.lower() in haystack for tok in distinctive)
    else:
        # All words are generic (e.g. "General Construction Company")
        # Require the full name to appear
        return company.lower().strip() in haystack


def resolve_candidate_fetch_url(session: requests.Session, candidate: dict, timeout: int) -> str:
    url = candidate.get("url_discovered", "") or ""
    if candidate.get("source_type") == "news":
        url = resolve_google_news_url(session, url, timeout=timeout)
    return canonicalize_url(url)


def init_status(args) -> dict:
    embedding_name = effective_embedding_name(args)
    return {
        "started_at": now_iso(),
        "updated_at": now_iso(),
        "stage": "starting",
        "config": {
            "persist_dir": args.persist_dir,
            "collection": args.collection,
            "embedding_backend": args.embedding_backend,
            "embedding_model": embedding_name,
            "fetch_batch": args.fetch_batch,
            "chunk_batch": args.chunk_batch,
        },
        "fetch": {
            "selected": 0,
            "processed": 0,
            "succeeded": 0,
            "failed": 0,
            "remaining": 0,
            "last_company": "",
            "last_link_candidate_id": None,
        },
        "chunk": {
            "selected": 0,
            "processed": 0,
            "documents_chunked": 0,
            "documents_no_chunks": 0,
            "chunks_written": 0,
            "chunks_embedded": 0,
            "remaining": 0,
            "last_company": "",
            "last_document_id": None,
            "collection_count": 0,
        },
    }


def save_status(status_file: Path, status: dict, stage: str | None = None) -> None:
    if stage:
        status["stage"] = stage
    status["updated_at"] = now_iso()
    save_json(status_file, status)


def fetch_candidates(args, status: dict, status_file: Path) -> dict[str, int]:
    session = build_session()
    stats = {
        "selected": 0,
        "processed": 0,
        "succeeded": 0,
        "failed": 0,
        "remaining": 0,
    }

    with connect_postgres(autocommit=False) as conn:
        rows = select_link_candidates_pending_fetch(
            conn,
            batch_size=args.fetch_batch,
            retry_failed=args.retry_failed_fetches,
        )
        stats["selected"] = len(rows)
        status["fetch"]["selected"] = len(rows)
        save_status(status_file, status, stage="fetch")

        for row in rows:
            target_url = resolve_candidate_fetch_url(session, row, timeout=args.timeout)
            fetched = fetch_url_text(
                session=session,
                url=target_url,
                timeout=args.timeout,
                max_chars=args.max_chars,
            )
            fetch_status = classify_fetch_status(fetched)

            # ── relevance gate: skip pages that don't mention the company ──
            if fetch_status == "ok":
                page_title = str(fetched.get("title") or "").strip()
                page_text = str(fetched.get("text") or "")
                if not _company_relevant(row["company"], page_text, page_title):
                    fetch_status = "irrelevant"
                    logger.info(
                        "Irrelevant page for %s — %s",
                        row["company"],
                        str(fetched.get("url") or target_url)[:120],
                    )

            record = DocumentRecord(
                link_candidate_id=row["link_candidate_id"],
                url_fetched=str(fetched.get("url") or target_url),
                page_title=(
                    str(fetched.get("title") or "").strip()
                    or str(row.get("title_discovered") or "").strip()
                ),
                fetch_status=fetch_status,
                http_status=fetched.get("http_status"),
                text_hash=sha1_hex(fetched.get("text", "")) if fetched.get("ok") else "",
                raw_text=fetched.get("text", "") if (fetched.get("ok") and fetch_status == "ok") else "",
            )

            try:
                document_id = insert_document(conn, record)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

            stats["processed"] += 1
            if fetched.get("ok"):
                stats["succeeded"] += 1
            else:
                stats["failed"] += 1

            status["fetch"]["processed"] += 1
            status["fetch"]["last_company"] = row["company"]
            status["fetch"]["last_link_candidate_id"] = row["link_candidate_id"]
            status["fetch"]["succeeded"] += 1 if fetched.get("ok") else 0
            status["fetch"]["failed"] += 0 if fetched.get("ok") else 1
            save_status(status_file, status)

            if args.fetch_delay > 0:
                time.sleep(args.fetch_delay)

        stats["remaining"] = count_link_candidates_pending_fetch(
            conn,
            retry_failed=args.retry_failed_fetches,
        )

    session.close()
    status["fetch"]["remaining"] = stats["remaining"]
    save_status(status_file, status)
    return stats


def chunk_documents(args, status: dict, status_file: Path) -> dict[str, int]:
    load_env_from_file(args.env_file)
    embed = get_embedder(args.embedding_backend, args.embedding_model, args.simple_dim)
    embedding_name = effective_embedding_name(args)
    collection = get_or_create_collection(args.persist_dir, args.collection)

    stats = {
        "selected": 0,
        "processed": 0,
        "documents_chunked": 0,
        "documents_no_chunks": 0,
        "chunks_written": 0,
        "chunks_embedded": 0,
        "remaining": 0,
        "collection_count": collection.count(),
    }

    with connect_postgres(autocommit=False) as conn:
        rows = select_documents_pending_chunking(
            conn,
            batch_size=args.chunk_batch,
            embedding_model=embedding_name,
            force_reembed=args.force_reembed,
        )
        stats["selected"] = len(rows)
        status["chunk"]["selected"] = len(rows)
        save_status(status_file, status, stage="chunk")

        for row in rows:
            text = row["raw_text"] or ""
            chunks = chunk_text(
                text,
                chunk_words=args.chunk_words,
                overlap_words=args.overlap_words,
            )

            status["chunk"]["processed"] += 1
            status["chunk"]["last_company"] = row["company"]
            status["chunk"]["last_document_id"] = row["document_id"]

            if args.debug_save_dir:
                debug_dir = Path(args.debug_save_dir) / f"document_{row['document_id']:06d}"
                write_debug_extract(
                    debug_dir=debug_dir,
                    source_idx=0,
                    item={
                        "source_type": row["source_type"],
                        "url_discovered": row["url_discovered"],
                    },
                    fetched_doc={
                        "url": row["url_fetched"],
                        "title": row["page_title"],
                        "text": text,
                    },
                    chunks_count=len(chunks),
                    preview_chars=args.debug_preview_chars,
                )

            if not chunks:
                update_document_fetch_status(conn, row["document_id"], "ok_no_chunks")
                conn.commit()
                stats["processed"] += 1
                stats["documents_no_chunks"] += 1
                status["chunk"]["documents_no_chunks"] += 1
                save_status(status_file, status)
                continue

            chunk_row_ids: list[int] = []
            chunk_docs: list[str] = []
            chunk_metas: list[dict] = []

            for chunk_index, chunk in enumerate(chunks):
                chunk_row_id = insert_document_chunk(
                    conn,
                    DocumentChunkRecord(
                        document_id=row["document_id"],
                        chunk_index=chunk_index,
                        chunk_text=chunk,
                        chunk_hash=sha1_hex(chunk),
                        embedding_model=embedding_name,
                        embedded_at=None,
                    ),
                )
                chunk_row_ids.append(chunk_row_id)
                chunk_docs.append(chunk)
                chunk_metas.append(
                    {
                        "result_id": row["result_id"],
                        "company": row["company"],
                        "source_type": row["source_type"],
                        "link_candidate_id": row["link_candidate_id"],
                        "document_id": row["document_id"],
                        "chunk_index": chunk_index,
                        "url_fetched": row["url_fetched"],
                        "page_title": row["page_title"],
                    }
                )

            embeddings = embed(chunk_docs)
            upsert_chunks(
                collection=collection,
                ids=[make_chroma_chunk_id(chunk_id) for chunk_id in chunk_row_ids],
                documents=chunk_docs,
                metadatas=chunk_metas,
                embeddings=embeddings,
            )
            mark_document_chunks_embedded(
                conn,
                chunk_ids=chunk_row_ids,
                embedding_model=embedding_name,
            )
            conn.commit()

            stats["processed"] += 1
            stats["documents_chunked"] += 1
            stats["chunks_written"] += len(chunk_row_ids)
            stats["chunks_embedded"] += len(chunk_row_ids)
            status["chunk"]["documents_chunked"] += 1
            status["chunk"]["chunks_written"] += len(chunk_row_ids)
            status["chunk"]["chunks_embedded"] += len(chunk_row_ids)
            save_status(status_file, status)

            if args.chunk_delay > 0:
                time.sleep(args.chunk_delay)

        stats["remaining"] = count_documents_pending_chunking(
            conn,
            embedding_model=embedding_name,
            force_reembed=args.force_reembed,
        )
        stats["collection_count"] = collection.count()

    status["chunk"]["remaining"] = stats["remaining"]
    status["chunk"]["collection_count"] = stats["collection_count"]
    save_status(status_file, status)
    return stats


def run_document_ingest_cycle(args, status: dict, status_file: Path) -> dict:
    fetch_stats = fetch_candidates(args, status, status_file)
    chunk_stats = chunk_documents(args, status, status_file)
    stage = "idle"
    if fetch_stats["selected"] or chunk_stats["selected"]:
        stage = "cycle_complete"
    save_status(status_file, status, stage=stage)
    return {"fetch": fetch_stats, "chunk": chunk_stats}
