"""
Web Research router — batch Google search + crawl4ai for selected companies.

Provides an SSE streaming endpoint that reports per-company, per-page progress
so the frontend can show a live activity log.
"""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from typing import Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from server.deps import dsn_for, get_conn

router = APIRouter()
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request / config models
# ---------------------------------------------------------------------------

class WebResearchRequest(BaseModel):
    db: str
    result_ids: list[int]
    max_google_results: int = 10
    max_crawl_pages: int = 8
    search_suffix: str = ""         # extra words appended to each query
    headless: bool = True
    run_ingest: bool = True         # chunk + embed after crawling


# ---------------------------------------------------------------------------
# SSE streaming endpoint
# ---------------------------------------------------------------------------

@router.post("/run")
def run_web_research(req: WebResearchRequest):
    """
    Run batch web-research with live SSE progress.

    For each ``result_id`` the server:
      1. Looks up the company name from the ``results`` table
      2. Runs a stealth Playwright Google search
      3. Crawls each result link with crawl4ai
      4. Stores link_candidates + documents in Postgres
      5. (optionally) triggers chunk + embed for the new documents

    SSE events:
      event: progress  — per-step updates
      event: result    — final batch summary
      event: error     — if something blew up
    """
    dsn = dsn_for(req.db)

    # Resolve company names up front
    companies = _resolve_companies(req.db, req.result_ids)

    def event_generator():
        progress_queue: queue.Queue = queue.Queue()
        result_holder: list = []
        error_holder: list = []

        def on_progress(phase: str, message: str, detail: dict | None = None):
            progress_queue.put({
                "event": "progress",
                "phase": phase,
                "message": message,
                "detail": detail or {},
                "ts": time.time(),
            })

        def _run():
            try:
                from broker.enrichment.web_research import (
                    WebResearchConfig,
                    research_batch,
                )

                cfg = WebResearchConfig(
                    max_google_results=req.max_google_results,
                    max_crawl_pages=req.max_crawl_pages,
                    headless=req.headless,
                    search_suffix=req.search_suffix,
                )

                summaries = research_batch(
                    dsn=dsn,
                    companies=companies,
                    config=cfg,
                    on_progress=on_progress,
                )

                # Optionally run the ingestion (chunk + embed) for new docs
                if req.run_ingest:
                    on_progress("ingest", "Running chunk + embed for new documents…")
                    _run_ingest_for_db(req.db, on_progress)

                result_holder.append(summaries)
            except Exception as exc:
                log.exception("Web research stream error")
                error_holder.append(str(exc))
            finally:
                progress_queue.put(None)  # sentinel

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        while True:
            try:
                item = progress_queue.get(timeout=120)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue

            if item is None:
                if error_holder:
                    payload = {"event": "error", "message": error_holder[0]}
                    yield f"event: error\ndata: {json.dumps(payload)}\n\n"
                elif result_holder:
                    payload = {
                        "event": "result",
                        "summaries": result_holder[0],
                        "companies_count": len(result_holder[0]),
                        "total_docs": sum(s["docs"] for s in result_holder[0]),
                        "total_skipped": sum(s["skipped"] for s in result_holder[0]),
                    }
                    yield f"event: result\ndata: {json.dumps(payload)}\n\n"
                break
            else:
                yield f"event: progress\ndata: {json.dumps(item)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _resolve_companies(db: str, result_ids: list[int]) -> list[dict]:
    """Look up company names for a list of result_ids."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()
    companies = []
    for rid in result_ids:
        cur.execute("SELECT data FROM results WHERE id = %s", (rid,))
        row = cur.fetchone()
        if not row:
            continue
        data = row[0] if isinstance(row[0], dict) else {}
        name = (data.get("title") or "").strip()
        website = (data.get("web_site") or "").strip()
        if name:
            companies.append({
                "result_id": rid,
                "company": name,
                "website": website,
            })
    cur.close()
    conn.close()
    if not companies:
        raise HTTPException(400, "No valid companies found for the given result_ids")
    return companies


def _run_ingest_for_db(db: str, on_progress):
    """Run one cycle of chunk + embed for the database."""
    import argparse
    from pathlib import Path

    from broker.orchestration.document_ingest import chunk_documents, init_status, save_status

    dsn = dsn_for(db)
    persist_dir = str(Path("runtime/chroma") / f"db_{db}")
    status_file = Path("runtime/status") / f"web_research_ingest_{db}.json"
    status_file.parent.mkdir(parents=True, exist_ok=True)

    args = argparse.Namespace(
        dsn=dsn,
        persist_dir=persist_dir,
        collection="construction_docs_openai1536_live",
        embedding_backend="openai",
        embedding_model="text-embedding-3-small",
        simple_dim=128,
        fetch_batch=50,
        chunk_batch=200,
        chunk_words=300,
        overlap_words=50,
        chunk_delay=0,
        fetch_delay=0,
        timeout=20,
        max_chars=100000,
        retry_failed_fetches=False,
        force_reembed=False,
        debug_save_dir="",
        debug_preview_chars=500,
        env_file=".env",
    )

    status = init_status(args)
    save_status(status_file, status, stage="ingest_chunks")

    # Override the DSN for chunk_documents to use
    import broker.config as _cfg
    _orig = _cfg.get_local_construction_dsn
    _cfg.get_local_construction_dsn = lambda: dsn
    try:
        stats = chunk_documents(args, status, status_file)
        on_progress("ingest_done", f"Ingested {stats['chunks_embedded']} chunks, {stats['documents_chunked']} documents", stats)
    finally:
        _cfg.get_local_construction_dsn = _orig
