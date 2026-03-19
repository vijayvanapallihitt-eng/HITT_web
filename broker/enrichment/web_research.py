"""
Batch web research — Google search + crawl4ai surface crawl.

For each company:
  1. Run a stealth Playwright Google search
  2. Crawl each result link with crawl4ai
  3. Store as link_candidates + documents in Postgres
  4. Pipe through the existing chunk + embed pipeline

All results get source_type='web_research' so they're distinguishable
from the existing 'news' pipeline.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
import re
import time
from dataclasses import dataclass
from typing import Callable

from broker.enrichment.google_search import SearchResult, google_search
from broker.models import DocumentRecord, LinkCandidateRecord
from broker.storage.postgres import (
    connect_postgres,
    insert_document,
    upsert_link_candidate,
)

logger = logging.getLogger(__name__)

# Maximum chars of extracted text to store per page
MAX_TEXT_CHARS = 100_000


@dataclass
class WebResearchConfig:
    """Knobs for the web-research pipeline."""
    max_google_results: int = 10
    max_crawl_pages: int = 8
    crawl_timeout_sec: int = 20
    delay_between_companies: float = 8.0
    delay_between_pages: float = 0.5
    headless: bool = True
    search_suffix: str = ""          # e.g. "construction" appended to query


def _sha1(text: str) -> str:
    return hashlib.sha1((text or "").encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# crawl4ai page fetcher
# ---------------------------------------------------------------------------

async def _crawl_single_url(url: str, timeout_sec: int = 20) -> dict:
    """Crawl one URL with crawl4ai and return {ok, url, title, text, error}."""
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

    browser_conf = BrowserConfig(headless=True, verbose=False)
    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=timeout_sec * 1000,
        word_count_threshold=30,
    )

    try:
        async with AsyncWebCrawler(config=browser_conf) as crawler:
            result = await crawler.arun(url=url, config=run_conf)
            if result.success:
                text = ""
                if hasattr(result, "markdown"):
                    if hasattr(result.markdown, "raw_markdown"):
                        text = result.markdown.raw_markdown or ""
                    elif isinstance(result.markdown, str):
                        text = result.markdown
                if not text:
                    text = result.extracted_content or ""

                title = ""
                if hasattr(result, "metadata") and isinstance(result.metadata, dict):
                    title = result.metadata.get("title", "") or ""
                if not title and hasattr(result, "title"):
                    title = result.title or ""

                return {
                    "ok": True,
                    "url": str(result.url or url),
                    "title": title.strip(),
                    "text": text[:MAX_TEXT_CHARS],
                }
            else:
                return {
                    "ok": False,
                    "url": url,
                    "title": "",
                    "text": "",
                    "error": f"crawl4ai failure: {getattr(result, 'error_message', 'unknown')}",
                }
    except Exception as exc:
        return {
            "ok": False,
            "url": url,
            "title": "",
            "text": "",
            "error": str(exc),
        }


def _crawl_url_sync(url: str, timeout_sec: int = 20) -> dict:
    """Synchronous wrapper around the async crawl4ai fetcher."""
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(
                    asyncio.run, _crawl_single_url(url, timeout_sec)
                ).result(timeout=timeout_sec + 30)
        else:
            return loop.run_until_complete(_crawl_single_url(url, timeout_sec))
    except RuntimeError:
        return asyncio.run(_crawl_single_url(url, timeout_sec))


# ---------------------------------------------------------------------------
# Company-name relevance (reuse logic from document_ingest)
# ---------------------------------------------------------------------------

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
    """Return True if the page text appears relevant to the company."""
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


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

ProgressCallback = Callable[[str, str, dict | None], None]
"""(phase, message, detail_dict | None)"""


def research_company_web(
    dsn: str,
    result_id: int,
    company: str,
    website: str = "",
    config: WebResearchConfig | None = None,
    on_progress: ProgressCallback | None = None,
) -> dict:
    """Run a Google search + crawl for a single company.

    Stores every discovered link as a ``link_candidate`` (source_type='web_research')
    and every successfully-crawled page as a ``document``.

    Returns a summary dict with counts.
    """
    cfg = config or WebResearchConfig()

    def _emit(phase: str, msg: str, detail: dict | None = None):
        logger.info("[%s] %s %s", company, phase, msg)
        if on_progress:
            on_progress(phase, msg, detail)

    # ── 1. Build search query ─────────────────────────────────────
    query_parts = [f'"{company}"']
    if cfg.search_suffix:
        query_parts.append(cfg.search_suffix)
    search_query = " ".join(query_parts)

    _emit("search", f"Googling: {search_query}")
    search_results = google_search(
        search_query,
        max_results=cfg.max_google_results,
        headless=cfg.headless,
    )

    # Fallback: if exact-quoted name gives 0 results, retry without quotes
    if not search_results:
        fallback_query = company
        if cfg.search_suffix:
            fallback_query += " " + cfg.search_suffix
        _emit("search", f"No exact results — retrying: {fallback_query}")
        search_results = google_search(
            fallback_query,
            max_results=cfg.max_google_results,
            headless=cfg.headless,
        )

    _emit("search_done", f"Found {len(search_results)} results", {
        "count": len(search_results),
        "urls": [r.url for r in search_results],
    })

    if not search_results:
        return {"company": company, "result_id": result_id, "links": 0, "docs": 0, "skipped": 0}

    # ── 2. Store link_candidates + crawl each link ────────────────
    links_stored = 0
    docs_stored = 0
    skipped = 0

    with connect_postgres(dsn=dsn, autocommit=False) as conn:
        for idx, sr in enumerate(search_results[:cfg.max_crawl_pages]):
            _emit("crawl", f"[{idx+1}/{min(len(search_results), cfg.max_crawl_pages)}] Crawling: {sr.url[:100]}", {
                "index": idx,
                "url": sr.url,
                "title": sr.title,
            })

            # Insert link_candidate
            lc_id = upsert_link_candidate(
                conn,
                LinkCandidateRecord(
                    result_id=result_id,
                    source_type="web_research",
                    query_text=search_query,
                    url_discovered=sr.url,
                    title_discovered=sr.title or sr.snippet[:120],
                    discovery_status="discovered",
                ),
            )
            conn.commit()
            links_stored += 1

            # Crawl the page
            fetched = _crawl_url_sync(sr.url, timeout_sec=cfg.crawl_timeout_sec)

            if fetched["ok"]:
                page_title = fetched.get("title", "") or sr.title
                page_text = fetched.get("text", "")

                # Relevance check
                if not _company_relevant(company, page_text, page_title):
                    fetch_status = "irrelevant"
                    raw_text = ""
                    skipped += 1
                    _emit("crawl_skip", f"Irrelevant: {sr.url[:80]}")
                else:
                    fetch_status = "ok"
                    raw_text = page_text
                    docs_stored += 1
                    _emit("crawl_ok", f"Relevant ({len(page_text):,} chars): {page_title[:60]}", {
                        "chars": len(page_text),
                        "title": page_title,
                    })
            else:
                fetch_status = "request_error"
                page_title = sr.title
                raw_text = ""
                skipped += 1
                _emit("crawl_fail", f"Failed: {fetched.get('error', '')[:100]}")

            insert_document(
                conn,
                DocumentRecord(
                    link_candidate_id=lc_id,
                    url_fetched=fetched.get("url", sr.url),
                    page_title=page_title or "",
                    fetch_status=fetch_status,
                    http_status=200 if fetched["ok"] else None,
                    text_hash=_sha1(raw_text) if raw_text else "",
                    raw_text=raw_text,
                ),
            )
            conn.commit()

            if cfg.delay_between_pages > 0 and idx < len(search_results) - 1:
                time.sleep(cfg.delay_between_pages)

    summary = {
        "company": company,
        "result_id": result_id,
        "links": links_stored,
        "docs": docs_stored,
        "skipped": skipped,
    }
    _emit("done", f"Done — {docs_stored} docs, {skipped} skipped", summary)
    return summary


def research_batch(
    dsn: str,
    companies: list[dict],
    config: WebResearchConfig | None = None,
    on_progress: ProgressCallback | None = None,
) -> list[dict]:
    """Run web research for a batch of companies.

    Parameters
    ----------
    dsn : str
        Postgres DSN.
    companies : list[dict]
        Each dict must have ``result_id`` (int), ``company`` (str),
        and optionally ``website`` (str).
    config : WebResearchConfig, optional
    on_progress : ProgressCallback, optional
        ``(phase, message, detail)`` — called for each step.

    Returns
    -------
    list[dict]
        One summary dict per company.
    """
    cfg = config or WebResearchConfig()
    results: list[dict] = []

    for i, comp in enumerate(companies):
        result_id = comp["result_id"]
        name = comp["company"]
        website = comp.get("website", "")

        if on_progress:
            on_progress("batch", f"[{i+1}/{len(companies)}] Starting: {name}", {
                "index": i,
                "total": len(companies),
                "company": name,
                "result_id": result_id,
            })

        summary = research_company_web(
            dsn=dsn,
            result_id=result_id,
            company=name,
            website=website,
            config=cfg,
            on_progress=on_progress,
        )
        results.append(summary)

        if cfg.delay_between_companies > 0 and i < len(companies) - 1:
            time.sleep(cfg.delay_between_companies)

    if on_progress:
        total_docs = sum(r["docs"] for r in results)
        total_skipped = sum(r["skipped"] for r in results)
        on_progress("batch_done", f"Batch complete: {len(results)} companies, {total_docs} docs", {
            "companies": len(results),
            "total_docs": total_docs,
            "total_skipped": total_skipped,
            "results": results,
        })

    return results
