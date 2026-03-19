"""
Link discovery & website spider worker.

Every poll cycle it:
  1. Selects `results` rows missing news discovery → searches Google News → writes link_candidates.
  2. Selects `results` rows missing website discovery → spiders company site with crawl4ai →
     writes link_candidates + pre-fetched documents (so the ingester skips re-fetching).

Usage:
    python worker_enrich.py
    python worker_enrich.py --batch 10 --poll 30
    python worker_enrich.py --once --batch 1 --news-top 10
"""

from __future__ import annotations

import argparse
import hashlib
import logging
import random
import signal
import time

import psycopg2
import psycopg2.errors
import requests

from broker.config import get_local_construction_dsn
from broker.enrichment.link_discovery import discover_company_links
from broker.enrichment.proxies import ProxyPool, fetch_proxifly_proxies, load_proxy_file
from broker.models import DocumentRecord, LinkCandidateRecord
from broker.storage.postgres import (
    count_results_pending_link_discovery,
    count_results_pending_website_discovery,
    ensure_pipeline_schema,
    insert_document,
    select_results_pending_link_discovery,
    select_results_pending_website_discovery,
    upsert_link_candidate,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ENRICH] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DSN: str = get_local_construction_dsn()
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)

_shutdown = False


def _handle_signal(sig, frame):
    del sig, frame
    global _shutdown
    log.info("Shutdown signal received; finishing current batch.")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def build_proxy_pool(no_proxifly: bool, proxy_file: str) -> ProxyPool | None:
    proxies: list[str] = []
    if not no_proxifly:
        proxies.extend(fetch_proxifly_proxies())
    if proxy_file:
        custom = load_proxy_file(proxy_file)
        proxies.extend(custom)
        log.info("Loaded %s proxies from %s", len(custom), proxy_file)
    proxies = list(dict.fromkeys(proxies))
    random.shuffle(proxies)
    return ProxyPool(proxies) if proxies else None


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


def persist_discovery(conn, row: dict, discovery: dict) -> dict[str, int | bool]:
    result_id = int(row["result_id"])
    news_candidates = 0
    wrote_news = False

    if discovery["news_status"] in {"ok", "no_results"}:
        news_articles = discovery["news_articles"]
        if news_articles:
            for item in news_articles:
                upsert_link_candidate(
                    conn,
                    LinkCandidateRecord(
                        result_id=result_id,
                        source_type="news",
                        query_text=discovery["news_query"],
                        url_discovered=str(item.get("url") or "").strip(),
                        title_discovered=str(item.get("title") or "").strip(),
                        discovery_status=discovery["news_status"],
                    ),
                )
            wrote_news = True
            news_candidates = len(news_articles)
        elif discovery["news_status"] == "no_results":
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
            wrote_news = True

    return {
        "wrote_news": wrote_news,
        "news_candidates": news_candidates,
        "completed": wrote_news,
    }


def poll_and_enrich(args, proxy_pool: ProxyPool | None) -> dict[str, int]:
    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    session = build_session()

    stats = {
        "rows_selected": 0,
        "rows_processed": 0,
        "row_errors": 0,
        "rows_completed": 0,
        "rows_with_writes": 0,
        "news_candidates": 0,
        "news_no_results": 0,
        "news_blocked": 0,
        "news_errors": 0,
    }

    try:
        rows = select_results_pending_link_discovery(conn, args.batch)
        stats["rows_selected"] = len(rows)
        if not rows:
            return stats

        for row in rows:
            if _shutdown:
                break

            company = row["company"]
            try:
                log.info("  Discovering links: %s", company)
                discovery = discover_company_links(
                    company=company,
                    session=session,
                    proxy_pool=proxy_pool,
                    news_top=args.news_top,
                    max_retries=args.max_retries,
                    delay_min=args.delay_min,
                    delay_max=args.delay_max,
                )

                persisted = persist_discovery(conn, row, discovery)
                conn.commit()

                stats["rows_processed"] += 1
                stats["news_candidates"] += int(persisted["news_candidates"])
                if persisted["completed"]:
                    stats["rows_completed"] += 1
                if persisted["wrote_news"]:
                    stats["rows_with_writes"] += 1

                if discovery["news_status"] == "no_results":
                    stats["news_no_results"] += 1
                elif discovery["news_status"] == "blocked":
                    stats["news_blocked"] += 1
                elif discovery["news_status"] == "error":
                    stats["news_errors"] += 1

                log.info(
                    "    result_id=%s news=%s(%s)",
                    row["result_id"],
                    discovery["news_status"],
                    persisted["news_candidates"],
                )
            except psycopg2.errors.ForeignKeyViolation:
                # Dedup worker deleted this result between SELECT and INSERT
                conn.rollback()
                log.info(
                    "    result_id=%s removed by dedup; skipping",
                    row["result_id"],
                )
            except Exception as exc:
                conn.rollback()
                stats["row_errors"] += 1
                log.error(
                    "    result_id=%s failed during discovery/persist: %s",
                    row["result_id"],
                    exc,
                )

            time.sleep(random.uniform(args.delay_min, args.delay_max))

        return stats
    except Exception:
        conn.rollback()
        raise
    finally:
        session.close()
        conn.close()


def seed_website_candidates(batch_size: int) -> int:
    """Spider company websites with crawl4ai and seed each page as a link_candidate.
    The document ingester will then chunk and embed them automatically."""
    from broker.documents.website_spider import crawl_company_website

    conn = psycopg2.connect(DSN)
    conn.autocommit = False
    seeded = 0
    try:
        rows = select_results_pending_website_discovery(conn, batch_size)
        if not rows:
            return 0

        for row in rows:
            url = row["web_site"].strip()
            if not url:
                continue
            # Normalise bare domains → https://
            if not url.startswith(("http://", "https://")):
                url = f"https://{url}"

            company = row["company"]
            result_id = row["result_id"]

            log.info("  Spidering website for %s: %s", company, url)

            # Use crawl4ai to spider the website (homepage + subpages)
            try:
                pages = crawl_company_website(url, timeout=20)
            except Exception as exc:
                log.warning("  crawl4ai spider failed for %s: %s — falling back to homepage only", company, exc)
                pages = []

            if pages:
                # Seed each discovered page as a separate link_candidate
                # AND store the already-fetched text in documents so the ingester
                # skips re-fetching and goes straight to chunking + embedding.
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
                    # Store pre-fetched text as a document
                    text = page.get("text", "")
                    insert_document(
                        conn,
                        DocumentRecord(
                            link_candidate_id=lc_id,
                            url_fetched=page["url"],
                            page_title=page.get("title", "")[:500] or "",
                            fetch_status="ok",
                            http_status=200,
                            text_hash=hashlib.sha1(text.encode()).hexdigest(),
                            raw_text=text,
                        ),
                    )
                    seeded += 1
                log.info("  Seeded %d pages from %s for %s", len(pages), url, company)
            else:
                # Fallback: just seed the homepage URL for the ingester to fetch
                upsert_link_candidate(
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
                seeded += 1
                log.info("  Fallback: seeded homepage only for %s", company)

            conn.commit()

        log.info("  Total website pages seeded: %d", seeded)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return seeded


def run(args) -> None:
    global DSN
    if getattr(args, 'dsn', None):
        DSN = args.dsn
        log.info("  DSN override: %s", DSN)
    ensure_pipeline_schema(dsn=DSN)
    proxy_pool = build_proxy_pool(no_proxifly=args.no_proxifly, proxy_file=args.proxy_file)

    log.info("=" * 55)
    log.info("  Link Discovery Worker started  (news + website)")
    log.info("  Poll interval : %ss", args.poll)
    log.info("  Batch size    : %s", args.batch)
    log.info("  News top      : %s", args.news_top)
    log.info("  Delay         : %s-%ss", args.delay_min, args.delay_max)
    log.info("=" * 55)

    total_rows_processed = 0
    total_candidates = 0
    consecutive_errors = 0
    max_retries = 10

    while not _shutdown:
        try:
            # Seed company website URLs (fast, no HTTP needed)
            website_seeded = seed_website_candidates(args.batch)
            if website_seeded:
                log.info("  Website URLs seeded: %d", website_seeded)

            stats = poll_and_enrich(args, proxy_pool=proxy_pool)
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            backoff = min(60, 5 * consecutive_errors)
            log.error("Crash #%s: %s; retrying in %ss", consecutive_errors, exc, backoff)
            if consecutive_errors >= max_retries:
                log.error("Too many consecutive errors (%s). Giving up.", max_retries)
                break
            time.sleep(backoff)
            continue

        processed = stats["rows_processed"]
        total_rows_processed += processed
        total_candidates += stats["news_candidates"]

        if processed > 0:
            try:
                verify_conn = psycopg2.connect(DSN)
                try:
                    remaining = count_results_pending_link_discovery(verify_conn)
                finally:
                    verify_conn.close()
            except Exception:
                remaining = "?"
            log.info(
                "  Completed %s rows this cycle | errors=%s | session total rows=%s candidates=%s | remaining=%s",
                processed,
                stats["row_errors"],
                total_rows_processed,
                total_candidates,
                remaining,
            )
        else:
            log.info("  No pending rows. Sleeping %ss.", args.poll)

        if args.once:
            break

        for _ in range(int(args.poll)):
            if _shutdown:
                break
            time.sleep(1)

    log.info(
        "Link discovery worker stopped. Total processed rows=%s total candidates=%s",
        total_rows_processed,
        total_candidates,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Link discovery worker daemon (news-only)")
    parser.add_argument(
        "--dsn",
        type=str,
        default="",
        help="PostgreSQL DSN override. If empty, uses .env / default.",
    )
    parser.add_argument(
        "--news-top",
        type=int,
        default=10,
        help="News URLs to keep per company.",
    )
    parser.add_argument("--batch", type=int, default=10, help="Rows per poll cycle.")
    parser.add_argument("--poll", type=int, default=30, help="Seconds between polls.")
    parser.add_argument("--delay-min", type=float, default=1.5, help="Min request delay in seconds.")
    parser.add_argument("--delay-max", type=float, default=3.5, help="Max request delay in seconds.")
    parser.add_argument("--max-retries", type=int, default=2, help="Retries on blocked search pages.")
    parser.add_argument("--no-proxifly", action="store_true", help="Disable Proxifly proxy bootstrap.")
    parser.add_argument("--proxy-file", default="", help="Optional proxy file to load.")
    parser.add_argument("--once", action="store_true", help="Run one poll cycle and exit.")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
