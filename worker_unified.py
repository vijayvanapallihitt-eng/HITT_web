"""
Unified pipeline worker — discover, fetch, chunk, embed in one flow.

Replaces the separate worker_enrich.py + run_document_ingest.py polling
loops with a single worker that processes each company end-to-end:

  1. Google News link discovery
  2. Website spidering (crawl4ai)
  3. Document fetching (HTTP)
  4. Chunking & embedding (ChromaDB)

Usage:
    python worker_unified.py                          # default settings
    python worker_unified.py --batch 5 --poll 20      # custom batch/poll
    python worker_unified.py --once                    # single pass, then exit
    python worker_unified.py --dsn "postgresql://..." # custom DB
"""

from __future__ import annotations

import argparse
import logging
import random
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from broker.config import CHROMA_DIR, STATUS_DIR, get_local_construction_dsn
from broker.enrichment.proxies import ProxyPool, fetch_proxifly_proxies, load_proxy_file
from broker.orchestration.status import now_iso, save_json
from broker.orchestration.unified_pipeline import (
    BatchResult,
    UnifiedPipelineConfig,
    run_pipeline_batch,
)
from broker.storage.postgres import ensure_pipeline_schema


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [UNIFIED] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_shutdown = False


def _handle_signal(sig, frame):
    del sig, frame
    global _shutdown
    log.info("Shutdown signal received; finishing current company.")
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


def init_status(cfg: UnifiedPipelineConfig) -> dict:
    return {
        "worker": "unified_pipeline",
        "started_at": now_iso(),
        "updated_at": now_iso(),
        "stage": "starting",
        "config": {
            "batch_size": cfg.batch_size,
            "embedding_backend": cfg.embedding_backend,
            "embedding_model": cfg.effective_embedding_name(),
            "collection": cfg.collection_name,
            "persist_dir": cfg.persist_dir,
        },
        "totals": {
            "companies_processed": 0,
            "companies_errors": 0,
            "news_links": 0,
            "website_pages": 0,
            "docs_fetched": 0,
            "docs_ok": 0,
            "chunks_embedded": 0,
            "remaining": "?",
        },
        "last_batch": {},
    }


def update_status(status: dict, batch: BatchResult, status_file: Path) -> None:
    totals = status["totals"]
    totals["companies_processed"] += batch.companies_processed
    totals["companies_errors"] += batch.companies_errors
    totals["news_links"] += batch.total_news_links
    totals["website_pages"] += batch.total_website_pages
    totals["docs_fetched"] += batch.total_docs_fetched
    totals["docs_ok"] += batch.total_docs_ok
    totals["chunks_embedded"] += batch.total_embedded
    totals["remaining"] = batch.remaining

    status["last_batch"] = {
        "companies_selected": batch.companies_selected,
        "companies_processed": batch.companies_processed,
        "news_links": batch.total_news_links,
        "website_pages": batch.total_website_pages,
        "docs_ok": batch.total_docs_ok,
        "chunks_embedded": batch.total_embedded,
    }
    status["updated_at"] = now_iso()
    save_json(status_file, status)


def run(args) -> None:
    dsn = args.dsn or get_local_construction_dsn()
    persist_dir = args.persist_dir or str(CHROMA_DIR / "chroma_smoke_db")

    cfg = UnifiedPipelineConfig(
        dsn=dsn,
        batch_size=args.batch,
        news_top=args.news_top,
        max_search_retries=args.max_retries,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        spider_timeout=args.spider_timeout,
        fetch_timeout=args.fetch_timeout,
        max_chars=args.max_chars,
        chunk_words=args.chunk_words,
        overlap_words=args.overlap_words,
        embedding_backend=args.embedding_backend,
        embedding_model=args.embedding_model,
        simple_dim=args.simple_dim,
        env_file=args.env_file,
        persist_dir=persist_dir,
        collection_name=args.collection,
    )

    ensure_pipeline_schema(dsn=dsn)
    proxy_pool = build_proxy_pool(no_proxifly=args.no_proxifly, proxy_file=args.proxy_file)

    status_file = Path(args.status_file)
    status = init_status(cfg)
    save_json(status_file, status)

    log.info("=" * 62)
    log.info("  Unified Pipeline Worker")
    log.info("  discover → fetch → chunk → embed (per company)")
    log.info("  Batch size   : %s", args.batch)
    log.info("  Poll interval: %ss", args.poll)
    log.info("  News top     : %s", args.news_top)
    log.info("  Embedding    : %s / %s", args.embedding_backend, cfg.effective_embedding_name())
    log.info("  Collection   : %s", args.collection)
    log.info("  Chroma dir   : %s", persist_dir)
    log.info("=" * 62)

    consecutive_errors = 0
    max_consecutive = 10

    while not _shutdown:
        try:
            status["stage"] = "running"
            status["updated_at"] = now_iso()
            save_json(status_file, status)

            batch = run_pipeline_batch(
                cfg,
                proxy_pool=proxy_pool,
                shutdown_flag=lambda: _shutdown,
            )
            consecutive_errors = 0
        except Exception as exc:
            consecutive_errors += 1
            backoff = min(60, 5 * consecutive_errors)
            log.error("Crash #%s: %s; retrying in %ss", consecutive_errors, exc, backoff)
            if consecutive_errors >= max_consecutive:
                log.error("Too many consecutive errors (%s). Giving up.", max_consecutive)
                status["stage"] = "failed"
                status["error"] = str(exc)
                status["updated_at"] = now_iso()
                save_json(status_file, status)
                break
            time.sleep(backoff)
            continue

        update_status(status, batch, status_file)

        if batch.companies_selected > 0:
            log.info(
                "  Batch done: %d companies | news=%d website=%d docs=%d chunks=%d | remaining=%s",
                batch.companies_processed,
                batch.total_news_links,
                batch.total_website_pages,
                batch.total_docs_ok,
                batch.total_embedded,
                batch.remaining,
            )
        else:
            log.info("  No pending companies. Sleeping %ss.", args.poll)

        if args.once:
            break

        for _ in range(int(args.poll)):
            if _shutdown:
                break
            time.sleep(1)

    status["stage"] = "completed_once" if args.once else "stopped"
    status["updated_at"] = now_iso()
    save_json(status_file, status)
    log.info(
        "Unified pipeline stopped. Total: %s companies, %s chunks embedded.",
        status["totals"]["companies_processed"],
        status["totals"]["chunks_embedded"],
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Unified pipeline worker: discover → fetch → chunk → embed",
    )
    p.add_argument("--dsn", default="", help="PostgreSQL DSN override")
    p.add_argument("--batch", type=int, default=10, help="Companies per poll cycle")
    p.add_argument("--poll", type=int, default=20, help="Seconds between poll cycles")
    p.add_argument("--once", action="store_true", help="Single pass then exit")

    # News discovery
    p.add_argument("--news-top", type=int, default=10, help="News URLs to keep per company")
    p.add_argument("--max-retries", type=int, default=2, help="Search retry count on blocks")
    p.add_argument("--delay-min", type=float, default=1.5, help="Min delay between companies (s)")
    p.add_argument("--delay-max", type=float, default=3.5, help="Max delay between companies (s)")

    # Website spidering
    p.add_argument("--spider-timeout", type=int, default=20, help="crawl4ai page timeout (s)")

    # Fetching
    p.add_argument("--fetch-timeout", type=int, default=20, help="HTTP fetch timeout (s)")
    p.add_argument("--max-chars", type=int, default=25000, help="Max chars per document")

    # Chunking
    p.add_argument("--chunk-words", type=int, default=220, help="Words per chunk")
    p.add_argument("--overlap-words", type=int, default=50, help="Overlap words between chunks")

    # Embedding
    p.add_argument(
        "--embedding-backend",
        choices=["simple", "chroma-default", "sentence-transformers", "openai"],
        default="openai",
        help="Embedding backend",
    )
    p.add_argument("--embedding-model", default="text-embedding-3-small", help="Embedding model name")
    p.add_argument("--simple-dim", type=int, default=384, help="Dimension for simple hash embeddings")
    p.add_argument("--env-file", default="", help="Optional .env file path")

    # ChromaDB
    p.add_argument("--persist-dir", default="", help="Chroma persist directory")
    p.add_argument("--collection", default="construction_docs_openai1536_live", help="Chroma collection name")

    # Status
    p.add_argument(
        "--status-file",
        default=str(STATUS_DIR / "unified_pipeline_status.json"),
        help="Status JSON file path",
    )

    # Proxies
    p.add_argument("--no-proxifly", action="store_true", help="Disable Proxifly proxy bootstrap")
    p.add_argument("--proxy-file", default="", help="Optional proxy file to load")

    return p


def main() -> None:
    args = build_parser().parse_args()
    run(args)


if __name__ == "__main__":
    main()
