"""
Pipeline orchestrator.

  1. Scraper         (Docker: gosom/google-maps-scraper) - populates Postgres
  2. Link discovery  (worker_enrich.py)                  - writes link_candidates
  3. Document ingest (scripts/run_document_ingest.py)    - writes documents/chunks + Chroma
  4. Deduper         (worker_dedup.py)                   - cleans duplicate leads
"""

from __future__ import annotations

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from queue import Empty, Queue
from threading import Thread

import psycopg2

from broker.config import (
    CHROMA_DIR,
    STATUS_DIR,
    get_docker_construction_dsn,
    get_local_construction_dsn,
    project_path,
)


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PIPELINE] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


WORK_DIR = str(project_path())
SCRAPER_DSN = get_docker_construction_dsn()
LOCAL_DSN = get_local_construction_dsn()
QUERY_FILE = str(project_path("queries", "construction_queries.txt"))
SCRAPER_IMAGE = "gosom/google-maps-scraper:latest"
SCRAPER_NAME = "construction-scraper"

ENRICHER_SCRIPT = os.path.join(WORK_DIR, "worker_enrich.py")
DEDUP_SCRIPT = os.path.join(WORK_DIR, "worker_dedup.py")
DOCUMENT_INGEST_SCRIPT = os.path.join(WORK_DIR, "scripts", "run_document_ingest.py")
UNIFIED_SCRIPT = os.path.join(WORK_DIR, "worker_unified.py")

_children: list[subprocess.Popen] = []
_shutdown = False


def _handle_signal(sig, frame):
    del sig, frame
    global _shutdown
    if _shutdown:
        log.warning("Force-killing all children.")
        for proc in _children:
            try:
                proc.kill()
            except Exception:
                pass
        sys.exit(1)

    _shutdown = True
    log.info("Shutting down pipeline. Press Ctrl+C again to force-kill.")
    log.info("Stopping scraper container.")
    subprocess.run(["docker", "stop", SCRAPER_NAME], capture_output=True)

    for proc in _children:
        try:
            proc.terminate()
        except Exception:
            pass


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def seed_jobs(query_file: str) -> None:
    log.info("Seeding jobs from %s", query_file)
    if not os.path.exists(query_file):
        raise SystemExit(f"Query file not found: {query_file}")

    cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        f"{SCRAPER_NAME}-seed",
        "-v",
        f"{os.path.abspath(query_file)}:/queries.txt",
        SCRAPER_IMAGE,
        "-dsn",
        SCRAPER_DSN,
        "-produce",
        "-input",
        "/queries.txt",
        "-lang",
        "en",
    ]
    log.info("  $ %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if result.returncode != 0:
        raise SystemExit(f"Seeding failed:\n{result.stderr}")

    try:
        conn = psycopg2.connect(LOCAL_DSN)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM gmaps_jobs WHERE status = 'new'")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        log.info("  Queued jobs in gmaps_jobs: %s", count)
    except Exception as exc:
        log.warning("Could not verify queued jobs: %s", exc)


def start_scraper(concurrency: int, depth: int, extra_flags: str) -> subprocess.Popen:
    log.info("Starting scraper container.")
    subprocess.run(["docker", "rm", "-f", SCRAPER_NAME], capture_output=True)

    cmd = [
        "docker",
        "run",
        "--rm",
        "--name",
        SCRAPER_NAME,
        SCRAPER_IMAGE,
        "-dsn",
        SCRAPER_DSN,
        "-c",
        str(concurrency),
        "-depth",
        str(depth),
        "-email",
        "-exit-on-inactivity",
        "5m",
    ]
    if extra_flags:
        cmd.extend(extra_flags.split())

    log.info("  $ %s", " ".join(cmd))
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        encoding="utf-8",
        errors="replace",
    )


def start_worker(script: str, label: str, extra_args: list[str] | None = None) -> subprocess.Popen:
    cmd = [sys.executable, script]
    if extra_args:
        cmd.extend(extra_args)
    log.info("Starting %s.", label)
    log.info("  $ %s", " ".join(cmd))
    return subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        encoding="utf-8",
        errors="replace",
    )


def _pipe_reader(label: str, proc: subprocess.Popen, out_queue: Queue[tuple[str, str]]) -> None:
    if proc.stdout is None:
        return
    try:
        for line in proc.stdout:
            out_queue.put((label, line))
    finally:
        try:
            proc.stdout.close()
        except Exception:
            pass


def stream_output(processes: dict[str, subprocess.Popen]) -> None:
    out_queue: Queue[tuple[str, str]] = Queue()
    readers: list[Thread] = []

    for label, proc in processes.items():
        reader = Thread(target=_pipe_reader, args=(label, proc, out_queue), daemon=True)
        reader.start()
        readers.append(reader)

    while not _shutdown:
        all_done = all(proc.poll() is not None for proc in processes.values())
        try:
            label, line = out_queue.get(timeout=0.5)
            print(f"[{label}] {line}", end="", flush=True)
        except Empty:
            if all_done:
                break

    while True:
        try:
            label, line = out_queue.get_nowait()
            print(f"[{label}] {line}", end="", flush=True)
        except Empty:
            break

    for reader in readers:
        reader.join(timeout=1.0)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Construction lead pipeline orchestrator")
    parser.add_argument("--no-scraper", action="store_true", help="Skip starting the scraper")
    parser.add_argument("--no-enricher", action="store_true", help="Skip starting the link-discovery worker")
    parser.add_argument("--no-document-ingest", action="store_true", help="Skip starting the DB document-ingest worker")
    parser.add_argument("--no-dedup", action="store_true", help="Skip starting the dedup worker")
    parser.add_argument("--seed-only", action="store_true", help="Only seed jobs, do not start workers")
    parser.add_argument("--skip-seed", action="store_true", help="Skip seeding (jobs already queued)")
    parser.add_argument("--concurrency", type=int, default=4, help="Scraper concurrency")
    parser.add_argument("--depth", type=int, default=1, help="Scraper scroll depth")
    parser.add_argument("--query-file", type=str, default=QUERY_FILE, help="Query file for seeding")
    parser.add_argument("--scraper-flags", type=str, default="", help="Extra scraper flags")

    parser.add_argument("--enrich-top", type=int, default=100, help="Legacy default for --news-top in worker_enrich")
    parser.add_argument("--enrich-batch", type=int, default=10, help="Link-discovery batch size")
    parser.add_argument("--enrich-poll", type=int, default=30, help="Link-discovery poll interval")

    parser.add_argument("--document-fetch-batch", type=int, default=25, help="Document-ingest fetch batch size")
    parser.add_argument("--document-chunk-batch", type=int, default=25, help="Document-ingest chunk batch size")
    parser.add_argument("--document-poll", type=int, default=30, help="Document-ingest poll interval")
    parser.add_argument("--document-persist-dir", type=str, default=str(CHROMA_DIR / "chroma_smoke_db"), help="Chroma persist dir for DB ingest")
    parser.add_argument("--document-collection", type=str, default="construction_docs_openai1536_live", help="Chroma collection for DB ingest")
    parser.add_argument("--document-embedding-backend", type=str, default="openai", help="Embedding backend for DB ingest")
    parser.add_argument("--document-embedding-model", type=str, default="text-embedding-3-small", help="Embedding model for DB ingest")
    parser.add_argument("--document-simple-dim", type=int, default=384, help="Simple embedding dimension for DB ingest")
    parser.add_argument("--document-env-file", type=str, default="", help="Optional .env path for DB ingest")
    parser.add_argument("--document-status-file", type=str, default=str(STATUS_DIR / "run_document_ingest_status.json"), help="Status file for DB ingest")

    parser.add_argument("--dedup-poll", type=int, default=60, help="Dedup poll interval")
    parser.add_argument("--dedup-dsn", type=str, default=LOCAL_DSN, help="PostgreSQL DSN for dedup worker")

    # ── Unified mode (replaces enricher + document-ingest with single flow) ──
    parser.add_argument("--unified", action="store_true",
                        help="Use unified pipeline (discover+fetch+chunk+embed in one flow per company)")
    parser.add_argument("--unified-batch", type=int, default=10, help="Unified worker: companies per cycle")
    parser.add_argument("--unified-poll", type=int, default=20, help="Unified worker: poll interval")
    parser.add_argument("--unified-news-top", type=int, default=10, help="Unified worker: news URLs per company")
    parser.add_argument("--unified-collection", type=str,
                        default="construction_docs_openai1536_live", help="Unified worker: Chroma collection")
    parser.add_argument("--unified-embedding-backend", type=str, default="openai",
                        help="Unified worker: embedding backend")
    parser.add_argument("--unified-embedding-model", type=str,
                        default="text-embedding-3-small", help="Unified worker: embedding model")
    parser.add_argument("--unified-persist-dir", type=str,
                        default=str(CHROMA_DIR / "chroma_smoke_db"), help="Unified worker: Chroma dir")
    return parser


def main() -> None:
    args = build_parser().parse_args()

    log.info("=" * 60)
    log.info("Construction Lead Pipeline")
    if args.unified:
        log.info("Mode: UNIFIED (Scraper -> Discover+Fetch+Embed -> Dedup)")
    else:
        log.info("Mode: Legacy  (Scraper -> Discovery -> Document Ingest -> Dedup)")
    log.info("Started at: %s", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    log.info("=" * 60)

    if not args.skip_seed and not args.no_scraper:
        seed_jobs(args.query_file)
        if args.seed_only:
            log.info("Seed-only mode complete.")
            return

    processes: dict[str, subprocess.Popen] = {}

    if not args.no_scraper:
        proc = start_scraper(args.concurrency, args.depth, args.scraper_flags)
        processes["SCRAPER"] = proc
        _children.append(proc)
        log.info("Waiting 10s for scraper initialization.")
        time.sleep(10)

    if args.unified:
        # ── Unified mode: single worker does discover + fetch + chunk + embed ──
        log.info("Using UNIFIED pipeline mode.")
        proc = start_worker(
            UNIFIED_SCRIPT,
            "Unified Pipeline Worker",
            extra_args=[
                "--batch",
                str(args.unified_batch),
                "--poll",
                str(args.unified_poll),
                "--news-top",
                str(args.unified_news_top),
                "--collection",
                args.unified_collection,
                "--embedding-backend",
                args.unified_embedding_backend,
                "--embedding-model",
                args.unified_embedding_model,
                "--persist-dir",
                args.unified_persist_dir,
            ],
        )
        processes["UNIFIED"] = proc
        _children.append(proc)
    else:
        # ── Legacy mode: separate enricher + document-ingest workers ──
        if not args.no_enricher:
            proc = start_worker(
                ENRICHER_SCRIPT,
                "Link Discovery Worker",
                extra_args=[
                    "--top",
                    str(args.enrich_top),
                    "--batch",
                    str(args.enrich_batch),
                    "--poll",
                    str(args.enrich_poll),
                ],
            )
            processes["DISCOVERY"] = proc
            _children.append(proc)

        if not args.no_document_ingest:
            proc = start_worker(
                DOCUMENT_INGEST_SCRIPT,
                "Document Ingest Worker",
                extra_args=[
                    "run",
                    "--fetch-batch",
                    str(args.document_fetch_batch),
                    "--chunk-batch",
                    str(args.document_chunk_batch),
                    "--poll",
                    str(args.document_poll),
                    "--persist-dir",
                    args.document_persist_dir,
                    "--collection",
                    args.document_collection,
                    "--embedding-backend",
                    args.document_embedding_backend,
                    "--embedding-model",
                    args.document_embedding_model,
                    "--simple-dim",
                    str(args.document_simple_dim),
                    "--env-file",
                    args.document_env_file,
                    "--status-file",
                    args.document_status_file,
                ],
            )
            processes["DOCINGEST"] = proc
            _children.append(proc)

    if not args.no_dedup:
        proc = start_worker(
            DEDUP_SCRIPT,
            "Dedup Worker",
            extra_args=[
                "--poll",
                str(args.dedup_poll),
                "--dsn",
                args.dedup_dsn,
            ],
        )
        processes["DEDUP"] = proc
        _children.append(proc)

    log.info("Running %s service(s). Ctrl+C to stop.", len(processes))
    stream_output(processes)

    for label, proc in processes.items():
        try:
            proc.wait(timeout=30)
            log.info("%s exited with code %s", label, proc.returncode)
        except subprocess.TimeoutExpired:
            log.warning("%s did not exit in time; killing.", label)
            proc.kill()

    log.info("Pipeline stopped.")


if __name__ == "__main__":
    main()
