from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from broker.config import CHROMA_DIR, STATUS_DIR
from broker.orchestration.document_ingest import init_status, run_document_ingest_cycle
from broker.orchestration.status import now_iso, save_json
from broker.storage.postgres import ensure_pipeline_schema


_shutdown = False


def _handle_signal(sig, frame):
    del sig, frame
    global _shutdown
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def run(args) -> None:
    status_file = Path(args.status_file)
    status = init_status(args)
    save_json(status_file, status)

    total_fetch_processed = 0
    total_chunk_processed = 0

    try:
        ensure_pipeline_schema()
        while not _shutdown:
            result = run_document_ingest_cycle(args, status, status_file)
            total_fetch_processed += result["fetch"]["processed"]
            total_chunk_processed += result["chunk"]["processed"]

            print(
                "[DOC-INGEST]",
                f"fetch_processed={result['fetch']['processed']}",
                f"chunk_processed={result['chunk']['processed']}",
                f"chunks_embedded={result['chunk']['chunks_embedded']}",
                f"collection_count={result['chunk']['collection_count']}",
            )

            if args.once:
                break

            if result["fetch"]["selected"] == 0 and result["chunk"]["selected"] == 0:
                status["stage"] = "idle"
                status["updated_at"] = now_iso()
                save_json(status_file, status)

            for _ in range(int(args.poll)):
                if _shutdown:
                    break
                time.sleep(1)
    except KeyboardInterrupt:
        status["stage"] = "interrupted"
        status["updated_at"] = now_iso()
        save_json(status_file, status)
        raise
    except Exception as exc:
        status["stage"] = "failed"
        status["error"] = str(exc)
        status["updated_at"] = now_iso()
        save_json(status_file, status)
        raise

    status["stage"] = "completed_once" if args.once else "stopped"
    status["totals"] = {
        "fetch_processed": total_fetch_processed,
        "chunk_processed": total_chunk_processed,
    }
    status["updated_at"] = now_iso()
    save_json(status_file, status)


def monitor(args) -> None:
    status_file = Path(args.status_file)
    while True:
        stamp = time.strftime("%Y-%m-%d %H:%M:%S")
        if not status_file.exists():
            print(f"[{stamp}] status file not found: {status_file}")
        else:
            payload = json.loads(status_file.read_text(encoding="utf-8"))
            fetch = payload.get("fetch", {}) or {}
            chunk = payload.get("chunk", {}) or {}
            print(
                f"[{stamp}] stage={payload.get('stage')} "
                f"fetch_processed={fetch.get('processed', 0)} "
                f"fetch_remaining={fetch.get('remaining', '?')} "
                f"chunk_processed={chunk.get('processed', 0)} "
                f"chunk_remaining={chunk.get('remaining', '?')} "
                f"collection_count={chunk.get('collection_count', '?')}"
            )
            if not args.follow and payload.get("stage") in {"completed_once", "failed", "interrupted"}:
                return
        if args.once:
            return
        time.sleep(args.interval)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="DB-driven document ingest worker.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    run_parser = sub.add_parser("run", help="Fetch, chunk, embed, and upsert from Postgres.")
    run_parser.add_argument("--persist-dir", default=str(CHROMA_DIR / "chroma_smoke_db"))
    run_parser.add_argument("--collection", default="construction_docs_db")
    run_parser.add_argument("--fetch-batch", type=int, default=25)
    run_parser.add_argument("--chunk-batch", type=int, default=25)
    run_parser.add_argument("--poll", type=int, default=30)
    run_parser.add_argument("--timeout", type=int, default=20)
    run_parser.add_argument("--max-chars", type=int, default=25000)
    run_parser.add_argument("--chunk-words", type=int, default=220)
    run_parser.add_argument("--overlap-words", type=int, default=50)
    run_parser.add_argument(
        "--embedding-backend",
        choices=["simple", "chroma-default", "sentence-transformers", "openai"],
        default="simple",
    )
    run_parser.add_argument("--embedding-model", default="text-embedding-3-small")
    run_parser.add_argument("--simple-dim", type=int, default=384)
    run_parser.add_argument("--env-file", default="")
    run_parser.add_argument("--retry-failed-fetches", action="store_true")
    run_parser.add_argument("--force-reembed", action="store_true")
    run_parser.add_argument("--fetch-delay", type=float, default=0.0)
    run_parser.add_argument("--chunk-delay", type=float, default=0.0)
    run_parser.add_argument("--debug-save-dir", default="")
    run_parser.add_argument("--debug-preview-chars", type=int, default=800)
    run_parser.add_argument(
        "--status-file",
        default=str(STATUS_DIR / "run_document_ingest_status.json"),
    )
    run_parser.add_argument("--once", action="store_true")

    monitor_parser = sub.add_parser("monitor", help="Monitor the document ingest status file.")
    monitor_parser.add_argument(
        "--status-file",
        default=str(STATUS_DIR / "run_document_ingest_status.json"),
    )
    monitor_parser.add_argument("--interval", type=int, default=20)
    monitor_parser.add_argument("--once", action="store_true")
    monitor_parser.add_argument("--follow", action="store_true")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    if args.cmd == "run":
        run(args)
        return
    if args.cmd == "monitor":
        monitor(args)
        return
    parser.error(f"Unknown command: {args.cmd}")


if __name__ == "__main__":
    main()
