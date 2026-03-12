"""
Deduplication worker.

Continuously polls the database and removes duplicate company entries
from the `results` table based on matching `data->>'title'` values,
keeping only the row with the lowest id.

Usage:
    python worker_dedup.py
    python worker_dedup.py --poll 60 --dsn postgresql://postgres:postgres@localhost:5432/construction
"""

from __future__ import annotations

import argparse
import logging
import signal
import time

import psycopg2

from broker.config import get_local_construction_dsn


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DEDUP] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

_shutdown = False


def _handle_signal(sig, frame):
    del sig, frame
    global _shutdown
    log.info("Shutdown signal received; finishing current cycle.")
    _shutdown = True


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def remove_duplicates(dsn: str) -> int:
    """Delete duplicate results rows, keeping the one with the lowest id.

    Returns the number of rows deleted.
    """
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    try:
        cur = conn.cursor()
        cur.execute("""
            DELETE FROM results
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM results
                WHERE COALESCE(data->>'title', '') <> ''
                GROUP BY LOWER(TRIM(data->>'title'))
            )
            AND COALESCE(data->>'title', '') <> ''
        """)
        deleted = cur.rowcount
        conn.commit()
        cur.close()
        return deleted
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run(args) -> None:
    dsn = args.dsn or get_local_construction_dsn()

    log.info("=" * 55)
    log.info("  Dedup Worker started")
    log.info("  Poll interval : %ss", args.poll)
    log.info("  DSN           : %s", dsn.split("@")[-1] if "@" in dsn else dsn)
    log.info("=" * 55)

    total_deleted = 0

    while not _shutdown:
        try:
            deleted = remove_duplicates(dsn)
            total_deleted += deleted
            if deleted > 0:
                log.info("  Removed %s duplicate(s) (total: %s)", deleted, total_deleted)
            else:
                log.info("  No duplicates found. Sleeping %ss.", args.poll)
        except Exception as exc:
            log.error("  Error during dedup: %s", exc)

        for _ in range(int(args.poll)):
            if _shutdown:
                break
            time.sleep(1)

    log.info("Dedup worker stopped. Total duplicates removed: %s", total_deleted)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplication worker daemon")
    parser.add_argument("--poll", type=int, default=60, help="Seconds between poll cycles.")
    parser.add_argument("--dsn", type=str, default="", help="PostgreSQL DSN (default: from .env).")
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()
