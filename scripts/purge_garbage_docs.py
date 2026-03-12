#!/usr/bin/env python
"""One-shot script: purge the 29 known-garbage documents and their chunks
from PostgreSQL and ChromaDB.

Garbage docs were identified by the quality audit — they contain content from
Wikipedia, Stack Overflow, Merriam-Webster, YouTube alphabet songs, etc.
that slipped in via bad Bing search results.

Run:  python scripts/purge_garbage_docs.py          (dry-run by default)
      python scripts/purge_garbage_docs.py --commit  (actually delete)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Allow running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from broker.storage.postgres import connect_postgres
from broker.storage.chroma_store import get_or_create_collection


GARBAGE_DOC_IDS = [
    7, 33, 59, 187, 188, 207, 212, 223, 321, 370, 380, 383, 387, 390,
    395, 398, 402, 405, 408, 413, 465, 468, 486, 487, 522, 537, 542, 555, 557,
]

CHROMA_PERSIST_DIR = str(Path(__file__).resolve().parent.parent / "runtime" / "chroma" / "chroma_smoke_db")
CHROMA_COLLECTION = "construction_docs_openai1536_live"


def purge_postgres(conn, dry_run: bool) -> dict:
    cur = conn.cursor()

    # 1. Find chunk IDs that belong to the garbage documents
    cur.execute(
        "SELECT id FROM document_chunks WHERE document_id = ANY(%s) ORDER BY id",
        (GARBAGE_DOC_IDS,),
    )
    chunk_ids = [row[0] for row in cur.fetchall()]

    # 2. Delete the chunks
    cur.execute(
        "DELETE FROM document_chunks WHERE document_id = ANY(%s)",
        (GARBAGE_DOC_IDS,),
    )
    chunks_deleted = cur.rowcount

    # 3. Mark the documents as 'irrelevant' (so they are not re-fetched)
    cur.execute(
        "UPDATE documents SET fetch_status = 'irrelevant', raw_text = '' WHERE id = ANY(%s)",
        (GARBAGE_DOC_IDS,),
    )
    docs_updated = cur.rowcount

    if dry_run:
        conn.rollback()
        print(f"[DRY-RUN] Would delete {chunks_deleted} chunks from {docs_updated} documents")
    else:
        conn.commit()
        print(f"[COMMITTED] Deleted {chunks_deleted} chunks, marked {docs_updated} documents as irrelevant")

    cur.close()
    return {"chunk_ids": chunk_ids, "chunks_deleted": chunks_deleted, "docs_updated": docs_updated}


def purge_chroma(chunk_ids: list[int], dry_run: bool) -> int:
    chroma_ids = [f"document_chunk:{cid}" for cid in chunk_ids]
    if not chroma_ids:
        print("No Chroma IDs to purge.")
        return 0

    collection = get_or_create_collection(CHROMA_PERSIST_DIR, CHROMA_COLLECTION)
    before = collection.count()

    # ChromaDB .get() to see which ones actually exist
    existing = collection.get(ids=chroma_ids)
    found_ids = existing["ids"] if existing and existing.get("ids") else []

    if not found_ids:
        print(f"None of the {len(chroma_ids)} chunk IDs found in Chroma (collection has {before} items).")
        return 0

    if dry_run:
        print(f"[DRY-RUN] Would delete {len(found_ids)} vectors from Chroma (collection has {before} items)")
        return len(found_ids)

    collection.delete(ids=found_ids)
    after = collection.count()
    print(f"[COMMITTED] Deleted {len(found_ids)} vectors from Chroma ({before} → {after})")
    return len(found_ids)


def main():
    parser = argparse.ArgumentParser(description="Purge garbage docs from DB and Chroma")
    parser.add_argument("--commit", action="store_true", help="Actually delete (default is dry-run)")
    args = parser.parse_args()
    dry_run = not args.commit

    if dry_run:
        print("=" * 60)
        print("  DRY-RUN MODE — pass --commit to actually delete")
        print("=" * 60)

    print(f"\nTarget: {len(GARBAGE_DOC_IDS)} garbage document IDs")

    # PostgreSQL
    with connect_postgres(autocommit=False) as conn:
        pg_result = purge_postgres(conn, dry_run)

    # ChromaDB
    purge_chroma(pg_result["chunk_ids"], dry_run)

    print("\nDone.")


if __name__ == "__main__":
    main()
