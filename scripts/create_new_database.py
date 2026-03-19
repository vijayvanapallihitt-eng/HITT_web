"""
Create a brand-new Postgres database for the Broker pipeline.

Usage:
    python scripts/create_new_database.py                          # creates 'construction_v2'
    python scripts/create_new_database.py --name construction_v2   # explicit name
    python scripts/create_new_database.py --name construction_v2 --switch
                                                                   # also updates .env
Examples:
    # 1. Create a fresh DB
    python scripts/create_new_database.py --name construction_v2

    # 2. Create + update .env so the pipeline uses it going forward
    python scripts/create_new_database.py --name construction_v2 --switch

    # 3. Create + seed + set depth (via pipeline.py)
    python scripts/create_new_database.py --name construction_v2 --switch
    python pipeline.py --seed-only --query-file queries/construction_queries.txt
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg2


ADMIN_DSN = "postgresql://postgres:postgres@localhost:5432/postgres"


def database_exists(name: str) -> bool:
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists


def create_database(name: str) -> None:
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f'CREATE DATABASE "{name}"')
    cur.close()
    conn.close()
    print(f"✅ Created database: {name}")


def create_results_table(dsn: str) -> None:
    """Create the base results table that the Google Maps scraper expects."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            data JSONB NOT NULL DEFAULT '{}'
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gmaps_jobs (
            id UUID PRIMARY KEY,
            priority SMALLINT DEFAULT 1,
            payload_type TEXT DEFAULT '',
            payload BYTEA,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            status TEXT DEFAULT 'new'
        )
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Created tables: results, gmaps_jobs")


def create_pipeline_schema(dsn: str) -> None:
    """Create link_candidates, documents, document_chunks tables."""
    from broker.storage.postgres import ensure_pipeline_schema
    # Temporarily patch the DSN
    import os
    old = os.environ.get("BROKER_CONSTRUCTION_DSN", "")
    os.environ["BROKER_CONSTRUCTION_DSN"] = dsn
    try:
        result = ensure_pipeline_schema(dsn=dsn)
        for t in result["tables"]:
            status = "already existed" if t["existed_before"] else "created"
            print(f"  ✅ {t['name']}: {status}")
    finally:
        if old:
            os.environ["BROKER_CONSTRUCTION_DSN"] = old
        else:
            os.environ.pop("BROKER_CONSTRUCTION_DSN", None)


def update_env_file(db_name: str) -> None:
    """Update .env to point at the new database."""
    env_path = ROOT / ".env"
    new_dsn = f"postgresql://postgres:postgres@localhost:5432/{db_name}"
    new_docker_dsn = f"postgres://postgres:postgres@host.docker.internal:5432/{db_name}"

    if env_path.exists():
        lines = env_path.read_text(encoding="utf-8").splitlines()
    else:
        lines = []

    # Update or append each key
    keys_to_set = {
        "BROKER_CONSTRUCTION_DSN": new_dsn,
        "BROKER_DOCKER_CONSTRUCTION_DSN": new_docker_dsn,
    }
    updated_keys = set()
    new_lines = []
    for line in lines:
        stripped = line.strip()
        replaced = False
        for key, value in keys_to_set.items():
            if stripped.startswith(f"{key}=") or stripped.startswith(f"# {key}="):
                new_lines.append(f"{key}={value}")
                updated_keys.add(key)
                replaced = True
                break
        if not replaced:
            new_lines.append(line)

    for key, value in keys_to_set.items():
        if key not in updated_keys:
            new_lines.append(f"{key}={value}")

    env_path.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
    print(f"✅ Updated .env → {new_dsn}")


def main():
    parser = argparse.ArgumentParser(description="Create a new pipeline database")
    parser.add_argument("--name", type=str, default="construction_v2",
                        help="Name for the new database (default: construction_v2)")
    parser.add_argument("--switch", action="store_true",
                        help="Update .env to use the new database")
    parser.add_argument("--force", action="store_true",
                        help="Drop existing database with same name first")
    args = parser.parse_args()

    db_name = args.name
    dsn = f"postgresql://postgres:postgres@localhost:5432/{db_name}"

    print(f"\n{'='*60}")
    print(f"  Creating new pipeline database: {db_name}")
    print(f"{'='*60}\n")

    if database_exists(db_name):
        if args.force:
            print(f"⚠️  Dropping existing database: {db_name}")
            conn = psycopg2.connect(ADMIN_DSN)
            conn.autocommit = True
            cur = conn.cursor()
            # Kill existing connections
            cur.execute(f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
            """, (db_name,))
            cur.execute(f'DROP DATABASE "{db_name}"')
            cur.close()
            conn.close()
        else:
            print(f"❌ Database '{db_name}' already exists. Use --force to drop it first.")
            sys.exit(1)

    # Step 1: Create database
    create_database(db_name)

    # Step 2: Create results + gmaps_jobs tables (scraper needs these)
    create_results_table(dsn)

    # Step 3: Create pipeline tables (link_candidates, documents, document_chunks)
    create_pipeline_schema(dsn)

    # Step 4: Optionally update .env
    if args.switch:
        update_env_file(db_name)

    # Summary
    print(f"\n{'='*60}")
    print(f"  ✅ Database '{db_name}' is ready!")
    print(f"{'='*60}")
    print(f"\n  DSN: {dsn}")
    print(f"  Tables: results, gmaps_jobs, link_candidates, documents, document_chunks")
    if args.switch:
        print(f"  .env updated — pipeline will use this DB on next start")
    else:
        print(f"\n  To switch the pipeline to this DB, run:")
        print(f"    python scripts/create_new_database.py --name {db_name} --switch")
        print(f"  Or manually set in .env:")
        print(f"    BROKER_CONSTRUCTION_DSN={dsn}")
    print(f"\n  To seed scrape queries:")
    print(f"    python pipeline.py --seed-only --query-file queries/construction_queries.txt")
    print()


if __name__ == "__main__":
    main()
