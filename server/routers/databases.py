"""Databases router — list, create, delete pipeline databases."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import psycopg2

from server.deps import dsn_for, get_conn, ADMIN_DSN

router = APIRouter()

SYSTEM_DBS = {"postgres", "template0", "template1"}


class CreateDBRequest(BaseModel):
    name: str


@router.get("")
def list_databases():
    """Return all non-system databases with pipeline table counts."""
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
    all_dbs = [r[0] for r in cur.fetchall() if r[0] not in SYSTEM_DBS]
    cur.close()
    conn.close()

    result = []
    for db_name in all_dbs:
        info = {"name": db_name, "results": 0, "link_candidates": 0, "documents": 0, "document_chunks": 0}
        try:
            c = get_conn(db_name, autocommit=True)
            cur = c.cursor()
            for table in ("results", "link_candidates", "documents", "document_chunks"):
                try:
                    cur.execute(f"SELECT count(*) FROM {table}")
                    info[table] = cur.fetchone()[0]
                except Exception:
                    c.rollback()
            cur.close()
            c.close()
        except Exception:
            pass
        result.append(info)
    return result


@router.post("")
def create_database(req: CreateDBRequest):
    """Create a new pipeline database with all required tables."""
    name = req.name.strip().lower()
    if not name or name in SYSTEM_DBS:
        raise HTTPException(400, f"Invalid database name: {name}")

    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    # Check if exists
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
    if cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(409, f"Database '{name}' already exists")

    cur.execute(f'CREATE DATABASE "{name}"')
    cur.close()
    conn.close()

    # Create base tables
    c = get_conn(name)
    c.autocommit = False
    cur = c.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS results (
            id SERIAL PRIMARY KEY,
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
    c.commit()
    cur.close()
    c.close()

    # Create pipeline schema
    from broker.storage.postgres import ensure_pipeline_schema
    ensure_pipeline_schema(dsn=dsn_for(name))

    return {"status": "created", "name": name}


@router.delete("/{name}")
def delete_database(name: str):
    """Drop a pipeline database."""
    if name in SYSTEM_DBS:
        raise HTTPException(400, f"Cannot delete system database: {name}")

    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,))
    if not cur.fetchone():
        raise HTTPException(404, f"Database '{name}' not found")

    # Kill connections
    cur.execute("""
        SELECT pg_terminate_backend(pid)
        FROM pg_stat_activity
        WHERE datname = %s AND pid <> pg_backend_pid()
    """, (name,))
    cur.execute(f'DROP DATABASE "{name}"')
    cur.close()
    conn.close()
    return {"status": "deleted", "name": name}


@router.get("/{name}/stats")
def database_stats(name: str):
    """Detailed stats for a single database."""
    from broker.storage.postgres import get_pipeline_monitoring_snapshot
    try:
        snapshot = get_pipeline_monitoring_snapshot(dsn=dsn_for(name))
    except Exception as e:
        raise HTTPException(500, str(e))
    return {"name": name, **snapshot}
