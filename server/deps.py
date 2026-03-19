"""
Shared helpers for the API layer.

The key abstraction: `dsn_for(db_name)` builds a Postgres DSN from a
database name so the frontend just passes `?db=construction_test` and
every query runs against that database.
"""
from __future__ import annotations

import os

import psycopg2
from fastapi import HTTPException, Query

POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
DOCKER_HOST = os.getenv("DOCKER_HOST_ALIAS", "host.docker.internal")

ADMIN_DSN = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/postgres"


def dsn_for(db: str) -> str:
    """Build a local Postgres DSN for the given database name."""
    return f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{db}"


def docker_dsn_for(db: str) -> str:
    """Build a Docker-reachable Postgres DSN for the given database name."""
    return f"postgres://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{DOCKER_HOST}:{POSTGRES_PORT}/{db}"


def _db_exists(db: str) -> bool:
    """Check whether a database exists in the Postgres cluster."""
    conn = psycopg2.connect(ADMIN_DSN)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db,))
    exists = cur.fetchone() is not None
    cur.close()
    conn.close()
    return exists


def get_conn(db: str, autocommit: bool = False):
    """Return a raw psycopg2 connection for `db`.  Raises 404 if the DB does not exist."""
    if not _db_exists(db):
        raise HTTPException(status_code=404, detail=f"Database '{db}' does not exist")
    conn = psycopg2.connect(dsn_for(db))
    conn.autocommit = autocommit
    return conn


def db_param(db: str = Query(..., description="Database name, e.g. 'construction_test'")) -> str:
    """FastAPI dependency — extracts the required `db` query-parameter."""
    return db
