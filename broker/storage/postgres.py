from __future__ import annotations

import argparse
import json
from contextlib import contextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any, Iterator

import psycopg2

from broker.config import get_local_construction_dsn
from broker.models import (
    CompanyEvaluationRecord,
    DocumentChunkRecord,
    DocumentRecord,
    LinkCandidateRecord,
)


RESULT_ID_SQL_TYPES = {
    "integer": "integer",
    "bigint": "bigint",
    "uuid": "uuid",
}


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


@contextmanager
def connect_postgres(dsn: str | None = None, autocommit: bool = False) -> Iterator[Any]:
    conn = psycopg2.connect(dsn or get_local_construction_dsn())
    conn.autocommit = autocommit
    try:
        yield conn
    finally:
        conn.close()


def get_results_id_sql_type(cur) -> str:
    cur.execute(
        """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'results'
          AND column_name = 'id'
        """
    )
    row = cur.fetchone()
    if not row:
        raise RuntimeError("results.id was not found. Create the canonical results table first.")

    data_type = str(row[0]).strip().lower()
    sql_type = RESULT_ID_SQL_TYPES.get(data_type)
    if not sql_type:
        raise RuntimeError(
            f"Unsupported results.id type '{data_type}'. "
            f"Supported types: {', '.join(sorted(RESULT_ID_SQL_TYPES))}."
        )
    return sql_type


def build_schema_statements(results_id_sql_type: str) -> list[str]:
    return [
        f"""
        CREATE TABLE IF NOT EXISTS link_candidates (
            id BIGSERIAL PRIMARY KEY,
            result_id {results_id_sql_type} NOT NULL REFERENCES results(id) ON DELETE CASCADE,
            source_type TEXT NOT NULL,
            query_text TEXT NOT NULL DEFAULT '',
            url_discovered TEXT NOT NULL DEFAULT '',
            title_discovered TEXT NOT NULL DEFAULT '',
            discovery_status TEXT NOT NULL DEFAULT '',
            discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_link_candidates_result_source_url
                UNIQUE (result_id, source_type, url_discovered),
            CONSTRAINT ck_link_candidates_source_type
                CHECK (source_type IN ('news', 'website', 'web_research'))
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_link_candidates_result_id
        ON link_candidates(result_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_link_candidates_source_type
        ON link_candidates(source_type)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_link_candidates_discovery_status
        ON link_candidates(discovery_status)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_link_candidates_discovered_at
        ON link_candidates(discovered_at DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS documents (
            id BIGSERIAL PRIMARY KEY,
            link_candidate_id BIGINT NOT NULL REFERENCES link_candidates(id) ON DELETE CASCADE,
            url_fetched TEXT NOT NULL DEFAULT '',
            page_title TEXT NOT NULL DEFAULT '',
            fetch_status TEXT NOT NULL DEFAULT '',
            http_status INTEGER,
            text_hash TEXT NOT NULL DEFAULT '',
            raw_text TEXT NOT NULL DEFAULT '',
            fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_documents_link_candidate_id
        ON documents(link_candidate_id)
        """,
        """
        CREATE UNIQUE INDEX IF NOT EXISTS uq_documents_link_candidate
        ON documents(link_candidate_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_documents_fetch_status
        ON documents(fetch_status)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_documents_fetched_at
        ON documents(fetched_at DESC)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_documents_text_hash
        ON documents(text_hash)
        """,
        """
        CREATE TABLE IF NOT EXISTS document_chunks (
            id BIGSERIAL PRIMARY KEY,
            document_id BIGINT NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
            chunk_index INTEGER NOT NULL,
            chunk_text TEXT NOT NULL DEFAULT '',
            chunk_hash TEXT NOT NULL DEFAULT '',
            embedding_model TEXT NOT NULL DEFAULT '',
            embedded_at TIMESTAMPTZ,
            CONSTRAINT uq_document_chunks_document_index
                UNIQUE (document_id, chunk_index)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_document_chunks_document_id
        ON document_chunks(document_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_document_chunks_chunk_hash
        ON document_chunks(chunk_hash)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_document_chunks_embedding_model
        ON document_chunks(embedding_model)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_document_chunks_embedded_at
        ON document_chunks(embedded_at DESC)
        """,
        f"""
        CREATE TABLE IF NOT EXISTS company_evaluations (
            id BIGSERIAL PRIMARY KEY,
            result_id {results_id_sql_type} NOT NULL REFERENCES results(id) ON DELETE CASCADE,
            company TEXT NOT NULL DEFAULT '',
            estimated_revenue TEXT NOT NULL DEFAULT '',
            revenue_confidence TEXT NOT NULL DEFAULT '',
            estimated_headcount TEXT NOT NULL DEFAULT '',
            headcount_confidence TEXT NOT NULL DEFAULT '',
            evidence_summary TEXT NOT NULL DEFAULT '',
            chunks_used INTEGER NOT NULL DEFAULT 0,
            evaluated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT uq_company_evaluations_result
                UNIQUE (result_id)
        )
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_company_evaluations_result_id
        ON company_evaluations(result_id)
        """,
        """
        CREATE INDEX IF NOT EXISTS idx_company_evaluations_evaluated_at
        ON company_evaluations(evaluated_at DESC)
        """,
    ]


def table_exists(cur, table_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (f"public.{table_name}",))
    return cur.fetchone()[0] is not None


def ensure_pipeline_schema(dsn: str | None = None) -> dict[str, Any]:
    with connect_postgres(dsn=dsn, autocommit=False) as conn:
        cur = conn.cursor()
        results_id_sql_type = get_results_id_sql_type(cur)
        existed_before = {
            name: table_exists(cur, name)
            for name in ("link_candidates", "documents", "document_chunks", "company_evaluations")
        }

        for statement in build_schema_statements(results_id_sql_type):
            cur.execute(statement)

        conn.commit()

        existed_after = {
            name: table_exists(cur, name)
            for name in ("link_candidates", "documents", "document_chunks", "company_evaluations")
        }
        cur.close()

    return {
        "dsn": dsn or get_local_construction_dsn(),
        "results_id_sql_type": results_id_sql_type,
        "tables": [
            {
                "name": name,
                "existed_before": existed_before[name],
                "exists_now": existed_after[name],
            }
            for name in ("link_candidates", "documents", "document_chunks", "company_evaluations")
        ],
    }


def get_pipeline_counts(dsn: str | None = None) -> dict[str, int]:
    table_names = ("results", "link_candidates", "documents", "document_chunks")
    counts: dict[str, int] = {}
    with connect_postgres(dsn=dsn, autocommit=True) as conn:
        cur = conn.cursor()
        for table_name in table_names:
            if not table_exists(cur, table_name):
                counts[table_name] = 0
                continue
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            counts[table_name] = int(cur.fetchone()[0] or 0)
        cur.close()
    return counts


def _group_count(cur, query: str, params: tuple[Any, ...] = ()) -> dict[str, int]:
    cur.execute(query, params)
    return {str(key): int(value or 0) for key, value in cur.fetchall()}


def get_pipeline_monitoring_snapshot(dsn: str | None = None) -> dict[str, Any]:
    with connect_postgres(dsn=dsn, autocommit=True) as conn:
        cur = conn.cursor()
        table_counts = get_pipeline_counts(dsn=dsn)

        link_source_counts = {}
        link_status_counts = {}
        document_status_counts = {}
        chunk_model_counts = {}
        chunk_source_counts = {}
        gmaps_job_counts = {}

        if table_exists(cur, "link_candidates"):
            link_source_counts = _group_count(
                cur,
                """
                SELECT source_type, COUNT(*)
                FROM link_candidates
                GROUP BY source_type
                ORDER BY source_type
                """,
            )
            link_status_counts = _group_count(
                cur,
                """
                SELECT source_type || ':' || discovery_status, COUNT(*)
                FROM link_candidates
                GROUP BY source_type, discovery_status
                ORDER BY source_type, discovery_status
                """,
            )

        if table_exists(cur, "documents"):
            document_status_counts = _group_count(
                cur,
                """
                SELECT fetch_status, COUNT(*)
                FROM documents
                GROUP BY fetch_status
                ORDER BY fetch_status
                """,
            )

        if table_exists(cur, "document_chunks"):
            chunk_model_counts = _group_count(
                cur,
                """
                SELECT
                    CASE
                        WHEN COALESCE(embedding_model, '') = '' THEN '<missing>'
                        ELSE embedding_model
                    END,
                    COUNT(*)
                FROM document_chunks
                GROUP BY 1
                ORDER BY 1
                """,
            )
            chunk_source_counts = _group_count(
                cur,
                """
                SELECT lc.source_type, COUNT(*)
                FROM document_chunks dc
                JOIN documents d
                  ON d.id = dc.document_id
                JOIN link_candidates lc
                  ON lc.id = d.link_candidate_id
                GROUP BY lc.source_type
                ORDER BY lc.source_type
                """,
            )

        if table_exists(cur, "gmaps_jobs"):
            gmaps_job_counts = _group_count(
                cur,
                """
                SELECT status, COUNT(*)
                FROM gmaps_jobs
                GROUP BY status
                ORDER BY status
                """,
            )

        pending = {
            "results_missing_link_discovery": count_results_pending_link_discovery(conn)
            if table_exists(cur, "link_candidates")
            else 0,
            "link_candidates_pending_fetch": count_link_candidates_pending_fetch(conn, retry_failed=False)
            if table_exists(cur, "documents")
            else 0,
            "documents_pending_chunk_embed": count_documents_pending_chunking(
                conn,
                embedding_model="",
                force_reembed=False,
            )
            if table_exists(cur, "document_chunks")
            else 0,
        }

        cur.close()

    return {
        "table_counts": table_counts,
        "pending": pending,
        "gmaps_jobs": gmaps_job_counts,
        "link_candidates": {
            "by_source_type": link_source_counts,
            "by_status": link_status_counts,
        },
        "documents": {
            "by_fetch_status": document_status_counts,
        },
        "document_chunks": {
            "by_embedding_model": chunk_model_counts,
            "by_source_type": chunk_source_counts,
        },
    }


def select_results_pending_link_discovery(conn, batch_size: int) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            r.id,
            r.data->>'title' AS company
        FROM results r
        WHERE COALESCE(r.data->>'title', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM link_candidates lc
              WHERE lc.result_id = r.id AND lc.source_type = 'news'
          )
        ORDER BY r.id
        LIMIT %s
        FOR UPDATE OF r SKIP LOCKED
        """,
        (batch_size,),
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "result_id": int(row[0]),
            "company": str(row[1] or "").strip(),
        }
        for row in rows
    ]


def count_results_pending_link_discovery(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM results r
        WHERE COALESCE(r.data->>'title', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM link_candidates lc
              WHERE lc.result_id = r.id AND lc.source_type = 'news'
          )
        """
    )
    remaining = int(cur.fetchone()[0] or 0)
    cur.close()
    return remaining


def select_results_pending_website_discovery(conn, batch_size: int) -> list[dict[str, Any]]:
    """Return results that have a web_site URL but no 'website' link_candidate yet."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            r.id,
            r.data->>'title' AS company,
            r.data->>'web_site' AS web_site
        FROM results r
        WHERE COALESCE(r.data->>'web_site', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM link_candidates lc
              WHERE lc.result_id = r.id AND lc.source_type = 'website'
          )
        ORDER BY r.id
        LIMIT %s
        FOR UPDATE OF r SKIP LOCKED
        """,
        (batch_size,),
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "result_id": int(row[0]),
            "company": str(row[1] or "").strip(),
            "web_site": str(row[2] or "").strip(),
        }
        for row in rows
    ]


def count_results_pending_website_discovery(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM results r
        WHERE COALESCE(r.data->>'web_site', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM link_candidates lc
              WHERE lc.result_id = r.id AND lc.source_type = 'website'
          )
        """
    )
    remaining = int(cur.fetchone()[0] or 0)
    cur.close()
    return remaining


def upsert_link_candidate(conn, record: LinkCandidateRecord) -> int:
    payload = asdict(record)
    payload["discovered_at"] = payload["discovered_at"] or now_utc()

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO link_candidates (
            result_id,
            source_type,
            query_text,
            url_discovered,
            title_discovered,
            discovery_status,
            discovered_at
        )
        VALUES (%(result_id)s, %(source_type)s, %(query_text)s, %(url_discovered)s,
                %(title_discovered)s, %(discovery_status)s, %(discovered_at)s)
        ON CONFLICT (result_id, source_type, url_discovered)
        DO UPDATE SET
            query_text = EXCLUDED.query_text,
            title_discovered = EXCLUDED.title_discovered,
            discovery_status = EXCLUDED.discovery_status,
            discovered_at = EXCLUDED.discovered_at
        RETURNING id
        """,
        payload,
    )
    row_id = int(cur.fetchone()[0])
    cur.close()
    return row_id


def insert_document(conn, record: DocumentRecord) -> int:
    payload = asdict(record)
    payload["fetched_at"] = payload["fetched_at"] or now_utc()

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO documents (
            link_candidate_id,
            url_fetched,
            page_title,
            fetch_status,
            http_status,
            text_hash,
            raw_text,
            fetched_at
        )
        VALUES (%(link_candidate_id)s, %(url_fetched)s, %(page_title)s, %(fetch_status)s,
                %(http_status)s, %(text_hash)s, %(raw_text)s, %(fetched_at)s)
        ON CONFLICT (link_candidate_id)
        DO UPDATE SET
            url_fetched = EXCLUDED.url_fetched,
            page_title = EXCLUDED.page_title,
            fetch_status = EXCLUDED.fetch_status,
            http_status = EXCLUDED.http_status,
            text_hash = EXCLUDED.text_hash,
            raw_text = EXCLUDED.raw_text,
            fetched_at = EXCLUDED.fetched_at
        RETURNING id
        """,
        payload,
    )
    row_id = int(cur.fetchone()[0])
    cur.close()
    return row_id


def insert_document_chunk(conn, record: DocumentChunkRecord) -> int:
    payload = asdict(record)

    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO document_chunks (
            document_id,
            chunk_index,
            chunk_text,
            chunk_hash,
            embedding_model,
            embedded_at
        )
        VALUES (%(document_id)s, %(chunk_index)s, %(chunk_text)s, %(chunk_hash)s,
                %(embedding_model)s, %(embedded_at)s)
        ON CONFLICT (document_id, chunk_index)
        DO UPDATE SET
            chunk_text = EXCLUDED.chunk_text,
            chunk_hash = EXCLUDED.chunk_hash,
            embedding_model = EXCLUDED.embedding_model,
            embedded_at = COALESCE(EXCLUDED.embedded_at, document_chunks.embedded_at)
        RETURNING id
        """,
        payload,
    )
    row_id = int(cur.fetchone()[0])
    cur.close()
    return row_id


def mark_document_chunks_embedded(
    conn,
    chunk_ids: list[int],
    embedding_model: str,
    embedded_at: datetime | None = None,
) -> int:
    if not chunk_ids:
        return 0
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE document_chunks
        SET embedding_model = %s,
            embedded_at = %s
        WHERE id = ANY(%s)
        """,
        (embedding_model, embedded_at or now_utc(), chunk_ids),
    )
    count = int(cur.rowcount or 0)
    cur.close()
    return count


def update_document_fetch_status(conn, document_id: int, fetch_status: str) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE documents
        SET fetch_status = %s
        WHERE id = %s
        """,
        (fetch_status, document_id),
    )
    count = int(cur.rowcount or 0)
    cur.close()
    return count


def select_link_candidates_pending_fetch(
    conn,
    batch_size: int,
    retry_failed: bool = False,
) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            lc.id,
            lc.result_id,
            COALESCE(r.data->>'title', '') AS company,
            lc.source_type,
            lc.query_text,
            lc.url_discovered,
            lc.title_discovered,
            lc.discovery_status,
            d.id AS document_id,
            d.fetch_status
        FROM link_candidates lc
        JOIN results r
          ON r.id = lc.result_id
        LEFT JOIN documents d
          ON d.link_candidate_id = lc.id
        WHERE lc.discovery_status = 'ok'
          AND COALESCE(lc.url_discovered, '') <> ''
          AND (
              d.id IS NULL
              OR (%s AND COALESCE(d.fetch_status, '') <> 'ok')
          )
        ORDER BY lc.id
        LIMIT %s
        FOR UPDATE OF lc SKIP LOCKED
        """,
        (retry_failed, batch_size),
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "link_candidate_id": int(row[0]),
            "result_id": int(row[1]),
            "company": str(row[2] or "").strip(),
            "source_type": str(row[3] or "").strip(),
            "query_text": str(row[4] or "").strip(),
            "url_discovered": str(row[5] or "").strip(),
            "title_discovered": str(row[6] or "").strip(),
            "discovery_status": str(row[7] or "").strip(),
            "document_id": int(row[8]) if row[8] is not None else None,
            "fetch_status": str(row[9] or "").strip(),
        }
        for row in rows
    ]


def count_link_candidates_pending_fetch(conn, retry_failed: bool = False) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM link_candidates lc
        LEFT JOIN documents d
          ON d.link_candidate_id = lc.id
        WHERE lc.discovery_status = 'ok'
          AND COALESCE(lc.url_discovered, '') <> ''
          AND (
              d.id IS NULL
              OR (%s AND COALESCE(d.fetch_status, '') <> 'ok')
          )
        """,
        (retry_failed,),
    )
    count = int(cur.fetchone()[0] or 0)
    cur.close()
    return count


def select_documents_pending_chunking(
    conn,
    batch_size: int,
    embedding_model: str,
    force_reembed: bool = False,
) -> list[dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            d.id,
            d.link_candidate_id,
            lc.result_id,
            COALESCE(r.data->>'title', '') AS company,
            lc.source_type,
            lc.url_discovered,
            lc.title_discovered,
            d.url_fetched,
            d.page_title,
            d.raw_text,
            EXISTS (
                SELECT 1
                FROM document_chunks dc
                WHERE dc.document_id = d.id
            ) AS has_chunks,
            EXISTS (
                SELECT 1
                FROM document_chunks dc
                WHERE dc.document_id = d.id
                  AND (
                      dc.embedded_at IS NULL
                      OR dc.embedding_model <> %s
                  )
            ) AS has_pending_chunks
        FROM documents d
        JOIN link_candidates lc
          ON lc.id = d.link_candidate_id
        JOIN results r
          ON r.id = lc.result_id
        WHERE d.fetch_status = 'ok'
          AND (
              NOT EXISTS (
                  SELECT 1 FROM document_chunks dc WHERE dc.document_id = d.id
              )
              OR EXISTS (
                  SELECT 1
                  FROM document_chunks dc
                  WHERE dc.document_id = d.id
                    AND (
                        dc.embedded_at IS NULL
                        OR (%s AND dc.embedding_model <> %s)
                    )
              )
          )
        ORDER BY d.id
        LIMIT %s
        FOR UPDATE OF d SKIP LOCKED
        """,
        (embedding_model, force_reembed, embedding_model, batch_size),
    )
    rows = cur.fetchall()
    cur.close()
    return [
        {
            "document_id": int(row[0]),
            "link_candidate_id": int(row[1]),
            "result_id": int(row[2]),
            "company": str(row[3] or "").strip(),
            "source_type": str(row[4] or "").strip(),
            "url_discovered": str(row[5] or "").strip(),
            "title_discovered": str(row[6] or "").strip(),
            "url_fetched": str(row[7] or "").strip(),
            "page_title": str(row[8] or "").strip(),
            "raw_text": str(row[9] or ""),
            "has_chunks": bool(row[10]),
            "has_pending_chunks": bool(row[11]),
        }
        for row in rows
    ]


def count_documents_pending_chunking(
    conn,
    embedding_model: str,
    force_reembed: bool = False,
) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(*)
        FROM documents d
        WHERE d.fetch_status = 'ok'
          AND (
              NOT EXISTS (
                  SELECT 1 FROM document_chunks dc WHERE dc.document_id = d.id
              )
              OR EXISTS (
                  SELECT 1
                  FROM document_chunks dc
                  WHERE dc.document_id = d.id
                    AND (
                        dc.embedded_at IS NULL
                        OR (%s AND dc.embedding_model <> %s)
                    )
              )
          )
        """,
        (force_reembed, embedding_model),
    )
    count = int(cur.fetchone()[0] or 0)
    cur.close()
    return count


# ---------------------------------------------------------------------------
# Company Evaluation helpers
# ---------------------------------------------------------------------------

def select_results_pending_evaluation(conn, batch_size: int) -> list[dict[str, Any]]:
    """Return results that have embedded chunks but no evaluation yet."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT r.id, COALESCE(r.data->>'title', '') AS company
        FROM results r
        JOIN link_candidates lc ON lc.result_id = r.id
        JOIN documents d ON d.link_candidate_id = lc.id
        JOIN document_chunks dc ON dc.document_id = d.id
        WHERE dc.embedded_at IS NOT NULL
          AND COALESCE(r.data->>'title', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM company_evaluations ce WHERE ce.result_id = r.id
          )
        ORDER BY r.id
        LIMIT %s
        """,
        (batch_size,),
    )
    rows = cur.fetchall()
    cur.close()
    return [{"result_id": int(row[0]), "company": str(row[1]).strip()} for row in rows]


def count_results_pending_evaluation(conn) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COUNT(DISTINCT r.id)
        FROM results r
        JOIN link_candidates lc ON lc.result_id = r.id
        JOIN documents d ON d.link_candidate_id = lc.id
        JOIN document_chunks dc ON dc.document_id = d.id
        WHERE dc.embedded_at IS NOT NULL
          AND COALESCE(r.data->>'title', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM company_evaluations ce WHERE ce.result_id = r.id
          )
        """
    )
    count = int(cur.fetchone()[0] or 0)
    cur.close()
    return count


def upsert_company_evaluation(conn, record: CompanyEvaluationRecord) -> int:
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO company_evaluations (
            result_id, company,
            estimated_revenue, revenue_confidence,
            estimated_headcount, headcount_confidence,
            evidence_summary, chunks_used, evaluated_at
        ) VALUES (
            %(result_id)s, %(company)s,
            %(estimated_revenue)s, %(revenue_confidence)s,
            %(estimated_headcount)s, %(headcount_confidence)s,
            %(evidence_summary)s, %(chunks_used)s, %(evaluated_at)s
        )
        ON CONFLICT (result_id)
        DO UPDATE SET
            company = EXCLUDED.company,
            estimated_revenue = EXCLUDED.estimated_revenue,
            revenue_confidence = EXCLUDED.revenue_confidence,
            estimated_headcount = EXCLUDED.estimated_headcount,
            headcount_confidence = EXCLUDED.headcount_confidence,
            evidence_summary = EXCLUDED.evidence_summary,
            chunks_used = EXCLUDED.chunks_used,
            evaluated_at = EXCLUDED.evaluated_at
        RETURNING id
        """,
        {
            "result_id": record.result_id,
            "company": record.company,
            "estimated_revenue": record.estimated_revenue,
            "revenue_confidence": record.revenue_confidence,
            "estimated_headcount": record.estimated_headcount,
            "headcount_confidence": record.headcount_confidence,
            "evidence_summary": record.evidence_summary,
            "chunks_used": record.chunks_used,
            "evaluated_at": record.evaluated_at or now_utc(),
        },
    )
    row_id = int(cur.fetchone()[0])
    cur.close()
    return row_id


def print_json(data: Any) -> None:
    print(json.dumps(data, indent=2, sort_keys=False, default=str))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Manage Broker Postgres pipeline schema.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ensure_parser = subparsers.add_parser("ensure-schema", help="Create/update pipeline tables.")
    ensure_parser.add_argument("--dsn", default="", help="Optional PostgreSQL DSN override.")
    ensure_parser.add_argument("--json", action="store_true", help="Print JSON output.")

    counts_parser = subparsers.add_parser("counts", help="Print pipeline table row counts.")
    counts_parser.add_argument("--dsn", default="", help="Optional PostgreSQL DSN override.")
    counts_parser.add_argument("--json", action="store_true", help="Print JSON output.")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    dsn = args.dsn or None

    if args.command == "ensure-schema":
        result = ensure_pipeline_schema(dsn=dsn)
    elif args.command == "counts":
        result = get_pipeline_counts(dsn=dsn)
    else:
        parser.error(f"Unsupported command: {args.command}")
        return

    if args.json:
        print_json(result)
        return

    if args.command == "ensure-schema":
        print(f"results.id type: {result['results_id_sql_type']}")
        for table in result["tables"]:
            state = "already existed" if table["existed_before"] else "created"
            print(f"{table['name']}: {state}")
        return

    for table_name, count in result.items():
        print(f"{table_name}: {count}")


if __name__ == "__main__":
    main()
