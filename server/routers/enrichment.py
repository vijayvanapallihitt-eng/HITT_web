"""Enrichment router — enrichment stats and pending counts."""
from __future__ import annotations

from fastapi import APIRouter, Depends
from server.deps import db_param, get_conn, dsn_for

router = APIRouter()


@router.get("/stats")
def enrichment_stats(db: str = Depends(db_param)):
    """Return enrichment pipeline statistics for the given database."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()

    out: dict = {}

    # Total results
    cur.execute("SELECT count(*) FROM results")
    out["total_results"] = cur.fetchone()[0]

    # Enriched (have at least one link_candidate)
    cur.execute("SELECT count(DISTINCT result_id) FROM link_candidates")
    out["results_enriched"] = cur.fetchone()[0]
    out["results_pending"] = out["total_results"] - out["results_enriched"]

    # By source type
    cur.execute("SELECT source_type, count(*) FROM link_candidates GROUP BY 1 ORDER BY 1")
    out["link_candidates_by_type"] = {r[0]: r[1] for r in cur.fetchall()}

    # Pending news
    cur.execute("""
        SELECT count(*) FROM results r
        WHERE COALESCE(r.data->>'title', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM link_candidates lc WHERE lc.result_id = r.id AND lc.source_type = 'news'
          )
    """)
    out["pending_news"] = cur.fetchone()[0]

    # Pending website
    cur.execute("""
        SELECT count(*) FROM results r
        WHERE COALESCE(r.data->>'web_site', '') <> ''
          AND NOT EXISTS (
              SELECT 1 FROM link_candidates lc WHERE lc.result_id = r.id AND lc.source_type = 'website'
          )
    """)
    out["pending_website"] = cur.fetchone()[0]

    # Documents
    cur.execute("SELECT fetch_status, count(*) FROM documents GROUP BY 1 ORDER BY 1")
    out["documents_by_status"] = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("SELECT count(*) FROM documents")
    out["total_documents"] = cur.fetchone()[0]

    cur.execute("SELECT count(*) FROM document_chunks")
    out["total_chunks"] = cur.fetchone()[0]

    cur.close()
    conn.close()
    return out
