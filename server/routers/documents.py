"""Documents router — fetch/chunk stats, browse documents."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from server.deps import db_param, get_conn

router = APIRouter()


@router.get("")
def list_documents(
    db: str = Depends(db_param),
    status: str = "",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    """Paginated list of documents with company context."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()

    clauses = []
    params: list = []
    if status:
        clauses.append("d.fetch_status = %s")
        params.append(status)
    where = " AND ".join(clauses) if clauses else "TRUE"

    cur.execute(f"SELECT count(*) FROM documents d WHERE {where}", params)
    total = cur.fetchone()[0]

    offset = (page - 1) * page_size
    cur.execute(f"""
        SELECT
            d.id,
            d.link_candidate_id,
            lc.result_id,
            COALESCE(r.data->>'title', '') AS company,
            lc.source_type,
            d.url_fetched,
            d.page_title,
            d.fetch_status,
            d.http_status,
            (SELECT count(*) FROM document_chunks dc WHERE dc.document_id = d.id) AS chunks,
            d.fetched_at
        FROM documents d
        JOIN link_candidates lc ON lc.id = d.link_candidate_id
        JOIN results r ON r.id = lc.result_id
        WHERE {where}
        ORDER BY d.id DESC
        LIMIT %s OFFSET %s
    """, params + [page_size, offset])

    cols = [desc[0] for desc in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]

    cur.close()
    conn.close()
    return {"total": total, "page": page, "page_size": page_size, "items": rows}
