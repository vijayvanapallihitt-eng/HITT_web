"""Companies router — browse, search, detail for results in a database."""
from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from typing import Any

from server.deps import db_param, get_conn

router = APIRouter()


# Allowed sort columns → SQL expressions
_SORT_MAP = {
    "id": "r.id",
    "company": "COALESCE(r.data->>'title', '')",
    "category": "COALESCE(r.data->>'category', '')",
    "city": "COALESCE(r.data->'complete_address'->>'city', '')",
    "state": "COALESCE(r.data->'complete_address'->>'state', '')",
    "rating": "COALESCE((r.data->>'review_rating')::numeric, 0)",
    "reviews": "COALESCE((r.data->>'review_count')::int, 0)",
    "revenue": "COALESCE(ce.estimated_revenue, '')",
    "employees": "COALESCE(ce.estimated_headcount, '')",
    "news": "news_count",
    "links": "links",
    "docs": "docs",
}


@router.get("")
def list_companies(
    db: str = Depends(db_param),
    search: str = "",
    category: str = "",
    state: str = "",
    city: str = "",
    min_rating: float = 0,
    has_website: str = "",        # "yes" | "no" | ""
    enriched: str = "",           # "yes" | "no" | ""
    sort_by: str = "id",
    sort_dir: str = "asc",        # "asc" | "desc"
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
):
    """Paginated, filterable, sortable company list."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()

    clauses: list[str] = []
    params: list[Any] = []

    if search.strip():
        clauses.append("(r.data->>'title' ILIKE %s OR r.data->>'address' ILIKE %s)")
        params += [f"%{search.strip()}%", f"%{search.strip()}%"]
    if category:
        clauses.append("r.data->>'category' = %s")
        params.append(category)
    if state:
        clauses.append("r.data->'complete_address'->>'state' = %s")
        params.append(state)
    if city:
        clauses.append("r.data->'complete_address'->>'city' = %s")
        params.append(city)
    if min_rating > 0:
        clauses.append("COALESCE((r.data->>'review_rating')::numeric, 0) >= %s")
        params.append(min_rating)
    if has_website == "yes":
        clauses.append("COALESCE(r.data->>'web_site', '') <> ''")
    elif has_website == "no":
        clauses.append("(r.data->>'web_site' IS NULL OR r.data->>'web_site' = '')")
    if enriched == "yes":
        clauses.append("EXISTS (SELECT 1 FROM link_candidates lc WHERE lc.result_id = r.id)")
    elif enriched == "no":
        clauses.append("NOT EXISTS (SELECT 1 FROM link_candidates lc WHERE lc.result_id = r.id)")

    where = " AND ".join(clauses) if clauses else "TRUE"
    offset = (page - 1) * page_size

    # Total count
    cur.execute(f"SELECT count(*) FROM results r WHERE {where}", params)
    total = cur.fetchone()[0]

    # Resolve sort
    sort_col = _SORT_MAP.get(sort_by, "r.id")
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"
    order_clause = f"{sort_col} {direction} NULLS LAST"

    # Page of results
    cur.execute(f"""
        SELECT
            r.id,
            COALESCE(r.data->>'title', '')                          AS company,
            COALESCE(r.data->>'phone', '')                          AS phone,
            COALESCE(r.data->>'web_site', '')                       AS website,
            COALESCE(r.data->>'address', '')                        AS address,
            COALESCE(r.data->>'category', '')                       AS category,
            COALESCE(r.data->'complete_address'->>'city', '')       AS city,
            COALESCE(r.data->'complete_address'->>'state', '')      AS state,
            COALESCE(r.data->>'review_rating', '')                  AS rating,
            COALESCE(r.data->>'review_count', '')                   AS reviews,
            (SELECT count(*) FROM link_candidates lc WHERE lc.result_id = r.id) AS links,
            (SELECT count(*) FROM documents d
             JOIN link_candidates lc ON lc.id = d.link_candidate_id
             WHERE lc.result_id = r.id AND d.fetch_status = 'ok')  AS docs,
            (SELECT count(*) FROM link_candidates lc
             WHERE lc.result_id = r.id AND lc.source_type = 'news') AS news_count,
            ce.estimated_revenue,
            ce.revenue_confidence,
            ce.estimated_headcount,
            ce.headcount_confidence
        FROM results r
        LEFT JOIN LATERAL (
            SELECT estimated_revenue, revenue_confidence,
                   estimated_headcount, headcount_confidence
            FROM company_evaluations
            WHERE result_id = r.id
            ORDER BY evaluated_at DESC
            LIMIT 1
        ) ce ON true
        WHERE {where}
        ORDER BY {order_clause}
        LIMIT %s OFFSET %s
    """, params + [page_size, offset])

    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, row)) for row in cur.fetchall()]
    cur.close()
    conn.close()
    return {"total": total, "page": page, "page_size": page_size, "items": rows}


@router.get("/filters")
def company_filters(db: str = Depends(db_param)):
    """Dynamic filter options for the company browser."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()

    cur.execute("""
        SELECT DISTINCT data->>'category' AS cat FROM results
        WHERE data->>'category' IS NOT NULL AND data->>'category' <> ''
        ORDER BY 1
    """)
    categories = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT data->'complete_address'->>'state' AS st FROM results
        WHERE data->'complete_address'->>'state' IS NOT NULL
          AND data->'complete_address'->>'state' <> ''
        ORDER BY 1
    """)
    states = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT data->'complete_address'->>'city' AS ct FROM results
        WHERE data->'complete_address'->>'city' IS NOT NULL
          AND data->'complete_address'->>'city' <> ''
        ORDER BY 1
    """)
    cities = [r[0] for r in cur.fetchall()]

    cur.close()
    conn.close()
    return {"categories": categories, "states": states, "cities": cities}


@router.get("/{result_id}")
def company_detail(result_id: int, db: str = Depends(db_param)):
    """Full JSONB data + enrichment detail for a single result."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()

    cur.execute("SELECT id, data FROM results WHERE id = %s", (result_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        from fastapi import HTTPException
        raise HTTPException(404, f"Result {result_id} not found")

    data = row[1] if isinstance(row[1], dict) else {}

    # Enrichment links
    cur.execute("""
        SELECT
            lc.id, lc.source_type, lc.discovery_status,
            lc.url_discovered, lc.title_discovered,
            d.id AS doc_id, d.fetch_status, d.page_title,
            (SELECT count(*) FROM document_chunks dc WHERE dc.document_id = d.id) AS chunks
        FROM link_candidates lc
        LEFT JOIN documents d ON d.link_candidate_id = lc.id
        WHERE lc.result_id = %s
        ORDER BY lc.source_type, lc.id
    """, (result_id,))

    lc_cols = [d[0] for d in cur.description]
    link_candidates = [dict(zip(lc_cols, r)) for r in cur.fetchall()]

    # Evaluation
    cur.execute("""
        SELECT * FROM company_evaluations WHERE result_id = %s
    """, (result_id,))
    eval_row = cur.fetchone()
    evaluation = None
    if eval_row:
        eval_cols = [d[0] for d in cur.description]
        evaluation = dict(zip(eval_cols, eval_row))

    cur.close()
    conn.close()

    return {
        "id": result_id,
        "data": data,
        "link_candidates": link_candidates,
        "evaluation": evaluation,
    }
