"""Research router — trigger LinkedIn + website employee research for a company."""
from __future__ import annotations

import json
import logging
import queue
import threading
import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from server.deps import db_param, dsn_for, get_conn

router = APIRouter()
log = logging.getLogger(__name__)


class ResearchRequest(BaseModel):
    db: str
    result_id: int
    fetch_linkedin: bool = True
    fetch_website: bool = True
    max_pages: int = 8


class ResearchByNameRequest(BaseModel):
    db: str
    company: str
    website: str = ""
    result_id: Optional[int] = None
    fetch_linkedin: bool = True
    fetch_website: bool = True
    max_pages: int = 8


# ---------------------------------------------------------------------------
# Original blocking endpoints (still available for backwards compatibility)
# ---------------------------------------------------------------------------

@router.post("/run")
def run_research(req: ResearchRequest):
    """Run the research agent (blocking). Returns when done."""
    dsn = dsn_for(req.db)
    company, website = _lookup_company(req.db, req.result_id)

    from research_agent import research_company, save_research_to_db

    research = research_company(
        company=company,
        website=website,
        fetch_linkedin=req.fetch_linkedin,
        fetch_website=req.fetch_website,
        max_pages=req.max_pages,
    )
    save_research_to_db(dsn, req.result_id, company, research)
    return _format_result(company, req.result_id, research)


@router.post("/run-by-name")
def run_research_by_name(req: ResearchByNameRequest):
    """Run the research agent by company name (blocking)."""
    from research_agent import research_company, save_research_to_db

    research = research_company(
        company=req.company,
        website=req.website,
        fetch_linkedin=req.fetch_linkedin,
        fetch_website=req.fetch_website,
        max_pages=req.max_pages,
    )
    if req.result_id:
        dsn = dsn_for(req.db)
        save_research_to_db(dsn, req.result_id, req.company, research)

    return _format_result(req.company, req.result_id, research)


# ---------------------------------------------------------------------------
# SSE streaming endpoint — live progress updates
# ---------------------------------------------------------------------------

@router.post("/run-stream")
def run_research_stream(req: ResearchRequest):
    """
    Run the research agent with Server-Sent Events (SSE) progress streaming.

    Streams JSON events as the agent works through each phase:
      event: progress  — phase updates (init, search, linkedin, website, extract)
      event: result    — final research result (same shape as /run response)
      event: error     — if something went wrong

    The final result is also saved to company_evaluations automatically.
    """
    dsn = dsn_for(req.db)
    company, website = _lookup_company(req.db, req.result_id)

    def event_generator():
        progress_queue: queue.Queue = queue.Queue()
        result_holder: list = []
        error_holder: list = []

        def on_progress(phase: str, message: str, detail: dict | None = None):
            """Called by research_agent at each step."""
            progress_queue.put({
                "event": "progress",
                "phase": phase,
                "message": message,
                "detail": detail or {},
                "ts": time.time(),
            })

        def _run():
            try:
                from research_agent import research_company, save_research_to_db

                research = research_company(
                    company=company,
                    website=website,
                    fetch_linkedin=req.fetch_linkedin,
                    fetch_website=req.fetch_website,
                    max_pages=req.max_pages,
                    on_progress=on_progress,
                )

                on_progress("saving", "Saving results to database…")
                save_research_to_db(dsn, req.result_id, company, research)
                on_progress("saved", "Results saved to company_evaluations")

                result_holder.append(research)
            except Exception as exc:
                log.exception("Research stream error for %s", company)
                error_holder.append(str(exc))
            finally:
                progress_queue.put(None)  # sentinel

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()

        while True:
            try:
                item = progress_queue.get(timeout=120)
            except queue.Empty:
                yield ": keepalive\n\n"
                continue

            if item is None:
                if error_holder:
                    payload = {"event": "error", "message": error_holder[0]}
                    yield f"event: error\ndata: {json.dumps(payload)}\n\n"
                elif result_holder:
                    payload = _format_result(company, req.result_id, result_holder[0])
                    yield f"event: result\ndata: {json.dumps(payload)}\n\n"
                break
            else:
                yield f"event: progress\ndata: {json.dumps(item)}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


# ---------------------------------------------------------------------------
# Get existing results
# ---------------------------------------------------------------------------

@router.get("/results")
def get_research_results(result_id: int, db: str = Depends(db_param)):
    """Get existing research/evaluation results for a company."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ce.*, r.data->>'title' AS company_name, r.data->>'web_site' AS website
        FROM company_evaluations ce
        JOIN results r ON r.id = ce.result_id
        WHERE ce.result_id = %s
        """,
        (result_id,),
    )
    row = cur.fetchone()
    if not row:
        cur.close()
        conn.close()
        raise HTTPException(404, f"No evaluation found for result_id={result_id}")

    cols = [d[0] for d in cur.description]
    evaluation = dict(zip(cols, row))

    employees = []
    evidence = evaluation.get("evidence_summary", "")
    if "--- EMPLOYEE LIST ---" in evidence:
        parts = evidence.split("--- EMPLOYEE LIST ---", 1)
        evaluation["evidence_summary"] = parts[0].strip()
        try:
            employees = json.loads(parts[1].strip())
        except (json.JSONDecodeError, IndexError):
            pass

    evaluation["employees"] = employees
    evaluation["employee_count"] = len(employees)

    cur.close()
    conn.close()
    return evaluation


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lookup_company(db: str, result_id: int) -> tuple[str, str]:
    """Return (company_name, website) for a result_id or raise 404."""
    conn = get_conn(db, autocommit=True)
    cur = conn.cursor()
    cur.execute("SELECT data FROM results WHERE id = %s", (result_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if not row:
        raise HTTPException(404, f"Result {result_id} not found in {db}")

    data = row[0] if isinstance(row[0], dict) else {}
    company = (data.get("title") or "").strip()
    website = (data.get("web_site") or "").strip()

    if not company:
        raise HTTPException(400, "Company has no name (title field is empty)")

    return company, website


def _format_result(company: str, result_id: int | None, research: dict) -> dict:
    """Normalize research output into a consistent API response."""
    return {
        "company": company,
        "result_id": result_id,
        "estimated_headcount": research.get("estimated_headcount", "Unknown"),
        "headcount_confidence": research.get("headcount_confidence", "none"),
        "employee_count": len(research.get("employees", [])),
        "employees": research.get("employees", []),
        "estimated_revenue": research.get("estimated_revenue", "Unknown"),
        "revenue_confidence": research.get("revenue_confidence", "none"),
        "evidence_summary": research.get("evidence_summary", ""),
        "search_hits": len(research.get("search_results", [])),
        "pages_fetched": research.get("pages_fetched", 0),
    }
