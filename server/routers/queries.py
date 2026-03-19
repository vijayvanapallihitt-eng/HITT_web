"""Queries router — manage query files for the scraper."""
from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter()

QUERIES_DIR = Path(__file__).resolve().parent.parent.parent / "queries"


@router.get("")
def list_query_files():
    """List all .txt query files with line counts."""
    QUERIES_DIR.mkdir(parents=True, exist_ok=True)
    files = []
    for qf in sorted(QUERIES_DIR.glob("*.txt")):
        lines = qf.read_text(encoding="utf-8").strip().splitlines()
        files.append({
            "name": qf.name,
            "queries": len(lines),
            "preview": lines[:10],
        })
    return {"files": files}


@router.get("/{filename}")
def get_query_file(filename: str):
    """Return the full content of a query file."""
    path = QUERIES_DIR / filename
    if not path.exists():
        raise HTTPException(404, f"File not found: {filename}")
    content = path.read_text(encoding="utf-8")
    lines = content.strip().splitlines()
    return {"name": filename, "queries": len(lines), "lines": lines}


class SaveQueriesRequest(BaseModel):
    filename: str
    queries: list[str]


@router.post("")
def save_query_file(req: SaveQueriesRequest):
    """Save a query file (one query per line)."""
    lines = [l.strip() for l in req.queries if l.strip()]
    if not lines:
        raise HTTPException(400, "No queries provided")
    path = QUERIES_DIR / req.filename
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return {"status": "saved", "name": req.filename, "queries": len(lines)}


class GenerateRequest(BaseModel):
    city: str
    state: str
    trades: list[str]


@router.post("/generate")
def generate_queries(req: GenerateRequest):
    """Generate trade-based queries for a city/state."""
    queries = [f"{trade} in {req.city.strip()} {req.state.strip()}" for trade in req.trades]
    return {"queries": queries}
