"""
Broker Pipeline API Server — FastAPI backend.

Replaces the Streamlit dashboard with a proper REST API.
Every endpoint receives a `db` query-parameter (database name)
so the frontend can switch databases without touching .env.

Launch:
    uvicorn server.main:app --reload --port 8000
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from server.routers import databases, companies, enrichment, documents, vectors, scraper, workers, queries, research, web_research

FRONTEND_DIR = ROOT / "frontend" / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup / shutdown hooks."""
    from broker.config import load_project_env
    load_project_env()
    yield


app = FastAPI(
    title="Broker Pipeline API",
    version="0.3.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Register routers ────────────────────────────────────────────────
app.include_router(databases.router, prefix="/api/databases", tags=["Databases"])
app.include_router(companies.router, prefix="/api/companies", tags=["Companies"])
app.include_router(enrichment.router, prefix="/api/enrichment", tags=["Enrichment"])
app.include_router(documents.router, prefix="/api/documents", tags=["Documents"])
app.include_router(vectors.router, prefix="/api/vectors", tags=["Vectors"])
app.include_router(scraper.router, prefix="/api/scraper", tags=["Scraper"])
app.include_router(workers.router, prefix="/api/workers", tags=["Workers"])
app.include_router(queries.router, prefix="/api/queries", tags=["Queries"])
app.include_router(research.router, prefix="/api/research", tags=["Research"])
app.include_router(web_research.router, prefix="/api/web-research", tags=["Web Research"])


@app.get("/api/health")
def health():
    return {"status": "ok"}


# ── Serve frontend static files (production / Docker) ───────────────
# Must be registered AFTER all /api routes so they take priority.
if FRONTEND_DIR.exists():
    from fastapi.responses import FileResponse

    # Serve static assets (JS, CSS, images)
    app.mount("/assets", StaticFiles(directory=str(FRONTEND_DIR / "assets")), name="assets")

    # Catch-all: serve index.html for any non-API route (SPA client-side routing)
    @app.get("/{full_path:path}")
    async def serve_spa(full_path: str):
        return FileResponse(str(FRONTEND_DIR / "index.html"))
