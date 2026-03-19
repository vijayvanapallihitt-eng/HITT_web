"""Scraper router — Docker scraper management tied to a specific DB."""
from __future__ import annotations

import subprocess
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from server.deps import db_param, docker_dsn_for

router = APIRouter()

QUERIES_DIR = Path(__file__).resolve().parent.parent.parent / "queries"


def _container_name(db: str) -> str:
    return f"{db}-scraper"


class SeedRequest(BaseModel):
    query_file: str
    db: str


class StartRequest(BaseModel):
    query_file: str = ""
    db: str
    concurrency: int = 4
    depth: int = 1


class StopRequest(BaseModel):
    db: str


@router.get("/status")
def scraper_status():
    """List all scraper Docker containers."""
    try:
        r = subprocess.run(
            ["docker", "ps", "-a", "--filter", "ancestor=gosom/google-maps-scraper:latest",
             "--format", "{{.Names}}\t{{.Status}}\t{{.CreatedAt}}\t{{.Ports}}"],
            capture_output=True, text=True, timeout=10, encoding="utf-8", errors="replace",
        )
        lines = [l.strip() for l in (r.stdout or "").splitlines() if l.strip()]
        containers = []
        for line in lines:
            parts = line.split("\t")
            containers.append({
                "name": parts[0] if len(parts) > 0 else "",
                "status": parts[1] if len(parts) > 1 else "",
                "created": parts[2] if len(parts) > 2 else "",
                "ports": parts[3] if len(parts) > 3 else "",
            })
        return {"containers": containers}
    except Exception as e:
        raise HTTPException(500, f"Docker error: {e}")


@router.post("/seed")
def seed_queries(req: SeedRequest):
    """Seed queries from a file into the scraper job queue."""
    seed_path = QUERIES_DIR / req.query_file
    if not seed_path.exists():
        raise HTTPException(404, f"Query file not found: {req.query_file}")

    cname = f"{_container_name(req.db)}-seed"
    cmd = [
        "docker", "run", "--rm", "--name", cname,
        "-v", f"{seed_path.resolve()}:/queries.txt",
        "gosom/google-maps-scraper:latest",
        "-dsn", docker_dsn_for(req.db),
        "-produce", "-input", "/queries.txt", "-lang", "en",
    ]
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                           encoding="utf-8", errors="replace")
        return {
            "status": "ok" if r.returncode == 0 else "error",
            "returncode": r.returncode,
            "stdout": r.stdout or "",
            "stderr": r.stderr or "",
        }
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "Seed command timed out")


@router.post("/start")
def start_scraper(req: StartRequest):
    """Start a scraper Docker container for the given database."""
    cname = _container_name(req.db)

    # Remove existing container if any
    subprocess.run(["docker", "rm", "-f", cname], capture_output=True,
                   encoding="utf-8", errors="replace")

    # Optionally seed first
    if req.query_file:
        seed_path = QUERIES_DIR / req.query_file
        if seed_path.exists():
            seed_cmd = [
                "docker", "run", "--rm", "--name", f"{cname}-seed",
                "-v", f"{seed_path.resolve()}:/queries.txt",
                "gosom/google-maps-scraper:latest",
                "-dsn", docker_dsn_for(req.db),
                "-produce", "-input", "/queries.txt", "-lang", "en",
            ]
            subprocess.run(seed_cmd, capture_output=True, text=True, timeout=120,
                           encoding="utf-8", errors="replace")

    # Start scraper
    scraper_cmd = [
        "docker", "run", "-d", "--name", cname,
        "gosom/google-maps-scraper:latest",
        "-dsn", docker_dsn_for(req.db),
        "-c", str(req.concurrency),
        "-depth", str(req.depth),
        "-email", "-exit-on-inactivity", "5m",
    ]
    r = subprocess.run(scraper_cmd, capture_output=True, text=True, timeout=30,
                       encoding="utf-8", errors="replace")
    return {
        "status": "ok" if r.returncode == 0 else "error",
        "container": cname,
        "returncode": r.returncode,
        "stdout": r.stdout or "",
        "stderr": r.stderr or "",
    }


@router.post("/stop")
def stop_scraper(req: StopRequest):
    """Stop the scraper container for the given database."""
    cname = _container_name(req.db)
    r = subprocess.run(["docker", "stop", cname], capture_output=True, text=True,
                       timeout=30, encoding="utf-8", errors="replace")
    return {
        "status": "ok" if r.returncode == 0 else "error",
        "container": cname,
        "returncode": r.returncode,
    }


@router.get("/logs")
def scraper_logs(
    db: str = Depends(db_param),
    tail: int = 50,
    container: str = "",
):
    """Fetch Docker container logs."""
    target = container or _container_name(db)
    try:
        r = subprocess.run(
            ["docker", "logs", "--tail", str(tail), target],
            capture_output=True, text=True, timeout=15, encoding="utf-8", errors="replace",
        )
        return {"container": target, "logs": (r.stdout or "") + (r.stderr or "")}
    except subprocess.TimeoutExpired:
        raise HTTPException(504, "Docker logs timed out")
