"""Workers router — start / stop / status enricher + ingester, tied to a DB."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from server.deps import dsn_for

router = APIRouter()

ROOT = Path(__file__).resolve().parent.parent.parent
STATUS_DIR = ROOT / "runtime" / "status"
VENV_PYTHON = str(ROOT / ".venv" / "Scripts" / "python.exe")

# Fallback if not on Windows
if not Path(VENV_PYTHON).exists():
    VENV_PYTHON = str(ROOT / ".venv" / "bin" / "python")
if not Path(VENV_PYTHON).exists():
    VENV_PYTHON = sys.executable


class StartEnricherRequest(BaseModel):
    db: str
    batch: int = 25
    poll: int = 10
    news_top: int = 10
    revenue_top: int = 3
    delay_min: float = 0.5
    delay_max: float = 1.5


class StartIngesterRequest(BaseModel):
    db: str
    fetch_batch: int = 25
    chunk_batch: int = 25
    poll: int = 15
    collection: str = "construction_docs_openai1536_live"
    embedding_model: str = "text-embedding-3-small"


class StartUnifiedRequest(BaseModel):
    db: str
    batch: int = 10
    poll: int = 20
    news_top: int = 10
    collection: str = "construction_docs_openai1536_live"
    embedding_model: str = "text-embedding-3-small"


class StopWorkerRequest(BaseModel):
    db: str
    worker: str   # "enricher" | "ingester" | "unified"


@router.get("/status")
def worker_status():
    """Read all worker status JSON files."""
    STATUS_DIR.mkdir(parents=True, exist_ok=True)
    statuses = []
    for sf in sorted(STATUS_DIR.glob("*.json")):
        try:
            data = json.loads(sf.read_text(encoding="utf-8"))
            data["_filename"] = sf.name
            statuses.append(data)
        except Exception as e:
            statuses.append({"_filename": sf.name, "error": str(e)})
    return {"statuses": statuses}


@router.post("/enricher/start")
def start_enricher(req: StartEnricherRequest):
    """Launch the enricher worker against a specific database."""
    dsn = dsn_for(req.db)
    cmd = (
        f'"{VENV_PYTHON}" worker_enrich.py'
        f' --dsn "{dsn}"'
        f' --batch {req.batch} --poll {req.poll}'
        f' --news-top {req.news_top}'
        f' --max-retries 1 --delay-min {req.delay_min} --delay-max {req.delay_max}'
    )
    try:
        subprocess.Popen(
            cmd, cwd=str(ROOT), shell=True,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to start enricher: {e}")
    return {"status": "started", "db": req.db, "command": cmd}


@router.post("/ingester/start")
def start_ingester(req: StartIngesterRequest):
    """Launch the document ingester worker against a specific database."""
    dsn = dsn_for(req.db)
    status_file = STATUS_DIR / f"ingest_{req.db}.json"
    chroma_dir = f'runtime/chroma/db_{req.db}'
    cmd = (
        f'"{ VENV_PYTHON}" scripts/run_document_ingest.py run'
        f' --dsn "{dsn}"'
        f' --fetch-batch {req.fetch_batch} --chunk-batch {req.chunk_batch}'
        f' --poll {req.poll}'
        f' --persist-dir {chroma_dir}'
        f' --collection {req.collection}'
        f' --embedding-backend openai --embedding-model {req.embedding_model}'
        f' --env-file .env'
        f' --status-file "{status_file}"'
    )
    try:
        subprocess.Popen(
            cmd, cwd=str(ROOT), shell=True,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to start ingester: {e}")
    return {"status": "started", "db": req.db, "status_file": str(status_file), "command": cmd}


@router.post("/unified/start")
def start_unified(req: StartUnifiedRequest):
    """Launch the unified pipeline worker (discover+fetch+chunk+embed in one flow)."""
    dsn = dsn_for(req.db)
    status_file = STATUS_DIR / f"unified_{req.db}.json"
    chroma_dir = f'runtime/chroma/db_{req.db}'
    cmd = (
        f'"{VENV_PYTHON}" worker_unified.py'
        f' --dsn "{dsn}"'
        f' --batch {req.batch} --poll {req.poll}'
        f' --news-top {req.news_top}'
        f' --collection {req.collection}'
        f' --embedding-backend openai --embedding-model {req.embedding_model}'
        f' --persist-dir {chroma_dir}'
        f' --env-file .env'
        f' --status-file "{status_file}"'
    )
    try:
        subprocess.Popen(
            cmd, cwd=str(ROOT), shell=True,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
    except Exception as e:
        raise HTTPException(500, f"Failed to start unified worker: {e}")
    return {"status": "started", "db": req.db, "status_file": str(status_file), "command": cmd}


@router.post("/stop")
def stop_worker(req: StopWorkerRequest):
    """Stop a worker process (best-effort via taskkill on Windows)."""
    target_map = {
        "enricher": "worker_enrich",
        "ingester": "run_document_ingest",
        "unified": "worker_unified",
    }
    target = target_map.get(req.worker, req.worker)
    try:
        # Try to find and kill by command line content
        r = subprocess.run(
            ["powershell", "-Command",
             f"Get-CimInstance Win32_Process | Where-Object {{ $_.CommandLine -like '*{target}*' -and $_.CommandLine -like '*{req.db}*' }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force }}"],
            capture_output=True, text=True, timeout=10,
        )
        return {"status": "stopped", "worker": req.worker, "db": req.db, "output": r.stdout}
    except Exception as e:
        raise HTTPException(500, f"Failed to stop {req.worker}: {e}")


@router.post("/stop-all")
def stop_all_workers():
    """Stop ALL worker processes across all databases."""
    targets = ["worker_enrich", "run_document_ingest", "worker_unified", "worker_evaluate"]
    killed = []
    for target in targets:
        try:
            r = subprocess.run(
                ["powershell", "-Command",
                 f"Get-CimInstance Win32_Process | Where-Object {{ $_.CommandLine -like '*{target}*' }} | ForEach-Object {{ Stop-Process -Id $_.ProcessId -Force; $_.ProcessId }}"],
                capture_output=True, text=True, timeout=10,
            )
            if r.stdout.strip():
                killed.extend(r.stdout.strip().splitlines())
        except Exception:
            pass
    return {"status": "stopped", "killed_pids": killed}


@router.get("/processes")
def list_python_processes():
    """List running Python worker processes."""
    try:
        r = subprocess.run(
            ["powershell", "-Command",
             "Get-CimInstance Win32_Process -Filter \"Name='python.exe'\" | "
             "Select-Object ProcessId, CreationDate, CommandLine | ConvertTo-Json"],
            capture_output=True, text=True, timeout=10,
        )
        procs = json.loads(r.stdout) if r.stdout.strip() else []
        if isinstance(procs, dict):
            procs = [procs]
        # Filter to just our workers
        workers = [
            p for p in procs
            if p.get("CommandLine") and (
                "worker_enrich" in p["CommandLine"]
                or "run_document_ingest" in p["CommandLine"]
                or "worker_evaluate" in p["CommandLine"]
                or "worker_unified" in p["CommandLine"]
            )
        ]
        return {"processes": workers}
    except Exception as e:
        return {"processes": [], "error": str(e)}
