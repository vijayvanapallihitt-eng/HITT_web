from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RUNTIME_DIR = PROJECT_ROOT / "runtime"
ARCHIVE_DIR = PROJECT_ROOT / "archive"
CHROMA_DIR = RUNTIME_DIR / "chroma"
STATUS_DIR = RUNTIME_DIR / "status"
LOGS_DIR = RUNTIME_DIR / "logs"
DOWNLOADS_DIR = RUNTIME_DIR / "downloaded_files"
GMAPSDATA_DIR = RUNTIME_DIR / "gmapsdata"
DEFAULT_LOCAL_CONSTRUCTION_DSN = "postgresql://postgres:postgres@localhost:5432/construction"
DEFAULT_DOCKER_CONSTRUCTION_DSN = "postgres://postgres:postgres@host.docker.internal:5432/construction"


def load_project_env(env_file: str | Path | None = None) -> None:
    if load_dotenv is None:
        return
    target = Path(env_file) if env_file else PROJECT_ROOT / ".env"
    if target.exists():
        load_dotenv(dotenv_path=target, override=False)


def env_or_default(names: tuple[str, ...], default: str) -> str:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


def get_local_construction_dsn() -> str:
    load_project_env()
    return env_or_default(
        ("BROKER_CONSTRUCTION_DSN", "CONSTRUCTION_DSN", "DATABASE_URL"),
        DEFAULT_LOCAL_CONSTRUCTION_DSN,
    )


def get_docker_construction_dsn() -> str:
    load_project_env()
    return env_or_default(
        ("BROKER_DOCKER_CONSTRUCTION_DSN",),
        DEFAULT_DOCKER_CONSTRUCTION_DSN,
    )


def project_path(*parts: str) -> Path:
    return PROJECT_ROOT.joinpath(*parts)


def data_path(*parts: str) -> Path:
    return DATA_DIR.joinpath(*parts)


def runtime_path(*parts: str) -> Path:
    return RUNTIME_DIR.joinpath(*parts)
