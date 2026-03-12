from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def save_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    # On Windows another process (e.g. Streamlit) may hold the target open;
    # retry a few times before giving up.
    import time, os
    for attempt in range(5):
        try:
            tmp.replace(path)
            return
        except PermissionError:
            if attempt < 4:
                time.sleep(0.2)
            else:
                # Last resort: write directly (non-atomic but won't crash)
                try:
                    path.write_text(
                        json.dumps(payload, ensure_ascii=False, indent=2),
                        encoding="utf-8",
                    )
                except Exception:
                    pass
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
