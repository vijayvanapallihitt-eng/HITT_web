"""
Start the Broker dashboard — API server + frontend dev server.

Usage:
    python start.py          # start both servers
    python start.py --build  # build frontend first, then start
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_PYTHON = str(ROOT / ".venv" / "Scripts" / "python.exe")
if not Path(VENV_PYTHON).exists():
    VENV_PYTHON = str(ROOT / ".venv" / "bin" / "python")
if not Path(VENV_PYTHON).exists():
    VENV_PYTHON = sys.executable

FRONTEND_DIR = str(ROOT / "frontend")

_children: list[subprocess.Popen] = []
_shutdown = False


def _handle_signal(sig, frame):
    global _shutdown
    if _shutdown:
        for p in _children:
            try:
                p.kill()
            except Exception:
                pass
        sys.exit(1)
    _shutdown = True
    print("\n[start] Shutting down...")
    for p in _children:
        try:
            p.terminate()
        except Exception:
            pass


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


def main():
    parser = argparse.ArgumentParser(description="Start Broker dashboard")
    parser.add_argument("--build", action="store_true", help="Build frontend before starting")
    parser.add_argument("--port", type=int, default=8000, help="API server port")
    parser.add_argument("--frontend-port", type=int, default=5173, help="Frontend dev server port")
    parser.add_argument("--no-frontend", action="store_true", help="Only start the API server")
    parser.add_argument("--no-api", action="store_true", help="Only start the frontend")
    args = parser.parse_args()

    print("=" * 50)
    print("  Broker Dashboard")
    print("=" * 50)

    # Optional: build frontend first
    if args.build:
        print("[start] Building frontend...")
        result = subprocess.run(
            ["npm", "run", "build"],
            cwd=FRONTEND_DIR,
            shell=True,
        )
        if result.returncode != 0:
            print("[start] Frontend build failed!")
            sys.exit(1)
        print("[start] Frontend built successfully.")

    # Start API server
    if not args.no_api:
        print(f"[start] Starting API server on port {args.port}...")
        api_proc = subprocess.Popen(
            [VENV_PYTHON, "-m", "uvicorn", "server.main:app", "--reload",
             "--host", "0.0.0.0", "--port", str(args.port)],
            cwd=str(ROOT),
        )
        _children.append(api_proc)
        time.sleep(2)

    # Start frontend dev server
    if not args.no_frontend:
        print(f"[start] Starting frontend on port {args.frontend_port}...")
        frontend_proc = subprocess.Popen(
            ["npm", "run", "dev", "--", "--port", str(args.frontend_port)],
            cwd=FRONTEND_DIR,
            shell=True,
        )
        _children.append(frontend_proc)
        time.sleep(2)

    # Detect LAN IP
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        lan_ip = s.getsockname()[0]
        s.close()
    except Exception:
        lan_ip = "<your-ip>"

    # Print summary
    print()
    print("  ✓ Broker Dashboard is running!")
    print()
    if not args.no_api:
        print(f"    API (local):   http://localhost:{args.port}")
        print(f"    API (network): http://{lan_ip}:{args.port}")
    if not args.no_frontend:
        print(f"    UI  (local):   http://localhost:{args.frontend_port}")
        print(f"    UI  (network): http://{lan_ip}:{args.frontend_port}")
    print()
    print("  Anyone on your network can open the network URL above.")
    print("  Press Ctrl+C to stop all servers.")
    print("=" * 50)

    # Wait for processes
    try:
        while not _shutdown:
            for p in _children:
                if p.poll() is not None:
                    print(f"[start] Process {p.pid} exited with code {p.returncode}")
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    # Cleanup
    for p in _children:
        try:
            p.terminate()
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
            except Exception:
                pass

    print("[start] All servers stopped.")


if __name__ == "__main__":
    main()
