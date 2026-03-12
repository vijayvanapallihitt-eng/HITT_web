from __future__ import annotations

from pathlib import Path

import requests


PROXIFLY_HTTPS_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/https/data.txt"
PROXIFLY_HTTP_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"


def fetch_proxifly_proxies() -> list[str]:
    """Download free HTTPS + HTTP proxies from Proxifly."""
    proxies = []
    for url in [PROXIFLY_HTTPS_URL, PROXIFLY_HTTP_URL]:
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            lines = [line.strip() for line in resp.text.splitlines() if line.strip()]
            proxies.extend(lines)
            print(f"  [OK] Downloaded {len(lines)} proxies from {url.split('/')[-2]}")
        except Exception as exc:
            print(f"  [WARN] Failed to fetch proxies from {url}: {exc}")
    return list(dict.fromkeys(proxies))


def load_proxy_file(path: str) -> list[str]:
    file_path = Path(path)
    if not file_path.exists():
        raise FileNotFoundError(f"Proxy file not found: {path}")
    lines = [line.strip() for line in file_path.read_text(encoding="utf-8").splitlines()]
    return [line for line in lines if line and not line.startswith("#")]


class ProxyPool:
    """Simple round-robin proxy rotator with optional fallback to direct."""

    def __init__(self, proxies: list[str]):
        self.proxies = proxies
        self._idx = 0

    def next(self) -> dict | None:
        if not self.proxies:
            return None
        proxy = self.proxies[self._idx % len(self.proxies)]
        self._idx += 1
        if not proxy.startswith("http"):
            proxy = f"http://{proxy}"
        return {"http": proxy, "https": proxy}

    def remove_bad(self, proxy_dict: dict | None) -> None:
        if not proxy_dict:
            return
        proxy_url = proxy_dict.get("http", "")
        self.proxies = [
            proxy
            for proxy in self.proxies
            if proxy_url not in f"http://{proxy}" and proxy_url != proxy
        ]

    def __len__(self) -> int:
        return len(self.proxies)
