from __future__ import annotations

from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

try:
    from googlenewsdecoder import gnewsdecoder as decode_google_news_url
except ImportError:
    decode_google_news_url = None


def canonicalize_url(url: str) -> str:
    parsed = urlparse((url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return (url or "").strip()
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
        and key.lower() not in {"fbclid", "gclid", "mc_cid", "mc_eid"}
    ]
    clean = parsed._replace(
        scheme=parsed.scheme.lower(),
        netloc=parsed.netloc.lower(),
        query=urlencode(query, doseq=True),
        fragment="",
    )
    return urlunparse(clean)


def resolve_google_news_url(session: requests.Session, url: str, timeout: int) -> str:
    parsed = urlparse(url)
    if "news.google.com" not in parsed.netloc.lower():
        return url
    if decode_google_news_url is not None:
        try:
            decoded = decode_google_news_url(url)
            if decoded.get("status") and decoded.get("decoded_url"):
                return str(decoded["decoded_url"])
        except Exception:
            pass
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        return resp.url or url
    except requests.RequestException:
        return url
