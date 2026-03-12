from __future__ import annotations

import requests

from broker.documents.extract import extract_text_from_html


def fetch_url_text(session: requests.Session, url: str, timeout: int, max_chars: int) -> dict:
    try:
        resp = session.get(url, timeout=timeout, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException as exc:
        response = getattr(exc, "response", None)
        return {
            "ok": False,
            "error": str(exc),
            "url": getattr(response, "url", None) or url,
            "http_status": getattr(response, "status_code", None),
        }

    content_type = resp.headers.get("Content-Type", "").lower()
    if "text/html" not in content_type:
        return {
            "ok": False,
            "error": f"unsupported content-type: {content_type}",
            "url": resp.url or url,
            "http_status": resp.status_code,
        }

    title, text = extract_text_from_html(resp.text)
    if not text:
        return {
            "ok": False,
            "error": "empty extracted text",
            "url": resp.url or url,
            "http_status": resp.status_code,
        }
    if max_chars > 0:
        text = text[:max_chars]
    return {
        "ok": True,
        "title": title,
        "text": text,
        "url": resp.url or url,
        "http_status": resp.status_code,
    }
