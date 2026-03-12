from __future__ import annotations

import random
import time
from urllib.parse import quote_plus

import requests

from broker.enrichment.news_google import (
    extract_google_news_oxylabs,
    is_valid_google_news_html,
)
from broker.enrichment.proxies import ProxyPool


BLOCK_TEXTS = [
    "unusual traffic",
    "captcha",
    "are not a robot",
    "automated requests",
    "verify you are a human",
]

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36 Edg/123.0.0.0",
]

PROXY_TRIES = 3


def is_blocked(url: str, text: str, status_code: int = 200) -> bool:
    if status_code in (429, 403, 503):
        return True
    low_text = (text or "").lower()
    return any(token in low_text for token in BLOCK_TEXTS)


def search_page(
    query: str,
    session: requests.Session,
    proxy_pool: ProxyPool | None,
    timeout: int = 15,
) -> tuple[str, str]:
    """Perform a Google News search request, return (status, html)."""
    url = (
        f"https://news.google.com/search?q={quote_plus(query)}"
        "&hl=en-US&gl=US&ceid=US%3Aen"
    )

    attempts = []
    if proxy_pool and len(proxy_pool) > 0:
        for _ in range(min(PROXY_TRIES, len(proxy_pool))):
            attempts.append(proxy_pool.next())
    attempts.append(None)

    for proxy in attempts:
        session.headers["User-Agent"] = random.choice(USER_AGENTS)
        try:
            resp = session.get(url, timeout=timeout, proxies=proxy or {})
        except requests.RequestException:
            if proxy and proxy_pool:
                proxy_pool.remove_bad(proxy)
            continue

        html = resp.text or ""
        if is_blocked(resp.url, html, resp.status_code):
            if proxy and proxy_pool:
                proxy_pool.remove_bad(proxy)
            if proxy is not None:
                continue
            return "blocked", html

        if not is_valid_google_news_html(html):
            if proxy and proxy_pool:
                proxy_pool.remove_bad(proxy)
            if proxy is not None:
                continue
            return "error", html

        return "ok", html

    return "error", ""


def search_with_retries(
    query: str,
    session: requests.Session,
    proxy_pool: ProxyPool | None,
    max_retries: int,
) -> tuple[str, str]:
    """Search Google News with exponential backoff retries on blocks."""
    for attempt in range(max_retries + 1):
        status, html = search_page(query, session, proxy_pool)
        if status != "blocked" or attempt >= max_retries:
            return status, html
        backoff = min(120, 2 ** (attempt + 1)) + random.uniform(0, 2)
        print(
            f"      [WARN] Blocked (attempt {attempt + 1}/{max_retries + 1}), "
            f"backoff {backoff:.0f}s"
        )
        time.sleep(backoff)
    return "blocked", ""


def get_company_name(row: dict) -> str:
    """Pick the best available company-name column from a CSV row."""
    for key in ("title", "name", "company", "Company", "business_name"):
        value = (row.get(key) or "").strip()
        if value:
            return value
    return ""


def build_news_query(company: str) -> str:
    return f'"{company}" construction'


def discover_company_links(
    company: str,
    session: requests.Session,
    proxy_pool: ProxyPool | None,
    news_top: int,
    max_retries: int,
    delay_min: float = 0.0,
    delay_max: float = 0.0,
) -> dict:
    news_query = build_news_query(company)

    news_status, news_html = search_with_retries(
        news_query,
        session,
        proxy_pool,
        max_retries,
    )
    news_articles = (
        extract_google_news_oxylabs(news_html, news_top) if news_status == "ok" else []
    )
    if news_status == "ok" and not news_articles:
        news_status = "no_results"

    return {
        "company": company,
        "news_query": news_query,
        "news_status": news_status,
        "news_articles": news_articles,
        "news_count": len(news_articles),
    }
