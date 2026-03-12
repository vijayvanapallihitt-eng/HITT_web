from __future__ import annotations

import importlib
import sys
from pathlib import Path
from urllib.parse import urljoin

from bs4 import BeautifulSoup


GOOGLE_NEWS_BASE = "https://news.google.com/"


def load_oxylabs_parser_class():
    """Load google_news_scraper.parser.GoogleNewsHTMLParser from site-packages."""
    try:
        from google_news_scraper.parser import GoogleNewsHTMLParser as parser_class

        return parser_class
    except Exception:
        pass

    for path in sys.path:
        if "site-packages" not in path.lower():
            continue
        parser_file = Path(path) / "google_news_scraper" / "parser.py"
        if not parser_file.exists():
            continue
        for key in [
            key
            for key in list(sys.modules.keys())
            if key == "google_news_scraper" or key.startswith("google_news_scraper.")
        ]:
            sys.modules.pop(key, None)
        if path in sys.path:
            sys.path.remove(path)
        sys.path.insert(0, path)
        try:
            importlib.invalidate_caches()
            module = importlib.import_module("google_news_scraper.parser")
            return module.GoogleNewsHTMLParser
        except Exception:
            continue
    return None


GoogleNewsHTMLParser = load_oxylabs_parser_class()


def is_valid_google_news_html(html: str) -> bool:
    if len(html) < 3000:
        return False
    low = html.lower()
    return "news.google.com" in low and ("c-wiz" in low or "gpfen" in low)


def extract_google_news_oxylabs(html: str, top_n: int) -> list[dict]:
    """Extract Google News articles using Oxylabs parser classes."""
    if GoogleNewsHTMLParser is None:
        return []

    parser = GoogleNewsHTMLParser()
    parsed = parser.parse(html)
    seen: set[tuple[str, str]] = set()
    items: list[dict] = []

    for article in parsed:
        title = (article.title or "").strip()
        href = (article.url or "").strip()
        if not title or not href:
            continue
        href = urljoin(GOOGLE_NEWS_BASE, href)
        key = (title, href)
        if key in seen:
            continue
        seen.add(key)
        items.append({"title": title, "url": href})
        if len(items) >= top_n:
            break

    if items:
        return items

    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.select("a.JtKRv[href], a[href^='./read/']"):
        title = anchor.get_text(" ", strip=True)
        href = (anchor.get("href") or "").strip()
        if not title or not href:
            continue
        href = urljoin(GOOGLE_NEWS_BASE, href)
        key = (title, href)
        if key in seen:
            continue
        seen.add(key)
        items.append({"title": title, "url": href})
        if len(items) >= top_n:
            break
    return items
