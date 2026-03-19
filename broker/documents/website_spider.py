"""
Website spider using crawl4ai.

Crawls a company's website to discover and extract content from key pages:
  - Homepage
  - /about, /about-us, /who-we-are
  - /team, /our-team, /leadership, /staff, /people
  - /careers, /jobs
  - /contact
  - /projects, /portfolio, /services

Returns a list of dicts with url, title, text for each page found.
"""

from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import urljoin, urlparse

log = logging.getLogger(__name__)

# Subpaths we actively look for (we'll also discover links from the homepage)
TARGET_PATHS = [
    "/about", "/about-us", "/who-we-are", "/our-story", "/company",
    "/team", "/our-team", "/leadership", "/staff", "/people", "/management",
    "/careers", "/jobs", "/join-us",
    "/contact", "/contact-us",
    "/projects", "/portfolio", "/services", "/our-work",
]

# URL patterns that indicate high-value pages (used to filter discovered links)
HIGH_VALUE_PATTERNS = re.compile(
    r"(about|team|staff|people|leadership|management|careers|jobs|contact|"
    r"projects|portfolio|services|our[_-]?story|who[_-]?we[_-]?are|company)",
    re.IGNORECASE,
)

MAX_PAGES = 12  # max pages to crawl per company website

# Titles/text patterns that indicate error pages — skip these
ERROR_PAGE_PATTERNS = re.compile(
    r"(page\s*not\s*found|404\s*error|404\s*not\s*found|not\s*found|"
    r"access\s*denied|403\s*forbidden|server\s*error|500\s*error)",
    re.IGNORECASE,
)


async def _crawl_urls(urls: list[str], timeout: int = 30) -> list[dict]:
    """Crawl a batch of URLs using crawl4ai and return page results."""
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

    results = []
    browser_conf = BrowserConfig(
        headless=True,
        verbose=False,
    )
    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=timeout * 1000,
        word_count_threshold=50,
    )

    try:
        async with AsyncWebCrawler(config=browser_conf) as crawler:
            for url in urls:
                try:
                    result = await crawler.arun(url=url, config=run_conf)
                    if result.success:
                        # Get markdown text (cleaner than raw HTML extraction)
                        text = ""
                        if hasattr(result, "markdown"):
                            if hasattr(result.markdown, "raw_markdown"):
                                text = result.markdown.raw_markdown or ""
                            elif isinstance(result.markdown, str):
                                text = result.markdown
                        if not text:
                            text = result.extracted_content or ""

                        # Clean up markdown artifacts
                        text = re.sub(r"\n{3,}", "\n\n", text).strip()

                        if len(text) > 200:  # skip trivially short pages
                            title = _extract_title(result, url)
                            # Skip error/404 pages
                            if ERROR_PAGE_PATTERNS.search(title):
                                log.info("    [crawl4ai] Skipping error page %s (%s)", url, title[:60])
                                continue
                            results.append({
                                "url": str(result.url or url),
                                "title": title,
                                "text": text[:30000],  # cap at 30k chars
                            })
                            log.info("    [crawl4ai] OK %s (%d chars)", url, len(text))
                        else:
                            log.info("    [crawl4ai] Too short, skipping %s (%d chars)", url, len(text))
                    else:
                        log.info("    [crawl4ai] Failed %s: %s", url, getattr(result, "error_message", "unknown"))
                except Exception as exc:
                    log.warning("    [crawl4ai] Error crawling %s: %s", url, exc)
    except Exception as exc:
        log.error("    [crawl4ai] Browser error: %s", exc)

    return results


def _extract_title(result, fallback_url: str) -> str:
    """Pull a page title from crawl4ai result metadata."""
    if hasattr(result, "metadata") and result.metadata:
        title = result.metadata.get("title", "") or ""
        if title.strip():
            return title.strip()
    return urlparse(fallback_url).path or fallback_url


def _discover_subpage_urls(homepage_url: str) -> list[str]:
    """Generate candidate subpage URLs from the homepage."""
    parsed = urlparse(homepage_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    candidates = set()
    for path in TARGET_PATHS:
        candidates.add(urljoin(base, path))
    return sorted(candidates)


async def _crawl_homepage_and_discover(homepage_url: str, timeout: int = 20) -> list[str]:
    """Crawl the homepage and extract internal links to high-value pages."""
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

    discovered = set()
    browser_conf = BrowserConfig(headless=True, verbose=False)
    run_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=timeout * 1000,
    )

    try:
        async with AsyncWebCrawler(config=browser_conf) as crawler:
            result = await crawler.arun(url=homepage_url, config=run_conf)
            if result.success and hasattr(result, "links"):
                internal_links = getattr(result.links, "internal", []) or []
                for link in internal_links:
                    href = link.get("href", "") if isinstance(link, dict) else str(link)
                    if href and HIGH_VALUE_PATTERNS.search(href):
                        full_url = urljoin(homepage_url, href)
                        if urlparse(full_url).netloc == urlparse(homepage_url).netloc:
                            discovered.add(full_url)
    except Exception as exc:
        log.warning("    [crawl4ai] Homepage link discovery error: %s", exc)

    return sorted(discovered)


def crawl_company_website(homepage_url: str, timeout: int = 20) -> list[dict]:
    """
    Spider a company website. Returns list of {url, title, text} dicts.

    Strategy:
    1. Generate candidate subpage URLs from known patterns
    2. Crawl homepage to discover linked high-value pages
    3. Merge and deduplicate
    4. Crawl homepage + up to MAX_PAGES subpages
    """
    if not homepage_url:
        return []

    # Normalise
    if not homepage_url.startswith(("http://", "https://")):
        homepage_url = f"https://{homepage_url}"

    log.info("    [crawl4ai] Spidering %s", homepage_url)

    # Build URL set: homepage + known paths + discovered links
    urls_to_crawl = {homepage_url}

    # Add known subpath candidates
    for url in _discover_subpage_urls(homepage_url):
        urls_to_crawl.add(url)

    # Discover links from homepage (quick crawl)
    try:
        discovered = asyncio.run(_crawl_homepage_and_discover(homepage_url, timeout))
        for url in discovered:
            urls_to_crawl.add(url)
    except RuntimeError:
        # Already in an event loop — use nest_asyncio or skip discovery
        try:
            import nest_asyncio
            nest_asyncio.apply()
            discovered = asyncio.run(_crawl_homepage_and_discover(homepage_url, timeout))
            for url in discovered:
                urls_to_crawl.add(url)
        except Exception:
            pass

    # Cap the number of pages
    urls_list = sorted(urls_to_crawl)[:MAX_PAGES]
    log.info("    [crawl4ai] Will crawl %d pages for %s", len(urls_list), homepage_url)

    # Crawl all pages
    try:
        pages = asyncio.run(_crawl_urls(urls_list, timeout))
    except RuntimeError:
        try:
            import nest_asyncio
            nest_asyncio.apply()
            pages = asyncio.run(_crawl_urls(urls_list, timeout))
        except Exception as exc:
            log.error("    [crawl4ai] Failed to run async crawl: %s", exc)
            pages = []

    log.info("    [crawl4ai] Got %d pages from %s", len(pages), homepage_url)
    return pages
