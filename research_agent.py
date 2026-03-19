"""
LinkedIn & Website Research Agent.

For a given company, this agent:
  1. Searches Google for LinkedIn company page + employee profiles
     (scoped: site:linkedin.com "<company>")
  2. Searches Google for company website employee / team / about pages
     (scoped: site:<company_website> employees OR team OR about OR staff)
  3. Fetches and extracts content from discovered pages via crawl4ai
  4. Sends all evidence to GPT-4o-mini to produce:
     - Estimated headcount with confidence
     - A list of named employees with titles (when available)
     - Revenue signals found during the research
     - Evidence summary with source citations

Usage (standalone):
    python research_agent.py --company "Turner Construction" --website "turnerconstruction.com" --db construction_test
    python research_agent.py --result-id 111 --db construction_test
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus, urljoin, urlparse

import psycopg2
import requests

# crawl4ai deep-crawl, filtering & content processing imports
from crawl4ai import (
    AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode,
)
from crawl4ai.deep_crawling import BestFirstCrawlingStrategy
from crawl4ai.deep_crawling.filters import FilterChain, DomainFilter
from crawl4ai.deep_crawling.scorers import KeywordRelevanceScorer
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from crawl4ai.content_filter_strategy import PruningContentFilter

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(dotenv_path=ROOT / ".env", override=False)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RESEARCH] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
]

BLOCK_TEXTS = [
    "unusual traffic", "captcha", "are not a robot",
    "automated requests", "verify you are a human",
]

# Pages we specifically target on the company website
EMPLOYEE_PATHS = [
    "/about", "/about-us", "/who-we-are", "/our-story", "/company",
    "/team", "/our-team", "/leadership", "/staff", "/people", "/management",
    "/careers", "/jobs", "/join-us", "/employees",
    "/executive-team", "/our-leaders", "/our-people", "/about/team",
    "/about/leadership", "/about/people", "/about/staff",
    "/who-we-are/team", "/who-we-are/leadership", "/who-we-are/people",
    "/company/team", "/company/leadership",
]

# URL patterns that indicate pages likely to contain employee/company info
EMPLOYEE_URL_PATTERNS = re.compile(
    r"(about|team|staff|people|leadership|management|executive|director|officer"
    r"|careers|jobs|who[_-]?we[_-]?are|our[_-]?(?:story|people|team|leaders)"
    r"|company|history|culture|values|employee|personnel|board|partner)",
    re.IGNORECASE,
)

# Keywords for URL scoring (used by crawl4ai KeywordRelevanceScorer)
EMPLOYEE_KEYWORDS = [
    "team", "leadership", "people", "about", "employees", "staff",
    "careers", "management", "executive", "directors", "officers",
    "who-we-are", "our-team", "our-people", "our-leaders", "board",
    "company", "history", "culture", "personnel", "partner",
    "headcount", "workforce", "human-resources", "hr",
]

# Patterns to skip — not useful for employee research
SKIP_URL_PATTERNS = re.compile(
    r"(\.pdf$|\.jpg$|\.png$|\.svg$|\.gif$|\.css$|\.js$|\?|#|"
    r"privacy|cookie|terms|disclaimer|sitemap|rss|feed|cdn|"
    r"facebook\.com|twitter\.com|instagram\.com|youtube\.com|"
    r"login|signup|cart|checkout|wp-content|wp-admin)",
    re.IGNORECASE,
)

# Page titles/content that indicate error pages
ERROR_PAGE_PATTERNS = re.compile(
    r"(page\s*not\s*found|404\s*error|404\s*not\s*found|not\s*found|"
    r"access\s*denied|403\s*forbidden|server\s*error|500\s*error)",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Google search via crawl4ai (headless Chrome — renders JS)
# ---------------------------------------------------------------------------

async def _google_search_crawl4ai(
    query: str,
    crawler,
    run_conf,
    num_results: int = 10,
) -> list[dict]:
    """Run a Google search using headless Chrome via crawl4ai."""
    url = (
        f"https://www.google.com/search?"
        f"q={quote_plus(query)}&num={num_results}&hl=en"
    )
    results: list[dict] = []
    try:
        page = await crawler.arun(url=url, config=run_conf)
        if not page.success:
            log.warning("  Google search crawl failed for: %s", query[:80])
            return results

        html = page.html or ""
        if any(tok in html.lower() for tok in BLOCK_TEXTS):
            log.warning("  Google search blocked for: %s", query[:80])
            return results

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Google renders search results with different structures;
        # try multiple selectors in priority order
        containers = (
            soup.select("div.g")
            or soup.select("div[data-sokoban-container]")
            or soup.select("div.MjjYud > div")
            or soup.select("div[data-hveid]")
        )

        for g in containers:
            anchor = g.select_one("a[href]")
            if not anchor:
                continue
            href = anchor.get("href", "")
            if not href.startswith("http") or "google.com" in href:
                continue

            title_el = g.select_one("h3")
            title = title_el.get_text(strip=True) if title_el else ""

            snippet = ""
            for sel in ["div[data-sncf]", "span.st", "div.VwiC3b", "div[style='-webkit-line-clamp:2']"]:
                snippet_el = g.select_one(sel)
                if snippet_el:
                    snippet = snippet_el.get_text(strip=True)
                    break
            if not snippet:
                # fallback: grab any text from the container that isn't the title
                all_text = g.get_text(" ", strip=True)
                if title and title in all_text:
                    snippet = all_text.replace(title, "", 1).strip()[:300]

            results.append({"title": title, "url": href, "snippet": snippet})

        # Fallback: extract all external links if selector-based approach got nothing
        if not results:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "google.com" not in href:
                    title = a.get_text(strip=True)[:120]
                    results.append({"title": title, "url": href, "snippet": ""})

    except Exception as exc:
        log.warning("  Google search crawl error: %s", exc)

    return results[:num_results]


# ---------------------------------------------------------------------------
# Async page fetcher (reuses a crawler session)
# ---------------------------------------------------------------------------

def _result_to_page(result, fallback_url: str = "") -> dict | None:
    """Convert a CrawlResult to our standard {url, title, text} dict.

    Prefers `fit_markdown` (pruned/filtered content) over `raw_markdown`
    since we configure PruningContentFilter to strip boilerplate.
    """
    if not result.success:
        return None

    text = ""
    # Prefer fit_markdown (content-filtered) → raw_markdown → extracted_content
    if hasattr(result, "markdown"):
        md = result.markdown
        if hasattr(md, "fit_markdown") and md.fit_markdown:
            text = md.fit_markdown
        elif hasattr(md, "raw_markdown") and md.raw_markdown:
            text = md.raw_markdown
        elif isinstance(md, str):
            text = md
    if not text:
        text = result.extracted_content or ""

    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    if len(text) < 150:
        return None

    title = ""
    if hasattr(result, "metadata") and result.metadata:
        title = (result.metadata.get("title") or "").strip()

    # Skip error pages
    if title and ERROR_PAGE_PATTERNS.search(title):
        return None

    url = str(result.url or fallback_url)
    return {
        "url": url,
        "title": title or urlparse(url).path,
        "text": text[:25000],
    }


async def _fetch_page(crawler, run_conf, url: str) -> dict | None:
    """Fetch a single page and return {url, title, text} or None."""
    try:
        result = await crawler.arun(url=url, config=run_conf)
        return _result_to_page(result, fallback_url=url)
    except Exception as exc:
        log.warning("    Fetch error %s: %s", url, exc)
        return None


async def _discover_links_from_page(crawler, run_conf, url: str, same_domain: str) -> list[str]:
    """Crawl a page and extract all internal links (same domain)."""
    discovered = []
    try:
        result = await crawler.arun(url=url, config=run_conf)
        if not result.success:
            return discovered

        # Method 1: crawl4ai's built-in link extraction
        if hasattr(result, "links"):
            internal = getattr(result.links, "internal", []) or []
            for link in internal:
                href = link.get("href", "") if isinstance(link, dict) else str(link)
                if href:
                    full_url = urljoin(url, href)
                    discovered.append(full_url)

        # Method 2: Parse links from HTML directly (more reliable for JS-rendered sites)
        html = result.html or ""
        if html:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
                    continue
                full_url = urljoin(url, href)
                discovered.append(full_url)

        # Filter: same domain, not a skip pattern, deduplicate
        filtered = []
        seen = set()
        for link_url in discovered:
            parsed = urlparse(link_url)
            if parsed.netloc != same_domain:
                continue
            if SKIP_URL_PATTERNS.search(link_url):
                continue
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path.rstrip('/')}/"
            if clean not in seen:
                seen.add(clean)
                # Store without trailing slash for consistency
                filtered.append(f"{parsed.scheme}://{parsed.netloc}{parsed.path}")

        return filtered

    except Exception as exc:
        log.warning("    Link discovery error %s: %s", url, exc)
    return []


async def _deep_crawl_website(
    crawler,
    fetch_conf,
    homepage_url: str,
    company: str,
    max_pages: int = 15,
) -> list[dict]:
    """
    Deep-crawl a company website. Strategy:

    1. Crawl homepage — extract text + discover ALL internal links
    2. Generate candidate URLs from known employee-related paths
    3. Prioritize: employee-related URLs first, then other discovered pages
    4. Crawl wave 1 (high-value pages) — also discover links from those
    5. Crawl wave 2 (deeper pages found from wave 1)
    6. Return all fetched page content

    This gives us 2 levels of depth from the homepage.
    """
    if not homepage_url:
        return []
    if not homepage_url.startswith(("http://", "https://")):
        homepage_url = f"https://{homepage_url}"

    domain = urlparse(homepage_url).netloc
    base = f"{urlparse(homepage_url).scheme}://{domain}"
    log.info("  Deep crawl: %s (max %d pages)", homepage_url, max_pages)

    fetched_pages: list[dict] = []
    crawled_urls: set[str] = set()
    all_discovered: set[str] = set()

    # --- Wave 0: Homepage ---
    log.info("    Wave 0: Homepage")
    homepage_links = await _discover_links_from_page(crawler, fetch_conf, homepage_url, domain)
    all_discovered.update(homepage_links)
    log.info("    Homepage discovered %d internal links", len(homepage_links))

    page = await _fetch_page(crawler, fetch_conf, homepage_url)
    if page:
        fetched_pages.append(page)
        log.info("    OK: %s (%d chars)", homepage_url[:80], len(page["text"]))
    crawled_urls.add(homepage_url)

    # --- Build prioritized URL list ---
    # Tier 1: Known employee-related paths (hardcoded)
    tier1_urls = []
    for path in EMPLOYEE_PATHS:
        candidate = urljoin(base, path)
        if candidate not in crawled_urls:
            tier1_urls.append(candidate)

    # Tier 2: Discovered links that match employee patterns
    tier2_urls = []
    for url in homepage_links:
        if url not in crawled_urls and EMPLOYEE_URL_PATTERNS.search(url):
            tier2_urls.append(url)

    # Tier 3: Other discovered links (lower priority)
    tier3_urls = []
    for url in homepage_links:
        if (url not in crawled_urls
                and url not in tier2_urls
                and not SKIP_URL_PATTERNS.search(url)):
            tier3_urls.append(url)

    # Dedupe across tiers
    seen = set(crawled_urls)
    ordered_urls = []
    for url_list in [tier1_urls, tier2_urls, tier3_urls]:
        for url in url_list:
            if url not in seen:
                ordered_urls.append(url)
                seen.add(url)

    # --- Wave 1: High-value pages ---
    wave1_budget = min(max_pages - len(fetched_pages), len(ordered_urls), 12)
    wave1_urls = ordered_urls[:wave1_budget]
    log.info("    Wave 1: Crawling %d pages (tier1=%d, tier2=%d candidates)",
             len(wave1_urls), len(tier1_urls), len(tier2_urls))

    wave1_new_links: list[str] = []
    for url in wave1_urls:
        if url in crawled_urls:
            continue
        crawled_urls.add(url)

        # Fetch content
        page = await _fetch_page(crawler, fetch_conf, url)
        if page:
            fetched_pages.append(page)
            log.info("    OK: %s (%d chars)", url[:80], len(page["text"]))

            # Also discover links from this page (for wave 2)
            if EMPLOYEE_URL_PATTERNS.search(url):
                sub_links = await _discover_links_from_page(crawler, fetch_conf, url, domain)
                for link in sub_links:
                    if link not in crawled_urls and link not in all_discovered:
                        wave1_new_links.append(link)
                        all_discovered.add(link)

        if len(fetched_pages) >= max_pages:
            break

    # --- Wave 2: Deeper pages discovered from wave 1 ---
    wave2_budget = max_pages - len(fetched_pages)
    if wave2_budget > 0 and wave1_new_links:
        # Prioritize employee-related from wave2
        wave2_employee = [u for u in wave1_new_links if EMPLOYEE_URL_PATTERNS.search(u)]
        wave2_other = [u for u in wave1_new_links if u not in wave2_employee]
        wave2_urls = (wave2_employee + wave2_other)[:wave2_budget]

        if wave2_urls:
            log.info("    Wave 2: Crawling %d deeper pages", len(wave2_urls))
            for url in wave2_urls:
                if url in crawled_urls:
                    continue
                crawled_urls.add(url)
                page = await _fetch_page(crawler, fetch_conf, url)
                if page:
                    fetched_pages.append(page)
                    log.info("    OK: %s (%d chars)", url[:80], len(page["text"]))
                if len(fetched_pages) >= max_pages:
                    break

    log.info("  Deep crawl done: %d pages fetched from %s", len(fetched_pages), domain)
    return fetched_pages


# ---------------------------------------------------------------------------
# Build direct website URLs for employee-relevant pages
# ---------------------------------------------------------------------------

def build_website_employee_urls(website: str) -> list[str]:
    """Generate candidate employee-related subpage URLs from the company website."""
    if not website:
        return []
    if not website.startswith(("http://", "https://")):
        website = f"https://{website}"
    parsed = urlparse(website)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return [urljoin(base, path) for path in EMPLOYEE_PATHS]


def _build_linkedin_direct_urls(company: str) -> list[str]:
    """Build direct LinkedIn URLs to try for a company."""
    slug = re.sub(r"[^a-z0-9]+", "-", company.lower()).strip("-")
    return [
        f"https://www.linkedin.com/company/{slug}/",
        f"https://www.linkedin.com/company/{slug}/about/",
        f"https://www.linkedin.com/company/{slug}/people/",
    ]


# ---------------------------------------------------------------------------
# Main async orchestrator — single browser session for all work
# ---------------------------------------------------------------------------

async def _research_async(
    company: str,
    website: str,
    fetch_linkedin: bool,
    fetch_website: bool,
    max_pages: int,
    on_progress: callable | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Perform all Google searches and page fetches in a single crawl4ai
    browser session.  Returns (linkedin_results, website_results, fetched_pages).

    Args:
        on_progress: Optional callback(phase, message, detail_dict) called at each step.
    """

    def progress(phase: str, msg: str, **kw):
        """Fire progress callback if provided."""
        if on_progress:
            on_progress(phase, msg, kw)

    progress("init", "Launching headless browser…")

    # ── Browser: headless, no images (faster), anti-detect flags ──
    browser_conf = BrowserConfig(
        headless=True,
        verbose=False,
        text_mode=True,   # disable image loading — big speed win
        extra_args=["--disable-extensions"],
    )

    # ── Search config (Google): lightweight, quick timeout ──
    search_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=20000,
        word_count_threshold=10,
    )

    # ── Fetch config: content-filtered markdown, LXML scraper ──
    fetch_conf = CrawlerRunConfig(
        cache_mode=CacheMode.BYPASS,
        page_timeout=25000,
        word_count_threshold=50,
        scraping_strategy=LXMLWebScrapingStrategy(),
        markdown_generator=DefaultMarkdownGenerator(
            content_filter=PruningContentFilter(
                threshold=0.48,
                threshold_type="fixed",
                min_word_threshold=10,
            ),
        ),
        excluded_tags=["nav", "footer", "header", "script", "style", "aside"],
        exclude_external_links=True,
        exclude_social_media_links=True,
        scan_full_page=True,   # scroll page to trigger lazy-loaded content
    )

    linkedin_results: list[dict] = []
    website_results: list[dict] = []
    fetched_pages: list[dict] = []

    async with AsyncWebCrawler(config=browser_conf) as crawler:
        progress("search", "Browser ready. Starting Google searches…")

        # ── Phase 1: Google searches ──
        if fetch_linkedin:
            query = f'site:linkedin.com/company/ "{company}"'
            progress("search", f"Searching LinkedIn companies…", query=query[:80])
            log.info("  LinkedIn company search: %s", query)
            hits = await _google_search_crawl4ai(query, crawler, search_conf, num_results=5)
            for h in hits:
                h["search_type"] = "linkedin_company"
            linkedin_results.extend(hits)
            progress("search", f"Found {len(hits)} LinkedIn company results")

            await asyncio.sleep(random.uniform(1.0, 2.0))

            query = f'site:linkedin.com/in/ "{company}"'
            progress("search", f"Searching LinkedIn people…", query=query[:80])
            log.info("  LinkedIn people search: %s", query)
            hits = await _google_search_crawl4ai(query, crawler, search_conf, num_results=15)
            for h in hits:
                h["search_type"] = "linkedin_people"
            linkedin_results.extend(hits)
            progress("search", f"Found {len(hits)} LinkedIn people results")

        if fetch_website and website:
            domain = urlparse(website if "://" in website else f"https://{website}").netloc
            if domain:
                await asyncio.sleep(random.uniform(0.5, 1.5))
                query = (
                    f'site:{domain} "{company}" '
                    f'(employees OR team OR staff OR about OR leadership OR people OR careers)'
                )
                progress("search", f"Searching company website: {domain}…", query=query[:80])
                log.info("  Website employee search: %s", query)
                hits = await _google_search_crawl4ai(query, crawler, search_conf, num_results=10)
                for h in hits:
                    h["search_type"] = "website_employees"
                website_results.extend(hits)
                progress("search", f"Found {len(hits)} website search results")

        progress("search", f"Search complete: {len(linkedin_results)} LinkedIn + {len(website_results)} website hits",
                 linkedin_hits=len(linkedin_results), website_hits=len(website_results))

        # ── Phase 2: Fetch LinkedIn pages (parallel via arun_many) ──
        linkedin_urls: list[str] = []
        seen_urls: set[str] = set()

        for r in linkedin_results:
            if (r.get("search_type") == "linkedin_company"
                    and "linkedin.com/company/" in r.get("url", "")):
                url = r["url"]
                if url not in seen_urls:
                    linkedin_urls.append(url)
                    seen_urls.add(url)

        if fetch_linkedin and not linkedin_urls:
            for url in _build_linkedin_direct_urls(company):
                if url not in seen_urls:
                    linkedin_urls.append(url)
                    seen_urls.add(url)

        if linkedin_urls:
            li_urls = linkedin_urls[:3]
            progress("linkedin", f"Fetching {len(li_urls)} LinkedIn pages…", urls=li_urls)
            log.info("  Fetching %d LinkedIn pages via arun_many ...", len(li_urls))
            li_conf = fetch_conf.clone(scan_full_page=False)  # LI doesn't need scroll
            li_results = await crawler.arun_many(urls=li_urls, config=li_conf)
            if not isinstance(li_results, list):
                li_results = [li_results]
            for res in li_results:
                page = _result_to_page(res)
                if page:
                    fetched_pages.append(page)
                    progress("linkedin", f"LinkedIn page: {page['title'][:60]}",
                             url=page["url"][:120], chars=len(page["text"]))
                    log.info("    LinkedIn OK: %s (%d chars)", page["url"][:80], len(page["text"]))
            progress("linkedin", f"LinkedIn fetch done: {len(fetched_pages)} pages with content")

        # ── Phase 3: Deep website crawl via BestFirstCrawlingStrategy ──
        if fetch_website and website:
            homepage = website if website.startswith("http") else f"https://{website}"
            domain = urlparse(homepage).netloc
            website_budget = max(max_pages, 15)

            # Build allowed domain list: include bare + www variant to handle
            # common redirects (e.g. hitt.com → www.hitt.com)
            allowed_domains = [domain]
            if domain.startswith("www."):
                allowed_domains.append(domain[4:])          # www.hitt.com → hitt.com
            else:
                allowed_domains.append(f"www.{domain}")     # hitt.com → www.hitt.com

            progress("website", f"Deep crawling {domain} (budget: {website_budget} pages)…",
                     domain=domain, budget=website_budget)
            log.info("  Deep crawl: %s (budget=%d pages, strategy=BestFirst, domains=%s)",
                     domain, website_budget, allowed_domains)

            deep_conf = fetch_conf.clone(
                deep_crawl_strategy=BestFirstCrawlingStrategy(
                    max_depth=3,
                    max_pages=website_budget,
                    include_external=False,
                    filter_chain=FilterChain([
                        DomainFilter(allowed_domains=allowed_domains),
                    ]),
                    url_scorer=KeywordRelevanceScorer(
                        keywords=EMPLOYEE_KEYWORDS,
                    ),
                ),
            )

            try:
                deep_results = await crawler.arun(url=homepage, config=deep_conf)
                if not isinstance(deep_results, list):
                    deep_results = [deep_results]

                pages_before = len(fetched_pages)
                for res in deep_results:
                    page = _result_to_page(res)
                    if page:
                        fetched_pages.append(page)
                        depth = ""
                        if hasattr(res, "metadata") and res.metadata:
                            depth = f" depth={res.metadata.get('depth', '?')}"
                        progress("website", f"Crawled: {page['title'][:60]}",
                                 url=page["url"][:120], chars=len(page["text"]),
                                 page_num=len(fetched_pages) - pages_before)
                        log.info("    Deep OK: %s (%d chars%s)",
                                 page["url"][:80], len(page["text"]), depth)

                site_pages = len(fetched_pages) - pages_before
                progress("website", f"Website crawl done: {site_pages} pages from {domain}",
                         site_pages=site_pages, total_pages=len(fetched_pages))
                log.info("  Deep crawl done: %d pages from %s", len(deep_results), domain)
            except Exception as exc:
                log.error("  Deep crawl error for %s: %s — falling back to manual", domain, exc)
                progress("website", f"Deep crawl failed, trying fallback…", error=str(exc)[:120])
                # Fallback: try the old manual approach if deep crawl fails
                website_pages = await _deep_crawl_website(
                    crawler, fetch_conf, homepage, company,
                    max_pages=website_budget,
                )
                fetched_pages.extend(website_pages)
                progress("website", f"Fallback crawl done: {len(website_pages)} pages")

    progress("crawl_done", f"All crawling complete: {len(fetched_pages)} pages fetched",
             total_pages=len(fetched_pages),
             linkedin_hits=len(linkedin_results),
             website_hits=len(website_results))
    log.info("  Total: %d pages fetched", len(fetched_pages))
    return linkedin_results, website_results, fetched_pages


def run_research_pipeline(
    company: str,
    website: str,
    fetch_linkedin: bool,
    fetch_website: bool,
    max_pages: int,
    on_progress: callable | None = None,
) -> tuple[list[dict], list[dict], list[dict]]:
    """Synchronous wrapper for the async research pipeline."""
    try:
        return asyncio.run(_research_async(
            company, website, fetch_linkedin, fetch_website, max_pages,
            on_progress=on_progress,
        ))
    except RuntimeError:
        try:
            import nest_asyncio
            nest_asyncio.apply()
            return asyncio.run(_research_async(
                company, website, fetch_linkedin, fetch_website, max_pages,
                on_progress=on_progress,
            ))
        except Exception as exc:
            log.error("  Pipeline error: %s", exc)
            return [], [], []


# ---------------------------------------------------------------------------
# GPT extraction
# ---------------------------------------------------------------------------

RESEARCH_PROMPT = """You are a business intelligence analyst researching the company "{company}".

You have been given search results from LinkedIn and the company's own website.
Your job is to extract employee/staffing information and any revenue signals.

Extract the following as JSON:

1. **estimated_headcount** — Total number of employees.  Use the most authoritative
   figure found (LinkedIn company page > company website > article mentions).
   Use exact numbers when available (e.g. "150", "1,200", "50-100").
   If not found, say "Unknown".

2. **headcount_confidence** — One of: high, medium, low, none.
   "high" = explicit number from LinkedIn company page or official source.
   "medium" = range or indirect estimate.  "low" = vague.  "none" = no data.

3. **employees** — A JSON array of named employees found.  Each entry:
   {{"name": "...", "title": "...", "source": "linkedin|website|article", "url": "..."}}
   Include up to 30 employees.  If none found, use an empty array [].

4. **estimated_revenue** — Revenue if mentioned anywhere in the evidence.
   Use exact figures (e.g. "$50M", "$1.2 billion").  If not found, "Unknown".

5. **revenue_confidence** — One of: high, medium, low, none.

6. **evidence_summary** — 3-8 sentences summarizing ALL employee and revenue evidence.
   Cite each source with title and URL.
   Mention: headcount figures, key leadership, hiring signals, revenue figures,
   contract values, growth indicators.

Respond ONLY with valid JSON (no markdown fences):
{{"estimated_headcount": "...", "headcount_confidence": "...", "employees": [...], "estimated_revenue": "...", "revenue_confidence": "...", "evidence_summary": "..."}}

--- EVIDENCE ({num_sources} sources) ---
{evidence}
"""


def build_evidence_text(
    linkedin_results: list[dict],
    website_search_results: list[dict],
    fetched_pages: list[dict],
) -> str:
    """Compile all evidence into a single text block for GPT."""
    parts = []

    # LinkedIn search results (titles + snippets are valuable even without fetching)
    if linkedin_results:
        parts.append("=== LINKEDIN SEARCH RESULTS ===")
        for i, r in enumerate(linkedin_results, 1):
            parts.append(
                f"[{r.get('search_type', 'linkedin')} #{i}] {r.get('title', '')}\n"
                f"URL: {r.get('url', '')}\n"
                f"Snippet: {r.get('snippet', '')}\n"
            )

    # Website search results
    if website_search_results:
        parts.append("=== COMPANY WEBSITE SEARCH RESULTS ===")
        for i, r in enumerate(website_search_results, 1):
            parts.append(
                f"[website #{i}] {r.get('title', '')}\n"
                f"URL: {r.get('url', '')}\n"
                f"Snippet: {r.get('snippet', '')}\n"
            )

    # Fetched full page content
    if fetched_pages:
        parts.append("=== FETCHED PAGE CONTENT ===")
        for i, p in enumerate(fetched_pages, 1):
            parts.append(
                f'--- Page {i}: "{p.get("title", "")}" ---\n'
                f'URL: {p.get("url", "")}\n'
                f'{p["text"][:8000]}\n'
            )

    return "\n\n".join(parts)


def extract_with_gpt(company: str, evidence_text: str, num_sources: int) -> dict:
    """Call GPT-4o-mini to extract structured employee + revenue data."""
    from openai import OpenAI

    api_key = os.getenv("OPENAI_API_KEY", "")
    if not api_key:
        return {"error": "OPENAI_API_KEY not set"}

    client = OpenAI(api_key=api_key)
    prompt = RESEARCH_PROMPT.format(
        company=company,
        evidence=evidence_text,
        num_sources=num_sources,
    )

    try:
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            temperature=0.1,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You extract structured business intelligence data from web research. "
                        "Respond only with valid JSON. Be thorough — include every employee "
                        "name and title you can find."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            max_tokens=2000,
        )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown fences
        if raw.startswith("```"):
            raw = raw.split("\n", 1)[-1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()
        return json.loads(raw)
    except json.JSONDecodeError:
        log.warning("  GPT response was not valid JSON")
        return {
            "estimated_headcount": "Unknown",
            "headcount_confidence": "none",
            "employees": [],
            "estimated_revenue": "Unknown",
            "revenue_confidence": "none",
            "evidence_summary": f"GPT response was not valid JSON: {raw[:300]}",
        }
    except Exception as exc:
        log.error("  GPT API error: %s", exc)
        return {
            "estimated_headcount": "Unknown",
            "headcount_confidence": "none",
            "employees": [],
            "estimated_revenue": "Unknown",
            "revenue_confidence": "none",
            "evidence_summary": f"API error: {exc}",
        }


# ---------------------------------------------------------------------------
# Main research function
# ---------------------------------------------------------------------------

def research_company(
    company: str,
    website: str = "",
    fetch_linkedin: bool = True,
    fetch_website: bool = True,
    max_pages: int = 8,
    on_progress: callable | None = None,
) -> dict:
    """
    Run the full research pipeline for a single company.

    Args:
        on_progress: Optional callback(phase, message, detail_dict) for progress tracking.

    Returns a dict with:
      - estimated_headcount, headcount_confidence
      - employees (list of {name, title, source, url})
      - estimated_revenue, revenue_confidence
      - evidence_summary
      - search_results (raw search hits for transparency)
      - pages_fetched (count)
    """
    def _progress(phase: str, msg: str, detail: dict | None = None):
        if on_progress:
            on_progress(phase, msg, detail or {})

    log.info("Researching: %s (website: %s)", company, website or "none")
    _progress("start", f"Starting research for {company}")

    # Run entire search + fetch pipeline in one browser session
    linkedin_results, website_results, fetched_pages = run_research_pipeline(
        company=company,
        website=website,
        fetch_linkedin=fetch_linkedin,
        fetch_website=fetch_website,
        max_pages=max_pages,
        on_progress=on_progress,
    )

    # Build evidence and call GPT
    total_sources = len(linkedin_results) + len(website_results) + len(fetched_pages)
    if total_sources == 0:
        log.warning("  No evidence found for %s", company)
        _progress("done", "No evidence found — research complete")
        return {
            "company": company,
            "estimated_headcount": "Unknown",
            "headcount_confidence": "none",
            "employees": [],
            "estimated_revenue": "Unknown",
            "revenue_confidence": "none",
            "evidence_summary": "No search results or pages could be retrieved.",
            "search_results": [],
            "pages_fetched": 0,
        }

    _progress("extract", f"Building evidence from {total_sources} sources…")
    evidence_text = build_evidence_text(linkedin_results, website_results, fetched_pages)
    _progress("extract", f"Sending {len(evidence_text):,} chars to GPT-4o-mini for extraction…")
    extracted = extract_with_gpt(company, evidence_text, total_sources)
    _progress("extract", "GPT extraction complete",
             {"headcount": extracted.get("estimated_headcount", "?"),
              "employees_found": len(extracted.get("employees", []))})

    result = {
        "company": company,
        "estimated_headcount": extracted.get("estimated_headcount", "Unknown"),
        "headcount_confidence": extracted.get("headcount_confidence", "none"),
        "employees": extracted.get("employees", []),
        "estimated_revenue": extracted.get("estimated_revenue", "Unknown"),
        "revenue_confidence": extracted.get("revenue_confidence", "none"),
        "evidence_summary": extracted.get("evidence_summary", ""),
        "search_results": linkedin_results + website_results,
        "pages_fetched": len(fetched_pages),
    }
    _progress("done", "Research complete", {
        "headcount": result["estimated_headcount"],
        "employees_found": len(result["employees"]),
        "revenue": result["estimated_revenue"],
        "pages_fetched": result["pages_fetched"],
    })
    return result


# ---------------------------------------------------------------------------
# Database integration
# ---------------------------------------------------------------------------

def save_research_to_db(dsn: str, result_id: int, company: str, research: dict) -> int:
    """Persist research results into company_evaluations (upsert)."""
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    cur = conn.cursor()

    # Store the employee list as JSON in evidence_summary alongside the text summary
    employee_data = json.dumps(research.get("employees", []), ensure_ascii=False)
    evidence = research.get("evidence_summary", "")
    combined_evidence = f"{evidence}\n\n--- EMPLOYEE LIST ---\n{employee_data}"

    cur.execute(
        """
        INSERT INTO company_evaluations (
            result_id, company,
            estimated_revenue, revenue_confidence,
            estimated_headcount, headcount_confidence,
            evidence_summary, chunks_used, evaluated_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (result_id)
        DO UPDATE SET
            company = EXCLUDED.company,
            estimated_revenue = EXCLUDED.estimated_revenue,
            revenue_confidence = EXCLUDED.revenue_confidence,
            estimated_headcount = EXCLUDED.estimated_headcount,
            headcount_confidence = EXCLUDED.headcount_confidence,
            evidence_summary = EXCLUDED.evidence_summary,
            chunks_used = EXCLUDED.chunks_used,
            evaluated_at = EXCLUDED.evaluated_at
        RETURNING id
        """,
        (
            result_id,
            company,
            research.get("estimated_revenue", "Unknown"),
            research.get("revenue_confidence", "none"),
            research.get("estimated_headcount", "Unknown"),
            research.get("headcount_confidence", "none"),
            combined_evidence,
            research.get("pages_fetched", 0),
            datetime.now(timezone.utc),
        ),
    )
    row_id = cur.fetchone()[0]
    conn.commit()
    cur.close()
    conn.close()
    log.info("  Saved evaluation id=%d for result_id=%d", row_id, result_id)
    return row_id


def get_company_from_db(dsn: str, result_id: int) -> dict | None:
    """Fetch company name + website from results table."""
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("SELECT data FROM results WHERE id = %s", (result_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if not row:
        return None
    data = row[0] if isinstance(row[0], dict) else {}
    return {
        "company": (data.get("title") or "").strip(),
        "website": (data.get("web_site") or "").strip(),
    }


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Research agent: LinkedIn + website employee lookup")
    p.add_argument("--company", type=str, help="Company name to research")
    p.add_argument("--website", type=str, default="", help="Company website URL")
    p.add_argument("--result-id", type=int, help="Result ID in the database (auto-fetches company/website)")
    p.add_argument("--db", type=str, default="construction_test", help="Database name")
    p.add_argument("--save", action="store_true", help="Save results to company_evaluations")
    p.add_argument("--max-pages", type=int, default=8, help="Max pages to fetch")
    p.add_argument("--no-linkedin", action="store_true", help="Skip LinkedIn search")
    p.add_argument("--no-website", action="store_true", help="Skip website search")
    return p


def main():
    args = build_parser().parse_args()

    dsn = f"postgresql://postgres:postgres@localhost:5432/{args.db}"

    company = args.company
    website = args.website
    result_id = args.result_id

    if result_id and not company:
        info = get_company_from_db(dsn, result_id)
        if not info:
            log.error("Result ID %d not found in database %s", result_id, args.db)
            sys.exit(1)
        company = info["company"]
        website = website or info["website"]

    if not company:
        log.error("Either --company or --result-id is required")
        sys.exit(1)

    research = research_company(
        company=company,
        website=website,
        fetch_linkedin=not args.no_linkedin,
        fetch_website=not args.no_website,
        max_pages=args.max_pages,
    )

    # Print results
    print(json.dumps({
        "company": research["company"],
        "estimated_headcount": research["estimated_headcount"],
        "headcount_confidence": research["headcount_confidence"],
        "employee_count": len(research["employees"]),
        "employees": research["employees"][:10],  # show first 10
        "estimated_revenue": research["estimated_revenue"],
        "revenue_confidence": research["revenue_confidence"],
        "evidence_summary": research["evidence_summary"],
        "search_hits": len(research["search_results"]),
        "pages_fetched": research["pages_fetched"],
    }, indent=2, ensure_ascii=False))

    # Save to DB
    if args.save and result_id:
        save_research_to_db(dsn, result_id, company, research)
        print(f"\nSaved to company_evaluations for result_id={result_id}")
    elif args.save and not result_id:
        log.warning("--save requires --result-id to know which row to update")


if __name__ == "__main__":
    main()
