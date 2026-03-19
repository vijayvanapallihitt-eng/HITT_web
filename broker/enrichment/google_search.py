"""
Google Search via Playwright in stealth mode.

Mimics real human behaviour: visits google.com homepage, types the query
character-by-character with realistic delays, presses Enter, then parses
the results page.  This avoids the CAPTCHA that Google returns when you
navigate directly to ``/search?q=…``.
"""
from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path

from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync

logger = logging.getLogger(__name__)

_LOG_DIR = Path("runtime/logs")


@dataclass
class SearchResult:
    title: str
    url: str
    snippet: str = ""


# Realistic Chrome UA strings (rotated randomly)
_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

_GOOGLE_SKIP = frozenset([
    "google.com", "google.co", "gstatic.com", "googleapis.com",
    "youtube.com", "youtu.be", "accounts.google", "support.google",
    "translate.google", "webcache.google", "maps.google",
    "play.google", "chrome.google", "policies.google",
])


def _is_google_link(href: str) -> bool:
    for d in _GOOGLE_SKIP:
        if d in href:
            return True
    return False


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def google_search(
    query: str,
    *,
    max_results: int = 10,
    timeout_ms: int = 20_000,
    headless: bool = True,
    on_progress=None,
) -> list[SearchResult]:
    """Run a Google search and return organic results.

    Visits google.com, types the query like a human, submits, and parses
    the results page.  Uses a persistent browser profile so cookies carry
    over between runs (avoids repeated consent prompts & builds trust).
    """

    def _log(msg: str):
        logger.info(msg)
        if on_progress:
            on_progress(msg)

    results: list[SearchResult] = []

    # Persistent profile directory — cookies / localStorage survive across runs
    profile_dir = Path("runtime/browser_profile")
    profile_dir.mkdir(parents=True, exist_ok=True)

    with sync_playwright() as pw:
        browser = pw.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--window-size=1366,768",
            ],
            viewport={"width": 1366, "height": 768},
            user_agent=random.choice(_USER_AGENTS),
            locale="en-US",
            timezone_id="America/New_York",
            geolocation={"latitude": 40.7128, "longitude": -74.0060},
            permissions=["geolocation"],
        )

        browser.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', { get: () => false });
            Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
            Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
            window.chrome = { runtime: {} };
        """)

        page = browser.new_page()
        stealth_sync(page)
        page.set_default_timeout(timeout_ms)

        # ── 1. Visit google.com homepage ──────────────────────────
        _log(f"Searching Google for: {query!r}")
        try:
            page.goto("https://www.google.com", wait_until="domcontentloaded")
        except Exception as exc:
            _log(f"Navigation error: {exc}")
            browser.close()
            return results

        time.sleep(random.uniform(1.0, 2.5))

        # ── 2. Handle cookie-consent ──────────────────────────────
        _handle_consent(page)

        # ── 3. Type query into search box ─────────────────────────
        search_box = None
        for sel in [
            "textarea[name='q']", "input[name='q']",
            "[aria-label='Search']", "#APjFqb",
        ]:
            try:
                search_box = page.wait_for_selector(sel, timeout=5000)
                if search_box:
                    break
            except Exception:
                continue

        if not search_box:
            _log("Could not find Google search box")
            _save_debug(page, "no_searchbox")
            browser.close()
            return results

        search_box.click()
        time.sleep(random.uniform(0.2, 0.5))

        # Type character-by-character with realistic delays
        for ch in query:
            page.keyboard.type(ch, delay=random.randint(25, 100))
            if random.random() < 0.04:
                time.sleep(random.uniform(0.15, 0.4))

        time.sleep(random.uniform(0.3, 0.8))

        # ── 4. Submit ─────────────────────────────────────────────
        page.keyboard.press("Enter")

        try:
            page.wait_for_selector(
                "div#search, div#rso, div#main, div[data-async-context]",
                timeout=12000,
            )
        except Exception:
            _log("Timeout waiting for search results")
            _save_debug(page, "no_results")

        time.sleep(random.uniform(1.0, 2.0))

        # ── 5. CAPTCHA check ─────────────────────────────────────
        body_text = ""
        try:
            body_text = page.inner_text("body")
        except Exception:
            pass
        if "unusual traffic" in body_text.lower() or "captcha" in body_text.lower():
            _log("Google CAPTCHA detected — waiting 30s and retrying once…")
            _save_debug(page, "captcha")
            page.close()

            # Wait, then retry once with a fresh page
            time.sleep(30)
            page = browser.new_page()
            stealth_sync(page)
            page.set_default_timeout(timeout_ms)

            try:
                page.goto("https://www.google.com", wait_until="domcontentloaded")
                time.sleep(random.uniform(2.0, 4.0))
                _handle_consent(page)

                sb2 = None
                for sel in ["textarea[name='q']", "input[name='q']", "#APjFqb"]:
                    try:
                        sb2 = page.wait_for_selector(sel, timeout=5000)
                        if sb2:
                            break
                    except Exception:
                        continue

                if sb2:
                    sb2.click()
                    time.sleep(random.uniform(0.3, 0.6))
                    for ch in query:
                        page.keyboard.type(ch, delay=random.randint(40, 130))
                    time.sleep(random.uniform(0.5, 1.0))
                    page.keyboard.press("Enter")
                    try:
                        page.wait_for_selector("div#search, div#rso", timeout=12000)
                    except Exception:
                        pass
                    time.sleep(random.uniform(1.0, 2.0))

                    body2 = ""
                    try:
                        body2 = page.inner_text("body")
                    except Exception:
                        pass
                    if "unusual traffic" not in body2.lower():
                        results = _parse_results(page, max_results)
                        if not results:
                            results = _parse_results_fallback(page, max_results)
                    else:
                        _log("CAPTCHA persists after retry — IP may be rate-limited")
                        _save_debug(page, "captcha_retry")
            except Exception as exc:
                _log(f"Retry failed: {exc}")

            page.close()
            browser.close()
            _log(f"Found {len(results)} results for: {query!r}")
            return results

        # ── 6. Parse results ──────────────────────────────────────
        results = _parse_results(page, max_results)
        if not results:
            _log("Primary selectors empty — trying fallback")
            _save_debug(page, "empty_primary")
            results = _parse_results_fallback(page, max_results)

        _log(f"Found {len(results)} results for: {query!r}")
        page.close()
        browser.close()

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _handle_consent(page):
    """Click 'Accept all' on the Google cookie-consent dialog if present."""
    for sel in [
        "button#L2AGLb",
        'button[aria-label="Accept all"]',
        'button:has-text("Accept all")',
        'button:has-text("I agree")',
        'form[action*="consent"] button',
        'div[role="dialog"] button:first-of-type',
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                time.sleep(random.uniform(0.5, 1.0))
                return
        except Exception:
            continue


def _parse_results(page, max_results: int) -> list[SearchResult]:
    """Parse organic results using multiple selector strategies."""
    selectors = [
        "div#rso div.g a[href]",
        "div#search div.g a[href]",
        "#rso a[href][data-ved]",
        "#search a[href][data-ved]",
        "#search a[href][ping]",
        "a[jsname][data-ved][href]",
        "div[data-hveid] a[href]",
    ]
    seen: set[str] = set()
    results: list[SearchResult] = []

    for selector in selectors:
        if len(results) >= max_results:
            break
        try:
            links = page.query_selector_all(selector)
        except Exception:
            continue

        for link in links:
            if len(results) >= max_results:
                break
            href = link.get_attribute("href") or ""
            if not href.startswith("http") or _is_google_link(href) or href in seen:
                continue
            seen.add(href)
            title = _extract_title(link)
            snippet = _extract_snippet(link)
            if title or snippet:
                results.append(SearchResult(title=title, url=href, snippet=snippet))

    return results


def _parse_results_fallback(page, max_results: int) -> list[SearchResult]:
    """Broad fallback — grab all external links with visible text."""
    seen: set[str] = set()
    results: list[SearchResult] = []
    try:
        all_links = page.query_selector_all("a[href]")
    except Exception:
        return results

    for link in all_links:
        if len(results) >= max_results:
            break
        href = link.get_attribute("href") or ""
        if not href.startswith("http") or _is_google_link(href) or href in seen:
            continue
        seen.add(href)
        title = ""
        try:
            title = (link.inner_text() or "").strip()[:150]
        except Exception:
            pass
        if title:
            results.append(SearchResult(title=title, url=href, snippet=""))

    return results


def _extract_title(link) -> str:
    try:
        h3 = link.query_selector("h3")
        if h3:
            return h3.inner_text().strip()
    except Exception:
        pass
    try:
        return (link.get_attribute("aria-label") or "").strip()
    except Exception:
        return ""


def _extract_snippet(link) -> str:
    try:
        parent = link.evaluate_handle(
            "el => el.closest('div.g') || el.closest('[data-hveid]')"
        )
        if parent:
            el = parent.as_element()
            if el:
                for sel in [
                    "div[data-sncf]", "div.VwiC3b", "span.aCOpRe",
                    "div[style*='line-clamp']", "div.IsZvec",
                ]:
                    snip = el.query_selector(sel)
                    if snip:
                        return snip.inner_text().strip()
    except Exception:
        pass
    return ""


def _save_debug(page, label: str):
    """Save screenshot + HTML for debugging (silent)."""
    _LOG_DIR.mkdir(parents=True, exist_ok=True)
    try:
        page.screenshot(path=str(_LOG_DIR / f"google_{label}.png"), full_page=True)
    except Exception:
        pass
    try:
        (_LOG_DIR / f"google_{label}.html").write_text(
            page.content(), encoding="utf-8"
        )
    except Exception:
        pass
