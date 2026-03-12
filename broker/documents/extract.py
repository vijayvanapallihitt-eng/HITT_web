from __future__ import annotations

import re

from bs4 import BeautifulSoup


def extract_text_from_html(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(
        [
            "script",
            "style",
            "noscript",
            "svg",
            "iframe",
            "header",
            "footer",
            "nav",
            "aside",
            "form",
        ]
    ):
        tag.decompose()

    title = soup.title.get_text(" ", strip=True) if soup.title else ""
    article = soup.find("article")
    article_text = article.get_text(" ", strip=True) if article else ""
    body_text = soup.body.get_text(" ", strip=True) if soup.body else soup.get_text(" ", strip=True)
    text = article_text if len(article_text) >= 400 else body_text
    text = re.sub(r"\s+", " ", text).strip()
    return title, text
