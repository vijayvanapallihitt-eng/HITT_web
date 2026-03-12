from __future__ import annotations

import json
from typing import Any, Mapping

from broker.models import EnrichedLeadRecord, NewsArticle


COMPANY_CANDIDATE_COLS = ("title", "name", "company", "Company", "business_name")


def coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def parse_json_list(raw: Any, default: list | None = None) -> list:
    default = [] if default is None else default
    if raw is None or raw == "":
        return default
    if isinstance(raw, list):
        return raw
    if isinstance(raw, tuple):
        return list(raw)
    if not isinstance(raw, str):
        return default
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return default
    return value if isinstance(value, list) else default


def pick_company_name(row: Mapping[str, Any]) -> str:
    for key in COMPANY_CANDIDATE_COLS:
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def normalize_news_articles(row: Mapping[str, Any]) -> list[NewsArticle]:
    raw_articles = parse_json_list(row.get("news_articles"), [])
    items: list[NewsArticle] = []
    for raw in raw_articles:
        if not isinstance(raw, Mapping):
            continue
        title = str(raw.get("title") or "").strip()
        url = str(raw.get("url") or "").strip()
        if not title and not url:
            continue
        items.append(NewsArticle(title=title, url=url))
    return items


def news_count_from_row(row: Mapping[str, Any]) -> int:
    count = coerce_int(row.get("news_count"), default=-1)
    if count >= 0:
        return count
    return len(normalize_news_articles(row))


def sort_rows_by_news_count(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(rows, key=news_count_from_row, reverse=True)


def normalize_enriched_row(row: Mapping[str, Any]) -> EnrichedLeadRecord:
    news_articles = normalize_news_articles(row)
    news_count = news_count_from_row(row)
    raw_row = dict(row) if isinstance(row, dict) else {key: row[key] for key in row}
    return EnrichedLeadRecord(
        company=pick_company_name(row),
        news_query=str(row.get("news_query") or "").strip(),
        news_articles=tuple(news_articles),
        news_status=str(row.get("news_search_status") or "").strip(),
        news_count=news_count,
        row_id=str(row.get("id") or "").strip(),
        raw_row=raw_row,
    )
