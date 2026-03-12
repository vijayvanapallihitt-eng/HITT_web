from __future__ import annotations

from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Literal


SourceType = Literal["news"]


@dataclass(frozen=True)
class NewsArticle:
    title: str
    url: str


@dataclass(frozen=True)
class EnrichedLeadRecord:
    company: str
    news_query: str = ""
    news_articles: tuple[NewsArticle, ...] = ()
    news_status: str = ""
    news_count: int = 0
    row_id: str = ""
    raw_row: dict[str, Any] = field(default_factory=dict, repr=False, compare=False)


@dataclass(frozen=True)
class LinkCandidateRecord:
    result_id: int | str
    source_type: SourceType
    query_text: str = ""
    url_discovered: str = ""
    title_discovered: str = ""
    discovery_status: str = ""
    discovered_at: datetime | None = None
    id: int | None = None


@dataclass(frozen=True)
class DocumentRecord:
    link_candidate_id: int
    url_fetched: str = ""
    page_title: str = ""
    fetch_status: str = ""
    http_status: int | None = None
    text_hash: str = ""
    raw_text: str = ""
    fetched_at: datetime | None = None
    id: int | None = None


@dataclass(frozen=True)
class DocumentChunkRecord:
    document_id: int
    chunk_index: int
    chunk_text: str = ""
    chunk_hash: str = ""
    embedding_model: str = ""
    embedded_at: datetime | None = None
    id: int | None = None


@dataclass(frozen=True)
class ChromaChunkMetadata:
    result_id: int | str
    company: str
    source_type: SourceType
    link_candidate_id: int
    document_id: int
    chunk_index: int
    url_fetched: str = ""
    page_title: str = ""
