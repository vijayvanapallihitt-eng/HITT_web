from __future__ import annotations

import re
from typing import Any

from broker.embeddings.factory import simple_hash_embedding


def build_where(source_type: str, company: str) -> dict[str, Any] | None:
    clauses = []
    if source_type in {"news"}:
        clauses.append({"source_type": source_type})
    if company:
        clauses.append({"company": company})
    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return {"$and": clauses}


def truncate(text: str, max_chars: int = 420) -> str:
    return (text[: max_chars - 3] + "...") if len(text) > max_chars else text


def chunk_has_company_evidence(company: str, meta: dict, doc: str) -> bool:
    company = (company or "").strip()
    if not company:
        return True

    meta = meta or {}
    title = meta.get("page_title") or meta.get("article_title") or ""
    url = meta.get("url_fetched") or ""
    text = doc or ""
    hay = "\n".join([title, url, text])

    if company in hay:
        return True

    company_low = company.lower()
    hay_low = hay.lower()
    if company_low not in hay_low:
        return False

    acronym_tokens = [
        token
        for token in re.findall(r"[A-Za-z0-9]+", company)
        if len(token) >= 2 and token.upper() == token and any(ch.isalpha() for ch in token)
    ]
    if acronym_tokens:
        return any(token in hay for token in acronym_tokens)

    return True


def query_collection(
    col,
    client,
    query_text: str,
    query_embedding_backend: str,
    embedding_model: str,
    simple_dim: int,
    source_type: str,
    company: str,
    k: int,
):
    where = build_where(
        source_type="" if source_type == "all" else source_type,
        company=company.strip(),
    )

    if query_embedding_backend == "openai":
        if client is None:
            raise SystemExit("OpenAI client is required for --query-embedding-backend openai.")
        emb = client.embeddings.create(model=embedding_model, input=[query_text]).data[0].embedding
    else:
        emb = simple_hash_embedding(query_text, simple_dim)

    try:
        result = col.query(
            query_embeddings=[emb],
            n_results=k,
            where=where,
            include=["documents", "metadatas", "distances"],
        )
    except Exception as exc:
        msg = str(exc)
        if "dimension" in msg.lower():
            raise SystemExit(
                "Embedding dimension mismatch. If this collection was ingested with "
                "--embedding-backend simple, query with --query-embedding-backend simple --simple-dim 384. "
                "If ingested with OpenAI embeddings, query with --query-embedding-backend openai."
            ) from exc
        raise

    ids = (result.get("ids") or [[]])[0]
    docs = (result.get("documents") or [[]])[0]
    metas = (result.get("metadatas") or [[]])[0]
    dists = (result.get("distances") or [[]])[0]
    return ids, docs, metas, dists
