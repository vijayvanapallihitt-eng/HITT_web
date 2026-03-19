"""Vectors router — ChromaDB collection stats + semantic search + AI answer."""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Query
from pydantic import BaseModel

router = APIRouter()
log = logging.getLogger(__name__)

CHROMA_BASE = Path(os.getenv("CHROMA_DIR", str(Path(__file__).resolve().parent.parent.parent / "runtime" / "chroma")))
LEGACY_DIR = CHROMA_BASE / "chroma_smoke_db"
DEFAULT_COLLECTION = "construction_docs_openai1536_live"
EMBEDDING_MODEL = "text-embedding-3-small"
ANSWER_MODEL = "gpt-4o-mini"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _discover_chroma_dirs() -> list[tuple[str, Path]]:
    """Return [(label, path)] for every ChromaDB persist directory."""
    dirs: list[tuple[str, Path]] = []
    if not CHROMA_BASE.exists():
        return dirs
    for child in sorted(CHROMA_BASE.iterdir()):
        if not child.is_dir():
            continue
        if child.name.startswith("db_"):
            dirs.append((child.name[3:], child))
        else:
            dirs.append((child.name, child))
    return dirs


def _resolve_persist_dir(chroma_dir: str, collection_name: str) -> Path:
    """Pick the right ChromaDB persist directory."""
    import chromadb
    if chroma_dir:
        return Path(chroma_dir)
    for _label, dirpath in _discover_chroma_dirs():
        try:
            client = chromadb.PersistentClient(path=str(dirpath))
            client.get_collection(collection_name)
            return dirpath
        except Exception:
            continue
    return LEGACY_DIR


def _get_openai_client():
    import openai
    return openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY", ""))


def _embed(text: str) -> list[float]:
    resp = _get_openai_client().embeddings.create(input=[text], model=EMBEDDING_MODEL)
    return resp.data[0].embedding


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class SearchRequest(BaseModel):
    query: str
    n_results: int = 10
    collection: str = DEFAULT_COLLECTION
    chroma_dir: str = ""
    company: str = ""           # filter to a specific company
    summarize: bool = False     # generate AI answer from results


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/collections")
def list_collections():
    """List all ChromaDB collections across all per-database stores."""
    import chromadb
    all_collections: list[dict] = []
    for label, dirpath in _discover_chroma_dirs():
        try:
            client = chromadb.PersistentClient(path=str(dirpath))
            for c in client.list_collections():
                all_collections.append({
                    "name": c.name,
                    "count": c.count(),
                    "db": label,
                    "chroma_dir": str(dirpath),
                })
        except Exception:
            continue
    return {"collections": all_collections}


@router.get("/companies")
def list_vector_companies(
    collection: str = DEFAULT_COLLECTION,
    chroma_dir: str = "",
):
    """Return sorted list of unique company names stored in a collection."""
    import chromadb
    persist_dir = _resolve_persist_dir(chroma_dir, collection)
    if not persist_dir.exists():
        return {"companies": []}
    client = chromadb.PersistentClient(path=str(persist_dir))
    try:
        col = client.get_collection(collection)
    except Exception:
        return {"companies": []}

    metas = col.get(include=["metadatas"])["metadatas"]
    companies = sorted({m.get("company", "") for m in metas if m.get("company")})
    return {"companies": companies, "count": len(companies)}


@router.post("/search")
def vector_search(req: SearchRequest):
    """Semantic search with optional company filter and AI summary."""
    import chromadb

    persist_dir = _resolve_persist_dir(req.chroma_dir, req.collection)
    if not persist_dir.exists():
        return {"results": [], "error": "ChromaDB directory not found"}

    query_embedding = _embed(req.query)

    client = chromadb.PersistentClient(path=str(persist_dir))
    try:
        collection = client.get_collection(req.collection)
    except Exception:
        return {"results": [], "error": f"Collection '{req.collection}' not found"}

    # Build query kwargs
    query_kwargs: dict = {
        "query_embeddings": [query_embedding],
        "n_results": req.n_results,
        "include": ["documents", "metadatas", "distances"],
    }
    if req.company:
        query_kwargs["where"] = {"company": req.company}

    results = collection.query(**query_kwargs)

    items = []
    if results and results["ids"]:
        for i, doc_id in enumerate(results["ids"][0]):
            items.append({
                "id": doc_id,
                "document": (results["documents"][0][i] if results["documents"] else "")[:800],
                "metadata": results["metadatas"][0][i] if results["metadatas"] else {},
                "distance": results["distances"][0][i] if results["distances"] else None,
            })

    response: dict = {
        "query": req.query,
        "collection": req.collection,
        "company_filter": req.company or None,
        "results": items,
    }

    # AI summary with citations
    if req.summarize and items:
        response["answer"] = _generate_answer(req.query, items)

    return response


# ---------------------------------------------------------------------------
# AI answer generation
# ---------------------------------------------------------------------------

def _generate_answer(question: str, chunks: list[dict]) -> dict:
    """Call GPT to produce a summary answer with numbered citations."""
    # Build context block with citation numbers
    context_parts = []
    for i, chunk in enumerate(chunks, 1):
        meta = chunk.get("metadata", {})
        source_label = meta.get("page_title") or meta.get("url_fetched") or meta.get("company") or f"Source {i}"
        context_parts.append(
            f"[{i}] {source_label}\n{chunk['document']}"
        )
    context_text = "\n\n---\n\n".join(context_parts)

    system_prompt = (
        "You are a helpful research assistant for a construction industry database. "
        "Answer the user's question using ONLY the provided sources. "
        "Be specific and cite your sources using bracket notation like [1], [2], etc. "
        "If the sources don't contain enough information, say so. "
        "Keep your answer concise but thorough (2-4 paragraphs max)."
    )
    user_prompt = (
        f"Question: {question}\n\n"
        f"Sources:\n{context_text}\n\n"
        "Provide a clear answer with citations."
    )

    try:
        client = _get_openai_client()
        resp = client.chat.completions.create(
            model=ANSWER_MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.2,
            max_tokens=1000,
        )
        answer_text = resp.choices[0].message.content.strip()

        # Build citations list
        citations = []
        for i, chunk in enumerate(chunks, 1):
            meta = chunk.get("metadata", {})
            citations.append({
                "index": i,
                "company": meta.get("company", ""),
                "title": meta.get("page_title", ""),
                "url": meta.get("url_fetched", ""),
                "source_type": meta.get("source_type", ""),
            })

        return {"text": answer_text, "citations": citations, "model": ANSWER_MODEL}

    except Exception as e:
        log.exception("AI answer generation failed")
        return {"text": f"Error generating answer: {e}", "citations": [], "model": ANSWER_MODEL}
