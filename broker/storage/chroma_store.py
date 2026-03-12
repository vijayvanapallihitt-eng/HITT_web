from __future__ import annotations

from typing import Any

import chromadb


def get_persistent_client(persist_dir: str):
    return chromadb.PersistentClient(path=persist_dir)


def get_or_create_collection(persist_dir: str, collection_name: str):
    client = get_persistent_client(persist_dir)
    return client.get_or_create_collection(
        name=collection_name,
        metadata={"hnsw:space": "cosine"},
    )


def upsert_chunks(
    collection,
    ids: list[str],
    documents: list[str],
    metadatas: list[dict[str, Any]],
    embeddings: list[list[float]],
) -> None:
    collection.upsert(
        ids=ids,
        documents=documents,
        metadatas=metadatas,
        embeddings=embeddings,
    )
