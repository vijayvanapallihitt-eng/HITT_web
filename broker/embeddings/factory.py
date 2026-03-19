from __future__ import annotations

import hashlib
import math
import os
import re
from typing import Callable

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None


def load_env_from_file(env_file: str) -> None:
    if load_dotenv is None:
        return
    if env_file:
        load_dotenv(dotenv_path=env_file, override=False)
    else:
        load_dotenv(override=False)


def get_openai_api_key() -> str | None:
    for name in ("OPENAI_API_KEY", "OPENAI_KEY", "OPENAI_TOKEN"):
        value = os.getenv(name)
        if value:
            return value
    return None


def simple_hash_embedding(text: str, dim: int) -> list[float]:
    vec = [0.0] * dim
    for token in re.findall(r"[A-Za-z0-9_]+", text.lower()):
        digest = hashlib.blake2b(token.encode("utf-8"), digest_size=16).digest()
        idx = int.from_bytes(digest[:4], "big") % dim
        sign = -1.0 if (digest[4] & 1) else 1.0
        vec[idx] += sign
    norm = math.sqrt(sum(value * value for value in vec)) or 1.0
    return [value / norm for value in vec]


def get_embedder(backend: str, model: str, dim: int) -> Callable[[list[str]], list[list[float]]]:
    if backend == "simple":
        return lambda texts: [simple_hash_embedding(text, dim) for text in texts]

    if backend == "chroma-default":
        from chromadb.utils.embedding_functions import DefaultEmbeddingFunction

        ef = DefaultEmbeddingFunction()
        return lambda texts: [list(item) for item in ef(texts)]

    if backend == "sentence-transformers":
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            raise SystemExit(
                "sentence-transformers is not installed. Install with: "
                "python -m pip install sentence-transformers"
            ) from exc
        model_client = SentenceTransformer(model)
        return lambda texts: model_client.encode(texts, normalize_embeddings=True).tolist()

    if backend == "openai":
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise SystemExit(
                "openai package is not installed. Install with: "
                "python -m pip install openai"
            ) from exc
        api_key = get_openai_api_key()
        if not api_key:
            raise SystemExit("OpenAI key not set. Use OPENAI_API_KEY (or OPENAI_KEY) in env/.env.")
        client = OpenAI(api_key=api_key)

        # text-embedding-3-small supports up to 8191 tokens.
        # Worst case: 1 token per char (URLs, code, special chars).
        # Safe limit: 6000 chars guarantees we stay well under 8191 tokens.
        MAX_CHARS = 6_000

        def _embed_openai(texts: list[str]) -> list[list[float]]:
            safe_texts = [t[:MAX_CHARS] if len(t) > MAX_CHARS else t for t in texts]
            res = client.embeddings.create(model=model, input=safe_texts)
            return [item.embedding for item in res.data]

        return _embed_openai

    raise SystemExit(f"Unknown embedding backend: {backend}")
