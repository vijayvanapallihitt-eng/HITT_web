from __future__ import annotations


def chunk_text(text: str, chunk_words: int, overlap_words: int) -> list[str]:
    words = text.split()
    if not words:
        return []
    step = max(1, chunk_words - overlap_words)
    chunks: list[str] = []
    for start in range(0, len(words), step):
        chunk = words[start : start + chunk_words]
        if not chunk:
            break
        if len(chunk) < 40 and chunks:
            break
        chunks.append(" ".join(chunk))
        if start + chunk_words >= len(words):
            break
    return chunks
