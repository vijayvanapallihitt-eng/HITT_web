from __future__ import annotations

from broker.query.retrieval import truncate


def format_context_block(
    ids: list[str], docs: list[str], metas: list[dict], max_doc_chars: int
) -> str:
    parts: list[str] = []
    for i, (doc_id, doc, meta) in enumerate(zip(ids, docs, metas), start=1):
        meta = meta or {}
        parts.append(
            "\n".join(
                [
                    f"[{i}] id={doc_id}",
                    f"source_type={meta.get('source_type', '')}",
                    f"company={meta.get('company', '')}",
                    f"url={meta.get('url_fetched', '')}",
                    f"title={meta.get('page_title', '') or meta.get('article_title', '')}",
                    f"text={truncate(doc or '', max_doc_chars)}",
                ]
            )
        )
    return "\n\n".join(parts)


def build_answer_prompt(
    question: str,
    context_block: str,
    summary_mode: bool,
    target_company: str = "",
) -> str:
    if summary_mode:
        company_line = f"Target company: {target_company}\n" if target_company else ""
        return (
            "You are a grounded research assistant.\n"
            "Use ONLY the provided context chunks.\n"
            "Ignore obviously irrelevant chunks (wrong company/domain/topic) and mention they were ignored.\n"
            "Do NOT assume a company is referenced just because words in its name appear as common words.\n"
            "Treat ambiguous phrase matches as weak evidence unless the text clearly refers to a business entity.\n"
            "If evidence is weak, explicitly say the evidence is insufficient.\n"
            "Cite chunk numbers like [1], [2] for every factual claim.\n\n"
            f"{company_line}"
            "Output format:\n"
            "1) Executive Summary (3-6 bullets)\n"
            "2) News Signals\n"
            "3) Risks / Data Gaps\n"
            "4) Sources Used (chunk refs)\n\n"
            f"Question: {question}\n\n"
            f"Context:\n{context_block}"
        )

    company_line = f"Target company: {target_company}\n" if target_company else ""
    return (
        "Answer the user's question using only the provided context chunks.\n"
        "If the context is insufficient, say so clearly.\n"
        "Do NOT assume a company is referenced just because its words appear in a sentence.\n"
        "For ambiguous names, require explicit business-entity evidence.\n"
        "Cite chunk numbers like [1], [2] for factual claims.\n\n"
        f"{company_line}"
        f"Question: {question}\n\n"
        f"Context:\n{context_block}"
    )


def call_openai_answer(
    client,
    model: str,
    question: str,
    context_block: str,
    temperature: float,
    summary_mode: bool,
    target_company: str = "",
) -> str:
    prompt = build_answer_prompt(
        question=question,
        context_block=context_block,
        summary_mode=summary_mode,
        target_company=target_company,
    )
    system_msg = "You are a grounded assistant. Use only provided context and cite chunk numbers."

    try:
        response = client.responses.create(
            model=model,
            temperature=temperature,
            input=prompt,
        )
        return (response.output_text or "").strip()
    except Exception:
        resp = client.chat.completions.create(
            model=model,
            temperature=temperature,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt},
            ],
        )
        return (resp.choices[0].message.content or "").strip()
