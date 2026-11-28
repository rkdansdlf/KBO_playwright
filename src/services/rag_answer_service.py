"""Turn retrieved chunks into a grounded natural-language answer.

The retrieval stack could already find the rows -- ``QueryRouter``,
``StructuredRetriever`` and ``HybridRetriever`` -- and the API endpoint returned
them with the query terms bolded. What it never did was *answer*: ``rag_ask``
listed snippets, and when retrieval came back empty it said "no results found"
even though the data existed. A chatbot built on that reads like a search page.

Two rules shape this layer, and both come from the shape of this corpus:

* **Only what the evidence says.** The KBO record has holes the repository
  cannot fill (2001~2007 game-by-game box scores: KBO's robots.txt closes the
  only source, and Naver never served those seasons). A model asked about them
  without instruction will produce a plausible score. So the instruction is
  fixed here rather than left to a caller: use only the evidence, and when the
  evidence is silent, say so. "I cannot find that in the records" is the correct
  answer to a question the corpus cannot support.
* **Silence is not an option.** The retrieved chunks are valid even when the
  model call fails, so a failure falls back to showing them and names why. The
  caller can then tell "no evidence" from "no summary".

The OpenRouter call follows ``metadata_enrichment_service``: bearer token from
the environment, ``choices[0].message.content`` out.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

logger = logging.getLogger(__name__)

OPENROUTER_CHAT_URL = "https://openrouter.ai/api/v1/chat/completions"

#: Default model for answering. Separate from the embedding model on purpose --
#: a chat model and an embedding model are different contracts.
DEFAULT_ANSWER_MODEL = "openai/gpt-4o-mini"

#: How many chunks are pasted into the prompt. Sources are listed for all of
#: them; this only bounds what the model reads, so a wide retrieval cannot push
#: the question out of the context window.
DEFAULT_MAX_CHUNKS = 8

#: The rule that makes an incomplete corpus safe to ask about. Kept as a module
#: constant so a test can assert the wording rather than paraphrase it.
ANSWER_RULES = (
    "당신은 KBO 기록 데이터베이스를 근거로 답하는 도우미입니다.\n"
    "다음 규칙을 반드시 지키세요.\n"
    "1. 아래 [근거]에 적힌 내용만 사용해 답하세요.\n"
    "2. 근거에 없는 내용은 답하지 마세요. 추측하거나 지어내지 마세요. "
    "당신의 배경지식으로 빈칸을 채우지 마세요.\n"
    "3. 근거에 답이 없으면 '제공된 기록에서 확인할 수 없습니다'라고만 답하세요.\n"
    "4. 연도·선수·팀을 함께 말해 어느 기록에 근거했는지 드러내세요.\n"
    "5. 2~3문장으로 간결하게 답하세요."
)

NOT_FOUND_ANSWER = "제공된 기록에서 확인할 수 없습니다."


@dataclass(frozen=True)
class RagAnswer:
    """The answer, its evidence, and how it was produced.

    ``llm_used`` and ``fallback_reason`` exist so a caller can tell a real answer
    from a fallback listing: they read the same to a person skimming the text.
    """

    answer: str
    sources: list[dict[str, Any]]
    llm_used: bool
    fallback_reason: str | None


def _source_entry(chunk: object) -> dict[str, Any]:
    """Read the fields the API returns for one retrieved chunk."""
    content = str(getattr(chunk, "content", "") or "")
    return {
        "title": getattr(chunk, "title", None) or content[:60],
        "source_url": getattr(chunk, "source_url", None),
        "document_type": getattr(chunk, "category", None),
        "snippet": content[:240],
    }


def _evidence_block(chunks: Sequence[object], limit: int) -> str:
    lines: list[str] = []
    for index, chunk in enumerate(chunks[:limit], start=1):
        content = str(getattr(chunk, "content", "") or "").strip()
        title = str(getattr(chunk, "title", "") or "").strip()
        lines.append(f"[{index}] {title}\n{content}")
    return "\n\n".join(lines)


def _fallback_answer(chunks: Sequence[object]) -> str:
    """Show the evidence when no summary could be produced.

    The chunks were retrieved by a real query, so they are worth showing even
    unsorted by a model -- withholding them would turn a model outage into an
    empty answer.
    """
    lines = [f"- {getattr(chunk, 'title', '') or ''}: {str(getattr(chunk, 'content', ''))[:200]}" for chunk in chunks]
    return "요약을 만들지 못했습니다. 검색된 기록은 다음과 같습니다.\n" + "\n".join(lines)


class RagAnswerService:
    """Compose an answer from retrieved chunks using a chat model."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str | None = None,
        client_factory: Callable[[], Any] | None = None,
        timeout: float = 30.0,
    ) -> None:
        """Configure the service.

        Args:
            api_key: OpenRouter key. Read from ``OPENROUTER_API_KEY`` when omitted.
            model: Chat model name. Read from ``ANSWER_MODEL`` when omitted.
            client_factory: Builds the HTTP client. Injectable so tests do not
                need a socket.
            timeout: Per-request timeout in seconds.

        """
        from src.config.env_loader import load_project_env

        load_project_env()
        self.api_key = api_key if api_key is not None else os.getenv("OPENROUTER_API_KEY", "")
        self.model = model or os.getenv("ANSWER_MODEL", DEFAULT_ANSWER_MODEL)
        self.timeout = timeout
        self._client_factory = client_factory or self._default_client

    def _default_client(self) -> httpx.Client:
        return httpx.Client(timeout=self.timeout)

    @staticmethod
    def _fallback(chunks: Sequence[object], sources: list[dict[str, Any]], reason: str) -> RagAnswer:
        return RagAnswer(answer=_fallback_answer(chunks), sources=sources, llm_used=False, fallback_reason=reason)

    def _call_model(self, payload: dict[str, Any]) -> tuple[str | None, str | None]:
        """Call the chat endpoint and return ``(text, failure_reason)``.

        One place decides what counts as a usable answer, so every failure mode
        arrives at the caller as the same shape. ``text`` is None exactly when
        ``failure_reason`` is set.
        """
        try:
            with self._client_factory() as client:
                response = client.post(
                    OPENROUTER_CHAT_URL,
                    json=payload,
                    headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                )
        except Exception as exc:  # noqa: BLE001
            logger.warning("RAG answer call failed: %s", type(exc).__name__)
            return None, f"transport_error:{type(exc).__name__}"

        if response.status_code != HTTPStatus.OK:
            logger.warning("RAG answer call returned %s", response.status_code)
            return None, f"http_{response.status_code}"

        try:
            content = response.json()["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError, json.JSONDecodeError) as exc:
            logger.warning("RAG answer envelope was not usable: %s", type(exc).__name__)
            return None, "malformed_response"

        text = str(content or "").strip()
        if not text:
            return None, "empty_completion"
        return text, None

    def answer(self, question: str, chunks: Sequence[object], *, max_chunks: int = DEFAULT_MAX_CHUNKS) -> RagAnswer:
        """Answer ``question`` from ``chunks`` alone.

        Args:
            question: The user's question.
            chunks: Retrieved chunks, best first.
            max_chunks: How many chunks to paste into the prompt.

        Returns:
            A :class:`RagAnswer`. ``llm_used`` is False when the answer is a
            fallback, and ``fallback_reason`` says which one.

        """
        sources = [_source_entry(chunk) for chunk in chunks]

        if not chunks:
            return RagAnswer(answer=NOT_FOUND_ANSWER, sources=[], llm_used=False, fallback_reason="no_evidence")

        if not self.api_key:
            return self._fallback(chunks, sources, "no_api_key")

        prompt = f"{ANSWER_RULES}\n\n[근거]\n{_evidence_block(chunks, max_chunks)}\n\n[질문]\n{question}"
        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.0,
        }

        text, failure = self._call_model(payload)
        if text is None:
            return self._fallback(chunks, sources, failure or "unknown")
        return RagAnswer(answer=text, sources=sources, llm_used=True, fallback_reason=None)


__all__ = [
    "ANSWER_RULES",
    "DEFAULT_ANSWER_MODEL",
    "DEFAULT_MAX_CHUNKS",
    "NOT_FOUND_ANSWER",
    "RagAnswer",
    "RagAnswerService",
]
