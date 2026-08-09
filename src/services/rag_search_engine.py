"""RAG Search Engine Service for KBO Knowledge Chunks."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import or_, select

from src.models.rag_chunk import RagChunk

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class RagSearchEngine:
    """RAG Search Engine for querying KBO knowledge base chunks."""

    def __init__(self, session: Session) -> None:
        """Initialize RAG search engine with DB session.

        Args:
            session: DB Session.

        """
        self.session = session

    def search(
        self,
        query: str,
        top_k: int = 5,
        category: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search relevant RAG chunks using keyword matching and scoring.

        Args:
            query: Search query string.
            top_k: Maximum number of results to return.
            category: Optional category filter.

        Returns:
            List of chunk dictionaries with relevance scores.

        """
        keywords = [k.strip() for k in query.split() if len(k.strip()) > 1]
        if not keywords:
            keywords = [query.strip()]

        stmt = select(RagChunk)
        conditions = [
            or_(
                RagChunk.title.icontains(kw),
                RagChunk.content.icontains(kw),
            )
            for kw in keywords
        ]
        if conditions:
            stmt = stmt.where(or_(*conditions))

        chunks = list(self.session.execute(stmt).scalars().all())

        # Score chunks based on keyword frequencies
        scored: list[tuple[float, RagChunk]] = []
        for c in chunks:
            if category and c.meta and c.meta.get("category") != category:
                continue
            score = 0.0
            text = f"{c.title} {c.content}".lower()
            for kw in keywords:
                kw_lower = kw.lower()
                count = text.count(kw_lower)
                if kw_lower in (c.title or "").lower():
                    score += count * 3.0
                else:
                    score += count * 1.0
            scored.append((score, c))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_chunks = scored[:top_k]

        return [
            {
                "chunk_id": str(c.id),
                "category": c.meta.get("category") if c.meta else "general",
                "title": c.title,
                "content": c.content,
                "source_url": c.source_row_id,
                "score": round(score, 2),
            }
            for score, c in top_chunks
        ]

    def answer_question(self, query: str, top_k: int = 5) -> dict[str, Any]:
        """Synthesize an answer and retrieve source references for a query.

        Args:
            query: Question string.
            top_k: Top K chunks to retrieve.

        Returns:
            Structured Q&A dictionary.

        """
        chunks = self.search(query, top_k=top_k)
        if not chunks:
            return {
                "query": query,
                "answer": f"'{query}'에 관한 검색 결과를 찾지 못했습니다.",
                "sources": [],
                "chunk_count": 0,
            }

        sources = list({c["source_url"] for c in chunks if c.get("source_url") and c["source_url"].startswith("http")})
        summaries = [f"• {c['title']}: {c['content'][:120]}..." for c in chunks[:3]]
        answer_text = f"검색된 주요 KBO 정보 ({len(chunks)}건):\n" + "\n".join(summaries)

        return {
            "query": query,
            "answer": answer_text,
            "sources": sources,
            "chunks": chunks,
            "chunk_count": len(chunks),
        }
