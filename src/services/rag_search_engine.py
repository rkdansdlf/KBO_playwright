"""RAG Search Engine Service for KBO Knowledge Chunks."""

from __future__ import annotations

import logging
import math
import os
import re
from typing import TYPE_CHECKING, Any

from sqlalchemy import bindparam, case, func, or_, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import defer

from src.constants import KBO_FOUNDING_YEAR, KBO_MAX_VALID_SEASON
from src.models.rag_chunk import RagChunk
from src.services.rag_index_identity import RETRIEVABLE_INDEX_STATUSES
from src.services.rag_sparse_terms import search_keywords as _sparse_search_keywords
from src.utils.kbo_entity_extractor import extract_kbo_entities

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

logger = logging.getLogger(__name__)

_SEASON_YEAR_PATTERN = re.compile(r"(?<!\d)(?:19|20)\d{2}(?!\d)")
_DATE_SCOPED_SOURCE_TABLES = frozenset(
    {
        "game",
        "game_lineups",
        "game_play_by_play",
        "game_highlights",
        "team_standings_daily",
    }
)

BM25_POSTGRES_CANDIDATE_MULTIPLIER = 100
BM25_POSTGRES_MIN_CANDIDATES = 1000
BM25_POSTGRES_MIN_PER_KEYWORD = 100
BM25_ORACLE_CANDIDATE_MULTIPLIER = 20
BM25_ORACLE_MIN_CANDIDATES = 100


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
        filters: dict[str, Any] | None = None,
        *,
        oracle_ranked_candidates: bool = True,
    ) -> list[dict[str, Any]]:
        """Search relevant RAG chunks using keyword matching and scoring.

        Args:
            query: Search query string.
            top_k: Maximum number of results to return.
            category: Optional category filter.
            filters: Optional metadata filters such as team_id, season_year, source_table,
                game_date, player_id, player_name, and stadium.
            oracle_ranked_candidates: Sort Oracle sparse candidates before Python BM25 scoring.

        Returns:
            List of chunk dictionaries with relevance scores.

        """
        keywords = _search_keywords(query)
        if not keywords:
            keywords = [query.strip()]
        filters = _resolved_search_filters(query, filters)

        conditions = [
            or_(
                RagChunk.title.icontains(kw),
                RagChunk.content.icontains(kw),
            )
            for kw in keywords
        ]
        if self._uses_postgresql():
            chunks = self._postgresql_candidates(keywords, top_k, filters)
        elif self._uses_oracle():
            chunks = self._oracle_candidates_for_backend(
                keywords,
                top_k,
                filters,
                rank_candidates=oracle_ranked_candidates,
            )
        else:
            stmt = select(RagChunk)
            if conditions:
                stmt = stmt.where(or_(*conditions))
            chunks = list(self.session.execute(stmt).scalars().all())

        filtered_chunks = [
            c
            for c in chunks
            if (not category or self._matches_category(c, category))
            and (c.index_status or "ACTIVE") in RETRIEVABLE_INDEX_STATUSES
            and self._matches_filters(c, filters)
        ]
        average_length = sum(len(f"{c.title or ''} {c.content}".lower().split()) for c in filtered_chunks) / max(
            len(filtered_chunks), 1
        )
        document_frequency = {
            keyword.lower(): sum(keyword.lower() in f"{c.title or ''} {c.content}".lower() for c in filtered_chunks)
            for keyword in keywords
        }

        # Score chunks with BM25 and apply a small title boost.
        scored: list[tuple[float, RagChunk]] = []
        for c in filtered_chunks:
            score = 0.0
            text = f"{c.title} {c.content}".lower()
            document_length = len(text.split())
            for kw in keywords:
                kw_lower = kw.lower()
                term_frequency = text.count(kw_lower)
                if not term_frequency:
                    continue
                idf = max(
                    0.0,
                    math.log(
                        (
                            (len(filtered_chunks) - document_frequency[kw_lower] + 0.5)
                            / (document_frequency[kw_lower] + 0.5)
                        )
                        + 1.0,
                    ),
                )
                normalization = 1.5 * (1.0 - 0.75 + 0.75 * document_length / max(average_length, 1.0))
                score += idf * (term_frequency * 2.5) / (term_frequency + normalization)
                if kw_lower in (c.title or "").lower():
                    score *= 1.2
            scored.append((score, c))

        scored.sort(key=lambda x: x[0], reverse=True)
        top_chunks = scored[:top_k]

        return [
            {
                "chunk_id": self._source_key(c.source_table, c.source_row_id),
                "source_table": c.source_table,
                "source_row_id": c.source_row_id,
                "category": ((c.meta or {}).get("category") or (c.meta or {}).get("document_type") or "general"),
                "title": c.title,
                "content": c.content,
                "source_url": self._source_url(c.source_row_id, c.meta),
                "score": round(score, 2),
                "content_hash": c.content_hash,
                "index_version": c.index_version,
                "index_status": c.index_status,
                "meta": c.meta or {},
            }
            for score, c in top_chunks
        ]

    def _uses_postgresql(self) -> bool:
        """Return whether the session can use the PostgreSQL text-search path."""
        bind = self.session.get_bind()
        dialect = getattr(bind, "dialect", None)
        return getattr(dialect, "name", None) == "postgresql"

    def _uses_oracle(self) -> bool:
        """Return whether the session uses the Oracle sparse/vector backend."""
        bind = self.session.get_bind()
        dialect = getattr(bind, "dialect", None)
        return getattr(dialect, "name", None) == "oracle"

    def _oracle_candidates_for_backend(
        self,
        keywords: list[str],
        top_k: int,
        filters: dict[str, Any] | None,
        *,
        rank_candidates: bool,
    ) -> list[RagChunk]:
        """Select the term index when enabled and retain the CLOB fallback."""
        if os.getenv("RAG_ORACLE_SPARSE_MODE", "terms").strip().lower() != "terms":
            return self._oracle_candidates(keywords, top_k, filters, rank_candidates=rank_candidates)
        try:
            from src.repositories.oracle_sparse_search_repository import OracleSparseSearchRepository

            return OracleSparseSearchRepository().search_candidates(
                self.session,
                keywords,
                top_k=top_k,
                filters=filters,
            )
        except (SQLAlchemyError, RuntimeError, ValueError, TypeError):
            logger.warning("Oracle sparse term search failed; falling back to the CLOB path", exc_info=True)
            return self._oracle_candidates(keywords, top_k, filters, rank_candidates=rank_candidates)

    def _oracle_candidates(
        self,
        keywords: list[str],
        top_k: int,
        filters: dict[str, Any] | None,
        *,
        rank_candidates: bool,
    ) -> list[RagChunk]:
        """Fetch bounded Oracle lexical candidates without transferring vectors."""
        conditions = []
        relevance_terms = []
        for index, keyword in enumerate(keywords):
            title_match = (
                func.instr(
                    RagChunk.title,
                    bindparam(f"oracle_title_keyword_{index}", value=keyword),
                )
                > 0
            )
            content_match = (
                func.instr(
                    RagChunk.content,
                    bindparam(f"oracle_content_keyword_{index}", value=keyword),
                )
                > 0
            )
            conditions.append(or_(title_match, content_match))
            relevance_terms.append(case((title_match, 2), else_=0) + case((content_match, 1), else_=0))
        candidate_limit = max(top_k * BM25_ORACLE_CANDIDATE_MULTIPLIER, BM25_ORACLE_MIN_CANDIDATES)
        stmt = (
            select(RagChunk)
            .options(defer(RagChunk.embedding))
            .where(
                or_(*conditions),
                RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
            )
        )
        if filters and filters.get("source_table"):
            stmt = stmt.where(RagChunk.source_table == filters["source_table"])
        if rank_candidates:
            stmt = stmt.order_by(sum(relevance_terms).desc())
        stmt = stmt.limit(candidate_limit)
        return list(self.session.execute(stmt).scalars().all())

    def _postgresql_candidates(
        self,
        keywords: list[str],
        top_k: int,
        filters: dict[str, Any] | None,
    ) -> list[RagChunk]:
        """Fetch a bounded candidate set using the indexed PostgreSQL tsvector."""
        search_vector = func.to_tsvector(
            "simple",
            func.coalesce(RagChunk.title, "") + " " + RagChunk.content,
        )
        ts_queries = [func.plainto_tsquery("simple", keyword) for keyword in keywords]
        candidate_limit = max(top_k * BM25_POSTGRES_CANDIDATE_MULTIPLIER, BM25_POSTGRES_MIN_CANDIDATES)
        per_keyword_limit = max(
            BM25_POSTGRES_MIN_PER_KEYWORD,
            math.ceil(candidate_limit / len(ts_queries)),
        )
        candidates: dict[int, RagChunk] = {}
        for ts_query in ts_queries:
            stmt = (
                select(RagChunk)
                .where(search_vector.op("@@")(ts_query))
                .where(
                    or_(
                        RagChunk.index_status.is_(None),
                        RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
                    )
                )
                .limit(per_keyword_limit)
            )
            stmt = self._apply_postgresql_filters(stmt, filters or {})
            for chunk in self.session.execute(stmt).scalars().all():
                candidates[chunk.id] = chunk
        if candidates:
            return list(candidates.values())
        return self._legacy_candidates(keywords, top_k, filters, candidate_limit)

    def _legacy_candidates(
        self,
        keywords: list[str],
        top_k: int,
        filters: dict[str, Any] | None,
        candidate_limit: int | None = None,
    ) -> list[RagChunk]:
        """Use portable substring matching when PostgreSQL text search has no candidates."""
        conditions = [
            or_(
                RagChunk.title.icontains(keyword),
                RagChunk.content.icontains(keyword),
            )
            for keyword in keywords
        ]
        stmt = select(RagChunk).where(or_(*conditions))
        if filters and filters.get("source_table"):
            stmt = stmt.where(RagChunk.source_table == filters["source_table"])
        if candidate_limit is not None:
            stmt = stmt.limit(max(top_k * BM25_POSTGRES_CANDIDATE_MULTIPLIER, candidate_limit))
        return list(self.session.execute(stmt).scalars().all())

    @staticmethod
    def _apply_postgresql_filters(
        stmt: Select[tuple[RagChunk]],
        filters: dict[str, Any],
    ) -> Select[tuple[RagChunk]]:
        """Apply first-class filters while preserving legacy metadata fallbacks."""
        column_filters = {
            "team_id": RagChunk.team_id,
            "season_year": RagChunk.season_year,
            "player_id": RagChunk.player_id,
            "index_version": RagChunk.index_version,
        }
        for key, column in column_filters.items():
            if key not in filters or filters[key] is None:
                continue
            stmt = stmt.where(or_(column == filters[key], column.is_(None)))
        if filters.get("source_table"):
            stmt = stmt.where(RagChunk.source_table == filters["source_table"])
        if filters.get("game_date"):
            stmt = stmt.where(RagChunk.meta["game_date"].as_string() == str(filters["game_date"]))
        return stmt

    @staticmethod
    def _matches_category(chunk: RagChunk, category: str) -> bool:
        """Match the public category against legacy and indexed document labels."""
        actual = (chunk.meta or {}).get("category") or (chunk.meta or {}).get("document_type")
        aliases = {
            "press_release": {"press_release", "notice"},
            "milestone": {"milestone", "player_milestone"},
            "futures_schedule": {"futures_schedule", "futures_game"},
            "player_splits": {"player_splits", "split", "player_split"},
            "stadium_facility": {"stadium_facility", "stadium_food", "stadium_ticket"},
        }
        return actual in aliases.get(category, {category})

    @staticmethod
    def _source_key(source_table: str, source_row_id: str) -> str:
        """Build the stable identity shared by SQLite and pgvector indexes."""
        return f"{source_table}:{source_row_id}"

    @staticmethod
    def _source_url(source_row_id: str, meta: dict[str, Any] | None) -> str | None:
        """Return a real source URL without treating arbitrary row IDs as URLs."""
        source_url = (meta or {}).get("source_url")
        if isinstance(source_url, str) and source_url.startswith(("http://", "https://")):
            return source_url
        if source_row_id.startswith(("http://", "https://")):
            return source_row_id
        return None

    @staticmethod
    def _matches_filters(chunk: RagChunk, filters: dict[str, Any]) -> bool:
        """Match supported metadata filters without leaking unscoped chunks."""
        meta = chunk.meta or {}
        text = f"{chunk.title or ''} {chunk.content}".lower()

        direct_filters = {
            "team_id": chunk.team_id or meta.get("team_id"),
            "season_year": chunk.season_year or meta.get("season_year"),
            "source_table": chunk.source_table,
            "game_date": meta.get("game_date"),
            "player_id": chunk.player_id or meta.get("player_id"),
            "index_version": chunk.index_version or meta.get("index_version"),
        }
        for key, expected in direct_filters.items():
            if key in filters and expected != filters[key]:
                return False

        expected_category = filters.get("document_type")
        if expected_category and not RagSearchEngine._matches_category(chunk, str(expected_category)):
            return False

        player_name = filters.get("player_name")
        if player_name and str(player_name).lower() not in text:
            return False

        stadium = filters.get("stadium")
        return not bool(stadium and str(stadium).lower() not in text)

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


def _search_keywords(query: str) -> list[str]:
    """Normalize whitespace-delimited search terms and remove Korean particles."""
    return _sparse_search_keywords(query)


def _resolved_search_filters(query: str, filters: dict[str, Any] | None) -> dict[str, Any]:
    """Add a date or year from the query without overriding caller filters."""
    resolved = dict(filters or {})
    if resolved.get("season_year") is None and resolved.get("game_date") is None:
        if resolved.get("source_table") in _DATE_SCOPED_SOURCE_TABLES:
            game_date = extract_kbo_entities(query, extract_player=False).game_date
            if game_date is not None:
                resolved["game_date"] = game_date
        if resolved.get("game_date") is None:
            season_year = _query_season_year(query)
            if season_year is not None:
                resolved["season_year"] = season_year
    return resolved


def _query_season_year(query: str) -> int | None:
    """Extract a valid KBO season year from a free-text query."""
    for value in _SEASON_YEAR_PATTERN.findall(query):
        year = int(value)
        if KBO_FOUNDING_YEAR <= year <= KBO_MAX_VALID_SEASON:
            return year
    return None
