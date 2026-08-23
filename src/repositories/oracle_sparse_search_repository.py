"""Oracle sparse postings search for the canonical RAG table."""

from __future__ import annotations

from collections import defaultdict
from typing import TYPE_CHECKING, Any

from sqlalchemy import select
from sqlalchemy.orm import defer

from src.models.rag_chunk import RagChunk
from src.models.rag_chunk_term import RagChunkTerm
from src.services.rag_index_identity import RETRIEVABLE_INDEX_STATUSES
from src.services.rag_sparse_terms import normalize_sparse_token

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select

_MIN_CANDIDATES = 100
_CANDIDATE_MULTIPLIER = 20
_PER_TOKEN_CANDIDATE_MULTIPLIER = 5
_FETCH_MULTIPLIER = 8
_FETCH_FLOOR = 40
_CHUNK_COLUMN_FILTERS: tuple[tuple[str, Any], ...] = (
    ("team_id", RagChunk.team_id),
    ("season_year", RagChunk.season_year),
    ("league_type_code", RagChunk.league_type_code),
    ("player_id", RagChunk.player_id),
    ("index_version", RagChunk.index_version),
)


class OracleSparseSearchRepository:
    """Retrieve bounded RAG candidates from Oracle term postings."""

    def search_candidates(
        self,
        session: Session,
        keywords: list[str],
        *,
        top_k: int,
        filters: dict[str, Any] | None = None,
    ) -> list[RagChunk]:
        """Return chunks ranked by indexed term/title frequency."""
        tokens = sorted({token for keyword in keywords if (token := normalize_sparse_token(keyword))})
        if not tokens:
            return []

        candidate_limit = max(top_k * _CANDIDATE_MULTIPLIER, _MIN_CANDIDATES)
        per_token_limit = max(candidate_limit * _PER_TOKEN_CANDIDATE_MULTIPLIER, _MIN_CANDIDATES)
        scores = self._merge_posting_scores(session, tokens, per_token_limit, filters)

        ranked_ids = [
            chunk_id for chunk_id, _ in sorted(scores.items(), key=lambda item: (-item[1], item[0]))[:candidate_limit]
        ]
        fetch_limit = max(top_k * _FETCH_MULTIPLIER, _FETCH_FLOOR)
        return self._fetch_ranked_chunks(session, ranked_ids[:fetch_limit])

    def _merge_posting_scores(
        self,
        session: Session,
        tokens: list[str],
        per_token_limit: int,
        filters: dict[str, Any] | None,
    ) -> defaultdict[int, int]:
        """Sum weighted postings per chunk using one bounded lookup per token."""
        scores: defaultdict[int, int] = defaultdict(int)
        needs_chunk_join = bool(filters) and any(filters.get(key) is not None for key, _ in _CHUNK_COLUMN_FILTERS)
        for token in tokens:
            postings = self._token_postings(session, token, per_token_limit, filters, needs_chunk_join=needs_chunk_join)
            for row in postings:
                scores[int(row.rag_chunk_id)] += int(row.term_count) + 2 * int(row.title_count)
        return scores

    def _token_postings(
        self,
        session: Session,
        token: str,
        per_token_limit: int,
        filters: dict[str, Any] | None,
        *,
        needs_chunk_join: bool,
    ) -> list[Any]:
        """Return bounded postings for one token without scanning its whole list."""
        if needs_chunk_join:
            # Chunk-column filters require a selective join; those result sets
            # are small enough to keep the score ordering.
            return self._scored_postings(session, token, per_token_limit, filters)

        # Score sorts scan every posting row for the token and dominate cold
        # latency on frequent tokens. Take an index-order STOPKEY slice instead
        # (TOKEN prefix keeps this index-only) and read counts by primary key.
        slice_stmt: Select = select(RagChunkTerm.rag_chunk_id).where(RagChunkTerm.token == token)
        if filters and filters.get("source_table"):
            slice_stmt = slice_stmt.where(RagChunkTerm.source_table == filters["source_table"])
        slice_stmt = slice_stmt.order_by(RagChunkTerm.rag_chunk_id).limit(per_token_limit)
        sliced_ids = [int(row[0]) for row in session.execute(slice_stmt).all()]
        if not sliced_ids:
            return []

        detail_stmt: Select = select(
            RagChunkTerm.rag_chunk_id,
            RagChunkTerm.term_count,
            RagChunkTerm.title_count,
        ).where(RagChunkTerm.token == token, RagChunkTerm.rag_chunk_id.in_(sliced_ids))
        detail_stmt = self._apply_denormalized_filters(detail_stmt, filters)
        return list(session.execute(detail_stmt).all())

    def _scored_postings(
        self,
        session: Session,
        token: str,
        per_token_limit: int,
        filters: dict[str, Any] | None,
    ) -> list[Any]:
        """Return the highest-weighted postings for a join-filtered token."""
        stmt: Select = select(
            RagChunkTerm.rag_chunk_id,
            RagChunkTerm.term_count,
            RagChunkTerm.title_count,
        ).where(RagChunkTerm.token == token)
        stmt = stmt.join(RagChunk, RagChunk.id == RagChunkTerm.rag_chunk_id).where(
            RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
            *self._chunk_filter_clauses(filters),
        )
        stmt = stmt.order_by(
            (RagChunkTerm.term_count + 2 * RagChunkTerm.title_count).desc(),
            RagChunkTerm.rag_chunk_id,
        ).limit(per_token_limit)
        return list(session.execute(stmt).all())

    @staticmethod
    def _apply_denormalized_filters(stmt: Select, filters: dict[str, Any] | None) -> Select:
        """Apply source and game-date filters stored directly on postings."""
        if filters and filters.get("source_table"):
            stmt = stmt.where(RagChunkTerm.source_table == filters["source_table"])
        if filters and filters.get("game_date"):
            stmt = stmt.where(RagChunkTerm.game_date == str(filters["game_date"]))
        return stmt

    @staticmethod
    def _chunk_filter_clauses(filters: dict[str, Any] | None) -> list[Any]:
        """Build chunk-column filter clauses for the joined postings path."""
        clauses: list[Any] = []
        for key, column in _CHUNK_COLUMN_FILTERS:
            value = filters.get(key) if filters else None
            if value is not None:
                clauses.append(column == value)
        return clauses

    @staticmethod
    def _fetch_ranked_chunks(session: Session, fetch_ids: list[int]) -> list[RagChunk]:
        """Fetch full rows for the highest-scored candidate IDs only."""
        if not fetch_ids:
            return []
        stmt = (
            select(RagChunk)
            .options(defer(RagChunk.embedding))
            .where(
                RagChunk.id.in_(fetch_ids),
                RagChunk.index_status.in_(tuple(RETRIEVABLE_INDEX_STATUSES)),
            )
        )
        chunks_by_id = {chunk.id: chunk for chunk in session.execute(stmt).scalars().all()}
        return [chunks_by_id[chunk_id] for chunk_id in fetch_ids if chunk_id in chunks_by_id]
