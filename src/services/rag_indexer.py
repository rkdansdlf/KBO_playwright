"""Service for indexing KBO press releases, milestones, futures schedules, and splits into RAG chunks."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.futures_schedule import FuturesGameSchedule
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat
from src.repositories.rag_chunk_repository import RagChunkRepository
from src.services.embedding_service import EmbeddingService

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class RagKnowledgeIndexer:
    """Indexer service for building RAG chunks from KBO data sources."""

    def __init__(
        self,
        session: Session,
        index_session: Session | None = None,
        embedding_service: EmbeddingService | None = None,
    ) -> None:
        """Initialize the source reader, sparse-index writer sessions, and embedding provider."""
        self.session = session
        self.rag_repo = RagChunkRepository(index_session or session)
        self.embedding_service = embedding_service or EmbeddingService(cache_enabled=True)

    def _embed_chunks(self, chunks: list[dict[str, Any]]) -> None:
        """Attach dense embeddings to chunk payloads before persistence."""
        texts = [f"{chunk['title']}\n{chunk['content']}" for chunk in chunks]
        embeddings = self.embedding_service.get_embeddings_batch(texts)
        if len(embeddings) != len(chunks):
            message = f"Embedding provider returned {len(embeddings)} vectors for {len(chunks)} chunks"
            raise RuntimeError(message)
        if any(not embedding or all(value == 0.0 for value in embedding) for embedding in embeddings):
            message = "Embedding provider returned an empty or zero vector"
            raise RuntimeError(message)
        for chunk, embedding in zip(chunks, embeddings, strict=True):
            chunk["embedding"] = embedding

    def index_press_releases(self) -> int:
        """Index KBO press releases as RAG chunks.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(KboPressRelease)
        releases = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for r in releases:
            content = f"[{r.published_date}] KBO 공식 공시: {r.title}\n카테고리: {r.category}\n출처: {r.source_url}"
            chunks.append(
                {
                    "title": f"KBO 공시 - {r.title}",
                    "content": content,
                    "meta": {
                        "category": "press_release",
                        "source_row_id": str(r.id),
                        "notice_id": r.notice_id,
                    },
                }
            )

        if chunks:
            self._embed_chunks(chunks)
            count = self.rag_repo.upsert_chunks(chunks)
            logger.info("Indexed %d press release RAG chunks.", count)
            return count
        return 0

    def index_milestones(self, season: int = 2026) -> int:
        """Index player milestone entries as RAG chunks.

        Args:
            season: Season year.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(PlayerMilestone).where(PlayerMilestone.season == season)
        milestones = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for m in milestones:
            status_str = "달성 완료" if m.is_achieved else f"달성 임박 (남은 수치: {m.remaining_val})"
            content = (
                f"[{season} 시즌] {m.team_code or ''} {m.player_name} 대기록 현황:\n"
                f"목표 기록: {m.milestone_category}\n"
                f"현재 기록: {m.current_val} / {m.target_val}\n"
                f"상태: {status_str}"
            )
            chunks.append(
                {
                    "title": f"선수 대기록 현황 - {m.player_name} ({m.milestone_category})",
                    "content": content,
                    "meta": {
                        "category": "milestone",
                        "source_row_id": str(m.id),
                        "season_year": season,
                        "player_id": m.player_id,
                    },
                }
            )

        if chunks:
            self._embed_chunks(chunks)
            count = self.rag_repo.upsert_chunks(chunks)
            logger.info("Indexed %d milestone RAG chunks for season %d.", count, season)
            return count
        return 0

    def index_futures_schedules(self, season: int = 2026) -> int:
        """Index Futures League schedule and game results as RAG chunks.

        Args:
            season: Season year.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(FuturesGameSchedule).where(FuturesGameSchedule.season == season)
        games = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for g in games:
            score_part = f"{g.away_score} : {g.home_score}" if g.away_score is not None else "예정"
            content = (
                f"[{g.game_date}] KBO 퓨처스리그 경기: {g.away_team} vs {g.home_team}\n"
                f"스코어/상태: {score_part} ({g.game_status})\n"
                f"구장: {g.stadium or '퓨처스 구장'}"
            )
            chunks.append(
                {
                    "title": f"퓨처스 경기 - {g.away_team} vs {g.home_team} ({g.game_date})",
                    "content": content,
                    "meta": {
                        "category": "futures_schedule",
                        "source_row_id": str(g.id),
                        "game_id": g.game_id,
                        "season_year": season,
                    },
                }
            )

        if chunks:
            self._embed_chunks(chunks)
            count = self.rag_repo.upsert_chunks(self.session, chunks)
            logger.info("Indexed %d futures schedule RAG chunks for season %d.", count, season)
            return count
        return 0

    def index_player_splits(self, season: int = 2026) -> int:
        """Index player situational split statistics as RAG chunks.

        Args:
            season: Season year.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(PlayerSplitsStat).where(PlayerSplitsStat.season == season)
        splits = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for s in splits:
            content = (
                f"[{season} 시즌] {s.team_code or ''} {s.player_name} 상황별 기록 ({s.split_type}:{s.split_key}):\n"
                f"타율: {s.avg:.3f}, 안타: {s.hits}, 홈런: {s.hr}, 타점: {s.rbi}, OPS: {s.ops:.3f}"
            )
            chunks.append(
                {
                    "title": f"선수 상황별 스탯 - {s.player_name} ({s.split_key})",
                    "content": content,
                    "meta": {
                        "category": "player_splits",
                        "source_row_id": str(s.id),
                        "season_year": season,
                        "player_id": s.player_id,
                    },
                }
            )

        if chunks:
            self._embed_chunks(chunks)
            count = self.rag_repo.upsert_chunks(self.session, chunks)
            logger.info("Indexed %d player splits RAG chunks for season %d.", count, season)
            return count
        return 0

    def index_incremental_all(self, season: int = 2026) -> dict[str, int]:
        """Index all KBO data sources into RAG chunks in one call.

        Args:
            season: Season year.

        Returns:
            Dict mapping category to upsert count.

        """
        pr_count = self.index_press_releases()
        ms_count = self.index_milestones(season=season)
        fut_count = self.index_futures_schedules(season=season)
        spl_count = self.index_player_splits(season=season)

        return {
            "press_releases": pr_count,
            "milestones": ms_count,
            "futures_schedules": fut_count,
            "player_splits": spl_count,
            "total_chunks": pr_count + ms_count + fut_count + spl_count,
        }
