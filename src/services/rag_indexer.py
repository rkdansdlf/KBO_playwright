"""Service for indexing KBO press releases and milestones into RAG chunks."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from sqlalchemy import select

from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.repositories.rag_chunk_repository import RagChunkRepository

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class RagKnowledgeIndexer:
    """Indexer service for building RAG chunks from KBO releases and milestones."""

    def __init__(self, session: Session) -> None:
        """Initialize indexer with DB session."""
        self.session = session
        self.rag_repo = RagChunkRepository()

    def index_press_releases(self) -> int:
        """Index KBO press releases as RAG chunks.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(KboPressRelease)
        releases = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict] = []
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
            count = self.rag_repo.upsert_chunks(self.session, chunks)
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

        chunks: list[dict] = []
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
            count = self.rag_repo.upsert_chunks(self.session, chunks)
            logger.info("Indexed %d milestone RAG chunks for season %d.", count, season)
            return count
        return 0
