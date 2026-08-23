"""Knowledge Indexer orchestrator building RAG chunks from KBO data models."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.models.futures_schedule import FuturesGameSchedule
from src.models.kbo_press_release import KboPressRelease
from src.models.player_milestone import PlayerMilestone
from src.models.player_splits_stat import PlayerSplitsStat
from src.rag.dto import RagDocument
from src.repositories.rag_chunk_repository import RagChunkRepository

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class KnowledgeIndexer:
    """Orchestrates extraction of KBO domain records into standard RAG knowledge chunks."""

    def __init__(self, session: Session, *, index_session: Session | None = None) -> None:
        """Initialize with source reader session and index writer repository."""
        self.session = session
        self.rag_repo = RagChunkRepository(index_session or session)

    def index_press_releases(self) -> int:
        """Index KBO official press releases as RAG knowledge chunks.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(KboPressRelease)
        releases = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for r in releases:
            content = f"[{r.published_date}] KBO 공식 공시: {r.title}\n카테고리: {r.category}\n출처: {r.source_url}"
            doc = RagDocument(
                chunk_id=f"press_release_{r.id}",
                title=f"KBO 공시 - {r.title}",
                content=content,
                category="press_release",
                source_table="kbo_press_release",
                source_row_id=str(r.id),
                published_at=str(r.published_date) if r.published_date else None,
                source_url=r.source_url,
                metadata={"notice_id": r.notice_id, "category": r.category},
            )
            chunks.append(
                {
                    "title": doc.title,
                    "content": doc.content,
                    "meta": {
                        "category": doc.category,
                        "source_row_id": doc.source_row_id,
                        "notice_id": r.notice_id,
                    },
                }
            )

        if chunks:
            count = self.rag_repo.upsert_chunks(chunks)
            logger.info("Indexed %d press release RAG chunks.", count)
            return count
        return 0

    def index_milestones(self) -> int:
        """Index player milestones as RAG knowledge chunks.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(PlayerMilestone)
        milestones = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for m in milestones:
            content = (
                f"[{m.achieved_date}] {m.player_name} ({m.team_name}) - {m.milestone_type}: {m.record_detail}\n"
                f"상대: {m.opponent_team or '미정'}, 구장: {m.stadium or '미정'}"
            )
            chunks.append(
                {
                    "title": f"기록 달성 - {m.player_name} {m.milestone_type}",
                    "content": content,
                    "meta": {
                        "category": "milestone",
                        "player_name": m.player_name,
                        "team_name": m.team_name,
                        "milestone_type": m.milestone_type,
                        "achieved_date": str(m.achieved_date) if m.achieved_date else None,
                    },
                }
            )

        if chunks:
            count = self.rag_repo.upsert_chunks(chunks)
            logger.info("Indexed %d milestone RAG chunks.", count)
            return count
        return 0

    def index_futures_schedule(self) -> int:
        """Index futures league schedules as RAG knowledge chunks.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(FuturesGameSchedule)
        games = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for g in games:
            content = (
                f"[{g.game_date}] 퓨처스리그 {g.away_team} vs {g.home_team}\n"
                f"구장: {g.stadium}, 시간: {g.game_time or '미정'}, 상태: {g.game_status or '예정'}"
            )
            chunks.append(
                {
                    "title": f"퓨처스 경기 - {g.game_date} {g.away_team} vs {g.home_team}",
                    "content": content,
                    "meta": {
                        "category": "futures_schedule",
                        "game_date": str(g.game_date) if g.game_date else None,
                        "away_team": g.away_team,
                        "home_team": g.home_team,
                        "stadium": g.stadium,
                    },
                }
            )

        if chunks:
            count = self.rag_repo.upsert_chunks(chunks)
            logger.info("Indexed %d futures schedule RAG chunks.", count)
            return count
        return 0

    def index_player_splits(self) -> int:
        """Index player situational splits as RAG knowledge chunks.

        Returns:
            Number of chunks upserted.

        """
        stmt = select(PlayerSplitsStat)
        splits = list(self.session.execute(stmt).scalars().all())

        chunks: list[dict[str, Any]] = []
        for s in splits:
            content = (
                f"[{s.season}] {s.player_name} ({s.team_code}) {s.split_type}/{s.split_value} 기록:\n"
                f"타율: {s.avg}, 안타: {s.hits}, 홈런: {s.hr}, 타점: {s.rbi}, OPS: {s.ops}"
            )
            chunks.append(
                {
                    "title": f"선수 스플릿 - {s.season} {s.player_name} {s.split_type}:{s.split_value}",
                    "content": content,
                    "meta": {
                        "category": "player_splits",
                        "season": s.season,
                        "player_name": s.player_name,
                        "team_code": s.team_code,
                        "split_type": s.split_type,
                        "split_value": s.split_value,
                    },
                }
            )

        if chunks:
            count = self.rag_repo.upsert_chunks(chunks)
            logger.info("Indexed %d player split RAG chunks.", count)
            return count
        return 0

    def index_all(self) -> dict[str, int]:
        """Index all available data sources into RAG chunks.

        Returns:
            Dictionary mapping category to number of upserted chunks.

        """
        return {
            "press_releases": self.index_press_releases(),
            "milestones": self.index_milestones(),
            "futures_schedule": self.index_futures_schedule(),
            "player_splits": self.index_player_splits(),
        }
