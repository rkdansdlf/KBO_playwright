"""Unit and integration tests for boxscore repair pipeline and RAG chunk metadata sync."""

from __future__ import annotations

import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.crawl_dead_letter import CrawlDeadLetter, DlqStatus
from src.models.game import Game, GameBattingStat, GamePitchingStat
from src.models.rag_chunk import RagChunk
from src.repositories.rag_chunk_repository import RagChunkRepository
from src.services.boxscore_repair_service import (
    RepairCandidate,
    discover_candidates_from_dlq,
    discover_candidates_from_integrity,
    discover_candidates_from_log,
    gather_repair_candidates,
    repair_games_async,
    run_boxscore_repair,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


@pytest.fixture
def sqlite_test_db():
    """In-memory SQLite database session for unit tests."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    session_factory = sessionmaker(bind=engine)
    session = session_factory()
    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(engine)


class TestBoxscoreRepairCandidateDiscovery:
    def test_discover_from_integrity(self, sqlite_test_db: Session) -> None:
        # Create a game with mismatched boxscore sums
        game = Game(
            game_id="20260505SSLG0",
            game_date=date(2026, 5, 5),
            home_team="LG",
            away_team="SS",
            home_score=4,
            away_score=2,
            game_status="COMPLETED",
            is_primary=True,
        )
        sqlite_test_db.add(game)
        # Add batting stat with 1 run while game says away scored 2
        batting_stat = GameBattingStat(
            game_id="20260505SSLG0",
            team_side="away",
            player_id=101,
            player_name="테스트타자",
            appearance_seq=1,
            at_bats=4,
            hits=1,
            runs=1,  # sum is 1, but game away_score is 2
        )
        sqlite_test_db.add(batting_stat)
        sqlite_test_db.commit()

        candidates = discover_candidates_from_integrity(sqlite_test_db, target_date="2026-05-05")
        assert len(candidates) >= 1
        assert any(c.game_id == "20260505SSLG0" for c in candidates)
        assert candidates[0].source == "integrity_checker"

    def test_discover_from_dlq(self, sqlite_test_db: Session) -> None:
        letter = CrawlDeadLetter(
            dlq_id="dlq-001",
            original_run_id="run-001",
            target_type="game",
            target_id="20260601KTNC0",
            game_id="20260601KTNC0",
            crawler="GameDetailCrawler",
            failure_stage="parse",
            error_code="TIMEOUT",
            error_message="Page wait timeout",
            status=DlqStatus.PENDING.value,
        )
        sqlite_test_db.add(letter)
        sqlite_test_db.commit()

        candidates = discover_candidates_from_dlq(sqlite_test_db)
        assert len(candidates) == 1
        assert candidates[0].game_id == "20260601KTNC0"
        assert candidates[0].source == "dlq"

    def test_discover_from_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "crawl_failures.log"
            log_path.write_text(
                "2026-05-05 18:00:00\tgame\t20260505HTOB0\thttps://.../Boxscore\t3\thitter_mismatch\n",
                encoding="utf-8",
            )
            candidates = discover_candidates_from_log(log_path=log_path)
            assert len(candidates) == 1
            assert candidates[0].game_id == "20260505HTOB0"
            assert candidates[0].source == "failure_log"

    def test_gather_repair_candidates_dedup(self, sqlite_test_db: Session) -> None:
        letter = CrawlDeadLetter(
            dlq_id="dlq-002",
            original_run_id="run-002",
            target_type="game",
            target_id="20260505SSLG0",
            game_id="20260505SSLG0",
            crawler="GameDetailCrawler",
            failure_stage="parse",
            error_code="PARSE_ERROR",
            error_message="Boxscore totals discrepancy",
            status=DlqStatus.PENDING.value,
        )
        sqlite_test_db.add(letter)
        sqlite_test_db.commit()

        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "crawl_failures.log"
            log_path.write_text(
                "2026-05-05 18:00:00\tgame\t20260505SSLG0\thttps://.../Boxscore\t3\thitter_mismatch\n",
                encoding="utf-8",
            )
            candidates = gather_repair_candidates(
                sqlite_test_db,
                source="all",
                target_date="2026-05-05",
                log_path=log_path,
            )
            # Both DLQ and log refer to 20260505SSLG0, should be deduplicated to 1 candidate
            assert len(candidates) == 1
            assert candidates[0].game_id == "20260505SSLG0"
            assert len(candidates[0].reasons) >= 2


class TestBoxscoreRepairExecution:
    @pytest.mark.asyncio
    async def test_repair_dry_run(self, sqlite_test_db: Session) -> None:
        candidate = RepairCandidate(
            game_id="20260505SSLG0",
            game_date="2026-05-05",
            source="manual",
            reasons=["hitter mismatch"],
        )
        mock_payload = {
            "game_id": "20260505SSLG0",
            "teams": {
                "away": {"code": "SS", "score": 2, "hits": 4},
                "home": {"code": "LG", "score": 4, "hits": 7},
            },
            "hitters": {
                "away": [{"player_id": 1002, "stats": {"at_bats": 4, "hits": 4, "runs": 2}}],
                "home": [{"player_id": 1001, "stats": {"at_bats": 4, "hits": 7, "runs": 4}}],
            },
            "pitchers": {
                "away": [{"player_id": 1005, "stats": {"innings_outs": 24, "runs_allowed": 4}}],
                "home": [{"player_id": 1006, "stats": {"innings_outs": 27, "runs_allowed": 2}}],
            },
        }

        with (
            patch(
                "src.crawlers.game_detail_crawler.GameDetailCrawler.crawl_game", new_callable=AsyncMock
            ) as mock_crawl,
            patch("src.repositories.game_save.save_game_detail") as mock_save,
        ):
            mock_crawl.return_value = mock_payload
            report = await repair_games_async([candidate], dry_run=True, session=sqlite_test_db)

            assert report.total_candidates == 1
            assert report.repaired == 1
            assert report.dry_run is True
            mock_save.assert_not_called()

    @pytest.mark.asyncio
    async def test_repair_apply_and_dlq_resolution(self, sqlite_test_db: Session) -> None:
        letter = CrawlDeadLetter(
            dlq_id="dlq-003",
            original_run_id="run-003",
            target_type="game",
            target_id="20260505SSLG0",
            game_id="20260505SSLG0",
            crawler="GameDetailCrawler",
            failure_stage="parse",
            error_code="PARSE_ERROR",
            error_message="Boxscore totals discrepancy",
            status=DlqStatus.PENDING.value,
        )
        sqlite_test_db.add(letter)
        sqlite_test_db.commit()

        candidate = RepairCandidate(
            game_id="20260505SSLG0",
            game_date="2026-05-05",
            source="dlq",
            reasons=["Boxscore discrepancy"],
        )
        mock_payload = {
            "game_id": "20260505SSLG0",
            "teams": {
                "away": {"code": "SS", "score": 2, "hits": 4},
                "home": {"code": "LG", "score": 4, "hits": 7},
            },
            "hitters": {
                "away": [{"player_id": 1002, "stats": {"at_bats": 4, "hits": 4, "runs": 2}}],
                "home": [{"player_id": 1001, "stats": {"at_bats": 4, "hits": 7, "runs": 4}}],
            },
            "pitchers": {
                "away": [{"player_id": 1005, "stats": {"innings_outs": 24, "runs_allowed": 4}}],
                "home": [{"player_id": 1006, "stats": {"innings_outs": 27, "runs_allowed": 2}}],
            },
        }

        with (
            patch(
                "src.crawlers.game_detail_crawler.GameDetailCrawler.crawl_game", new_callable=AsyncMock
            ) as mock_crawl,
            patch("src.repositories.game_save.save_game_detail", return_value=True) as mock_save,
        ):
            mock_crawl.return_value = mock_payload
            report = await repair_games_async([candidate], dry_run=False, session=sqlite_test_db)

            assert report.repaired == 1
            assert report.outcomes[0].persisted is True
            mock_save.assert_called_once()

            # Verify DLQ entry transitioned to RESOLVED
            sqlite_test_db.refresh(letter)
            assert letter.status == DlqStatus.RESOLVED.value


class TestRagChunkMetadataSync:
    def test_rag_chunk_metadata_upsert(self, sqlite_test_db: Session) -> None:
        repo = RagChunkRepository(sqlite_test_db)
        chunk_data = {
            "source_table": "game",
            "source_row_id": "20260505SSLG0",
            "title": "2026-05-05 삼성 vs LG",
            "content": "삼성 라이온즈 2, LG 트윈스 4",
            "season_year": 2026,
            "document_type": "game_boxscore",
            "game_date": "2026-05-05",
            "published_at": "2026-05-05T22:00:00",
            "source_url": "https://www.koreabaseball.com/Game/Boxscore.aspx?gameId=20260505SSLG0",
            "language": "ko",
            "meta": {"weather": "clear"},
        }

        count = repo.upsert_chunks(sqlite_test_db, [chunk_data])
        assert count == 1

        chunk = sqlite_test_db.query(RagChunk).filter_by(source_table="game", source_row_id="20260505SSLG0").one()
        assert chunk.document_type == "game_boxscore"
        assert chunk.game_date == date(2026, 5, 5)
        assert chunk.published_at == datetime(2026, 5, 5, 22, 0, 0)
        assert chunk.source_url == "https://www.koreabaseball.com/Game/Boxscore.aspx?gameId=20260505SSLG0"
        assert chunk.language == "ko"

        # Now test updating the chunk with modified score/content (re-crawl repair)
        updated_data = {
            "source_table": "game",
            "source_row_id": "20260505SSLG0",
            "title": "2026-05-05 삼성 vs LG (정정)",
            "content": "삼성 라이온즈 2, LG 트윈스 5 (정정 반영)",
            "season_year": 2026,
            "document_type": "game_boxscore",
            "game_date": "2026-05-05",
            "published_at": "2026-05-06T10:00:00",
            "source_url": "https://www.koreabaseball.com/Game/Boxscore.aspx?gameId=20260505SSLG0",
            "language": "ko",
            "meta": {"weather": "clear", "revised": True},
        }

        count2 = repo.upsert_chunks(sqlite_test_db, [updated_data])
        assert count2 == 1

        # Must not insert a second row: exactly one chunk remains
        all_chunks = sqlite_test_db.query(RagChunk).filter_by(source_table="game", source_row_id="20260505SSLG0").all()
        assert len(all_chunks) == 1
        updated_chunk = all_chunks[0]
        assert updated_chunk.title == "2026-05-05 삼성 vs LG (정정)"
        assert "정정 반영" in updated_chunk.content
        assert updated_chunk.published_at == datetime(2026, 5, 6, 10, 0, 0)
        assert updated_chunk.meta.get("revised") is True
