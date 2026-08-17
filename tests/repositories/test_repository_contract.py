"""Repository Contract tests.

Verify that repositories follow the transaction ownership contract:
- No internal commit/rollback/session creation
- Transaction boundary owned by caller
"""

from __future__ import annotations

import pytest
import contextlib
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.models.base import Base
from src.models.player import PlayerBasic
from src.models.team import TeamDailyRoster
from src.repositories.player_basic_repository import PlayerBasicRepository
from src.repositories.team_repository import TeamRepository


@pytest.fixture
def engine():
    """Create an in-memory SQLite engine for tests with StaticPool."""
    from sqlalchemy.pool import StaticPool

    return create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False}, poolclass=StaticPool)


@pytest.fixture
def session(engine):
    """Create a new database session with all tables created."""
    Base.metadata.create_all(bind=engine)
    SessionMaker = sessionmaker(bind=engine, expire_on_commit=False)
    session = SessionMaker()
    try:
        yield session
    finally:
        session.close()


class TestRepositoryContract:
    """Test that repositories do not own transaction boundaries."""

    def test_team_repo_does_not_commit(self, session: Session) -> None:
        """TeamRepository.save_daily_rosters should not commit.

        After repo operation + rollback, no data should persist.
        """
        repo = TeamRepository(session)
        roster_data = [
            {
                "roster_date": date(2025, 4, 1),
                "team_code": "LG",
                "player_id": 99999,
                "player_name": "테스트선수",
                "position": "투수",
                "back_number": "99",
            }
        ]
        repo.save_daily_rosters(roster_data)
        # Data should be in session but not committed
        assert session.query(TeamDailyRoster).count() > 0
        session.rollback()
        assert session.query(TeamDailyRoster).count() == 0

    def test_player_basic_repo_does_not_commit(self, session: Session) -> None:
        """PlayerBasicRepository.upsert_players should not commit."""
        repo = PlayerBasicRepository(session)
        player_data = [
            {
                "player_id": 88888,
                "name": "계약테스트",
            }
        ]
        repo.upsert_players(player_data)
        assert session.query(PlayerBasic).filter_by(player_id=88888).count() > 0
        session.rollback()
        assert session.query(PlayerBasic).filter_by(player_id=88888).count() == 0


class TestMultiRepoSingleSession:
    """Test that multiple repositories can share a single session."""

    def test_multiple_repos_share_session(self, session: Session) -> None:
        """Multiple repos using same session see each other's changes."""
        team_repo = TeamRepository(session)
        player_repo = PlayerBasicRepository(session)

        player_repo.upsert_players([{"player_id": 77777, "name": "공유세션"}])
        team_repo.save_daily_rosters(
            [
                {
                    "roster_date": date(2025, 4, 1),
                    "team_code": "LG",
                    "player_id": 77777,
                    "player_name": "공유세션",
                    "position": "투수",
                    "back_number": "77",
                }
            ]
        )

        # Both changes visible in same session
        assert session.query(PlayerBasic).filter_by(player_id=77777).count() == 1
        assert session.query(TeamDailyRoster).filter_by(player_id=77777).count() == 1

        # Rollback removes both
        session.rollback()
        assert session.query(PlayerBasic).filter_by(player_id=77777).count() == 0
        assert session.query(TeamDailyRoster).filter_by(player_id=77777).count() == 0


class TestTransactionRollback:
    """Test that caller-managed transactions roll back all changes on failure."""

    def test_rollback_on_failure_reverts_all(self, session: Session) -> None:
        """If an error occurs after repo operations, rollback reverts everything."""
        team_repo = TeamRepository(session)
        player_repo = PlayerBasicRepository(session)

        player_repo.upsert_players([{"player_id": 66666, "name": "롤백테스트"}])
        team_repo.save_daily_rosters(
            [
                {
                    "roster_date": date(2025, 4, 1),
                    "team_code": "SS",
                    "player_id": 66666,
                    "player_name": "롤백테스트",
                    "position": "내야수",
                    "back_number": "66",
                }
            ]
        )

        # Simulate failure
        session.rollback()

        # Everything reverted
        assert session.query(PlayerBasic).filter_by(player_id=66666).count() == 0
        assert session.query(TeamDailyRoster).filter_by(player_id=66666).count() == 0


class TestUpsertIdempotency:
    """Test that UPSERT operations remain idempotent after contract change."""

    def test_team_roster_upsert_idempotent(self, session: Session) -> None:
        """Same data upserted twice produces same row count."""
        repo = TeamRepository(session)
        data = [
            {
                "roster_date": date(2025, 5, 1),
                "team_code": "KIA",
                "player_id": 55555,
                "player_name": "멱등테스트",
                "position": "외야수",
                "back_number": "55",
            }
        ]
        repo.save_daily_rosters(data)
        session.flush()
        count1 = session.query(TeamDailyRoster).count()

        repo.save_daily_rosters(data)
        session.flush()
        count2 = session.query(TeamDailyRoster).count()

        assert count1 == count2 == 1

    def test_player_basic_upsert_idempotent(self, session: Session) -> None:
        """Same player data upserted twice produces same row count."""
        repo = PlayerBasicRepository(session)
        data = [{"player_id": 44444, "name": "멱등선수"}]

        repo.upsert_players(data)
        session.flush()
        count1 = session.query(PlayerBasic).filter_by(player_id=44444).count()

        repo.upsert_players(data)
        session.flush()
        count2 = session.query(PlayerBasic).filter_by(player_id=44444).count()

        assert count1 == count2 == 1


from src.repositories.team_stats_repository import TeamSeasonBattingRepository
from src.models.team_stats import TeamSeasonBatting
from src.repositories.player_game_stats import upsert_player_game_batting
from src.models.game import PlayerGameBatting
from src.repositories.safe_batting_repository import save_batting_stats_safe
from src.models.player import PlayerSeasonBatting


class TestAdditionalContracts:
    def test_team_season_batting_repo_does_not_commit(self, session: Session) -> None:
        repo = TeamSeasonBattingRepository(session)
        repo.upsert_many([{"team_id": "KT", "team_name": "KT Wiz", "season": 2024, "league": "REGULAR", "games": 144}])
        assert session.query(TeamSeasonBatting).filter_by(team_id="KT").count() > 0
        session.rollback()
        assert session.query(TeamSeasonBatting).filter_by(team_id="KT").count() == 0

    def test_upsert_player_game_batting_does_not_commit(self, session: Session) -> None:
        upsert_player_game_batting(
            session,
            [
                {
                    "game_id": "20241015KTSS",
                    "player_id": 999,
                    "player_name": "Test Player",
                    "team_side": "away",
                    "team_code": "KT",
                    "plate_appearances": 4,
                    "at_bats": 4,
                    "hits": 1,
                }
            ],
        )
        assert session.query(PlayerGameBatting).filter_by(player_id=999).count() > 0
        session.rollback()
        assert session.query(PlayerGameBatting).filter_by(player_id=999).count() == 0

    def test_save_batting_stats_safe_does_not_commit(self, session: Session) -> None:
        from unittest.mock import patch

        with patch("src.repositories.safe_batting_repository.filter_valid_season_stat_payloads") as mock_filter:
            payloads = [
                {"player_id": 999, "season": 2024, "league": "REGULAR", "level": "KBO1", "team_code": "KT", "games": 10}
            ]
            from collections import Counter

            mock_filter.return_value = (payloads, Counter())
            result = save_batting_stats_safe(payloads, session=session)
        assert result > 0
        assert session.query(PlayerSeasonBatting).filter_by(player_id=999).count() > 0
        session.rollback()
        assert session.query(PlayerSeasonBatting).filter_by(player_id=999).count() == 0

    def test_save_pitching_stats_to_db_does_not_commit(self, session: Session) -> None:
        from unittest.mock import patch
        from src.repositories.player_season_pitching_repository import save_pitching_stats_to_db
        from src.models.player import PlayerSeasonPitching

        with patch(
            "src.repositories.player_season_pitching_repository.filter_valid_season_stat_payloads"
        ) as mock_filter:
            payloads = [
                {"player_id": 999, "season": 2024, "league": "REGULAR", "level": "KBO1", "team_code": "KT", "games": 10}
            ]
            from collections import Counter

            mock_filter.return_value = (payloads, Counter())
            save_pitching_stats_to_db(payloads, session=session)
        assert session.query(PlayerSeasonPitching).filter_by(player_id=999).count() > 0
        session.rollback()
        assert session.query(PlayerSeasonPitching).filter_by(player_id=999).count() == 0

    def test_save_schedule_game_does_not_commit(self, session: Session) -> None:
        from src.repositories.game_save import save_schedule_game
        from src.models.game import Game
        from src.models.season import KboSeason

        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.flush()

        save_schedule_game(
            {
                "game_id": "20241015SSLG0",
                "game_date": "2024-10-15",
                "away_team_code": "SS",
                "home_team_code": "LG",
                "season_year": 2024,
                "game_status": "scheduled",
                "game_time": "18:30",
                "stadium": "Jamsil",
            },
            session=session,
        )
        assert session.query(Game).filter_by(game_id="20241015SSLG0").count() > 0
        session.rollback()
        assert session.query(Game).filter_by(game_id="20241015SSLG0").count() == 0

    def test_update_game_status_does_not_commit(self, session: Session) -> None:
        from src.repositories.game_status import update_game_status
        from src.models.game import Game

        session.add(Game(game_id="20241015STATUS", game_date=date(2024, 10, 15), game_status="scheduled"))
        session.flush()

        update_game_status("20241015STATUS", "completed", session=session)
        assert session.query(Game).filter_by(game_id="20241015STATUS").first().game_status == "completed"
        session.rollback()
        assert session.query(Game).filter_by(game_id="20241015STATUS").count() == 0

    def test_save_relay_data_does_not_commit(self, session: Session) -> None:
        from src.repositories.game_relay import save_relay_data
        from src.models.game import GamePlayByPlay

        save_relay_data("20241015RELAY", [{"seq": 1, "text": "Test Event"}], session=session)
        assert session.query(GamePlayByPlay).filter_by(game_id="20241015RELAY").count() > 0
        session.rollback()
        assert session.query(GamePlayByPlay).filter_by(game_id="20241015RELAY").count() == 0

    def test_rag_chunk_repository_does_not_commit(self, session: Session) -> None:
        from src.repositories.rag_chunk_repository import RagChunkRepository
        from src.models.rag_chunk import RagChunk

        repo = RagChunkRepository(session)
        repo.upsert_chunks(
            [
                {
                    "title": "Contract Test",
                    "content": "Does not commit",
                    "source_table": "test",
                    "source_row_id": "1",
                }
            ]
        )
        assert session.query(RagChunk).filter_by(source_table="test").count() > 0
        session.rollback()
        assert session.query(RagChunk).filter_by(source_table="test").count() == 0


class TestStaticASTContractGuard:
    """Static AST Guard ensuring all repository modules adhere to the contract."""

    def test_zero_commit_rollback_sessionlocal_in_all_repositories(self) -> None:
        """Scan all 44 repository files and assert 0 commit/rollback/SessionLocal."""
        import ast
        from pathlib import Path

        repo_dir = Path("src/repositories")
        assert repo_dir.exists(), "src/repositories directory not found"

        violations = []
        repo_files = [f for f in sorted(repo_dir.glob("*.py")) if f.name not in ("__init__.py", "oracle_upsert.py")]
        assert len(repo_files) >= 40, f"Expected >= 40 repository files, found {len(repo_files)}"

        for file_path in repo_files:
            source = file_path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(file_path))

            for node in ast.walk(tree):
                # Check for .commit() and .rollback() calls
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                    if node.func.attr in ("commit", "rollback"):
                        violations.append(f"{file_path.name}:{node.lineno} calls .{node.func.attr}()")
                # Check for direct SessionLocal references
                if isinstance(node, ast.Name) and node.id == "SessionLocal":
                    violations.append(f"{file_path.name}:{node.lineno} references SessionLocal directly")

        assert not violations, "Repository Contract Violations found:\n" + "\n".join(violations)


class TestCompatibilityFacadeSemantics:
    """Test that compatibility facades (session=None) commit on success and rollback on failure."""

    def test_compat_facade_without_session_commits_on_success(self, engine) -> None:
        """Calling a compatibility function with session=None automatically commits on success."""
        from unittest.mock import patch
        from src.repositories.game_save import save_schedule_game
        from src.models.game import Game
        from src.models.season import KboSeason

        # Ensure all tables are created on the in-memory engine
        Base.metadata.create_all(bind=engine)

        # Create sessionmaker bound to test engine
        TestSessionMaker = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)

        @contextlib.contextmanager
        def mock_get_db_session():
            sess = TestSessionMaker()
            try:
                yield sess
                sess.commit()
            except Exception:
                sess.rollback()
                raise
            finally:
                sess.close()

        # Seed season
        with mock_get_db_session() as s:
            s.add(KboSeason(season_id=10, season_year=2025, league_type_code=0, league_type_name="regular"))

        with patch("src.repositories.game_save.get_db_session", mock_get_db_session):
            # Call facade with session=None
            saved = save_schedule_game(
                {
                    "game_id": "20250501OBLG0",
                    "game_date": "2025-05-01",
                    "away_team_code": "OB",
                    "home_team_code": "LG",
                    "season_year": 2025,
                    "game_status": "scheduled",
                    "game_time": "18:30",
                    "stadium": "Jamsil",
                },
                session=None,
            )
            assert saved is True

        # Verify in a completely separate session that data was committed
        verify_session = TestSessionMaker()
        try:
            game = verify_session.query(Game).filter_by(game_id="20250501OBLG0").first()
            assert game is not None
            assert game.game_id == "20250501OBLG0"
            assert game.game_status is not None
        finally:
            verify_session.close()


class TestMultiRepositoryE2EComposition:
    """Full E2E test verifying cross-domain atomic composition across 5+ repositories."""

    def test_cross_domain_atomic_commit_and_rollback(self, session: Session) -> None:
        """Compose Schedule, Team Roster, Player Profile, Status, and Relay in one transaction."""
        from src.models.game import Game, GamePlayByPlay
        from src.models.season import KboSeason
        from src.repositories.game_save import save_schedule_game
        from src.repositories.game_status import update_game_status
        from src.repositories.game_relay import save_relay_data
        from src.repositories.team_repository import TeamRepository
        from src.repositories.player_basic_repository import PlayerBasicRepository

        # 1. Setup season dependency
        session.add(KboSeason(season_id=100, season_year=2026, league_type_code=0, league_type_name="regular"))
        session.flush()

        team_repo = TeamRepository(session)
        player_repo = PlayerBasicRepository(session)

        # 2. Execute multi-domain operations
        player_repo.upsert_players([{"player_id": 11111, "name": "통합테스트선수"}])
        team_repo.save_daily_rosters(
            [
                {
                    "roster_date": date(2026, 4, 1),
                    "team_code": "LG",
                    "player_id": 11111,
                    "player_name": "통합테스트선수",
                    "position": "투수",
                    "back_number": "11",
                }
            ]
        )
        save_schedule_game(
            {
                "game_id": "20260401E2E0",
                "game_date": "2026-04-01",
                "away_team_code": "SS",
                "home_team_code": "LG",
                "season_year": 2026,
                "game_status": "scheduled",
                "game_time": "18:30",
                "stadium": "Jamsil",
            },
            session=session,
        )
        update_game_status("20260401E2E0", "live", session=session)
        save_relay_data("20260401E2E0", [{"seq": 1, "text": "1회초 시작"}], session=session)

        # 3. Verify uncommitted state: all records exist in transaction
        assert session.query(PlayerBasic).filter_by(player_id=11111).count() == 1
        assert session.query(TeamDailyRoster).filter_by(player_id=11111).count() == 1
        assert session.query(Game).filter_by(game_id="20260401E2E0").count() == 1
        assert session.query(GamePlayByPlay).filter_by(game_id="20260401E2E0").count() == 1

        # 4. Atomic Rollback: everything reverts cleanly
        session.rollback()

        assert session.query(PlayerBasic).filter_by(player_id=11111).count() == 0
        assert session.query(TeamDailyRoster).filter_by(player_id=11111).count() == 0
        assert session.query(Game).filter_by(game_id="20260401E2E0").count() == 0
        assert session.query(GamePlayByPlay).filter_by(game_id="20260401E2E0").count() == 0
