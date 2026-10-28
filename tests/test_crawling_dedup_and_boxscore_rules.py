"""Unit and integration tests for crawling deduplication, UPSERT, fraction parsing, failure logging, and boxscore verification."""

from __future__ import annotations

import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.base import Base
from src.models.game import (
    Game,
    GameBattingStat,
    GameInningScore,
    GamePitchingStat,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.models.player import PlayerBasic
from src.repositories.game_helpers import _deduplicate_mappings
from src.repositories.player_game_stats import (
    _upsert_bulk,
    upsert_player_game_batting,
    upsert_player_game_pitching,
)
from src.utils.crawler_failure_logger import record_crawl_failure
from src.utils.type_helpers import (
    parse_innings,
    parse_innings_to_outs,
    safe_float,
    safe_float_or_none,
    safe_int,
    safe_int_or_none,
    to_int,
)
from src.validators.boxscore_verifier import (
    verify_boxscore_sums,
    verify_daily_game_count,
    verify_game_db_boxscore_integrity,
)


# ============================================================================
# 1. Type Helpers & Fraction / Special Character Parsing Tests
# ============================================================================
class TestFractionAndSpecialCharParsing:
    def test_parse_innings_fractions(self) -> None:
        assert parse_innings_to_outs("1/3") == 1
        assert parse_innings_to_outs("2/3") == 2
        assert parse_innings_to_outs("5 1/3") == 16
        assert parse_innings_to_outs("5 2/3") == 17
        assert parse_innings_to_outs("0 1/3") == 1
        assert parse_innings_to_outs("0 2/3") == 2

    def test_parse_innings_unicode(self) -> None:
        assert parse_innings_to_outs("⅓") == 1
        assert parse_innings_to_outs("⅔") == 2
        assert parse_innings_to_outs("5 ⅓") == 16
        assert parse_innings_to_outs("5 ⅔") == 17
        assert parse_innings_to_outs("7⅓") == 22
        assert parse_innings_to_outs("7⅔") == 23

    def test_parse_innings_decimal(self) -> None:
        assert parse_innings_to_outs("5.1") == 16
        assert parse_innings_to_outs("5.2") == 17
        assert parse_innings_to_outs("5.0") == 15
        assert parse_innings_to_outs("0.1") == 1
        assert parse_innings_to_outs("0.2") == 2
        assert parse_innings_to_outs("9") == 27

    def test_parse_innings_colons(self) -> None:
        assert parse_innings_to_outs("5:1") == 16
        assert parse_innings_to_outs("5:2") == 17
        assert parse_innings_to_outs("9:0") == 27

    def test_parse_innings_float(self) -> None:
        assert parse_innings("5 1/3") == 5.333
        assert parse_innings("5 2/3") == 5.667
        assert parse_innings("1/3") == 0.333
        assert parse_innings("2/3") == 0.667
        assert parse_innings("5.1") == 5.1
        assert parse_innings("5.2") == 5.2
        assert parse_innings("5.0") == 5.0
        assert parse_innings("-") == 0.0
        assert parse_innings("") == 0.0
        assert parse_innings(None) == 0.0

    def test_safe_float_special_values(self) -> None:
        assert safe_float_or_none("-") is None
        assert safe_float_or_none(" - ") is None
        assert safe_float_or_none("") is None
        assert safe_float_or_none("null") is None
        assert safe_float_or_none("inf") is None
        assert safe_float_or_none("-inf") is None
        assert safe_float_or_none("infinity") is None
        assert safe_float_or_none("nan") is None
        assert safe_float_or_none("무한대") is None
        assert safe_float_or_none("3.45") == 3.45
        assert safe_float_or_none(4.5) == 4.5

        assert safe_float("-") == 0.0
        assert safe_float("inf") == 0.0
        assert safe_float("무한대") == 0.0
        assert safe_float("2.75") == 2.75

    def test_safe_int_special_values(self) -> None:
        assert safe_int_or_none("-") is None
        assert safe_int_or_none(" - ") is None
        assert safe_int_or_none("null") is None
        assert safe_int_or_none("15") == 15
        assert safe_int_or_none(10) == 10

        assert safe_int("-") == 0
        assert safe_int("inf") == 0
        assert safe_int("12") == 12
        assert to_int("-", default=-1) == -1


# ============================================================================
# 2. Database Deduplication & UPSERT Tests
# ============================================================================
@pytest.fixture
def sqlite_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(
        engine,
        tables=[
            PlayerBasic.__table__,
            Game.__table__,
            GameBattingStat.__table__,
            GamePitchingStat.__table__,
            GameInningScore.__table__,
            PlayerGameBatting.__table__,
            PlayerGamePitching.__table__,
        ],
    )
    Session = sessionmaker(bind=engine)
    session = Session()

    # Seed basic player
    session.add(PlayerBasic(player_id=1001, name="홍길동", team="LG"))
    session.add(PlayerBasic(player_id=1002, name="이순신", team="SS"))
    session.add(
        Game(
            game_id="20260505SSLG0",
            game_date=date(2026, 5, 5),
            home_team="LG",
            away_team="SS",
            home_score=5,
            away_score=3,
            game_status="COMPLETED",
        )
    )
    session.commit()

    try:
        yield session
    finally:
        session.close()


class TestDeduplicationAndUpsert:
    def test_deduplicate_mappings(self) -> None:
        # Batting duplicate player_id
        mappings = [
            {"game_id": "20260505SSLG0", "player_id": 1001, "player_name": "홍길동", "hits": 1},
            {"game_id": "20260505SSLG0", "player_id": 1001, "player_name": "홍길동", "hits": 2},  # Latest wins
            {"game_id": "20260505SSLG0", "player_id": 1002, "player_name": "이순신", "hits": 0},
        ]
        deduped = _deduplicate_mappings(GameBattingStat, mappings)
        assert len(deduped) == 2
        hong = next(m for m in deduped if m["player_id"] == 1001)
        assert hong["hits"] == 2

    def test_upsert_player_game_batting(self, sqlite_session) -> None:
        records = [
            {
                "game_id": "20260505SSLG0",
                "player_id": 1001,
                "player_name": "홍길동",
                "team_side": "home",
                "hits": 1,
                "at_bats": 4,
                "runs": 1,
            },
            {
                "game_id": "20260505SSLG0",
                "player_id": 1001,
                "player_name": "홍길동",
                "team_side": "home",
                "hits": 3,  # Updated stats
                "at_bats": 4,
                "runs": 2,
            },
        ]
        count = upsert_player_game_batting(sqlite_session, records)
        assert count == 1
        sqlite_session.commit()

        row = sqlite_session.query(PlayerGameBatting).filter_by(game_id="20260505SSLG0", player_id=1001).one()
        assert row.hits == 3
        assert row.runs == 2

    def test_upsert_player_game_pitching(self, sqlite_session) -> None:
        records = [
            {
                "game_id": "20260505SSLG0",
                "player_id": 1002,
                "player_name": "이순신",
                "team_side": "away",
                "innings_outs": 15,
                "runs_allowed": 4,
            },
            {
                "game_id": "20260505SSLG0",
                "player_id": 1002,
                "player_name": "이순신",
                "team_side": "away",
                "innings_outs": 18,
                "runs_allowed": 5,
            },
        ]
        count = upsert_player_game_pitching(sqlite_session, records)
        assert count == 1
        sqlite_session.commit()

        row = sqlite_session.query(PlayerGamePitching).filter_by(game_id="20260505SSLG0", player_id=1002).one()
        assert row.innings_outs == 18
        assert row.runs_allowed == 5


# ============================================================================
# 3. Boxscore Verification Logic Tests
# ============================================================================
class TestBoxscoreVerification:
    def test_verify_boxscore_sums_matching(self) -> None:
        game_data = {
            "game_id": "20260505SSLG0",
            "teams": {
                "away": {"code": "SS", "score": 3, "hits": 6},
                "home": {"code": "LG", "score": 5, "hits": 8},
            },
            "hitters": {
                "away": [
                    {"player_id": 1002, "stats": {"at_bats": 4, "hits": 3, "runs": 2}},
                    {"player_id": 1003, "stats": {"at_bats": 4, "hits": 3, "runs": 1}},
                ],
                "home": [
                    {"player_id": 1001, "stats": {"at_bats": 4, "hits": 4, "runs": 3}},
                    {"player_id": 1004, "stats": {"at_bats": 4, "hits": 4, "runs": 2}},
                ],
            },
            "pitchers": {
                "away": [{"player_id": 1005, "stats": {"innings_outs": 24, "runs_allowed": 5}}],
                "home": [{"player_id": 1006, "stats": {"innings_outs": 27, "runs_allowed": 3}}],
            },
        }
        team_totals = {
            "away": {"at_bats": 8, "hits": 6, "runs": 3},
            "home": {"at_bats": 8, "hits": 8, "runs": 5},
        }
        is_valid, discrepancies = verify_boxscore_sums(game_data, team_totals)
        assert is_valid is True
        assert len(discrepancies) == 0

    def test_verify_boxscore_sums_mismatch(self) -> None:
        game_data = {
            "game_id": "20260505SSLG0",
            "teams": {
                "away": {"code": "SS", "score": 3, "hits": 6},
                "home": {"code": "LG", "score": 5, "hits": 8},
            },
            "hitters": {
                "away": [
                    {"player_id": 1002, "stats": {"at_bats": 4, "hits": 2, "runs": 1}},  # Missing 1 run & 1 hit!
                ],
                "home": [],
            },
        }
        team_totals = {
            "away": {"at_bats": 8, "hits": 6, "runs": 3},
        }
        is_valid, discrepancies = verify_boxscore_sums(game_data, team_totals)
        assert is_valid is False
        assert len(discrepancies) > 0

    def test_verify_daily_game_count(self, sqlite_session) -> None:
        # Target date 2026-05-05 has 1 game in sqlite_session fixture
        is_match, count, expected, notes = verify_daily_game_count("2026-05-05", sqlite_session, expected_count=1)
        assert is_match is True
        assert count == 1

        is_match, count, expected, notes = verify_daily_game_count("2026-05-05", sqlite_session, expected_count=5)
        assert is_match is False
        assert count == 1
        assert expected == 5
        assert len(notes) > 0


# ============================================================================
# 4. Failure Logger Tests
# ============================================================================
class TestFailureLogger:
    def test_record_crawl_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            log_path = Path(tmpdir) / "failures.log"
            record_crawl_failure(
                target_type="game",
                identifier="20260505SSLG0",
                url="https://www.koreabaseball.com/Game/Boxscore.aspx?gameId=20260505SSLG0",
                reason="hitter_totals_mismatch",
                retry_count=3,
                log_path=log_path,
            )

            assert log_path.exists()
            content = log_path.read_text(encoding="utf-8")
            assert "20260505SSLG0" in content
            assert "hitter_totals_mismatch" in content
            assert "Retries: 3" in content or "\t3\t" in content
