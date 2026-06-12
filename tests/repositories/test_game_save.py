from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GameSummary,
)
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.season import KboSeason
from src.models.team import Team
from src.repositories.game_save import (
    _clean_pregame_text,
    _extract_existing_preview_payload,
    _resolve_pregame_starter,
    get_games_by_date,
    resolve_canonical_game_id,
    save_pregame_lineups,
    save_schedule_game,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    Game.__table__.create(engine)
    GameIdAlias.__table__.create(engine)
    GameInningScore.__table__.create(engine)
    GameLineup.__table__.create(engine)
    GameMetadata.__table__.create(engine)
    GameSummary.__table__.create(engine)
    PlayerBasic.__table__.create(engine)
    PlayerSeasonBatting.__table__.create(engine)
    KboSeason.__table__.create(engine)
    Team.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_deps(session):
    with (
        patch("src.repositories.game_save.SessionLocal", return_value=session),
        patch("src.repositories.game_save._auto_sync_to_oci"),
    ):
        yield


class TestGetGamesByDate:
    def test_get_games_by_date(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(Game(game_id="20241015SSLG0", game_date=date(2024, 10, 15)))
        session.add(Game(game_id="20241016LGSS0", game_date=date(2024, 10, 16)))
        session.commit()

        results = get_games_by_date("20241015")
        assert len(results) == 2

    def test_get_games_by_date_invalid(self):
        results = get_games_by_date("not-a-date")
        assert results == []


class TestResolveCanonicalGameId:
    def test_resolve_canonical_game_id(self, session):
        session.add(Game(game_id="CANONICAL", game_date=date(2024, 10, 15)))
        session.add(GameIdAlias(alias_game_id="ALIAS", canonical_game_id="CANONICAL", source="test", reason="test"))
        session.commit()

        result = resolve_canonical_game_id("ALIAS")
        assert result == "CANONICAL"

    def test_resolve_canonical_game_id_direct(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.commit()

        result = resolve_canonical_game_id("20241015LGSS0")
        assert result == "20241015LGSS0"

    def test_resolve_canonical_game_id_none(self):
        result = resolve_canonical_game_id("")
        assert result is None


class TestCleanPregameText:
    def test_clean_pregame_text_none(self):
        assert _clean_pregame_text(None) == ""

    def test_clean_pregame_text_with_value(self):
        assert _clean_pregame_text("  AAA  ") == "AAA"

    def test_clean_pregame_text_empty(self):
        assert _clean_pregame_text("") == ""


class TestExtractExistingPreviewPayload:
    def test_extract_existing_preview_payload_none(self):
        assert _extract_existing_preview_payload(None) == {}

    def test_extract_existing_preview_payload_valid(self):
        summary = MagicMock()
        summary.detail_text = '{"key": "value"}'
        result = _extract_existing_preview_payload(summary)
        assert result == {"key": "value"}

    def test_extract_existing_preview_payload_invalid_json(self):
        summary = MagicMock()
        summary.detail_text = "not-json"
        result = _extract_existing_preview_payload(summary)
        assert result == {}


class TestResolvePregameStarter:
    def test_resolve_pregame_starter_from_preview(self):
        game = MagicMock()
        game.away_pitcher = None
        preview = {"away_starter": "Kim", "away_starter_id": 1001}
        existing = {}
        result, rid = _resolve_pregame_starter(preview, game, existing, "away")
        assert result == "Kim"
        assert rid == 1001

    def test_resolve_pregame_starter_from_game(self):
        game = MagicMock()
        game.away_pitcher = "Park"
        preview = {}
        existing = {}
        result, _ = _resolve_pregame_starter(preview, game, existing, "away")
        assert result == "Park"

    def test_resolve_pregame_starter_id_fallback(self):
        game = MagicMock()
        game.away_pitcher = None
        preview = {"away_starter": "Kim"}
        existing = {"away_starter": "Kim", "away_starter_id": 999}
        result, rid = _resolve_pregame_starter(preview, game, existing, "away")
        assert result == "Kim"
        assert rid == 999


class TestSaveScheduleGame:
    def test_save_schedule_game_success(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        result = save_schedule_game(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_code": "SS",
                "home_team_code": "LG",
                "season_year": 2024,
                "game_status": "scheduled",
                "game_time": "18:30",
                "stadium": "Jamsil",
            }
        )
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None
        assert "LG" in (game.home_team or "")
        assert "SS" in (game.away_team or "")

    def test_save_schedule_game_invalid_date(self):
        result = save_schedule_game({"game_date": "invalid"})
        assert result is False

    def test_save_schedule_game_no_game_id(self):
        result = save_schedule_game({"game_date": "2024-10-15"})
        assert result is False


class TestSavePregameLineups:
    def test_save_pregame_lineups_empty_data(self):
        result = save_pregame_lineups({})
        assert result is False

    def test_save_pregame_lineups_invalid_date(self):
        result = save_pregame_lineups({"game_id": "20241015LGSS0", "game_date": "invalid"})
        assert result is False

    def test_save_pregame_lineups_success(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        result = save_pregame_lineups(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_name": "SSG",
                "home_team_name": "LG",
                "away_lineup": [{"player_name": "Kim", "batting_order": 1, "position": "CF"}],
                "home_lineup": [{"player_name": "Park", "batting_order": 1, "position": "SS"}],
            }
        )
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None
        assert game.away_team is not None
