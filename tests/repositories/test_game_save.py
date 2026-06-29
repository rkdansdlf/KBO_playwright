from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.season import KboSeason
from src.models.team import Team
from src.repositories.game_save import (
    _clean_pregame_text,
    _extract_existing_preview_payload,
    _get_or_create_game,
    _parse_detail_game_date,
    _resolve_pregame_starter,
    _apply_snapshot_status_and_winner,
    _apply_pregame_game_fields,
    _validate_inning_score_consistency,
    get_games_by_date,
    resolve_canonical_game_id,
    save_game_detail,
    save_game_snapshot,
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
    GameBattingStat.__table__.create(engine)
    GamePitchingStat.__table__.create(engine)
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
            },
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
            },
        )
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None
        assert game.away_team is not None


class TestSaveScheduleGameWithWriteContract:
    def test_save_schedule_game_with_write_contract_claim(self, session):
        from src.services.game_write_contract import GameWriteSource

        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        mock_contract = MagicMock()
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
            },
            write_contract=mock_contract,
        )
        assert result is True
        mock_contract.claim_game.assert_called()

    def test_save_schedule_game_with_write_contract_new_game(self, session):
        from src.services.game_write_contract import GameWriteSource

        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        mock_contract = MagicMock()
        result = save_schedule_game(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_code": "SS",
                "home_team_code": "LG",
                "season_year": 2024,
                "game_status": "scheduled",
            },
            write_contract=mock_contract,
        )
        assert result is True
        mock_contract.field_updated.assert_called()

    def test_save_schedule_game_metadata_with_stadium(self, session):
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
                "stadium": "Jamsil",
            },
        )
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None

    def test_save_schedule_game_no_metadata(self, session):
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
            },
        )
        assert result is True


class TestSaveGameDetail:
    def _make_detail_data(self, **overrides):
        data = {
            "game_id": "20241015LGSS0",
            "game_date": "2024-10-15",
            "teams": {
                "away": {"code": "SS", "score": 5},
                "home": {"code": "LG", "score": 3},
            },
            "metadata": {"stadium": "Jamsil"},
            "hitters": {"away": [], "home": []},
            "pitchers": {"away": [], "home": []},
            "game_status": "completed",
        }
        data.update(overrides)
        return data

    def test_save_game_detail_empty(self):
        result = save_game_detail({})
        assert result is False

    def test_save_game_detail_none(self):
        result = save_game_detail(None)
        assert result is False

    def test_save_game_detail_success(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        result = save_game_detail(data)
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None
        assert game.home_score == 3
        assert game.away_score == 5
        assert game.home_team == "LG"
        assert game.away_team == "SS"

    def test_save_game_detail_with_write_contract(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        mock_contract = MagicMock()
        data = self._make_detail_data()
        result = save_game_detail(data, write_contract=mock_contract)
        assert result is True
        mock_contract.claim_game.assert_called()

    def test_save_game_detail_existing_game(self, session):
        session.add(Game(game_id="20241015SSLG0", game_date=date(2024, 10, 15)))
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        result = save_game_detail(data)
        assert result is True

        game = session.query(Game).filter(Game.game_id == "20241015SSLG0").first()
        assert game.home_score == 3

    def test_save_game_detail_with_pitchers(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        data["pitchers"] = {
            "away": [{"player_name": "Kim", "is_starting": True}],
            "home": [{"player_name": "Park", "is_starting": True}],
        }
        result = save_game_detail(data)
        assert result is True

        game = session.query(Game).filter(Game.game_id == "20241015SSLG0").first()
        assert game is not None
        assert game.away_pitcher == "Kim"
        assert game.home_pitcher == "Park"

    def test_save_game_detail_with_summary(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_detail_data()
        data["summary"] = [
            {"summary_type": "MVP", "detail_text": "Kim (3 hits, 2 RBI)"},
        ]
        result = save_game_detail(data)
        assert result is True

    def test_save_game_detail_invalid_date(self, session):
        data = self._make_detail_data()
        data["game_id"] = "INVALID"
        data["game_date"] = "not-a-date"
        result = save_game_detail(data)
        assert result is True

        game = session.query(Game).filter(Game.game_id == "INVALID").first()
        assert game is not None

    def test_save_game_detail_db_error(self, session):
        data = self._make_detail_data()

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.commit.side_effect = OSError("DB Error")

        with (
            patch("src.repositories.game_save.SessionLocal", return_value=mock_session),
            patch("src.repositories.game_save._auto_sync_to_oci"),
        ):
            result = save_game_detail(data)
            assert result is False


class TestSaveGameSnapshot:
    def _make_snapshot_data(self, **overrides):
        data = {
            "game_id": "20241015LGSS0",
            "game_date": "2024-10-15",
            "teams": {
                "away": {"code": "SS", "score": 5},
                "home": {"code": "LG", "score": 3},
            },
            "metadata": {"stadium": "Jamsil"},
            "pitchers": {"away": [], "home": []},
        }
        data.update(overrides)
        return data

    def test_save_game_snapshot_empty(self):
        result = save_game_snapshot({})
        assert result is False

    def test_save_game_snapshot_none(self):
        result = save_game_snapshot(None)
        assert result is False

    def test_save_game_snapshot_success(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_snapshot_data()
        result = save_game_snapshot(data, status="completed")
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None
        assert game.home_score == 3
        assert game.away_score == 5

    def test_save_game_snapshot_with_pitchers(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_snapshot_data()
        data["pitchers"] = {
            "away": [{"player_name": "Kim", "is_starting": True}],
            "home": [{"player_name": "Park", "is_starting": True}],
        }
        result = save_game_snapshot(data, status="completed")
        assert result is True

        game = session.query(Game).filter(Game.game_id == "20241015SSLG0").first()
        assert game is not None
        assert game.away_pitcher == "Kim"
        assert game.home_pitcher == "Park"

    def test_save_game_snapshot_existing_game(self, session):
        session.add(Game(game_id="20241015SSLG0", game_date=date(2024, 10, 15)))
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        data = self._make_snapshot_data()
        result = save_game_snapshot(data, status="completed")
        assert result is True

    def test_save_game_snapshot_invalid_date(self, session):
        data = self._make_snapshot_data()
        data["game_id"] = "INVALID"
        data["game_date"] = "not-a-date"
        result = save_game_snapshot(data)
        assert result is True

        game = session.query(Game).filter(Game.game_id == "INVALID").first()
        assert game is not None

    def test_save_game_snapshot_db_error(self, session):
        data = self._make_snapshot_data()

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.commit.side_effect = OSError("DB Error")

        with (
            patch("src.repositories.game_save.SessionLocal", return_value=mock_session),
            patch("src.repositories.game_save._auto_sync_to_oci"),
        ):
            result = save_game_snapshot(data)
            assert result is False


class TestSavePregameLineupsExtended:
    def test_save_pregame_lineups_no_game_id(self):
        result = save_pregame_lineups({})
        assert result is False

    def test_save_pregame_lineups_no_date(self):
        result = save_pregame_lineups({"game_id": "20241015LGSS0", "game_date": "not-a-date"})
        assert result is False

    def test_save_pregame_lineups_no_provisional_id(self):
        result = save_pregame_lineups({"game_date": "2024-10-15"})
        assert result is False

    def test_save_pregame_lineups_with_starters(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        result = save_pregame_lineups(
            {
                "game_id": "20241015LGSS0",
                "game_date": "2024-10-15",
                "away_team_name": "SSG",
                "home_team_name": "LG",
                "away_starter": "Kim",
                "home_starter": "Park",
                "away_lineup": [{"player_name": "Kim", "batting_order": 1, "position": "CF"}],
                "home_lineup": [{"player_name": "Park", "batting_order": 1, "position": "SS"}],
            },
        )
        assert result is True

        game = session.query(Game).filter(Game.game_id.like("20241015%")).first()
        assert game is not None
        assert game.away_pitcher == "Kim"
        assert game.home_pitcher == "Park"

    def test_save_pregame_lineups_db_error(self, session):
        session.add(KboSeason(season_id=1, season_year=2024, league_type_code=0, league_type_name="regular"))
        session.commit()

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.commit.side_effect = OSError("DB Error")

        with (
            patch("src.repositories.game_save.SessionLocal", return_value=mock_session),
            patch("src.repositories.game_save._auto_sync_to_oci"),
        ):
            result = save_pregame_lineups(
                {
                    "game_id": "20241015LGSS0",
                    "game_date": "2024-10-15",
                    "away_team_name": "SSG",
                    "home_team_name": "LG",
                },
            )
            assert result is False


class TestParseDetailGameDate:
    def test_parse_detail_game_date_from_data(self):
        game_date_str, game_date = _parse_detail_game_date({"game_date": "2024-10-15"}, None)
        assert game_date_str == "20241015"
        assert game_date == date(2024, 10, 15)

    def test_parse_detail_game_date_from_game_id(self):
        game_date_str, game_date = _parse_detail_game_date({}, "20241015LGSS0")
        assert game_date_str == "20241015"
        assert game_date == date(2024, 10, 15)

    def test_parse_detail_game_date_invalid(self):
        game_date_str, game_date = _parse_detail_game_date({"game_date": "invalid"}, None)
        assert game_date_str == "invalid"
        assert game_date is not None


class TestGetOrCreateGame:
    def test_get_or_create_existing(self, session):
        from src.services.game_write_contract import GameWriteSource

        existing = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(existing)
        session.commit()

        source = GameWriteSource("detail", "GameDetailCrawler", "detail_recovery")
        game, created = _get_or_create_game(session, "20241015LGSS0", date(2024, 10, 15), source, None)
        assert created is False
        assert game.game_id == "20241015LGSS0"

    def test_get_or_create_new(self, session):
        from src.services.game_write_contract import GameWriteSource

        source = GameWriteSource("detail", "GameDetailCrawler", "detail_recovery")
        game, created = _get_or_create_game(session, "20241015LGSS0", date(2024, 10, 15), source, None)
        assert created is True
        assert game.game_id == "20241015LGSS0"

    def test_get_or_create_with_write_contract(self, session):
        from src.services.game_write_contract import GameWriteSource

        source = GameWriteSource("detail", "GameDetailCrawler", "detail_recovery")
        mock_contract = MagicMock()
        game, created = _get_or_create_game(session, "20241015LGSS0", date(2024, 10, 15), source, mock_contract)
        assert created is True
        mock_contract.field_updated.assert_called_once()


class TestValidateInningScoreConsistency:
    def test_matching_scores_no_warnings(self) -> None:
        teams = {
            "away": {"code": "LG", "score": 5, "line_score": [1, 0, 2, 0, 1, 0, 0, 1, 0]},
            "home": {"code": "SS", "score": 3, "line_score": [0, 1, 0, 0, 2, 0, 0, 0, 0]},
        }
        records = [
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 2},
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "home", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "home", "runs": 2},
        ]
        warnings = _validate_inning_score_consistency(teams, records, "20241015LGSS0")
        assert warnings == []

    def test_mismatched_away_score(self) -> None:
        teams = {
            "away": {"code": "LG", "score": 10, "line_score": [1, 0, 2, 0, 1, 0, 0, 1, 0]},
            "home": {"code": "SS", "score": 3, "line_score": [0, 1, 0, 0, 2, 0, 0, 0, 0]},
        }
        records = [
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 2},
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "home", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "home", "runs": 2},
        ]
        warnings = _validate_inning_score_consistency(teams, records, "20241015LGSS0")
        assert len(warnings) == 1
        assert "away" in warnings[0]
        assert "sum=5 vs score=10" in warnings[0]

    def test_none_score_skipped(self) -> None:
        teams = {
            "away": {"code": "LG", "score": None, "line_score": [1, 0, 2]},
            "home": {"code": "SS", "score": 3, "line_score": [0, 1, 0, 0, 2]},
        }
        records = [
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "away", "runs": 2},
            {"game_id": "20241015LGSS0", "team_side": "home", "runs": 1},
            {"game_id": "20241015LGSS0", "team_side": "home", "runs": 2},
        ]
        warnings = _validate_inning_score_consistency(teams, records, "20241015LGSS0")
        assert warnings == []
