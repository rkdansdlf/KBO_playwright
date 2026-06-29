from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameValidationMetrics,
)
from src.models.player import PlayerBasic
from src.repositories.game_relay import (
    ValidationMetricsData,
    _RelayResolutionContext,
    _RelayValidationResult,
    _apply_relay_lifecycle_state,
    _build_relay_event_rows,
    _build_relay_pbp_rows,
    _duplicate_provider_count,
    _game_date_from_game_id,
    _get_or_create_game_parent,
    _has_repairable_game_children,
    _log_relay_save_result,
    _relay_resolution_context,
    _replace_relay_rows,
    _resolve_event_batter,
    _resolve_pbp_player,
    _upsert_relay_validation_metadata,
)


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    Game.__table__.create(engine)
    GameEvent.__table__.create(engine)
    GameMetadata.__table__.create(engine)
    GamePlayByPlay.__table__.create(engine)
    GameValidationMetrics.__table__.create(engine)
    GameIdAlias.__table__.create(engine)
    PlayerBasic.__table__.create(engine)
    GameBattingStat.__table__.create(engine)
    GamePitchingStat.__table__.create(engine)
    GameInningScore.__table__.create(engine)
    GameLineup.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_session(session):
    with patch("src.repositories.game_relay.SessionLocal", return_value=session):
        yield


class TestHasRepairableGameChildren:
    def test_no_children(self, session):
        assert _has_repairable_game_children(session, "20241015LGSS0") is False

    def test_with_batting(self, session):
        session.add(
            GameBattingStat(
                game_id="20241015LGSS0",
                player_name="Kim",
                team_side="away",
                appearance_seq=1,
                at_bats=4,
            ),
        )
        session.commit()
        assert _has_repairable_game_children(session, "20241015LGSS0") is True


class TestGameDateFromGameId:
    def test_valid_game_id(self):
        result = _game_date_from_game_id("20241015LGSS0")
        assert result == date(2024, 10, 15)

    def test_invalid_date_returns_today(self):
        result = _game_date_from_game_id("invalid_game_id")
        assert result is not None

    def test_short_game_id_returns_today(self):
        result = _game_date_from_game_id("2024")
        assert result is not None


class TestGetOrCreateGameParent:
    def test_returns_existing(self, session):
        existing = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(existing)
        session.commit()

        game = _get_or_create_game_parent(session, "20241015LGSS0", date(2024, 10, 15))
        assert game.game_id == existing.game_id

    def test_creates_new(self, session):
        game = _get_or_create_game_parent(session, "20241015LGSS0", date(2024, 10, 15))
        assert game.game_id == "20241015LGSS0"
        assert game.game_date == date(2024, 10, 15)


class TestApplyRepairedGameSeason:
    def test_sets_season_id(self, session):
        from src.repositories.game_relay import _apply_repaired_game_season

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.flush()

        with patch("src.repositories.game_relay._resolve_game_season_id", return_value=42):
            _apply_repaired_game_season(session, game, date(2024, 10, 15), 2024)
            assert game.season_id == 42

    def test_no_season_id_resolved(self, session):
        from src.repositories.game_relay import _apply_repaired_game_season

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.flush()

        with patch("src.repositories.game_relay._resolve_game_season_id", return_value=None):
            _apply_repaired_game_season(session, game, date(2024, 10, 15), 2024)


class TestApplyRepairedGameTeams:
    def test_sets_teams_from_children(self, session):
        from src.repositories.game_relay import _apply_repaired_game_teams

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.flush()

        with patch("src.repositories.game_relay._infer_team_code_from_children", side_effect=["LG", "SS"]):
            _apply_repaired_game_teams(session, game, "20241015LGSS0", 2024)
            assert game.away_team == "LG"
            assert game.home_team == "SS"


class TestApplyRepairedGameScores:
    def test_sets_scores(self, session):
        from src.repositories.game_relay import _apply_repaired_game_scores

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.flush()

        with patch("src.repositories.game_relay._infer_score_from_children", side_effect=[5, 3]):
            _apply_repaired_game_scores(session, game, "20241015LGSS0")
            assert game.away_score == 5
            assert game.home_score == 3

    def test_no_scores(self, session):
        from src.repositories.game_relay import _apply_repaired_game_scores

        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.flush()

        with patch("src.repositories.game_relay._infer_score_from_children", return_value=None):
            _apply_repaired_game_scores(session, game, "20241015LGSS0")


class TestRelayResolutionContextParsing:
    def test_uses_game_row_if_exists(self, session):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15), away_team="KT", home_team="NC")
        session.add(game)
        session.flush()

        with patch("src.repositories.game_relay.team_code_from_game_id_segment", side_effect=["LG", "SS"]):
            ctx = _relay_resolution_context(session, "20241015LGSS0")
            assert ctx.away_team_code == "KT"
            assert ctx.home_team_code == "NC"

    def test_resolver_init_exception(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.flush()

        with patch(
            "src.services.player_id_resolver.PlayerIdResolver",
            side_effect=RuntimeError("init failed"),
        ):
            ctx = _relay_resolution_context(session, "20241015LGSS0")
            assert ctx.resolver is None

    def test_parse_invalid_game_id(self, session):
        ctx = _relay_resolution_context(session, "INVALID")
        assert ctx.season_year is None or ctx.season_year == 0

    def test_no_game_row_returns_parsed_codes(self, session):
        from src.repositories.game_relay import _relay_resolution_context

        with patch("src.repositories.game_relay.team_code_from_game_id_segment", side_effect=["LG", "SS"]):
            mock_session = MagicMock()
            mock_session.query.return_value.filter.return_value.one_or_none.return_value = None
            ctx = _relay_resolution_context(mock_session, "20241015LGSS0")
            assert ctx.away_team_code == "LG"
            assert ctx.home_team_code == "SS"


class TestResolvePbpPlayer:
    def test_no_resolver(self):
        resolution = _RelayResolutionContext(None, 2024, "SS", "LG")
        row = {"batter_name": "Kim", "pitcher_name": "Park", "inning_half": "top"}
        result = _resolve_pbp_player(row, resolution)
        assert result == (None, None, None, None)

    def test_no_batter_name(self):
        mock_resolver = MagicMock()
        resolution = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        row = {"batter_name": None, "pitcher_name": None, "inning_half": "top"}
        pid, conf, reason, unresolved = _resolve_pbp_player(row, resolution)
        assert pid is None

    def test_offensive_batter_resolved(self):
        mock_resolver = MagicMock()
        mock_resolver.resolve_id.return_value = 12345
        resolution = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        row = {
            "batter_name": "1번타자 Kim",
            "pitcher_name": "Park",
            "inning_half": "top",
            "play_description": "Strikeout",
        }
        pid, conf, reason, unresolved = _resolve_pbp_player(row, resolution)
        assert pid == 12345
        assert conf == "resolved"
        assert unresolved is None

    def test_fallback_to_pitcher_only(self):
        mock_resolver = MagicMock()
        mock_resolver.resolve_id.return_value = 99999
        resolution = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        row = {
            "batter_name": "no match pattern",
            "pitcher_name": "Park",
            "inning_half": "top",
            "play_description": "Strikeout",
        }
        pid, conf, reason, unresolved = _resolve_pbp_player(row, resolution)
        assert pid == 99999


class TestBuildRelayPbpRows:
    def test_builds_rows(self):
        mock_resolver = MagicMock()
        mock_resolver.resolve_id.return_value = 12345
        resolution = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        raw_pbp_rows = [
            {
                "batter_name": "1번타자 Kim",
                "pitcher_name": "Park",
                "inning_half": "top",
                "play_description": "Strikeout",
                "inning": 1,
                "event_type": "strikeout",
                "result": "K",
            },
        ]
        rows = _build_relay_pbp_rows("20241015LGSS0", raw_pbp_rows, "kbo", resolution)
        assert len(rows) == 1
        assert rows[0].game_id == "20241015LGSS0"


class TestResolveEventBatter:
    def test_resolves_batter_name(self):
        mock_resolver = MagicMock()
        mock_resolver.resolve_id.return_value = 54321
        resolution = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        result = _resolve_event_batter("1번타자 Kim", "top", "Strikeout", resolution)
        name, team, pid, conf, reason = result
        assert name == "Kim"
        assert pid == 54321
        assert team == "SS"

    def test_returns_name_for_plain_batter(self):
        mock_resolver = MagicMock()
        resolution = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        result = _resolve_event_batter("Kim", "top", "Strikeout", resolution)
        name, team, pid, conf, reason = result
        assert name == "Kim"


class TestBuildRelayEventRows:
    def test_builds_event_rows(self):
        mock_resolver = MagicMock()
        mock_resolver.resolve_id.return_value = 11111
        resolution = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        valid_events = [
            {
                "event_seq": 1,
                "inning": 1,
                "inning_half": "top",
                "batter_name": "1번타자 Kim",
                "pitcher_name": "Park",
                "description": "Strikeout",
                "event_type": "strikeout",
                "result_code": "K",
            },
        ]
        rows = _build_relay_event_rows("20241015LGSS0", valid_events, "kbo", None, resolution)
        assert len(rows) == 1
        assert rows[0].game_id == "20241015LGSS0"
        assert rows[0].extra_json["relay_source"] == "kbo"


class TestReplaceRelayRows:
    def test_replaces_pbp_rows(self, session):
        from src.repositories.game_helpers import GameWriteSource
        from src.repositories.game_relay import RelayRowReplaceContext

        session.add(GamePlayByPlay(game_id="20241015LGSS0", inning=1))
        session.commit()

        source = GameWriteSource("relay", "RelayCrawler", "relay")
        new_row = GamePlayByPlay(game_id="20241015LGSS0", inning=2)
        ctx = RelayRowReplaceContext(
            pbp_rows=[new_row],
            event_rows=[],
            source=source,
            write_contract=None,
        )
        changed = _replace_relay_rows(session, "20241015LGSS0", ctx=ctx)
        assert changed is True


class TestLogRelaySaveResult:
    def test_warning_when_no_valid_rows(self):
        with patch("src.repositories.game_relay.logger") as mock_logger:
            _log_relay_save_result("g1", [{}], [], 0, 5)
            mock_logger.warning.assert_called_once()

    def test_info_when_valid_rows(self):
        with patch("src.repositories.game_relay.logger") as mock_logger:
            _log_relay_save_result("g1", [{}], [{}], 1, 1)
            mock_logger.info.assert_called_once()


class TestApplyRelayLifecycleState:
    def test_no_game_row(self):
        _apply_relay_lifecycle_state(None, "g1", "final")

    def test_no_state(self):
        game = MagicMock()
        game.game_lifecycle_state = "initial"
        _apply_relay_lifecycle_state(game, "g1", None)

    def test_valid_transition(self):
        game = MagicMock()
        game.game_lifecycle_state = "live"
        with patch("src.utils.game_state.validate_transition", return_value=(True, None)):
            _apply_relay_lifecycle_state(game, "g1", "final")
            assert game.game_lifecycle_state == "final"

    def test_invalid_transition(self):
        game = MagicMock()
        game.game_lifecycle_state = "completed"
        with patch("src.utils.game_state.validate_transition", return_value=(False, "bad transition")):
            _apply_relay_lifecycle_state(game, "g1", "scheduled")
            assert game.game_lifecycle_state == "completed"
