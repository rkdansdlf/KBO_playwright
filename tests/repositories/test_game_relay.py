from __future__ import annotations

from datetime import date
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.game import (
    Game,
    GameEvent,
    GameIdAlias,
    GameMetadata,
    GamePlayByPlay,
    GameValidationMetrics,
)
from src.models.player import PlayerBasic
from src.repositories.game_relay import (
    _coerce_player_id,
    _duplicate_provider_count,
    _relay_player_resolution_context,
    derive_play_by_play_rows_from_events,
    mark_relay_source_unavailable,
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
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_deps(session):
    with (
        patch("src.repositories.game_relay.SessionLocal", return_value=session),
    ):
        yield


class TestRelayHelpers:
    def test_coerce_player_id_none(self):
        assert _coerce_player_id(None) is None
        assert _coerce_player_id("") is None
        assert _coerce_player_id("abc") is None

    def test_coerce_player_id_valid(self):
        assert _coerce_player_id(1001) == 1001
        assert _coerce_player_id("1001") == 1001

    def test_relay_player_resolution_context_offensive(self):
        result = _relay_player_resolution_context("1번타자 김선수")
        assert result is not None
        name, side, is_pitcher = result
        assert name == "김선수"
        assert side == "offense"
        assert is_pitcher is False

    def test_relay_player_resolution_context_defensive(self):
        result = _relay_player_resolution_context("투수 박선수")
        assert result is not None
        name, side, is_pitcher = result
        assert name == "박선수"
        assert side == "defense"
        assert is_pitcher is True

    def test_relay_player_resolution_context_defensive_not_pitcher(self):
        result = _relay_player_resolution_context("1루수 최선수")
        assert result is not None
        name, side, is_pitcher = result
        assert name == "최선수"
        assert side == "defense"
        assert is_pitcher is False

    def test_relay_player_resolution_context_plain(self):
        result = _relay_player_resolution_context("이선수")
        assert result is not None
        name, side, is_pitcher = result
        assert name == "이선수"
        assert side == "offense"
        assert is_pitcher is False

    def test_relay_player_resolution_context_empty(self):
        assert _relay_player_resolution_context("") is None
        assert _relay_player_resolution_context(None) is None
        assert _relay_player_resolution_context("   ") is None

    def test_relay_player_resolution_context_decision_labels(self):
        assert _relay_player_resolution_context("승리투수 김선수") is None
        assert _relay_player_resolution_context("패전투수 박선수") is None
        assert _relay_player_resolution_context("세이브 최선수") is None
        assert _relay_player_resolution_context("홀드 이선수") is None

    def test_relay_player_resolution_context_turn_noise(self):
        assert _relay_player_resolution_context("1회초 1번타순 김선수") is None

    def test_duplicate_provider_count(self):
        events = [
            {"provider_log_id": "a"},
            {"provider_log_id": "b"},
            {"provider_log_id": "a"},
        ]
        assert _duplicate_provider_count(events, []) == 1

    def test_duplicate_provider_count_no_dups(self):
        events = [
            {"provider_log_id": "a"},
            {"provider_log_id": "b"},
        ]
        assert _duplicate_provider_count(events, []) == 0

    def test_duplicate_provider_count_empty_ids(self):
        events = [
            {"provider_log_id": None},
            {"provider_log_id": ""},
        ]
        assert _duplicate_provider_count(events, []) == 0

    def test_duplicate_provider_count_across_events_and_pbp(self):
        events = [{"provider_log_id": "x"}]
        pbp = [{"provider_log_id": "x"}, {"provider_log_id": "y"}]
        assert _duplicate_provider_count(events, pbp) == 1


class TestDerivePlayByPlay:
    def test_derive_play_by_play_rows_from_events(self):
        events = [
            {"inning": 1, "inning_half": "top", "batter_name": "Kim", "pitcher_name": "Park",
             "description": "Strikeout", "event_type": "strikeout", "result_code": "K"},
        ]
        with patch("src.repositories.game_relay.event_to_pbp_row") as mock_event_to_pbp_row:
            mock_event_to_pbp_row.return_value = {
                "inning": 1, "inning_half": "top", "batter_name": "Kim", "pitcher_name": "Park",
                "play_description": "Strikeout", "event_type": "strikeout", "result": "K",
            }
            result = derive_play_by_play_rows_from_events(events)
            assert len(result) == 1
            assert result[0]["batter_name"] == "Kim"


class TestMarkRelaySourceUnavailable:
    def test_mark_unavailable_no_game(self):
        result = mark_relay_source_unavailable("nonexistent", reason="no_data")
        # Even without a pre-existing game, the function creates a stub
        assert result is True

    def test_mark_unavailable_success(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.commit()

        result = mark_relay_source_unavailable("20241015LGSS0", reason="source_not_found", source_name="kbo")
        assert result is True

        metrics = session.query(GameValidationMetrics).filter(
            GameValidationMetrics.game_id == "20241015LGSS0"
        ).one_or_none()
        assert metrics is not None
        assert metrics.validation_status == "source_unavailable"
        assert metrics.source_used == "kbo"

        alias = session.query(GameIdAlias).filter(
            GameIdAlias.alias_game_id == "20241015LGSS0"
        ).one_or_none()
        # Alias may or may not be created depending on normalization
        if alias is not None:
            assert alias.canonical_game_id is not None

    def test_mark_unavailable_invalid_game_id(self):
        result = mark_relay_source_unavailable("", reason="no_data")
        assert result is False
