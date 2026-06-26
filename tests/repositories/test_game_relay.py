from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import src.repositories.game_relay
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
from src.models.season import KboSeason
from src.models.team import Team
from src.repositories.game_relay import (
    _coerce_player_id,
    _duplicate_provider_count,
    _relay_player_resolution_context,
    _relay_text_indicates_defense_side,
    _upsert_validation_metrics,
    derive_play_by_play_rows_from_events,
    mark_relay_source_unavailable,
)
from src.repositories.game_relay import (
    ValidationMetricsData,
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
    KboSeason.__table__.create(engine)
    Team.__table__.create(engine)
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

    def test_relay_player_resolution_context_designated_hitter_defensive_change(self):
        result = _relay_player_resolution_context(
            "지명타자 오스틴",
            "지명타자 오스틴 : 1루수(으)로 수비위치 변경",
        )
        assert result is not None
        name, side, is_pitcher = result
        assert name == "오스틴"
        assert side == "defense"
        assert is_pitcher is False

    def test_relay_player_resolution_context_pinch_runner_defensive_change(self):
        result = _relay_player_resolution_context(
            "대주자 박시원",
            "대주자 박시원 : 우익수(으)로 수비위치 변경",
        )
        assert result is not None
        name, side, is_pitcher = result
        assert name == "박시원"
        assert side == "defense"
        assert is_pitcher is False

    def test_relay_player_resolution_context_pinch_hitter_batting_play_stays_offense(self):
        result = _relay_player_resolution_context(
            "대타 장성우",
            "대타 장성우 : 좌전 안타",
        )
        assert result is not None
        name, side, is_pitcher = result
        assert name == "장성우"
        assert side == "offense"
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
        assert _relay_player_resolution_context("5회초 4번타순 5구 후 18") is None

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
            {
                "inning": 1,
                "inning_half": "top",
                "batter_name": "Kim",
                "pitcher_name": "Park",
                "description": "Strikeout",
                "event_type": "strikeout",
                "result_code": "K",
            },
        ]
        with patch("src.repositories.game_relay.event_to_pbp_row") as mock_event_to_pbp_row:
            mock_event_to_pbp_row.return_value = {
                "inning": 1,
                "inning_half": "top",
                "batter_name": "Kim",
                "pitcher_name": "Park",
                "play_description": "Strikeout",
                "event_type": "strikeout",
                "result": "K",
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

        metrics = (
            session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == "20241015LGSS0").one_or_none()
        )
        assert metrics is not None
        assert metrics.validation_status == "source_unavailable"
        assert metrics.source_used == "kbo"

        alias = session.query(GameIdAlias).filter(GameIdAlias.alias_game_id == "20241015LGSS0").one_or_none()
        # Alias may or may not be created depending on normalization
        if alias is not None:
            assert alias.canonical_game_id is not None

    def test_mark_unavailable_invalid_game_id(self):
        result = mark_relay_source_unavailable("", reason="no_data")
        assert result is False

    def test_mark_unavailable_with_evidence(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.commit()

        result = mark_relay_source_unavailable(
            "20241015LGSS0",
            reason="source_not_found",
            source_name="kbo",
            evidence={"url": "http://example.com", "detail": "page not found"},
        )
        assert result is True

        metrics = (
            session.query(GameValidationMetrics).filter(GameValidationMetrics.game_id == "20241015LGSS0").one_or_none()
        )
        assert metrics is not None
        assert metrics.fallback_trigger_reason == "source_not_found"
        assert metrics.evidence_json is not None
        assert metrics.evidence_json.get("url") == "http://example.com"

    def test_mark_unavailable_sync_to_oci(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.commit()

        with patch("src.repositories.game_relay._auto_sync_to_oci") as mock_sync:
            result = mark_relay_source_unavailable("20241015LGSS0", reason="no_data", sync_to_oci=True)
            assert result is True
            mock_sync.assert_called_once_with("20241015LGSS0")


class TestRelayTextIndicatesDefenseSide:
    def test_no_description(self):
        assert _relay_text_indicates_defense_side("투수 김선수", None) is False
        assert _relay_text_indicates_defense_side("투수 김선수", "") is False

    def test_defensive_position_change(self):
        assert _relay_text_indicates_defense_side("1루수 김선수", "1루수 김선수 : 1루수(으)로 수비위치 변경") is True

    def test_no_replacement_marker(self):
        assert _relay_text_indicates_defense_side("투수 김선수", "투수 김선수 : 볼넣기") is False

    def test_source_is_defensive_role(self):
        assert _relay_text_indicates_defense_side("투수 김선수", "투수 김선수 : 1루수(으)로 교체") is True
        assert _relay_text_indicates_defense_side("1루수 김선수", "1루수 김선수 : 2루수(으)로 교체") is True

    def test_target_is_defensive(self):
        assert _relay_text_indicates_defense_side("대타 김선수", "대타 김선수 : 좌익수(으)로 교체") is True

    def test_target_not_defensive(self):
        assert _relay_text_indicates_defense_side("대타 김선수", "대타 김선수 : 좌전 안타") is False


class TestUpsertValidationMetrics:
    def test_create_new_metrics(self, session):
        data = ValidationMetricsData(
            validation_status="verified",
            source_name="kbo",
            parser_version="1.0",
            source_schema_version="2.0",
            payload_hash="abc123",
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics is not None
        assert metrics.game_id == "20241015LGSS0"
        assert metrics.validation_status == "verified"
        assert metrics.source_used == "kbo"
        assert metrics.parser_version == "1.0"
        assert metrics.source_schema_version == "2.0"
        assert metrics.payload_hash == "abc123"

    def test_update_existing_metrics_status_change(self, session):
        existing = GameValidationMetrics(
            game_id="20241015LGSS0",
            validation_status="pending",
            source_used="old",
        )
        session.add(existing)
        session.commit()

        data = ValidationMetricsData(validation_status="verified", source_name="kbo")
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.validation_status == "verified"
        assert metrics.previous_status == "pending"

    def test_update_existing_metrics_no_status_change(self, session):
        existing = GameValidationMetrics(
            game_id="20241015LGSS0",
            validation_status="verified",
            source_used="kbo",
        )
        session.add(existing)
        session.commit()

        data = ValidationMetricsData(validation_status="verified", source_name="kbo")
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.validation_status == "verified"
        assert metrics.previous_status is None

    def test_source_used_truncated(self, session):
        data = ValidationMetricsData(
            validation_status="verified",
            source_name="a_very_long_source_name_that_exceeds_16_chars",
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert len(metrics.source_used) == 16

    def test_source_used_fallback(self, session):
        existing = GameValidationMetrics(
            game_id="20241015LGSS0",
            validation_status="verified",
            source_used="existing_source",
        )
        session.add(existing)
        session.commit()

        data = ValidationMetricsData(validation_status="verified", source_name=None)
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.source_used == "existing_source"

    def test_duplicate_event_count(self, session):
        data = ValidationMetricsData(
            validation_status="verified",
            events=[{"provider_log_id": "a"}, {"provider_log_id": "a"}, {"provider_log_id": "b"}],
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.duplicate_event_count == 1

    def test_unclassified_event_count(self, session):
        data = ValidationMetricsData(
            validation_status="verified",
            events=[
                {"event_type": "unknown"},
                {"event_type": "unclassified"},
                {"event_type": "other"},
                {"event_type": "strikeout"},
            ],
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.unclassified_event_count == 3

    def test_finish_mismatch_count(self, session):
        data = ValidationMetricsData(
            validation_status="verified",
            error_reason="score_mismatch",
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.finish_mismatch_count == 1

    def test_finish_mismatch_count_increments(self, session):
        existing = GameValidationMetrics(
            game_id="20241015LGSS0",
            validation_status="verified",
            finish_mismatch_count=2,
        )
        session.add(existing)
        session.commit()

        data = ValidationMetricsData(
            validation_status="verified",
            error_reason="inning_score_mismatch",
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.finish_mismatch_count == 3

    def test_last_successful_event_at(self, session):
        data = ValidationMetricsData(
            validation_status="verified",
            events=[{"event_type": "strikeout"}],
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.last_successful_event_at is not None

    def test_fallback_trigger_reason(self, session):
        data = ValidationMetricsData(
            validation_status="source_unavailable",
            error_reason="some_error_reason_that_is_very_long_and_exceeds_64_chars_limit_xxxxxxxxxxxxxxx",
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert len(metrics.fallback_trigger_reason) == 64

    def test_evidence_merge(self, session):
        existing = GameValidationMetrics(
            game_id="20241015LGSS0",
            validation_status="verified",
            evidence_json={"existing_key": "existing_value"},
        )
        session.add(existing)
        session.commit()

        data = ValidationMetricsData(
            validation_status="verified",
            evidence={"new_key": "new_value"},
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.evidence_json["existing_key"] == "existing_value"
        assert metrics.evidence_json["new_key"] == "new_value"

    def test_evidence_non_dict_existing(self, session):
        existing = GameValidationMetrics(
            game_id="20241015LGSS0",
            validation_status="verified",
            evidence_json="not_a_dict",
        )
        session.add(existing)
        session.commit()

        data = ValidationMetricsData(
            validation_status="verified",
            evidence={"new_key": "new_value"},
        )
        metrics = _upsert_validation_metrics(session, "20241015LGSS0", data)
        assert metrics.evidence_json["new_key"] == "new_value"


class TestBackfillGamePlayByPlayFromExistingEvents:
    def test_backfill_no_game_id(self, session):
        with patch("src.repositories.game_relay.SessionLocal", return_value=session):
            result = src.repositories.game_relay.backfill_game_play_by_play_from_existing_events("")
            assert result == 0

    def test_backfill_no_stored_events(self, session):
        with patch("src.repositories.game_relay.SessionLocal", return_value=session):
            result = src.repositories.game_relay.backfill_game_play_by_play_from_existing_events("20241015LGSS0")
            assert result == 0

    def test_backfill_success(self, session):
        game = Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15))
        session.add(game)
        session.add(
            GameEvent(
                game_id="20241015LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                batter_name="Kim",
                pitcher_name="Park",
                description="Strikeout",
                event_type="strikeout",
                result_code="K",
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._auto_sync_to_oci"),
            patch("src.repositories.game_relay.derive_play_by_play_rows_from_events") as mock_derive,
        ):
            mock_derive.return_value = [
                {
                    "inning": 1,
                    "inning_half": "top",
                    "batter_name": "Kim",
                    "pitcher_name": "Park",
                    "play_description": "Strikeout",
                    "event_type": "strikeout",
                    "result": "K",
                }
            ]
            result = src.repositories.game_relay.backfill_game_play_by_play_from_existing_events("20241015LGSS0")
            assert result == 1

    def test_backfill_db_error(self, session):
        from sqlalchemy.exc import SQLAlchemyError

        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.commit()

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_session.commit.side_effect = SQLAlchemyError("DB Error")

        with patch("src.repositories.game_relay.SessionLocal", return_value=mock_session):
            result = src.repositories.game_relay.backfill_game_play_by_play_from_existing_events("20241015LGSS0")
            assert result == 0


class TestBackfillMissingGameStubsForRelays:
    def test_no_candidates(self, session):
        with patch("src.repositories.game_relay.SessionLocal", return_value=session):
            result = src.repositories.game_relay.backfill_missing_game_stubs_for_relays()
            assert result == 0

    def test_with_events_missing_game(self, session):
        session.add(
            GameEvent(
                game_id="20241015LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                batter_name="Kim",
                pitcher_name="Park",
                description="Single",
                event_type="single",
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._ensure_game_stub"),
        ):
            result = src.repositories.game_relay.backfill_missing_game_stubs_for_relays()
            assert result == 1

    def test_with_season_filter(self, session):
        session.add(
            GameEvent(
                game_id="20241015LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                batter_name="Kim",
                pitcher_name="Park",
                description="Single",
                event_type="single",
            )
        )
        session.add(
            GameEvent(
                game_id="20231015LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                batter_name="Lee",
                pitcher_name="Choi",
                description="Single",
                event_type="single",
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._ensure_game_stub"),
        ):
            result = src.repositories.game_relay.backfill_missing_game_stubs_for_relays(seasons=[2024])
            assert result == 1

    def test_existing_game_not_counted(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(
            GameEvent(
                game_id="20241015LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                batter_name="Kim",
                pitcher_name="Park",
                description="Single",
                event_type="single",
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._ensure_game_stub"),
        ):
            result = src.repositories.game_relay.backfill_missing_game_stubs_for_relays()
            assert result == 0

    def test_sync_to_oci(self, session):
        session.add(
            GameEvent(
                game_id="20241015LGSS0",
                event_seq=1,
                inning=1,
                inning_half="top",
                batter_name="Kim",
                pitcher_name="Park",
                description="Single",
                event_type="single",
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._ensure_game_stub"),
            patch("src.repositories.game_relay._auto_sync_to_oci") as mock_sync,
        ):
            result = src.repositories.game_relay.backfill_missing_game_stubs_for_relays(sync_to_oci=True)
            assert result == 1
            mock_sync.assert_called_once_with("20241015LGSS0")


class TestRepairGameParentFromExistingChildren:
    def test_repair_no_game_id(self):
        with patch("src.repositories.game_relay.SessionLocal", return_value=MagicMock()):
            result = src.repositories.game_relay.repair_game_parent_from_existing_children("")
            assert result is False

    def test_repair_no_children(self, session):
        with patch("src.repositories.game_relay.SessionLocal", return_value=session):
            result = src.repositories.game_relay.repair_game_parent_from_existing_children("20241015LGSS0")
            assert result is False

    def test_repair_with_batting_children(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(
            GameBattingStat(
                game_id="20241015LGSS0",
                player_name="Kim",
                team_side="away",
                appearance_seq=1,
                at_bats=4,
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._record_game_id_alias"),
        ):
            result = src.repositories.game_relay.repair_game_parent_from_existing_children("20241015LGSS0")
            assert result is True

    def test_repair_with_pitching_children(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(
            GamePitchingStat(
                game_id="20241015LGSS0",
                player_name="Park",
                team_side="home",
                appearance_seq=1,
                innings_pitched=Decimal("7.0"),
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._record_game_id_alias"),
        ):
            result = src.repositories.game_relay.repair_game_parent_from_existing_children("20241015LGSS0")
            assert result is True

    def test_repair_with_inning_score_children(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(
            GameInningScore(
                game_id="20241015LGSS0",
                team_side="top",
                inning=1,
                runs=2,
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._record_game_id_alias"),
        ):
            result = src.repositories.game_relay.repair_game_parent_from_existing_children("20241015LGSS0")
            assert result is True

    def test_repair_with_lineup_children(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(
            GameLineup(
                game_id="20241015LGSS0",
                player_name="Kim",
                team_side="away",
                batting_order=1,
                position="CF",
                appearance_seq=1,
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._record_game_id_alias"),
            patch("src.repositories.game_relay._has_game_child_rows", return_value=True),
        ):
            result = src.repositories.game_relay.repair_game_parent_from_existing_children("20241015LGSS0")
            assert result is True

    def test_repair_sync_to_oci(self, session):
        session.add(Game(game_id="20241015LGSS0", game_date=date(2024, 10, 15)))
        session.add(
            GameBattingStat(
                game_id="20241015LGSS0",
                player_name="Kim",
                team_side="away",
                appearance_seq=1,
                at_bats=4,
            )
        )
        session.commit()

        with (
            patch("src.repositories.game_relay.SessionLocal", return_value=session),
            patch("src.repositories.game_relay._record_game_id_alias"),
            patch("src.repositories.game_relay._auto_sync_to_oci") as mock_sync,
        ):
            result = src.repositories.game_relay.repair_game_parent_from_existing_children(
                "20241015LGSS0", sync_to_oci=True
            )
            assert result is True
            mock_sync.assert_called_once_with("20241015LGSS0")


class TestRelayResolutionContext:
    def test_offense_team_top(self):
        from src.repositories.game_relay import _RelayResolutionContext

        ctx = _RelayResolutionContext(None, 2024, "SS", "LG")
        assert ctx.offense_team("top") == "SS"

    def test_offense_team_bottom(self):
        from src.repositories.game_relay import _RelayResolutionContext

        ctx = _RelayResolutionContext(None, 2024, "SS", "LG")
        assert ctx.offense_team("bottom") == "LG"

    def test_offense_team_unknown(self):
        from src.repositories.game_relay import _RelayResolutionContext

        ctx = _RelayResolutionContext(None, 2024, "SS", "LG")
        assert ctx.offense_team("middle") is None

    def test_defense_team_top(self):
        from src.repositories.game_relay import _RelayResolutionContext

        ctx = _RelayResolutionContext(None, 2024, "SS", "LG")
        assert ctx.defense_team("top") == "LG"

    def test_defense_team_bottom(self):
        from src.repositories.game_relay import _RelayResolutionContext

        ctx = _RelayResolutionContext(None, 2024, "SS", "LG")
        assert ctx.defense_team("bottom") == "SS"

    def test_defense_team_unknown(self):
        from src.repositories.game_relay import _RelayResolutionContext

        ctx = _RelayResolutionContext(None, 2024, "SS", "LG")
        assert ctx.defense_team("middle") is None

    def test_resolve_participant_no_resolver(self):
        from src.repositories.game_relay import _RelayResolutionContext

        ctx = _RelayResolutionContext(None, 2024, "SS", "LG")
        result = ctx.resolve_participant("Kim", "SS", is_pitcher=False)
        assert result == (None, None, None)

    def test_resolve_participant_no_name(self):
        from src.repositories.game_relay import _RelayResolutionContext

        mock_resolver = MagicMock()
        ctx = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        result = ctx.resolve_participant(None, "SS", is_pitcher=False)
        assert result == (None, None, None)

    def test_resolve_participant_no_team(self):
        from src.repositories.game_relay import _RelayResolutionContext

        mock_resolver = MagicMock()
        ctx = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        result = ctx.resolve_participant("Kim", None, is_pitcher=False)
        assert result == (None, None, None)

    def test_resolve_participant_no_season(self):
        from src.repositories.game_relay import _RelayResolutionContext

        mock_resolver = MagicMock()
        ctx = _RelayResolutionContext(mock_resolver, None, "SS", "LG")
        result = ctx.resolve_participant("Kim", "SS", is_pitcher=False)
        assert result == (None, None, None)

    def test_resolve_participant_success(self):
        from src.repositories.game_relay import _RelayResolutionContext

        mock_resolver = MagicMock()
        mock_resolver.resolve_id.return_value = 12345
        ctx = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        result = ctx.resolve_participant("Kim", "SS", is_pitcher=False)
        assert result == (12345, "resolved", "name_match_SS_2024")

    def test_resolve_participant_unresolved(self):
        from src.repositories.game_relay import _RelayResolutionContext

        mock_resolver = MagicMock()
        mock_resolver.resolve_id.return_value = None
        ctx = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        result = ctx.resolve_participant("Kim", "SS", is_pitcher=False)
        assert result == (None, "unresolved", "no_match_SS_2024")

    def test_resolve_participant_exception(self):
        from src.repositories.game_relay import _RelayResolutionContext

        mock_resolver = MagicMock()
        mock_resolver.resolve_id.side_effect = RuntimeError("DB error")
        ctx = _RelayResolutionContext(mock_resolver, 2024, "SS", "LG")
        result = ctx.resolve_participant("Kim", "SS", is_pitcher=False)
        assert result == (None, "error", "resolve_exception")


class TestPrepareRelayPayloads:
    def test_both_none(self):
        from src.repositories.game_relay import _prepare_relay_payloads

        events, pbp, valid = _prepare_relay_payloads(None, None)
        assert events == []
        assert pbp == []
        assert valid == []

    def test_events_only(self):
        from src.repositories.game_relay import _prepare_relay_payloads

        events = [
            {
                "event_type": "strikeout",
                "outs": 1,
                "inning": 1,
                "inning_half": "top",
                "description": "Strikeout",
                "home_score": 0,
                "away_score": 0,
                "wpa": 0.1,
                "win_expectancy_before": 0.5,
                "win_expectancy_after": 0.4,
                "bases_before": 0,
                "bases_after": 0,
            }
        ]
        result_events, result_pbp, result_valid = _prepare_relay_payloads(events, None)
        assert len(result_events) == 1
        assert len(result_pbp) == 1
        assert len(result_valid) == 1

    def test_pbp_only(self):
        from src.repositories.game_relay import _prepare_relay_payloads

        pbp = [{"batter_name": "Kim", "pitcher_name": "Park"}]
        result_events, result_pbp, result_valid = _prepare_relay_payloads(None, pbp)
        assert len(result_events) == 0
        assert len(result_pbp) == 1
        assert len(result_valid) == 0

    def test_events_missing_state(self):
        from src.repositories.game_relay import _prepare_relay_payloads

        events = [{"event_type": "unknown"}]
        result_events, result_pbp, result_valid = _prepare_relay_payloads(events, None)
        assert len(result_events) == 1
        assert len(result_valid) == 0


class TestBuildPlayerResolutionPayload:
    def test_batter_only(self):
        from src.repositories.game_relay import _build_player_resolution_payload, PlayerResolutionContext

        ctx = PlayerResolutionContext(
            batter_name="Kim",
            resolved_batter_name="Kim",
            batter_team="SS",
            batter_confidence="high",
            batter_reason="name_match",
        )
        result = _build_player_resolution_payload(ctx)
        assert "batter" in result
        assert result["batter"]["name"] == "Kim"
        assert "pitcher" not in result

    def test_pitcher_only(self):
        from src.repositories.game_relay import _build_player_resolution_payload, PlayerResolutionContext

        ctx = PlayerResolutionContext(
            pitcher_name="Park",
            pitcher_team="LG",
            pitcher_confidence="high",
            pitcher_reason="name_match",
        )
        result = _build_player_resolution_payload(ctx)
        assert "pitcher" in result
        assert result["pitcher"]["name"] == "Park"
        assert "batter" not in result

    def test_empty_context(self):
        from src.repositories.game_relay import _build_player_resolution_payload, PlayerResolutionContext

        ctx = PlayerResolutionContext()
        result = _build_player_resolution_payload(ctx)
        assert result == {}

    def test_batter_name_only_no_details(self):
        from src.repositories.game_relay import _build_player_resolution_payload, PlayerResolutionContext

        ctx = PlayerResolutionContext(batter_name="Kim")
        result = _build_player_resolution_payload(ctx)
        assert result == {}

    def test_pitcher_name_only_no_details(self):
        from src.repositories.game_relay import _build_player_resolution_payload, PlayerResolutionContext

        ctx = PlayerResolutionContext(pitcher_name="Park")
        result = _build_player_resolution_payload(ctx)
        assert result == {}
