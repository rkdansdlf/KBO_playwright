from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.relay_recovery_service import (
    GameStateInput,
    RecoveryLoopContext,
    RecoveryTargetCriteria,
    RelayRecoveryConfig,
    RelayRecoveryResult,
    RelayRecoveryTarget,
    RelaySaveCounts,
    RelayValidationConfig,
    _bucket_targets,
    _classify_relay_failure,
    _coerce_int,
    _handle_empty_relay_result,
    _handle_filtered_relay_result,
    _join_notes,
    _last_event_score,
    _load_final_scores,
    _load_target_rows,
    _maybe_derive_pbp,
    _normalize_valid_pbp_row,
    _relay_validator,
    _sanitize_relay_result,
    _save_or_count_rows,
    _should_mark_source_unavailable,
    _validate_relay_final_score,
    _validate_relay_inning_continuity,
    _validate_relay_result,
    build_relay_recovery_orchestrator,
    load_relay_recovery_targets,
    parse_source_order,
)


class TestBucketTargets:
    def test_groups_by_bucket(self):
        targets = [
            RelayRecoveryTarget(game_id="20240315LGSS0", bucket_id="2024_regular"),
            RelayRecoveryTarget(game_id="20240316LGSS0", bucket_id="2024_regular"),
            RelayRecoveryTarget(game_id="20230315LGSS0", bucket_id="2023_regular"),
        ]
        result = _bucket_targets(targets)
        assert len(result["2024_regular"]) == 2
        assert len(result["2023_regular"]) == 1


class TestRelayValidator:
    def test_returns_none_when_valid(self):
        config = RelayValidationConfig(final_scores={"g1": (5, 3)}, validate_final_score=True)
        validator = _relay_validator("g1", config)
        result = validator(MagicMock(events=[{"away_score": 5, "home_score": 3}], raw_pbp_rows=[]))
        assert result is None

    def test_returns_mismatch(self):
        config = RelayValidationConfig(final_scores={"g1": (5, 3)}, validate_final_score=True)
        validator = _relay_validator("g1", config)
        result = validator(MagicMock(events=[{"away_score": 4, "home_score": 3}], raw_pbp_rows=[]))
        assert "final_score_mismatch" in result

    def test_too_few_events(self):
        config = RelayValidationConfig(final_scores={}, min_result_events=5, validate_final_score=False)
        validator = _relay_validator("g1", config)
        result = validator(MagicMock(events=[{"away_score": 1, "home_score": 0}], raw_pbp_rows=[]))
        assert "too_few_result_events" in result


class TestMaybeDerivePbp:
    def test_not_allowed_returns_false(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1", has_event_state=True, needs_pbp_recovery=True),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        config = RelayRecoveryConfig(allow_derived_pbp=False)
        assert _maybe_derive_pbp(ctx, config) is False

    def test_already_has_pbp_returns_false(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1", has_event_state=True, has_pbp=True, needs_pbp_recovery=False),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        config = RelayRecoveryConfig(allow_derived_pbp=True)
        assert _maybe_derive_pbp(ctx, config) is False

    def test_derive_pbp_success(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1", has_event_state=True, needs_pbp_recovery=True),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        config = RelayRecoveryConfig(allow_derived_pbp=True)
        with patch(
            "src.services.relay_recovery_service.backfill_game_play_by_play_from_existing_events",
            return_value=10,
        ):
            assert _maybe_derive_pbp(ctx, config) is True
            assert ctx.run_result.derived_pbp_games == 1
            assert ctx.run_result.saved_rows == 10

    def test_derive_pbp_dry_run(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1", has_event_state=True, needs_pbp_recovery=True),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=True,
            log=MagicMock(),
        )
        config = RelayRecoveryConfig(allow_derived_pbp=True)
        with patch(
            "src.services.relay_recovery_service.backfill_game_play_by_play_from_existing_events",
            return_value=10,
        ):
            assert _maybe_derive_pbp(ctx, config) is True
            assert ctx.run_result.derived_pbp_games == 1


class TestHandleEmptyRelayResult:
    def test_increments_empty_games(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = MagicMock()
        relay_result.notes = "no data"
        _handle_empty_relay_result(ctx, [], relay_result)
        assert ctx.run_result.empty_games == 1

    def test_match_failed_classification(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = MagicMock()
        relay_result.notes = "invalid_relay_match"
        _handle_empty_relay_result(ctx, [], relay_result)
        assert ctx.run_result.match_failed_games == 1

    def test_api_failed_classification(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = MagicMock()
        relay_result.notes = "relay_api_error timeout"
        _handle_empty_relay_result(ctx, [], relay_result)
        assert ctx.run_result.api_failed_games == 1

    def test_marks_unavailable_for_old_legacy(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2009_legacy",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = MagicMock()
        relay_result.notes = "no data found"
        with patch("src.services.relay_recovery_service.mark_relay_source_unavailable") as mock_mark:
            _handle_empty_relay_result(ctx, [], relay_result)
            mock_mark.assert_called_once()


class TestHandleFilteredRelayResult:
    def test_increments_filtered_games(self):
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = MagicMock()
        relay_result.source_name = "naver"
        relay_result.has_event_state = True
        relay_result.has_raw_pbp = False
        _handle_filtered_relay_result(ctx, relay_result, ["filtered_event_rows:2"])
        assert ctx.run_result.filtered_games == 1
        assert len(ctx.run_result.report_rows) == 1


class TestSanitizeRelayResult:
    def test_filters_invalid_events(self):
        from src.sources.relay.base import NormalizedRelayResult

        result = NormalizedRelayResult(
            game_id="g1",
            source_name="naver",
            events=[
                {"inning": 1, "inning_half": "top", "play_description": "hit"},
                {"inning": None, "inning_half": "top", "play_description": ""},
            ],
            raw_pbp_rows=[{"inning": 1, "inning_half": "top", "play_description": "hit"}],
        )
        with patch("src.services.relay_recovery_service.apply_wpa_transitions"):
            with patch("src.services.relay_recovery_service.event_has_minimum_state", side_effect=[True, False]):
                with patch(
                    "src.services.relay_recovery_service.normalize_pbp_row",
                    return_value={"inning": 1, "inning_half": "top", "play_description": "hit"},
                ):
                    sanitized, notes, filtered = _sanitize_relay_result(result)
                    assert len(sanitized.events) == 1
                    assert filtered == 1


class TestNormalizeValidPbpRow:
    def test_valid_row(self):
        with patch(
            "src.services.relay_recovery_service.normalize_pbp_row",
            return_value={"inning": 1, "inning_half": "top", "play_description": "hit"},
        ):
            result = _normalize_valid_pbp_row({"key": "val"})
            assert result["inning"] == 1

    def test_missing_inning(self):
        with patch(
            "src.services.relay_recovery_service.normalize_pbp_row",
            return_value={"inning": None, "inning_half": "top", "play_description": "hit"},
        ):
            assert _normalize_valid_pbp_row({"key": "val"}) is None

    def test_missing_inning_half(self):
        with patch(
            "src.services.relay_recovery_service.normalize_pbp_row",
            return_value={"inning": 1, "inning_half": None, "play_description": "hit"},
        ):
            assert _normalize_valid_pbp_row({"key": "val"}) is None

    def test_empty_description(self):
        with patch(
            "src.services.relay_recovery_service.normalize_pbp_row",
            return_value={"inning": 1, "inning_half": "top", "play_description": ""},
        ):
            assert _normalize_valid_pbp_row({"key": "val"}) is None


class TestValidateRelayResult:
    def test_no_events_returns_none(self):
        config = RelayValidationConfig(final_scores={}, validate_final_score=True)
        result = _validate_relay_result("g1", MagicMock(events=[]), config)
        assert result is None

    def test_validates_inning_continuity(self):
        config = RelayValidationConfig(final_scores={}, validate_inning_continuity=True, validate_final_score=False)
        events = [{"inning": 1}, {"inning": 3}]
        result = _validate_relay_result("g1", MagicMock(events=events), config)
        assert "missing_middle_inning" in result

    def test_missing_starting_inning(self):
        config = RelayValidationConfig(final_scores={}, validate_inning_continuity=True, validate_final_score=False)
        events = [{"inning": 2}, {"inning": 3}]
        result = _validate_relay_result("g1", MagicMock(events=events), config)
        assert "missing_starting_inning" in result


class TestValidateRelayInningContinuity:
    def test_valid_continuous(self):
        events = [{"inning": 1}, {"inning": 2}, {"inning": 3}]
        assert _validate_relay_inning_continuity(events) is None

    def test_missing_first(self):
        events = [{"inning": 2}, {"inning": 3}]
        result = _validate_relay_inning_continuity(events)
        assert "missing_starting_inning" in result

    def test_gap_in_middle(self):
        events = [{"inning": 1}, {"inning": 3}]
        result = _validate_relay_inning_continuity(events)
        assert "missing_middle_inning" in result

    def test_empty_events(self):
        assert _validate_relay_inning_continuity([]) is None


class TestValidateRelayFinalScore:
    def test_missing_expected_score(self):
        result = _validate_relay_final_score("g1", [{"away_score": 5, "home_score": 3}], {})
        assert result == "missing_game_final_score"

    def test_missing_event_score(self):
        result = _validate_relay_final_score("g1", [{"no_score": True}], {"g1": (5, 3)})
        assert result == "missing_event_final_score"

    def test_mismatch(self):
        result = _validate_relay_final_score("g1", [{"away_score": 4, "home_score": 3}], {"g1": (5, 3)})
        assert "final_score_mismatch" in result

    def test_match(self):
        result = _validate_relay_final_score("g1", [{"away_score": 5, "home_score": 3}], {"g1": (5, 3)})
        assert result is None


class TestSaveOrCountRows:
    def test_dry_run(self):
        relay_result = MagicMock()
        relay_result.events = [{"inning": 1}]
        relay_result.raw_pbp_rows = [{"inning": 1}]
        result = _save_or_count_rows("g1", relay_result, dry_run=True, allow_derived_pbp=False)
        assert result.saved_rows == 1
        assert result.saved_event_rows == 1

    def test_dry_run_no_events_with_pbp(self):
        relay_result = MagicMock()
        relay_result.events = []
        relay_result.raw_pbp_rows = [{"inning": 1}]
        result = _save_or_count_rows("g1", relay_result, dry_run=True, allow_derived_pbp=False)
        assert result.saved_rows == 1
        assert result.skipped_event_rows_reason == "no_valid_event_state"

    def test_save_success(self):
        relay_result = MagicMock()
        relay_result.events = [{"inning": 1}]
        relay_result.raw_pbp_rows = []
        with patch("src.services.relay_recovery_service.save_relay_data", return_value=1):
            result = _save_or_count_rows("g1", relay_result, dry_run=False, allow_derived_pbp=False)
            assert result.saved_rows == 1
            assert result.saved_event_rows == 1

    def test_save_returns_zero(self):
        relay_result = MagicMock()
        relay_result.events = [{"inning": 1}]
        relay_result.raw_pbp_rows = []
        with patch("src.services.relay_recovery_service.save_relay_data", return_value=0):
            result = _save_or_count_rows("g1", relay_result, dry_run=False, allow_derived_pbp=False)
            assert result.saved_rows == 0


class TestLoadTargetRows:
    def test_with_requested_ids(self):
        mock_session = MagicMock()
        mock_session.query.return_value.outerjoin.return_value.filter.return_value.all.return_value = [
            ("20240315LGSS0", "Regular"),
        ]
        criteria = RecoveryTargetCriteria(game_ids=["20240315LGSS0"])
        result = _load_target_rows(mock_session, criteria)
        assert result == [("20240315LGSS0", "Regular")]

    def test_invalid_date_raises(self):
        mock_session = MagicMock()
        criteria = RecoveryTargetCriteria(date="invalid")
        with pytest.raises(ValueError, match="Invalid date format"):
            _load_target_rows(mock_session, criteria)


class TestLoadFinalScores:
    def test_empty_game_ids(self):
        result = _load_final_scores([])
        assert result == {}

    def test_returns_scores(self):
        with patch("src.services.relay_recovery_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.filter.return_value.all.return_value = [
                ("g1", 5, 3),
                ("g2", 2, 4),
            ]
            result = _load_final_scores(["g1", "g2"])
            assert result["g1"] == (5, 3)
            assert result["g2"] == (2, 4)


class TestBuildRelayRecoveryOrchestrator:
    def test_builds_with_defaults(self):
        with patch("src.services.relay_recovery_service.read_manifest_entries", return_value=[]):
            orchestrator = build_relay_recovery_orchestrator()
            assert orchestrator is not None

    def test_builds_with_custom_paths(self):
        with patch("src.services.relay_recovery_service.read_manifest_entries", return_value=[]):
            orchestrator = build_relay_recovery_orchestrator(
                import_manifest=Path("/tmp/manifest.csv"),
                capability_path=Path("/tmp/capability.csv"),
                source_timeout=60.0,
            )
            assert orchestrator is not None


class TestLoadRelayRecoveryTargets:
    def test_no_criteria_raises(self):
        criteria = RecoveryTargetCriteria()
        with pytest.raises(ValueError, match="Must provide season"):
            load_relay_recovery_targets(criteria)

    def test_missing_only_skips_complete(self):
        with patch("src.services.relay_recovery_service.SessionLocal") as mock_sl:
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            mock_session.query.return_value.outerjoin.return_value.filter.return_value.order_by.return_value.all.return_value = [
                ("20240315LGSS0", "Regular"),
            ]
            mock_event = MagicMock()
            mock_event.game_id = "20240315LGSS0"
            mock_event.wpa = 0.5
            mock_session.query.return_value.filter.return_value.all.return_value = [mock_event]
            mock_session.query.return_value.filter.return_value.distinct.return_value.all.return_value = [
                ("20240315LGSS0",),
            ]
            criteria = RecoveryTargetCriteria(season=2024, missing_only=True)
            result = load_relay_recovery_targets(criteria)
            assert len(result) == 0


class TestWriteRelayRecoveryReport:
    def test_no_path_does_nothing(self):
        from src.services.relay_recovery_service import write_relay_recovery_report

        write_relay_recovery_report(None, [])

    def test_writes_csv(self, tmp_path):
        from src.services.relay_recovery_service import write_relay_recovery_report

        report_path = tmp_path / "report.csv"
        rows = [{"game_id": "g1", "status": "saved"}]
        write_relay_recovery_report(report_path, rows)
        assert report_path.exists()
        content = report_path.read_text()
        assert "game_id" in content
        assert "g1" in content


class TestRecoveryTargetCriteria:
    def test_defaults(self):
        criteria = RecoveryTargetCriteria()
        assert criteria.season is None
        assert criteria.month is None
        assert criteria.missing_only is True

    def test_with_values(self):
        criteria = RecoveryTargetCriteria(season=2024, month=3, missing_only=False)
        assert criteria.season == 2024
        assert criteria.month == 3
        assert criteria.missing_only is False


class TestRelayRecoveryConfig:
    def test_defaults(self):
        config = RelayRecoveryConfig()
        assert config.dry_run is False
        assert config.source_timeout == 30.0
        assert config.validate_final_score is True

    def test_with_values(self):
        config = RelayRecoveryConfig(dry_run=True, source_timeout=60.0)
        assert config.dry_run is True
        assert config.source_timeout == 60.0
