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
    _log_relay_save,
    _mark_unavailable_relay_source,
    _manifest_base_dir,
    _save_relay_result,
    _save_or_count_rows,
    _should_mark_source_unavailable,
    _validate_relay_final_score,
    _validate_relay_inning_continuity,
    _validate_relay_result,
    build_relay_recovery_orchestrator,
    load_game_ids_from_file,
    load_relay_recovery_targets,
    parse_source_order,
    recover_relay_data,
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


class TestParseSourceOrder:
    def test_none_returns_none(self):
        assert parse_source_order(None) is None

    def test_empty_tokens_return_none(self):
        assert parse_source_order(" , ") is None

    def test_trims_tokens(self):
        assert parse_source_order(" naver, kbo ,, import ") == ["naver", "kbo", "import"]


class TestLoadGameIdsFromFile:
    def test_none_returns_empty_list(self):
        assert load_game_ids_from_file(None) == []

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_game_ids_from_file(tmp_path / "missing.csv")

    def test_skips_headers_comments_empty_and_duplicates(self, tmp_path):
        path = tmp_path / "ids.csv"
        path.write_text("game_id\n# comment\n\n g1,extra\ng1\ng2\n", encoding="utf-8")

        assert load_game_ids_from_file(path) == ["g1", "g2"]


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

    def test_filters_invalid_pbp_rows(self):
        from src.sources.relay.base import NormalizedRelayResult

        result = NormalizedRelayResult(
            game_id="g1",
            source_name="naver",
            events=[{"inning": 1, "inning_half": "top", "play_description": "hit"}],
            raw_pbp_rows=[{"bad": "row"}, {"inning": 1, "inning_half": "top", "play_description": "hit"}],
        )
        with patch("src.services.relay_recovery_service.apply_wpa_transitions"):
            with patch("src.services.relay_recovery_service.event_has_minimum_state", return_value=True):
                with patch(
                    "src.services.relay_recovery_service.normalize_pbp_row",
                    side_effect=[
                        {"inning": None, "inning_half": "top", "play_description": "bad"},
                        {"inning": 1, "inning_half": "top", "play_description": "hit"},
                    ],
                ):
                    sanitized, notes, filtered = _sanitize_relay_result(result)

        assert len(sanitized.raw_pbp_rows) == 1
        assert notes == ["filtered_pbp_rows:1"]
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

    def test_skips_final_score_when_disabled_after_continuity_check(self):
        config = RelayValidationConfig(final_scores={}, validate_inning_continuity=True, validate_final_score=False)
        events = [{"inning": 1}, {"inning": 2}]
        assert _validate_relay_result("g1", MagicMock(events=events), config) is None

    def test_skips_continuity_when_disabled(self):
        config = RelayValidationConfig(final_scores={}, validate_inning_continuity=False, validate_final_score=False)
        events = [{"inning": 3}]
        assert _validate_relay_result("g1", MagicMock(events=events), config) is None

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


class TestSaveRelayResult:
    def test_updates_run_result_and_report_rows(self):
        from src.sources.relay.base import NormalizedRelayResult

        run_result = RelayRecoveryResult()
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=run_result,
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = NormalizedRelayResult(
            game_id="g1",
            source_name="naver",
            events=[{"inning": 1}],
            raw_pbp_rows=[{"inning": 1}],
            has_event_state=True,
            has_raw_pbp=True,
            notes="ok",
        )
        with patch(
            "src.services.relay_recovery_service._save_or_count_rows",
            return_value=RelaySaveCounts(saved_rows=2, saved_event_rows=1, saved_pbp_rows=1),
        ):
            _save_relay_result(ctx, RelayRecoveryConfig(), relay_result, ["filtered_event_rows:1"], filtered_rows=1)

        assert run_result.saved_games == 1
        assert run_result.saved_rows == 2
        assert run_result.report_rows[0]["status"] == "partial_relay"
        assert run_result.report_rows[0]["notes"] == "ok;filtered_event_rows:1"

    def test_dry_run_status(self):
        from src.sources.relay.base import NormalizedRelayResult

        run_result = RelayRecoveryResult()
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=run_result,
            dry_run=True,
            log=MagicMock(),
        )
        relay_result = NormalizedRelayResult(game_id="g1", source_name="naver", events=[{"inning": 1}])
        with patch(
            "src.services.relay_recovery_service._save_or_count_rows",
            return_value=RelaySaveCounts(saved_rows=1, saved_event_rows=1),
        ):
            _save_relay_result(ctx, RelayRecoveryConfig(dry_run=True), relay_result, [], filtered_rows=0)

        assert run_result.report_rows[0]["status"] == "dry_run"

    def test_zero_saved_rows_still_reports_attempt(self):
        from src.sources.relay.base import NormalizedRelayResult

        run_result = RelayRecoveryResult()
        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2024_regular",
            source_order=["naver"],
            run_result=run_result,
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = NormalizedRelayResult(game_id="g1", source_name="naver", events=[{"inning": 1}])
        with patch(
            "src.services.relay_recovery_service._save_or_count_rows",
            return_value=RelaySaveCounts(saved_rows=0, saved_event_rows=0),
        ):
            _save_relay_result(ctx, RelayRecoveryConfig(), relay_result, [], filtered_rows=0)

        assert run_result.saved_games == 0
        assert run_result.report_rows[0]["status"] == "saved"


class TestLogRelaySave:
    def test_non_dry_run_log_message(self):
        from src.sources.relay.base import NormalizedRelayResult

        log = MagicMock()
        _log_relay_save(
            RelayRecoveryTarget(game_id="g1"),
            NormalizedRelayResult(game_id="g1", source_name="naver"),
            RelaySaveCounts(saved_rows=2, saved_event_rows=1, saved_pbp_rows=1),
            dry_run=False,
            log=log,
        )
        assert "[SUCCESS]" in log.call_args.args[0]

    def test_dry_run_log_message(self):
        from src.sources.relay.base import NormalizedRelayResult

        log = MagicMock()
        _log_relay_save(
            RelayRecoveryTarget(game_id="g1"),
            NormalizedRelayResult(game_id="g1", source_name="naver"),
            RelaySaveCounts(saved_rows=2, saved_event_rows=1, saved_pbp_rows=1),
            dry_run=True,
            log=log,
        )
        assert "[DRY-RUN]" in log.call_args.args[0]


class TestMarkUnavailableRelaySource:
    def test_non_dry_run_marks_source_unavailable(self):
        from src.sources.relay.base import NormalizedRelayResult

        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2009_legacy",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=False,
            log=MagicMock(),
        )
        relay_result = NormalizedRelayResult(game_id="g1", source_name="naver", notes="no data")
        with patch("src.services.relay_recovery_service.mark_relay_source_unavailable") as mock_mark:
            _mark_unavailable_relay_source(ctx, [{"source": "naver"}], relay_result)

        mock_mark.assert_called_once()
        assert ctx.run_result.report_rows[0]["status"] == "source_unavailable"

    def test_dry_run_does_not_mark_source_unavailable(self):
        from src.sources.relay.base import NormalizedRelayResult

        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2009_legacy",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=True,
            log=MagicMock(),
        )
        relay_result = NormalizedRelayResult(game_id="g1", source_name="naver", notes="no data")
        with patch("src.services.relay_recovery_service.mark_relay_source_unavailable") as mock_mark:
            _mark_unavailable_relay_source(ctx, [], relay_result)

        mock_mark.assert_not_called()
        assert ctx.run_result.report_rows[0]["status"] == "source_unavailable_dry_run"

    def test_dry_run_records_without_marking_source_unavailable(self):
        from src.sources.relay.base import NormalizedRelayResult

        ctx = RecoveryLoopContext(
            target=RelayRecoveryTarget(game_id="g1"),
            bucket_id="2009_legacy",
            source_order=["naver"],
            run_result=RelayRecoveryResult(),
            dry_run=True,
            log=MagicMock(),
        )

        with patch("src.services.relay_recovery_service.mark_relay_source_unavailable") as mock_mark:
            _mark_unavailable_relay_source(ctx, [], NormalizedRelayResult(game_id="g1", source_name="naver"))

        mock_mark.assert_not_called()
        assert ctx.run_result.report_rows[0]["status"] == "source_unavailable_dry_run"


class TestShouldMarkSourceUnavailable:
    def test_non_legacy_bucket_returns_false(self):
        relay_result = MagicMock(notes="no data")
        assert _should_mark_source_unavailable("2024_regular", [], relay_result) is False

    def test_modern_legacy_bucket_returns_false(self):
        relay_result = MagicMock(notes="no data")
        assert _should_mark_source_unavailable("2010_legacy", [], relay_result) is False

    def test_malformed_legacy_bucket_can_mark_when_non_transient(self):
        relay_result = MagicMock(notes="no data")
        assert _should_mark_source_unavailable("old_legacy", [], relay_result) is True

    def test_transient_failure_returns_false(self):
        relay_result = MagicMock(notes="timeout")
        attempts = [{"status": "failed", "notes": "http_500"}]
        assert _should_mark_source_unavailable("2009_legacy", attempts, relay_result) is False


class TestRecoverRelayData:
    @pytest.mark.asyncio
    async def test_empty_targets_returns_empty_result(self):
        result = await recover_relay_data([], RelayRecoveryConfig(log=MagicMock()))
        assert result.total_targets == 0

    @pytest.mark.asyncio
    async def test_dry_run_success_flow(self):
        from src.sources.relay.base import NormalizedRelayResult

        relay_result = NormalizedRelayResult(
            game_id="g1",
            source_name="naver",
            events=[{"inning": 1}],
            raw_pbp_rows=[],
            has_event_state=True,
        )
        orchestrator = MagicMock()
        orchestrator.source_order_for_bucket.return_value = ["naver"]
        orchestrator.probe_bucket = AsyncMock()
        orchestrator.fetch_game = AsyncMock(return_value=(relay_result, [{"game_id": "g1", "status": "attempt"}]))

        config = RelayRecoveryConfig(dry_run=True, validate_final_score=False, sleep_seconds=0, log=MagicMock())
        with (
            patch("src.services.relay_recovery_service._sanitize_relay_result", return_value=(relay_result, [], 0)),
            patch("src.services.relay_recovery_service.write_relay_recovery_report") as mock_write,
        ):
            result = await recover_relay_data(
                [RelayRecoveryTarget(game_id="g1", bucket_id="2024_regular")], config, orchestrator
            )

        assert result.total_targets == 1
        assert result.saved_games == 1
        assert result.report_rows[-1]["status"] == "dry_run"
        orchestrator.probe_bucket.assert_awaited_once()
        orchestrator.fetch_game.assert_awaited_once()
        mock_write.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_relay_result_flow(self):
        from src.sources.relay.base import NormalizedRelayResult

        relay_result = NormalizedRelayResult(game_id="g1", source_name="naver", notes="no data")
        orchestrator = MagicMock()
        orchestrator.source_order_for_bucket.return_value = ["naver"]
        orchestrator.probe_bucket = AsyncMock()
        orchestrator.fetch_game = AsyncMock(return_value=(relay_result, []))

        config = RelayRecoveryConfig(validate_final_score=False, sleep_seconds=0, log=MagicMock())
        with patch("src.services.relay_recovery_service.write_relay_recovery_report"):
            result = await recover_relay_data(
                [RelayRecoveryTarget(game_id="g1", bucket_id="2024_regular")], config, orchestrator
            )

        assert result.empty_games == 1
        assert result.saved_games == 0

    @pytest.mark.asyncio
    async def test_derive_pbp_short_circuits_fetch(self):
        orchestrator = MagicMock()
        orchestrator.source_order_for_bucket.return_value = ["naver"]
        orchestrator.probe_bucket = AsyncMock()
        orchestrator.fetch_game = AsyncMock()

        config = RelayRecoveryConfig(validate_final_score=False, sleep_seconds=0, log=MagicMock())
        with (
            patch("src.services.relay_recovery_service._maybe_derive_pbp", return_value=True),
            patch("src.services.relay_recovery_service.write_relay_recovery_report"),
        ):
            result = await recover_relay_data(
                [RelayRecoveryTarget(game_id="g1", bucket_id="2024_regular")], config, orchestrator
            )

        assert result.total_targets == 1
        orchestrator.fetch_game.assert_not_called()

    @pytest.mark.asyncio
    async def test_filtered_empty_result_flow(self):
        from src.sources.relay.base import NormalizedRelayResult

        fetched = NormalizedRelayResult(
            game_id="g1",
            source_name="naver",
            events=[{"inning": None}],
            raw_pbp_rows=[],
        )
        filtered = NormalizedRelayResult(game_id="g1", source_name="naver", events=[], raw_pbp_rows=[])
        orchestrator = MagicMock()
        orchestrator.source_order_for_bucket.return_value = ["naver"]
        orchestrator.probe_bucket = AsyncMock()
        orchestrator.fetch_game = AsyncMock(return_value=(fetched, []))

        config = RelayRecoveryConfig(validate_final_score=False, sleep_seconds=0, log=MagicMock())
        with (
            patch(
                "src.services.relay_recovery_service._sanitize_relay_result",
                return_value=(filtered, ["filtered_event_rows:1"], 1),
            ),
            patch("src.services.relay_recovery_service.write_relay_recovery_report"),
        ):
            result = await recover_relay_data(
                [RelayRecoveryTarget(game_id="g1", bucket_id="2024_regular")], config, orchestrator
            )

        assert result.filtered_games == 1
        assert result.report_rows[-1]["status"] == "skipped_filtered"

    @pytest.mark.asyncio
    async def test_success_flow_sleeps_when_configured(self):
        from src.sources.relay.base import NormalizedRelayResult

        relay_result = NormalizedRelayResult(
            game_id="g1",
            source_name="naver",
            events=[{"inning": 1}],
            raw_pbp_rows=[],
            has_event_state=True,
        )
        orchestrator = MagicMock()
        orchestrator.source_order_for_bucket.return_value = ["naver"]
        orchestrator.probe_bucket = AsyncMock()
        orchestrator.fetch_game = AsyncMock(return_value=(relay_result, []))
        config = RelayRecoveryConfig(dry_run=True, validate_final_score=False, sleep_seconds=0.1, log=MagicMock())

        with (
            patch("src.services.relay_recovery_service._sanitize_relay_result", return_value=(relay_result, [], 0)),
            patch("src.services.relay_recovery_service.asyncio.sleep", new=AsyncMock()) as mock_sleep,
            patch("src.services.relay_recovery_service.write_relay_recovery_report"),
        ):
            await recover_relay_data(
                [RelayRecoveryTarget(game_id="g1", bucket_id="2024_regular")], config, orchestrator
            )

        mock_sleep.assert_awaited_once_with(0.1)


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

    def test_date_filter(self):
        mock_session = MagicMock()
        criteria = RecoveryTargetCriteria(date="20240601")
        rows = [("20240601LGSS0", "Regular")]
        query = mock_session.query.return_value.outerjoin.return_value.filter.return_value
        query.filter.return_value = query
        query.order_by.return_value.all.return_value = rows

        result = _load_target_rows(mock_session, criteria)

        assert result == rows
        assert query.filter.called

    def test_season_month_filter(self):
        mock_session = MagicMock()
        criteria = RecoveryTargetCriteria(season=2024, month=6)
        rows = [("20240601LGSS0", "Regular")]
        query = mock_session.query.return_value.outerjoin.return_value.filter.return_value
        query.filter.return_value = query
        query.order_by.return_value.all.return_value = rows

        result = _load_target_rows(mock_session, criteria)

        assert result == rows
        assert query.filter.called


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


class TestManifestBaseDir:
    def test_path_uses_parent(self, tmp_path):
        manifest = tmp_path / "manifest.csv"
        assert _manifest_base_dir(manifest) == tmp_path.resolve()

    def test_comma_string_uses_first_token_parent(self, tmp_path):
        first = tmp_path / "first.csv"
        second = tmp_path / "second.csv"
        assert _manifest_base_dir(f" {first}, {second}") == tmp_path.resolve()

    def test_iterable_uses_first_item(self, tmp_path):
        first = tmp_path / "first.csv"
        assert _manifest_base_dir([first]) == tmp_path.resolve()

    def test_empty_iterable_uses_cwd(self):
        assert _manifest_base_dir([]) == Path.cwd()


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

    def test_include_incomplete_extends_allowed_statuses(self):
        from src.utils.game_status import GAME_STATUS_SCHEDULED, GAME_STATUS_UNRESOLVED

        with (
            patch("src.services.relay_recovery_service.SessionLocal") as mock_sl,
            patch("src.services.relay_recovery_service._load_target_rows", return_value=[]) as mock_load,
        ):
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            criteria = RecoveryTargetCriteria(season=2024, include_incomplete=True)
            result = load_relay_recovery_targets(criteria)

        assert result == []
        allowed_statuses = mock_load.call_args.kwargs["allowed_statuses"]
        assert GAME_STATUS_SCHEDULED in allowed_statuses
        assert GAME_STATUS_UNRESOLVED in allowed_statuses

    def test_builds_targets_when_missing_only_disabled(self):
        with (
            patch("src.services.relay_recovery_service.SessionLocal") as mock_sl,
            patch("src.services.relay_recovery_service._load_target_rows", return_value=[("g1", "Regular")]),
            patch("src.services.relay_recovery_service.derive_bucket_id", return_value="2024_regular"),
        ):
            mock_session = MagicMock()
            mock_sl.return_value.__enter__.return_value = mock_session
            event_row = MagicMock()
            event_row.game_id = "g1"
            mock_session.query.return_value.filter.return_value.all.return_value = [event_row]
            mock_session.query.return_value.filter.return_value.distinct.return_value.all.return_value = [("g1",)]
            criteria = RecoveryTargetCriteria(season=2024, missing_only=False)
            result = load_relay_recovery_targets(criteria, log=MagicMock())

        assert len(result) == 1
        assert result[0].has_events is True
        assert result[0].has_pbp is True


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
