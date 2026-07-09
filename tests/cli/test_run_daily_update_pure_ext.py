"""Additional pure helper tests for run_daily_update."""

from datetime import date
from pathlib import Path

from src.cli.run_daily_update import (
    _RunContext,
    _build_pbp_failed_details,
    _build_pbp_recovery_blocks,
    _build_stability_summary,
    _daily_summary_path,
    _failure_status,
    _failure_reason_summary,
    _finalize_manifest_game_ids,
    _format_counts,
    _format_target_date,
    _is_recoverable_detail_reason,
    _merge_oci_skip_summary,
    _normalize_pbp_attempt_notes,
    _set_candidate_sync_game_ids,
    _summarize_pbp_failed_game,
    format_stability_alert_summary,
)
from src.services.game_write_contract import GameWriteContract
from src.utils.game_status import GAME_STATUS_CANCELLED, GAME_STATUS_UNRESOLVED


def _ctx() -> _RunContext:
    return _RunContext(
        target_date="20260402",
        year=2026,
        month=4,
        today_kst=date(2026, 4, 3),
        runner=lambda *_args, **_kwargs: None,
        write_contract=GameWriteContract(run_label="test"),
    )


class _FailureItem:
    def __init__(self, failure_reason: str | None) -> None:
        self.failure_reason = failure_reason


def test_is_recoverable_detail_reason_normalizes_known_values():
    assert _is_recoverable_detail_reason(" TIMEOUT ") is True
    assert _is_recoverable_detail_reason("incomplete_detail") is True
    assert _is_recoverable_detail_reason("bad_status") is False
    assert _is_recoverable_detail_reason(None) is False


def test_format_counts_sorts_and_skips_zero_values():
    assert _format_counts({"timeout": 2, "bad_status": 0, "missing": 1}) == "missing=1, timeout=2"
    assert _format_counts({"timeout": 0}) == "none"
    assert _format_counts({}) == "none"


def test_failure_reason_summary_counts_groups_and_deduplicates_ids():
    counts, game_ids = _failure_reason_summary(
        {
            "G2": _FailureItem("timeout"),
            "G1": _FailureItem("timeout"),
            "G2_DUP": _FailureItem("missing"),
            "": _FailureItem("missing"),
            "G3": _FailureItem(None),
        },
    )

    assert counts == {"timeout": 2, "missing": 2}
    assert game_ids == {"missing": ["G2_DUP"], "timeout": ["G1", "G2"]}


def test_daily_summary_path_uses_default_and_custom_directory(tmp_path: Path):
    assert _daily_summary_path("20260402").name == "20260402.json"
    assert _daily_summary_path("20260402", tmp_path) == tmp_path / "20260402.json"


def test_format_target_date_accepts_date_strings_and_fallbacks():
    assert _format_target_date(date(2026, 4, 2), fallback_game_id="20260403LGOB0") == "20260402"
    assert _format_target_date("2026-04-02", fallback_game_id="20260403LGOB0") == "20260402"
    assert _format_target_date("20260402", fallback_game_id="20260403LGOB0") == "20260402"
    assert _format_target_date("bad", fallback_game_id="20260403LGOB0") == "20260403"


def test_set_candidate_sync_game_ids_unions_all_context_sources():
    ctx = _ctx()
    ctx.daily_games = [{"game_id": "G3"}, {"game_id": "G1"}]
    ctx.status_refresh_game_ids = ["G2", "G1"]
    ctx.processed_game_ids = ["G4"]
    ctx.reconciliation_changed_ids = ["G5", "G4"]
    ctx.healer_recovery_targets = [{"game_id": "G6"}, {"game_id": "G1"}]
    ctx.relay_recovery_target_ids = {"G7", "G2"}

    _set_candidate_sync_game_ids(ctx)

    assert ctx.candidate_sync_game_ids == ["G1", "G2", "G3", "G4", "G5", "G6", "G7"]


def test_merge_oci_skip_summary_handles_lists_counts_and_invalid_values():
    counter: dict[str, int] = {}
    game_ids: dict[str, list[str]] = {}

    _merge_oci_skip_summary(
        counter,
        game_ids,
        {
            "skipped_empty_relay": ["G2", "G3", ""],
            "skipped_cancelled": "2",
            "skipped_incomplete_detail": "bad",
        },
        "G1",
    )
    _merge_oci_skip_summary(counter, game_ids, object(), "ignored")

    assert counter == {"skipped_cancelled": 2, "skipped_empty_relay": 3}
    assert game_ids == {"skipped_cancelled": ["G1"], "skipped_empty_relay": ["G2", "G3"]}


def test_build_stability_summary_collects_candidates_and_deduplicates_ids(tmp_path: Path):
    ctx = _ctx()
    ctx.detail_failure_counts = {"timeout": 2, "bad_status": 1}
    ctx.detail_failure_game_ids = {"timeout": ["G2", "G1", "G2"], "bad_status": ["G9"]}
    ctx.oci_skip_counts = {"skipped_empty_relay": 2}
    ctx.oci_skip_game_ids = {"skipped_empty_relay": ["G3", "G3"], "skipped_cancelled": ["G4"]}
    ctx.relay_recovery_target_ids = {"G3", "G5"}
    ctx.non_p0_quality_gate_counts = {"ticket": 1}
    ctx.non_p0_quality_gate_ids = {"ticket": ["T1", "T1"]}
    ctx.p0_non_game_counts = {"events": 1}
    ctx.p0_non_game_errors = {"events": "boom"}
    ctx.detail_recovery_passes = 2
    ctx.detail_recovered_after_retry = 1
    ctx.detail_still_missing = {"G8"}
    ctx.detail_recovery_attempts = {"G8": 3}
    ctx.detail_retry_escalation_game_ids = ["G8", "G8"]

    summary = _build_stability_summary(ctx, tmp_path / "daily.json")

    assert summary["retry_candidates"] == {"detail": ["G1", "G2"], "relay": ["G3"]}
    assert summary["affected_game_ids"] == ["G1", "G2", "G3", "G4", "G9"]
    assert summary["detail_recovery"]["still_missing"] == ["G8"]
    assert summary["quality_gates"]["non_p0_failure_ids"] == {"ticket": ["T1"]}


def test_format_stability_alert_summary_handles_missing_and_full_payload():
    assert format_stability_alert_summary(object()) is None
    assert format_stability_alert_summary({"target_date": "20260402"}) is None

    text = format_stability_alert_summary(
        {
            "target_date": "20260402",
            "stability": {
                "detail": {"failure_counts": {"timeout": 2}},
                "relay": {"target_count": 3},
                "oci": {"skip_counts": {"skipped_empty_relay": 1}},
                "quality_gates": {"non_p0_failure_counts": {"ticket": 1}},
                "detail_recovery": {"passes": 2, "recovered_after_retry": 1, "still_missing_count": 4},
            },
            "p0_readiness": {"summary": {"ok": False, "failure_count": 1}},
        },
    )

    assert text is not None
    assert "target_date=20260402" in text
    assert "detail_failures=timeout=2" in text
    assert "relay_targets=3" in text


def test_failure_status_for_cancelled_past_and_invalid_dates():
    assert _failure_status("bad", None, date(2026, 4, 3)) is None
    assert _failure_status("20260402", "cancelled", date(2026, 4, 3)) == GAME_STATUS_CANCELLED
    assert _failure_status("20260402", None, date(2026, 4, 3)) == GAME_STATUS_UNRESOLVED
    assert _failure_status("20260403", None, date(2026, 4, 3)) is None


def test_finalize_manifest_game_ids_priority_order():
    ctx = _ctx()
    ctx.daily_games = [{"game_id": "D2"}, {"game_id": "D1"}]
    assert _finalize_manifest_game_ids(ctx) == ["D2", "D1"]

    ctx.status_refresh_game_ids = ["S1"]
    assert _finalize_manifest_game_ids(ctx) == ["S1"]

    ctx.processed_game_ids = ["P2", "P1"]
    ctx.reconciliation_changed_ids = ["P1", "R1"]
    assert _finalize_manifest_game_ids(ctx) == ["P1", "P2", "R1"]


def test_pbp_failed_detail_helpers_and_blocks_truncate_long_details():
    assert _normalize_pbp_attempt_notes("final_score_mismatch: 3-2") == "score_mismatch"
    assert _normalize_pbp_attempt_notes("missing_middle_inning: 5") == "inning_gap"
    assert _summarize_pbp_failed_game("G0", []) == "- `G0`: No logs found"

    attempts = [{"source_name": "naver", "status": "failed", "notes": "missing_middle_inning"}]
    assert _build_pbp_failed_details({"G1", "G0"}, {"G1": attempts}) == [
        "- `G0`: No logs found",
        "- `G1`: *naver*:failed (inning_gap)",
    ]

    ctx = _ctx()
    ctx.relay_recovery_target_ids = {"G1", "G2"}
    blocks = _build_pbp_recovery_blocks(ctx, success_count=1, failed_count=1, failed_details=["x" * 3000])

    assert blocks[0]["type"] == "header"
    assert blocks[1]["fields"][0]["text"] == "*Target Games:* 2"
    assert "truncated" in blocks[2]["text"]["text"]
