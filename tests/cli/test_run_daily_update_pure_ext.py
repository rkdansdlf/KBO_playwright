"""Additional pure helper tests for run_daily_update."""

import json
import asyncio
from contextlib import ExitStack
from datetime import date
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError

from src.cli.run_daily_update import (
    DailyUpdateOptions,
    _RunContext,
    _build_pbp_failed_details,
    _build_pbp_recovery_blocks,
    _build_p0_readiness_for_context,
    _build_stability_summary,
    _collect_past_scheduled_recovery_targets,
    _daily_summary_path,
    _failure_status,
    _failure_reason_summary,
    _finalize_manifest_game_ids,
    _finalize_run_update,
    _load_pbp_attempts_by_game,
    _format_counts,
    _format_target_date,
    _is_recoverable_detail_reason,
    _merge_oci_skip_summary,
    _normalize_pbp_attempt_notes,
    _log_finalize_summaries,
    _run_game_status_integrity_audit,
    _run_oci_parity_quality_gate,
    _run_python_step,
    _send_pbp_recovery_report,
    _set_candidate_sync_game_ids,
    _step_0_auto_healer,
    _step_1_schedule,
    _summarize_pbp_failed_game,
    _write_daily_update_summary,
    _write_finalize_outputs,
    format_stability_alert_summary,
    run_update,
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


def test_write_daily_update_summary_serializes_payload(tmp_path: Path):
    summary_path = tmp_path / "nested" / "summary.json"

    result = _write_daily_update_summary(
        target_date="20260402",
        stability={"detail": {"failure_counts": {}}},
        p0_readiness={"summary": {"ok": True}},
        manifest_path=tmp_path / "manifest.json",
        summary_path=summary_path,
    )

    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    assert result == summary_path
    assert payload["phase"] == "postgame_finalize"
    assert payload["target_date"] == "20260402"
    assert payload["stability"]["detail"] == {"failure_counts": {}}


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


def test_integrity_and_parity_quality_gates_handle_success_and_failures():
    with patch("src.cli.run_daily_update.audit_game_status", return_value=[]):
        _run_game_status_integrity_audit()

    violations = [{"game_id": f"G{i}", "game_date": "20260402", "status": "bad", "reason": "x"} for i in range(6)]
    with patch("src.cli.run_daily_update.audit_game_status", return_value=violations):
        try:
            _run_game_status_integrity_audit()
        except RuntimeError as exc:
            assert "1 more" in str(exc)
        else:  # pragma: no cover - defensive assertion
            raise AssertionError("expected RuntimeError")


def test_python_step_and_past_scheduled_recovery_targets():
    with patch("subprocess.run") as run:
        _run_python_step(["-m", "src.cli.example"])
    assert run.call_args.args[0][1:] == ["-m", "src.cli.example"]
    assert run.call_args.kwargs == {"check": True}

    session = MagicMock()
    session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [
        ("20260401LGSS0", date(2026, 4, 1)),
        (None, date(2026, 4, 1)),
    ]
    with patch("src.cli.run_daily_update.SessionLocal") as session_local:
        session_local.return_value.__enter__.return_value = session
        targets = _collect_past_scheduled_recovery_targets(date(2026, 4, 3))
    assert targets == [{"game_id": "20260401LGSS0", "game_date": "20260401"}]

    with patch("src.cli.run_daily_update.SessionLocal", side_effect=SQLAlchemyError("db down")):
        assert _collect_past_scheduled_recovery_targets(date(2026, 4, 3)) == []


def test_auto_healer_and_schedule_steps_cover_skip_success_and_error_paths():
    skipped = _ctx()
    skipped.run_auto_healer = False
    asyncio.run(_step_0_auto_healer(skipped))
    assert skipped.healer_recovery_targets == []

    ctx = _ctx()
    with (
        patch("src.cli.run_daily_update._collect_past_scheduled_recovery_targets", return_value=[{"game_id": "G1"}]),
        patch("src.cli.run_daily_update.run_healer_async", new=AsyncMock(side_effect=RuntimeError("boom"))),
    ):
        asyncio.run(_step_0_auto_healer(ctx))
    assert ctx.healer_recovery_targets == []

    schedule_ctx = _ctx()
    schedule_ctx.limit = 1
    crawler = MagicMock()
    crawler.crawl_schedule = AsyncMock(
        return_value=[
            {"game_id": "G1", "game_date": "20260402"},
            {"game_id": "G2", "game_date": "20260402"},
            {"game_id": "G3", "game_date": "20260403"},
        ],
    )
    save_result = MagicMock(discovered=3, saved=2, failed=1)
    with (
        patch("src.cli.run_daily_update.ScheduleCrawler", return_value=crawler),
        patch("src.cli.run_daily_update.save_schedule_games", return_value=save_result),
        patch("src.cli.run_daily_update.is_detail_candidate_game", side_effect=[True, True]),
    ):
        asyncio.run(_step_1_schedule(schedule_ctx))

    assert [game["game_id"] for game in schedule_ctx.daily_games] == ["G1", "G2"]
    assert [game["game_id"] for game in schedule_ctx.detail_games] == ["G1"]

    with patch("src.cli.run_daily_update.run_legacy_quality_gate", return_value={"ok": True, "failures": []}):
        assert _run_oci_parity_quality_gate()["ok"] is True

    with patch(
        "src.cli.run_daily_update.run_legacy_quality_gate",
        return_value={"ok": False, "failures": ["a", "b", "c", "d", "e", "f"]},
    ):
        try:
            _run_oci_parity_quality_gate()
        except RuntimeError as exc:
            assert "1 more" in str(exc)
        else:  # pragma: no cover - defensive assertion
            raise AssertionError("expected RuntimeError")


def test_finalize_manifest_game_ids_priority_order():
    ctx = _ctx()
    ctx.daily_games = [{"game_id": "D2"}, {"game_id": "D1"}]
    assert _finalize_manifest_game_ids(ctx) == ["D2", "D1"]

    ctx.status_refresh_game_ids = ["S1"]
    assert _finalize_manifest_game_ids(ctx) == ["S1"]

    ctx.processed_game_ids = ["P2", "P1"]
    ctx.reconciliation_changed_ids = ["P1", "R1"]
    assert _finalize_manifest_game_ids(ctx) == ["P1", "P2", "R1"]


def test_p0_readiness_and_finalize_output_helpers(tmp_path: Path):
    ctx = _ctx()
    ctx.oci_skip_counts = {"skipped_empty_relay": 1}
    ctx.oci_skip_game_ids = {"skipped_empty_relay": ["G1"]}

    session = MagicMock()
    with (
        patch("src.cli.run_daily_update.SessionLocal") as session_local,
        patch("src.cli.run_daily_update.build_p0_readiness", return_value={"summary": {"ok": True}}) as build,
    ):
        session_local.return_value.__enter__.return_value = session
        assert _build_p0_readiness_for_context(ctx) == {"summary": {"ok": True}}
    build.assert_called_once()

    with patch("src.cli.run_daily_update.SessionLocal", side_effect=RuntimeError("db down")):
        fallback = _build_p0_readiness_for_context(ctx)
    assert fallback["summary"]["ok"] is False
    assert fallback["oci"]["skip_counts"] == ctx.oci_skip_counts

    with (
        patch("src.cli.run_daily_update.write_refresh_manifest", return_value=tmp_path / "manifest.json") as manifest,
        patch("src.cli.run_daily_update._write_daily_update_summary") as write_summary,
    ):
        result = _write_finalize_outputs(ctx, {"stable": True}, {"summary": {"ok": True}}, tmp_path / "summary.json")
    assert result == tmp_path / "manifest.json"
    manifest.assert_called_once()
    write_summary.assert_called_once()


def test_log_finalize_summaries_uses_contract_and_counts():
    ctx = _ctx()
    ctx.detail_failure_counts = {"timeout": 2}
    ctx.relay_recovery_target_ids = {"G1", "G2"}
    ctx.oci_skip_counts = {"skipped_empty_relay": 1}
    ctx.non_p0_quality_gate_counts = {"ticket": 1}
    ctx.p0_non_game_counts = {"events": 1}
    ctx.detail_still_missing = {"G3"}

    with patch("src.cli.run_daily_update.logger") as mock_logger:
        _log_finalize_summaries(ctx, {"summary": {"ok": True}})

    rendered = "\n".join(str(call.args) for call in mock_logger.info.call_args_list)
    assert "timeout" in rendered
    assert "P0 readiness" in rendered


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


def test_pbp_attempt_loading_and_recovery_report(tmp_path: Path, monkeypatch):
    report_dir = tmp_path / "logs" / "daily_update_summary"
    report_dir.mkdir(parents=True)
    (report_dir / "pbp_report_naver_20260402.csv").write_text(
        "game_id,source_name,status,notes\nG2,naver,failed,missing_middle_inning\n",
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    attempts = _load_pbp_attempts_by_game("20260402")
    assert attempts["G2"][0]["source_name"] == "naver"

    ctx = _ctx()
    _send_pbp_recovery_report(ctx)

    ctx.relay_recovery_target_ids = {"G1", "G2"}
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = ["G1"]
    with (
        patch("src.cli.run_daily_update.SessionLocal") as session_local,
        patch("src.cli.run_daily_update.SlackWebhookClient.send_alert") as send_alert,
    ):
        session_local.return_value.__enter__.return_value = session
        _send_pbp_recovery_report(ctx)

    send_alert.assert_called_once()
    assert "Daily PBP Recovery Report" in send_alert.call_args.args[0]

    with patch("src.cli.run_daily_update.SessionLocal", side_effect=RuntimeError("db down")):
        _send_pbp_recovery_report(ctx)


def test_finalize_run_update_and_run_update_orchestration(tmp_path: Path):
    ctx = _ctx()
    ctx.summary_dir = tmp_path

    with (
        patch("src.cli.run_daily_update._build_stability_summary_for_context", return_value={"stable": True}),
        patch("src.cli.run_daily_update._build_p0_readiness_for_context", return_value={"summary": {"ok": True}}),
        patch("src.cli.run_daily_update._write_finalize_outputs", return_value=tmp_path / "manifest.json"),
        patch("src.cli.run_daily_update._log_finalize_summaries") as log_summaries,
        patch("src.cli.run_daily_update._send_pbp_recovery_report") as send_report,
    ):
        result = _finalize_run_update(ctx)

    assert result["phase"] == "postgame_finalize"
    assert result["manifest_path"].endswith("manifest.json")
    assert result["summary_path"].endswith("20260402.json")
    log_summaries.assert_called_once()
    send_report.assert_called_once()

    queue = MagicMock()
    queue.get_due_detail_recovery_targets.return_value = ["G9"]
    expected = {"phase": "postgame_finalize", "target_date": "20260402"}
    step_patches = [
        "_step_0_auto_healer",
        "_step_1_schedule",
        "_step_2_detail_crawl",
        "_step_3_refresh_status",
        "_step_4_relay_recovery",
        "_step_4_5_proactive_relay",
        "_step_5_content_generation",
        "_step_6_player_stats",
        "_step_6_5_maintenance",
        "_step_7_rosters",
        "_step_7_5_p0_non_game",
        "_step_8_derived_stats",
        "_step_10_7_enrichment",
        "_step_11_sync_pipeline",
        "_step_14_tomorrow_preview",
    ]
    with ExitStack() as stack:
        stack.enter_context(patch("src.cli.run_daily_update._today_kst", return_value=date(2026, 4, 3)))
        stack.enter_context(patch("src.cli.run_daily_update.RecoveryManager", return_value=queue))
        stack.enter_context(patch("src.cli.run_daily_update._finalize_run_update", return_value=expected))
        for name in step_patches:
            stack.enter_context(patch(f"src.cli.run_daily_update.{name}", new=AsyncMock()))
        assert (
            asyncio.run(
                run_update("20260402", DailyUpdateOptions(summary_dir=tmp_path, limit=3)),
            )
            == expected
        )

    queue.purge_detail_recovery_queue.assert_called_once()
    queue.get_due_detail_recovery_targets.assert_called_once()
