from __future__ import annotations

import json
from unittest.mock import patch

import src.cli.morning_pbp_report as morning_pbp_report
from src.cli.morning_pbp_report import (
    _build_telegram_message,
    _find_latest_summary,
)


def _write_summary(monkeypatch, tmp_path, date_str: str, payload: dict | None = None):
    summary_dir = tmp_path / "daily_update_summary"
    summary_dir.mkdir()
    monkeypatch.setattr(morning_pbp_report, "DAILY_SUMMARY_DIR", summary_dir)
    (summary_dir / f"{date_str}.json").write_text(
        json.dumps(payload or {"stability": {}}, ensure_ascii=False),
        encoding="utf-8",
    )


def _use_empty_summary_dir(monkeypatch, tmp_path):
    summary_dir = tmp_path / "daily_update_summary"
    summary_dir.mkdir()
    monkeypatch.setattr(morning_pbp_report, "DAILY_SUMMARY_DIR", summary_dir)


# ===================================================================
# _find_latest_summary tests
# ===================================================================


class TestFindLatestSummary:
    def test_nonexistent_date_returns_none(self, monkeypatch, tmp_path):
        _use_empty_summary_dir(monkeypatch, tmp_path)
        assert _find_latest_summary("19900101") is None

    def test_valid_date_found(self, monkeypatch, tmp_path):
        _write_summary(monkeypatch, tmp_path, "20260528")
        result = _find_latest_summary("20260528")
        assert result is not None
        date_str, data = result
        assert date_str == "20260528"
        assert "stability" in data

    def test_none_default_looks_for_yesterday(self, monkeypatch, tmp_path):
        _use_empty_summary_dir(monkeypatch, tmp_path)
        result = _find_latest_summary(None)
        assert result is None


# ===================================================================
# _build_telegram_message tests
# ===================================================================

SAMPLE_SUMMARY = {
    "phase": "postgame_finalize",
    "target_date": "20260528",
    "stability": {
        "relay": {
            "target_count": 6,
            "target_game_ids": ["g1", "g2", "g3", "g4", "g5", "g6"],
        },
        "retry_candidates": {
            "detail": [],
            "relay": ["g1", "g2"],
        },
        "oci": {
            "skip_counts": {
                "skipped_empty_relay": 2,
                "skipped_incomplete_detail": 1,
            },
            "skip_game_ids": {
                "skipped_empty_relay": ["g1", "g3"],
                "skipped_incomplete_detail": ["g1"],
            },
        },
        "affected_game_ids": ["g1", "g2", "g3"],
    },
}

SAMPLE_SUMMARY_NO_FAILURES = {
    "phase": "postgame_finalize",
    "target_date": "20260529",
    "stability": {
        "relay": {
            "target_count": 5,
            "target_game_ids": ["g1", "g2", "g3", "g4", "g5"],
        },
        "retry_candidates": {
            "detail": [],
            "relay": [],
        },
        "oci": {
            "skip_counts": {},
            "skip_game_ids": {},
        },
        "affected_game_ids": [],
    },
}


class TestBuildTelegramMessage:
    def test_includes_header_with_date(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "PBP Morning Report" in msg
        assert "20260528" in msg

    def test_lists_failed_relay_games(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "Failed Relay" in msg
        assert "g1" in msg
        assert "g2" in msg

    def test_shows_relay_target_count(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "Relay Targets" in msg
        assert "6" in msg

    def test_shows_oci_skips(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "OCI Skips" in msg
        assert "skipped_empty_relay" in msg
        assert "skipped_incomplete_detail" in msg

    def test_all_success_shows_no_failures(self):
        msg = _build_telegram_message("20260529", SAMPLE_SUMMARY_NO_FAILURES, {}, dry_run=False)
        assert "All targets recovered" in msg
        assert "Failed" not in msg

    def test_no_oci_skips_omitted_when_empty(self):
        msg = _build_telegram_message("20260529", SAMPLE_SUMMARY_NO_FAILURES, {}, dry_run=False)
        assert "OCI Skips" not in msg

    def test_validation_counts_shown(self):
        val = {"verified": 10, "unverified": 2, "other": 1}
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, val, dry_run=False)
        assert "Validation" in msg
        assert "10 verified" in msg
        assert "2 unverified" in msg

    def test_no_validation_data_shows_info(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "No data" in msg

    def test_affected_games_listed(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "Affected Games" in msg
        assert "g1" in msg
        assert "g2" in msg
        assert "g3" in msg

    def test_dry_run_flag_in_message(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=True)
        assert "Dry-run" in msg

    def test_no_dry_run_flag_when_not_dry(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "Dry-run" not in msg

    def test_detail_failures_listed_when_present(self):
        summary = {
            **SAMPLE_SUMMARY,
            "stability": {
                **SAMPLE_SUMMARY["stability"],
                "retry_candidates": {
                    "detail": ["d1", "d2"],
                    "relay": ["g1"],
                },
            },
        }
        msg = _build_telegram_message("20260528", summary, {}, dry_run=False)
        assert "Failed Detail" in msg
        assert "d1" in msg

    def test_truncates_long_lists(self):
        summary = {
            **SAMPLE_SUMMARY,
            "stability": {
                **SAMPLE_SUMMARY["stability"],
                "retry_candidates": {
                    "detail": [],
                    "relay": [f"g{i:02d}" for i in range(20)],
                },
                "affected_game_ids": [f"g{i:02d}" for i in range(20)],
            },
        }
        msg = _build_telegram_message("20260528", summary, {}, dry_run=False)
        assert "... and 10 more" in msg  # truncated relay failures
        assert "... and 10 more" in msg  # truncated affected games

    def test_telegram_html_format(self):
        msg = _build_telegram_message("20260528", SAMPLE_SUMMARY, {}, dry_run=False)
        assert "<b>" in msg or "<i>" in msg
        assert "20260528" in msg


# ===================================================================
# run_morning_report integration tests
# ===================================================================


class TestRunMorningReport:
    def test_dry_run_returns_true(self, monkeypatch, tmp_path):
        _write_summary(monkeypatch, tmp_path, "20260528", SAMPLE_SUMMARY)
        from src.cli.morning_pbp_report import run_morning_report

        result = run_morning_report("20260528", dry_run=True)
        assert result is True

    def test_nonexistent_date_returns_true_in_dry_run(self, monkeypatch, tmp_path):
        """Should still return True and show the fallback message."""
        _use_empty_summary_dir(monkeypatch, tmp_path)
        from src.cli.morning_pbp_report import run_morning_report

        result = run_morning_report("19900101", dry_run=True)
        assert result is True

    @patch("src.utils.alerting.SlackWebhookClient.send_alert")
    def test_sends_alert_when_not_dry(self, mock_send, monkeypatch, tmp_path):
        _write_summary(monkeypatch, tmp_path, "20260528", SAMPLE_SUMMARY)
        mock_send.return_value = True
        from src.cli.morning_pbp_report import run_morning_report

        result = run_morning_report("20260528")
        assert result is True
        mock_send.assert_called_once()
        msg = mock_send.call_args[0][0]
        assert "PBP Morning Report" in msg

    @patch("src.utils.alerting.SlackWebhookClient.send_alert")
    def test_sends_alert_for_nonexistent_date(self, mock_send, monkeypatch, tmp_path):
        _use_empty_summary_dir(monkeypatch, tmp_path)
        mock_send.return_value = True
        from src.cli.morning_pbp_report import run_morning_report

        result = run_morning_report("19900101")
        assert result is True
        mock_send.assert_called_once()
        msg = mock_send.call_args[0][0]
        assert "No daily summary found" in msg
