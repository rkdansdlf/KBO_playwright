"""The daily stability summary must separate "not attempted" from "not failed".

Context (2026-10): ``detail.failure_counts: {}`` appeared on every summary for
two weeks while the crawler produced no evidence at all, because an empty
failure map cannot be told apart from a fully successful sweep. The summary now
carries the schedule/detail/success ledger that makes the difference explicit.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import src.cli.run_daily_update as daily
from src.services.game_write_contract import GameWriteContract


def _build_run_context(target_date: str) -> daily._RunContext:
    return daily._RunContext(
        target_date=target_date,
        year=int(target_date[:4]),
        month=int(target_date[4:6]),
        today_kst=date(int(target_date[:4]), int(target_date[4:6]), int(target_date[6:8])),
        runner=lambda _args: None,
        write_contract=GameWriteContract(run_label=f"test:{target_date}", log=lambda _message: None),
    )


def _summary(ctx: daily._RunContext, tmp_path: Path) -> dict:
    return daily._build_stability_summary(ctx, tmp_path / "summary.json")


def _attempt(ctx: daily._RunContext, *game_ids: str) -> None:
    for game_id in game_ids:
        ctx.detail_games_by_id[game_id] = {"game_id": game_id, "game_date": ctx.target_date}


class TestTheSummaryLedger:
    def test_a_healthy_sweep_records_attempts_and_saves(self, tmp_path: Path) -> None:
        ctx = _build_run_context("20260921")
        ctx.daily_games = [{"game_id": "20260921LGSS0"}, {"game_id": "20260921WOHT0"}]
        ctx.detail_games = list(ctx.daily_games)
        _attempt(ctx, "20260921LGSS0", "20260921WOHT0")
        ctx.processed_game_ids = ["20260921LGSS0", "20260921WOHT0"]

        stability = _summary(ctx, tmp_path)

        assert stability["schedule"]["game_count"] == 2
        assert stability["detail"]["target_count"] == 2
        assert stability["detail"]["success_count"] == 2
        assert stability["detail"]["failure_counts"] == {}

    def test_games_on_the_slate_with_zero_attempts_are_visible(self, tmp_path: Path) -> None:
        ctx = _build_run_context("20260921")
        ctx.daily_games = [{"game_id": f"20260921GG{i}"} for i in range(5)]

        stability = _summary(ctx, tmp_path)

        assert stability["schedule"]["game_count"] == 5
        assert stability["detail"]["target_count"] == 0
        assert stability["detail"]["success_count"] == 0
        assert stability["detail"]["failure_counts"] == {}

    def test_attempted_but_unsaved_is_not_reported_as_all_clear(self, tmp_path: Path) -> None:
        ctx = _build_run_context("20260921")
        ctx.daily_games = [{"game_id": "20260921LGSS0"}]
        ctx.detail_games = list(ctx.daily_games)
        _attempt(ctx, "20260921LGSS0")
        # The 2026-09 shape: results disappeared without a failure_reason, so the
        # failure map stayed empty while nothing was written.

        stability = _summary(ctx, tmp_path)

        detail = stability["detail"]
        assert detail["target_count"] == 1
        assert detail["success_count"] == 0
        assert detail["failure_counts"] == {}
        # "Attempted, unsaved, no failure reason, nothing still missing" is now
        # readable as the contradiction it is instead of passing as all-clear.
        assert stability["detail_recovery"]["still_missing_count"] == 0


class TestTheAlertCarriesTheLedger:
    def test_alert_includes_targets_and_success(self, tmp_path: Path) -> None:
        ctx = _build_run_context("20260921")
        _attempt(ctx, "20260921LGSS0")

        text = daily.format_stability_alert_summary(
            {"target_date": "20260921", "stability": _summary(ctx, tmp_path)},
        )

        assert text is not None
        assert "detail_targets=1" in text
        assert "detail_success=0" in text
