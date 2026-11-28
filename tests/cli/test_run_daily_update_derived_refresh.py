"""The daily finalize result must report which derived tables step 8 refreshed."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import src.cli.run_daily_update as daily
from src.services.game_write_contract import GameWriteContract

if TYPE_CHECKING:
    import pytest


def test_finalize_result_carries_derived_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Master DAG reads this list to avoid recomputing standings and rankings.

    Without it the DAG has no way to tell "step 8 finished" from "step 8 was
    skipped because every step is allow_failure=True", so it either recomputes
    a full season unnecessarily or leaves a failed step unrepaired.
    """
    monkeypatch.setattr(daily, "_build_stability_summary_for_context", lambda _ctx, _path: {})
    monkeypatch.setattr(daily, "_build_p0_readiness_for_context", lambda _ctx: {})
    monkeypatch.setattr(daily, "_write_finalize_outputs", lambda _ctx, _s, _p, path: path)
    monkeypatch.setattr(daily, "_log_finalize_summaries", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(daily, "_send_pbp_recovery_report", lambda _ctx: None)

    target_date = "20260921"
    ctx = daily._RunContext(
        target_date=target_date,
        year=int(target_date[:4]),
        month=int(target_date[4:6]),
        today_kst=date(2026, 9, 21),
        runner=lambda _args: None,
        write_contract=GameWriteContract(run_label=f"test:{target_date}", log=lambda _message: None),
    )
    ctx.derived_refresh = ["standings", "matchups", "stat_rankings"]

    result = daily._finalize_run_update(ctx)

    assert result["derived_refresh"] == ["standings", "matchups", "stat_rankings"]
    assert result["phase"] == "postgame_finalize"
