"""Content history refresh step tests.

Milestone, split and draft crawls only ever ran in the manually dispatched
``daily-extras`` job, so the scheduled pipeline never ran them and their data was
refreshed only when a human triggered a full run.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.cli.pipelines import run_daily_update as pipeline

EXPECTED_MODULES = [
    "src.cli.crawl_milestones",
    "src.cli.crawl_player_splits",
    "src.cli.crawl_player_drafts",
]


def _context(**overrides: object) -> SimpleNamespace:
    base: dict[str, object] = {"year": 2026, "runner": MagicMock()}
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.mark.parametrize("flag_val", [None, "", "  "])
def test_step_12_skips_milestones_when_flag_off(monkeypatch: pytest.MonkeyPatch, flag_val: str | None) -> None:
    """When MILESTONE_CRAWL_ENABLED is off (unset or empty), milestones are skipped and splits/drafts run."""
    if flag_val is None:
        monkeypatch.delenv("MILESTONE_CRAWL_ENABLED", raising=False)
    else:
        monkeypatch.setenv("MILESTONE_CRAWL_ENABLED", flag_val)
    ctx = _context()

    asyncio.run(pipeline._step_12_content_history(ctx))

    calls = [call.args[0] for call in ctx.runner.call_args_list]
    modules = [argv[1] for argv in calls]
    assert "src.cli.crawl_milestones" not in modules
    assert modules == ["src.cli.crawl_player_splits", "src.cli.crawl_player_drafts"]
    for argv in calls:
        assert argv[:1] == ["-m"]
        assert argv[2:] == ["--season", "2026", "--save"]


def test_all_three_crawls_run_for_the_season_with_save(monkeypatch: pytest.MonkeyPatch) -> None:
    """When MILESTONE_CRAWL_ENABLED is on, all three crawlers are invoked with --save."""
    monkeypatch.setenv("MILESTONE_CRAWL_ENABLED", "1")
    ctx = _context()

    asyncio.run(pipeline._step_12_content_history(ctx))

    calls = [call.args[0] for call in ctx.runner.call_args_list]
    assert [argv[1] for argv in calls] == EXPECTED_MODULES
    for argv in calls:
        assert argv[:1] == ["-m"]
        assert argv[2:] == ["--season", "2026", "--save"]


def test_a_failing_crawl_does_not_stop_the_others(monkeypatch: pytest.MonkeyPatch) -> None:
    """One broken crawler must not skip the remaining refreshes."""
    monkeypatch.setenv("MILESTONE_CRAWL_ENABLED", "1")
    ctx = _context()
    ctx.runner.side_effect = [RuntimeError("boom"), None, None]

    asyncio.run(pipeline._step_12_content_history(ctx))  # must not raise

    assert ctx.runner.call_count == len(EXPECTED_MODULES)


def test_dag_chains_content_history_after_enrichment_before_preview() -> None:
    """Ordering keeps the refresh inside the scheduled daily run."""
    ctx = _context(target_date="20261005")

    tasks = pipeline._build_daily_update_dag(ctx)._tasks

    assert tasks["step_12_content_history"].dependencies == {"step_10_7_enrichment"}
    assert tasks["step_14_tomorrow_preview"].dependencies == {"step_12_content_history"}
