"""Entry points claim their origin so the ledger can attribute their runs.

The claim travels with the call stack (``use_run_origin``); the daily pipeline's
child processes cannot inherit a context variable, so its step runner injects the
environment override into exactly those children instead. These tests pin the
claim at each boundary -- the pipelines themselves are exercised elsewhere, so
the inner calls are replaced with probes that record what was in force.
"""

from __future__ import annotations

import asyncio
import os

import src.cli.live.live_crawler as live_crawler_module
import src.cli.pipelines.run_daily_update as daily_module
from src.crawlers.run_origin import RUN_ORIGIN_ENV_VAR, CrawlRunOrigin, current_run_origin


class TestTheDailyPipelineClaimsItsOrigin:
    def test_the_pipeline_run_claims_daily_update(self, monkeypatch) -> None:
        seen: list[str | None] = []

        async def _probe(target_date, options=None):
            seen.append(current_run_origin())
            return {"ok": True}

        monkeypatch.setattr(daily_module, "_run_update", _probe)

        result = asyncio.run(daily_module.run_update("20261107"))

        assert result == {"ok": True}
        assert seen == ["daily_update"]
        assert current_run_origin() is None

    def test_a_child_step_carries_the_environment_override(self, monkeypatch) -> None:
        """The override is additive: the child keeps the rest of the environment."""
        captured: dict[str, object] = {}

        def _fake_run(cmd, **kwargs):
            captured["cmd"] = cmd
            captured["env"] = kwargs.get("env")

        monkeypatch.setattr("subprocess.run", _fake_run)

        daily_module._run_python_step(["-m", "src.cli.collect_games"])

        env = captured["env"]
        assert isinstance(env, dict)
        assert env[RUN_ORIGIN_ENV_VAR] == CrawlRunOrigin.DAILY_UPDATE.value
        assert env.get("PATH") == os.environ.get("PATH")


class TestTheLiveCrawlerClaimsItsOrigin:
    def test_a_live_cycle_claims_live_crawler(self, monkeypatch) -> None:
        seen: list[str | None] = []

        async def _probe(*, max_active_games=None, detail_snapshot_background=False):
            seen.append(current_run_origin())
            return {"cycle": "done"}

        monkeypatch.setattr(live_crawler_module, "_run_live_crawler_cycle", _probe)

        result = asyncio.run(live_crawler_module.run_live_crawler_cycle())

        assert result == {"cycle": "done"}
        assert seen == ["live_crawler"]
        assert current_run_origin() is None


class TestTheChildProcessEnvDoesNotLeakIntoThisProcess:
    def test_running_a_step_does_not_claim_anything_here(self, monkeypatch) -> None:
        monkeypatch.setattr("subprocess.run", lambda *a, **k: None)

        daily_module._run_python_step(["-m", "src.cli.collect_games"])

        assert current_run_origin() is None
