"""A run a scheduled job starts must be attributable to the scheduler.

The ledger's ``origin`` column existed before anything populated it for scheduled
runs: only the replay paths passed a value, so every ordinary run read NULL and
"who started this" stayed unanswered. The provenance is claimed once, at the
job's boundary (``_scheduled_job``), rather than threaded through every
crawler -- most call sites cannot name themselves anyway.

The two properties pinned here are the ones a naive implementation gets wrong:

* the wrap is applied when the job is REGISTERED, so a job added later is
  covered by construction -- tested through the real ``_start_scheduler`` with a
  fake scheduler, because "the helper works" says nothing about whether it is
  used;
* the claim is scoped to the call. APScheduler reuses its worker threads, so a
  value that outlived its job would mislabel the next job scheduled onto the
  same thread.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest

from src.crawlers.run_origin import CrawlRunOrigin, current_run_origin, use_run_origin
from src.scheduler.registry import _scheduled_job


def _register_with_fake_scheduler(monkeypatch):
    """Register the real schedule against a fake that keeps the callables.

    Mirrors ``tests/scheduler/test_scheduler_registry.py``: ``scripts.scheduler``
    is patched alongside the registry global because ``_start_scheduler`` resolves
    the class through ``sys.modules``; patching one side lets the real blocking
    scheduler start and hang the run instead of failing it.
    """
    added: dict[str, object] = {}

    class _FakeScheduler:
        def add_job(self, fn, **kwargs):
            if kwargs.get("id"):
                added[str(kwargs["id"])] = fn

        def add_listener(self, *_args, **_kwargs):
            return None

        def start(self):
            return None

    import scripts.scheduler as scripts_scheduler

    from src.scheduler import registry

    monkeypatch.setattr(scripts_scheduler, "BlockingScheduler", lambda **kwargs: _FakeScheduler())
    monkeypatch.setattr(registry, "BlockingScheduler", lambda **kwargs: _FakeScheduler())
    monkeypatch.setattr(registry, "_SCHEDULER_REF", None, raising=False)
    monkeypatch.setenv("STARTUP_RUN", "0")
    with patch("signal.signal"):
        registry._start_scheduler(registry.build_arg_parser().parse_args(["--no-startup-run"]))
    return added


class TestTheTagIsScopedToTheCall:
    def test_a_wrapped_job_sees_the_scheduler_origin(self) -> None:
        seen: list[str | None] = []

        _scheduled_job(lambda: seen.append(current_run_origin()))()

        assert seen == ["scheduler"]

    def test_the_tag_is_released_when_the_job_returns(self) -> None:
        """Worker threads are reused; the next job must not inherit this one's label."""
        _scheduled_job(lambda: None)()

        assert current_run_origin() is None

    def test_the_tag_is_released_when_the_job_raises(self) -> None:
        def _boom() -> None:
            message = "job failed"
            raise RuntimeError(message)

        with pytest.raises(RuntimeError, match="job failed"):
            _scheduled_job(_boom)()

        assert current_run_origin() is None

    def test_arguments_and_return_value_pass_through(self) -> None:
        assert _scheduled_job(lambda a, b=1: (a, b))(2, b=3) == (2, 3)

    def test_an_inner_claim_wins(self) -> None:
        """The daily pipeline claims its own origin inside the job; that one is truthful."""
        seen: list[str | None] = []

        def _pipeline() -> None:
            with use_run_origin(CrawlRunOrigin.DAILY_UPDATE):
                seen.append(current_run_origin())
            seen.append(current_run_origin())

        _scheduled_job(_pipeline)()

        assert seen == ["daily_update", "scheduler"]


class TestTheRegistryWrapsWhatItRegisters:
    def test_a_job_added_by_name_is_origin_tagged(self, monkeypatch) -> None:
        """Individual ``add_job`` calls are the majority of the registrations."""
        from src.scheduler import registry

        seen: list[str | None] = []
        monkeypatch.setattr(registry, "crawl_pregame_refresh", lambda *_a, **_k: seen.append(current_run_origin()))

        added = _register_with_fake_scheduler(monkeypatch)
        fn = added["crawl_pregame_refresh"]
        fn()  # type: ignore[operator]

        assert seen == ["scheduler"]

    def test_a_job_from_the_spec_list_is_origin_tagged(self, monkeypatch) -> None:
        """The two loops that build the nightly chain go through the same wrap."""
        from src.scheduler import registry

        seen: list[str | None] = []
        monkeypatch.setattr(registry, "crawl_daily_games", lambda *_a, **_k: seen.append(current_run_origin()))

        added = _register_with_fake_scheduler(monkeypatch)
        fn = added["crawl_daily_games"]
        fn()  # type: ignore[operator]

        assert seen == ["scheduler"]
