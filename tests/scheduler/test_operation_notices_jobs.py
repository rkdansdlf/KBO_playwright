"""The in-process notices jobs must pass argv their CLI parser accepts.

Regression: ``crawl_operation_notices_naver_job`` passed ``--naver``, a flag the
CLI never defined (it has ``--source {official,naver,all}``), so argparse raised
``SystemExit(2)`` inside the job on every run. ``SystemExit`` is a
``BaseException`` and is not caught by ``SCHEDULER_JOB_EXCEPTIONS``, so the job
died before saving anything -- the retained log shows two starts and zero
completions.
"""

from __future__ import annotations

import sys
from contextlib import ExitStack
from typing import TYPE_CHECKING
from unittest.mock import patch

from src.cli.collection.crawl_operation_notices import build_arg_parser
from src.scheduler.jobs import stadium

if TYPE_CHECKING:
    from collections.abc import Callable

NOTICES_MAIN = "src.cli.collection.crawl_operation_notices.main"


def _run_job_capturing_argv(job: Callable[[], None]) -> list[str]:
    captured: list[list[str]] = []

    def _fake_main(argv: list[str] | None = None) -> None:
        captured.append(list(argv or []))

    with ExitStack() as stack:
        stack.enter_context(patch(NOTICES_MAIN, _fake_main))
        # The job looks the lock up through whichever scheduler module is loaded,
        # so patch every namespace it can find and keep the real DAILY_LOCK out
        # of the test.
        stack.enter_context(patch.object(stadium, "_scheduler_job_lock"))
        for module_name in ("scripts.scheduler", "src.scheduler"):
            module = sys.modules.get(module_name)
            if module is not None and hasattr(module, "_scheduler_job_lock"):
                stack.enter_context(patch.object(module, "_scheduler_job_lock"))
        job()

    assert len(captured) == 1
    return captured[0]


def test_naver_job_passes_argv_the_cli_can_parse() -> None:
    argv = _run_job_capturing_argv(stadium.crawl_operation_notices_naver_job)

    parsed = build_arg_parser().parse_args(argv)  # SystemExit(2) if the flag regresses
    assert parsed.source == "naver"
    assert parsed.save is True


def test_official_job_passes_argv_the_cli_can_parse() -> None:
    argv = _run_job_capturing_argv(stadium.crawl_operation_notices_job)

    parsed = build_arg_parser().parse_args(argv)
    assert parsed.source == "official"
    assert parsed.save is True
