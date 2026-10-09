"""Run-based replay handlers must explicitly request persistence.

A dead letter is an unresolved failure. Resolving one asserts that the work
behind it is done -- the source was re-read *and* its rows landed. That gives
each replay a second obligation beyond "narrow to one unit": it must write.
This module checks the handlers that delegate persistence through a crawler's
``run``/``crawl_schedule`` call; handlers with a separate service or repository
writer need their own persistence contract.

BH11 found the schedule handler omitting it. `_execute_schedule_replay` calls:

    await crawler.crawl_schedule(year, month, run_spec=spec, record_dead_letters=False)

`crawl_schedule`'s `save` defaults to False, so the month is fetched, nothing is
persisted, `records_written` stays 0, and the run still lands as `success`.
`_outcome_from_persisted_run` reads `status == success` and reports the letter
resolved. The incident closes over a month of schedule that was never refreshed.

This is the exact failure the dispatcher's own comment warns about, and it is
quoted in that comment:

    The crawl arguments are pinned explicitly. `save` defaults to False on most
    of these crawlers, so inheriting the default would mean a replay that
    fetched the page and stored nothing, and reported success.

The ten other handlers pass `save=True` explicitly. The contract is written down
and applied everywhere except where a schedule replay needs it. This module
enforces it as a structural rule rather than trusting each handler to remember.
"""

from __future__ import annotations

import ast
import inspect
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.services import crawl_replay_dispatcher as dispatcher_mod
from src.services.crawl_replay_dispatcher import build_default_dispatcher

if TYPE_CHECKING:
    from collections.abc import Iterator

RUN_ID = "REPLAY-RUN"
STAGE = "fetch"


@dataclass
class _Recorded:
    """What a stubbed crawler was asked to do."""

    kwargs: dict[str, Any]
    stored: bool = False


@contextmanager
def _ledger(monkeypatch: pytest.MonkeyPatch) -> Iterator[sessionmaker]:
    """An in-memory ledger holding one successful replay run."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    factory = sessionmaker(bind=engine, expire_on_commit=False)
    with factory() as session:
        session.add(
            CrawlExecutionRun(
                run_id=RUN_ID,
                crawler="probe",
                target_type="unit",
                status="success",
                started_at=datetime.now(UTC).replace(tzinfo=None),
            ),
        )
        session.commit()
    monkeypatch.setattr(dispatcher_mod, "SessionLocal", factory)
    yield factory
    engine.dispose()


def _letter(crawler: str, **overrides: Any) -> CrawlDeadLetter:
    # The dispatcher refuses a `target_type` that contradicts the crawler's own
    # contract, so the placeholder this file used to pass would stop at the
    # guard instead of reaching the handler under test.
    fields: dict[str, Any] = {
        "dlq_id": "DLQ-1",
        "original_run_id": "RUN-A",
        "crawler": crawler,
        "target_type": dispatcher_mod._EXPECTED_TARGET_TYPES[crawler],
        "target_id": "2026-05",
        "failure_stage": STAGE,
        "error_code": "FETCH_TIMEOUT",
    }
    fields.update(overrides)
    return CrawlDeadLetter(**fields)


def _is_truthy(value: object) -> bool:
    """A `save`-style flag may arrive positionally, so inspect the call shape."""
    return value is True or (value is not None and value is not False)


class TestAScheduleReplayMustWrite:
    """BUG-010: the schedule replay fetched the month and stored nothing.

    Fixed 2026-10-06. The `xfail(strict)` marker that initially pinned this was
    removed after the handler passed. This behavioral check remains alongside
    the source-level argument check because it observes the actual call.
    """

    def test_the_handler_passes_save_to_crawl_schedule(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The contract, stated directly: this handler writes."""
        recorded = _Recorded(kwargs={})

        class _Schedule:
            async def crawl_schedule(self, year, month, series_id=None, *args, **kwargs):
                recorded.kwargs = dict(kwargs)
                recorded.stored = _is_truthy(kwargs.get("save")) or _is_truthy(args[0] if args else None)
                return []

        with _ledger(monkeypatch):
            with patch.object(dispatcher_mod, "ScheduleCrawler", return_value=_Schedule()):
                build_default_dispatcher().replay(_letter("schedule"), replay_run_id=RUN_ID)

        assert recorded.stored, (
            "_execute_schedule_replay must pass save=True. crawl_schedule defaults to save=False, so "
            "the replay reads the month, persists nothing, lands as success with records_written=0, "
            "and _outcome_from_persisted_run resolves the letter over work that never landed."
        )


class TestRunBasedReplayHandlersDeclareSaving:
    """The source-level contract for selected run-based replay handlers.

    These handlers expose a ``save`` argument that defaults to false. The
    separate persistence paths for game detail, relay, player movement, and
    preview are intentionally not represented by this direct-argument check.
    """

    @pytest.mark.parametrize(
        ("crawler", "coroutine"),
        [
            # `food` and `parking` share `_replay_team_page`, so neither handler
            # names an `_execute_*` coroutine of its own.
            ("awards", "_execute_award_replay"),
            ("roster_transactions", "_execute_roster_replay"),
            ("food", "_execute_team_page_replay"),
            ("parking", "_execute_team_page_replay"),
            ("kbo_event", "_execute_kbo_event_replay"),
            ("team_history", "_execute_team_history_replay"),
            ("realtime_issue", "_execute_realtime_issue_replay"),
            # BUG-010: `save` was omitted here, so a replay fetched the month and
            # stored nothing while landing as `success` -- which resolved the
            # letter over work that never landed. Added to this list on the fix.
            ("schedule", "_execute_schedule_replay"),
        ],
    )
    def test_a_handler_that_calls_run_pins_save(self, crawler: str, coroutine: str) -> None:
        source = inspect.getsource(getattr(dispatcher_mod, coroutine))

        assert _passes_save_true(source), (
            f"{coroutine} ({crawler}) does not pin save=True. These run-based handlers must request "
            "persistence because save defaults to False and an empty write can still report success."
        )

    def test_the_schedule_handler_no_longer_has_a_save_gap(self) -> None:
        """The BUG-010 regression, named so the fix is not quietly reverted.

        ``schedule`` is now in the parametrised list above, so this looks
        redundant until it stops being true. It stays because the failure mode is
        invisible in a running system: the letter resolves, the run reports
        success, and nothing anywhere says a problem.
        """
        source = _execute_source("schedule")

        assert "crawl_schedule(" in source
        assert _passes_save_true(source), (
            "BUG-010 regressed: _execute_schedule_replay must pass save=True. Without it the replay "
            "fetches the month, persists nothing, lands as success with records_written=0, and the "
            "letter is resolved over work that never landed."
        )


def _passes_save_true(source: str) -> bool:
    """Check call syntax, not comments or docstrings, for an explicit save flag."""
    tree = ast.parse(source)
    return any(
        isinstance(node, ast.Call)
        and any(
            keyword.arg == "save" and isinstance(keyword.value, ast.Constant) and keyword.value.value is True
            for keyword in node.keywords
        )
        for node in ast.walk(tree)
    )


def test_a_docstring_does_not_satisfy_the_save_argument_contract() -> None:
    """Keep the source check from passing on explanatory prose alone."""
    source = '''
async def replay(crawler):
    """save=True is required for replay."""
    await crawler.run()
'''

    assert not _passes_save_true(source)


def _execute_source(crawler: str) -> str:
    """Return the source of the `_execute_*` coroutine a handler drives.

    ``food`` and ``parking`` are excluded by name above because they share
    `_replay_team_page`; resolving through the handler body covers the rest.
    """
    handler_source = inspect.getsource(build_default_dispatcher()._handlers[crawler])
    for line in handler_source.splitlines():
        if "_execute_" in line and "(" in line:
            name = line.split("_execute_", 1)[1].split("(", 1)[0]
            target = getattr(dispatcher_mod, f"_execute_{name}", None)
            if target is not None:
                return inspect.getsource(target)
    message = f"could not resolve the _execute_* coroutine for {crawler}"
    raise AssertionError(message)
