"""The game-detail dead letter lifecycle, end to end.

The distinction these tests exist to hold is that `records_written = 1` and
`success` mean different things. A partial box score is stored, so there is data
worth keeping, and it is still incomplete, so there is work left to do. Collapse
the two and the queue either fills with games that will never change or empties
of games that need another attempt.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.game_detail_outcome import attempt_from_result
from src.crawlers.failure_taxonomy import FailureCode
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun
from src.services.crawl_dead_letter_service import retry_dead_letter
from src.services.crawl_replay_dispatcher import build_default_dispatcher

if TYPE_CHECKING:
    from src.services.crawl_dead_letter_service import DlqRetryResult

#: Stands in for a `lightweight` keyword that was never passed.
_NOT_REQUESTED = object()

GAME = "20250501LGOB0"
FULL_DETAIL: dict[str, Any] = {
    "game_id": GAME,
    "hitters": {"away": [{"name": "a"}], "home": [{"name": "b"}]},
    "pitchers": {"away": [{"name": "c"}], "home": [{"name": "d"}]},
    "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "OB", "score": 1}},
    "metadata": {},
}
#: Has the shape of a game but no usable detail rows on either side.
DEGRADED_DETAIL: dict[str, Any] = {
    "game_id": GAME,
    "hitters": {"away": [{}], "home": []},
    "pitchers": {"away": [{}], "home": []},
    "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "OB", "score": 1}},
    "metadata": {},
}


@pytest.fixture
def factory() -> sessionmaker:
    """An isolated database holding both the run ledger and the queue."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    CrawlDeadLetter.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def wired(factory: sessionmaker, monkeypatch: pytest.MonkeyPatch) -> None:
    for module in (
        "src.services.crawl_run_service",
        "src.services.game_detail_runs",
        "src.services.game_collection_service",
        "src.services.crawl_dead_letter_service",
        "src.services.crawl_replay_dispatcher",
    ):
        monkeypatch.setattr(f"{module}.SessionLocal", factory)


class _StubCrawler:
    """Yields a fixed payload per call, so one letter can be replayed repeatedly.

    `lightweight` is accepted and recorded rather than ignored, because whether a
    request was a lightweight one decides what a reduced payload means, and that
    is the distinction these tests are about.
    """

    def __init__(
        self,
        payloads: dict[str, dict[str, Any] | None],
        *,
        lightweight: bool = False,
        attempt_lightweight: bool | None = None,
    ) -> None:
        self.payloads = payloads
        self.lightweight = lightweight
        # The request flag and the verdict are separate: a crawler decides the
        # verdict while it knows what it asked for, and the tests need to hand the
        # service a verdict the batch path would not itself produce.
        self.attempt_lightweight = lightweight if attempt_lightweight is None else attempt_lightweight
        self.calls: list[str] = []
        self.lightweight_requests: list[bool] = []

    async def crawl_game_attempts(
        self,
        games: list[dict[str, Any]],
        *,
        concurrency: int | None = None,
        lightweight: Any = _NOT_REQUESTED,
    ) -> list[Any]:
        """Record what was asked for, distinguishing False from never passed.

        A plain False default would make an omitted keyword look like an explicit
        one, which is the exact difference these tests exist to hold.
        """
        from src.services.game_collection_service import GameCollectionTarget

        self.lightweight_requests.append(lightweight)
        attempts = []
        for game in games:
            target = GameCollectionTarget(game_id=game["game_id"], game_date=game["game_date"])
            self.calls.append(target.game_id)
            payload = self.payloads.get(target.game_id)
            attempts.append(
                attempt_from_result(target.game_id, payload, lightweight=self.attempt_lightweight),
            )
        return attempts

    async def close(self) -> None:
        return None


def _only_a_missing_payload_fails(target: Any, payload: Any, crawler: Any, should_save_detail: Any) -> Any:
    """Fail only a game that produced no payload at all.

    Mirrors the production rule while leaving payload validation out of the way,
    so these tests observe how an outcome is recorded rather than how a payload
    is judged.
    """
    if not payload:
        return ("crawl_failed", "timeout", "no_detail_payload")
    return None


def _collect(
    crawler: _StubCrawler,
    *,
    save: bool = True,
    save_error: Exception | None = None,
) -> None:
    """Collect one game, stubbing the write only when the test is not about it.

    `save_error` is raised from the real `save_game_detail`, not from a stub, so
    the code that classifies a write failure is the code under test.
    """
    """Run one detail collection for a single game, with the save stubbed."""
    from src.services.game_collection_service import (
        GameCollectionConfig,
        GameCollectionItemResult,
        GameCollectionResult,
        GameCollectionTarget,
        _collect_detail_phase,
    )
    from src.services.game_collection_service import ExistingGameData

    config = GameCollectionConfig()
    result = GameCollectionResult()
    result.items = {GAME: GameCollectionItemResult(game_id=GAME, game_date="20250501")}
    ctx = MagicMock()
    ctx.cfg = config
    ctx.detail_crawler = crawler
    ctx.contract = MagicMock()
    ctx.detail_source = MagicMock()
    ctx.result = result
    targets = [GameCollectionTarget(game_id=GAME, game_date="20250501")]

    def _save(target: Any, payload: dict[str, Any], _ctx: Any) -> tuple[bool, Any]:
        return (save, None)

    def _raise(*args: Any, **kwargs: Any) -> Any:
        raise save_error

    patches = [
        patch(
            "src.services.game_collection_service._detail_payload_failure_reason",
            side_effect=_only_a_missing_payload_fails,
        ),
    ]
    if save_error is not None:
        patches.append(patch("src.services.game_collection_service.save_game_detail", side_effect=_raise))
    else:
        patches.append(patch("src.services.game_collection_service._save_detail_payload", side_effect=_save))

    with patches[0], patches[1]:
        asyncio.run(
            _collect_detail_phase(
                targets,
                {GAME: ExistingGameData()},
                ctx,
            ),
        )


def _run_by_id(factory: sessionmaker, run_id: str | None) -> CrawlExecutionRun | None:
    """Fetch one run by its own id.

    Keying by game would not work here: the original run and its retry are both
    about the same game, and only one of them would survive the dict.
    """
    if run_id is None:
        return None
    with factory() as session:
        return session.query(CrawlExecutionRun).filter(CrawlExecutionRun.run_id == run_id).one_or_none()


def _letters(factory: sessionmaker) -> list[CrawlDeadLetter]:
    with factory() as session:
        return list(session.query(CrawlDeadLetter).order_by(CrawlDeadLetter.id).all())


def _runs(factory: sessionmaker) -> dict[str, CrawlExecutionRun]:
    with factory() as session:
        return {run.target_id: run for run in session.query(CrawlExecutionRun).all()}


def _retry(factory: sessionmaker, dlq_id: str) -> DlqRetryResult:
    """Drive one retry the way the worker does.

    The write itself is stubbed: whether a payload is acceptable is covered by
    the run-ledger tests, and leaving the real save in would make these assert
    about a missing parent game row rather than about the queue.
    """
    with (
        patch("src.services.game_collection_service.save_game_detail", return_value=True),
        patch(
            "src.services.game_collection_service._detail_payload_failure_reason",
            side_effect=_only_a_missing_payload_fails,
        ),
    ):
        return retry_dead_letter(dlq_id, build_default_dispatcher(), session_factory=factory)


class TestOnlyUnfinishedWorkIsQueued:
    def test_a_full_save_queues_nothing(self, factory: sessionmaker, wired: None) -> None:
        """Nothing is left to fetch, so there is nothing to remember."""
        _collect(_StubCrawler({GAME: FULL_DETAIL}))

        assert _letters(factory) == []
        assert _runs(factory)[GAME].status == "success"

    def test_a_lightweight_result_is_a_success_and_queues_nothing(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """A reduced payload is the answer to a reduced request.

        Queueing it would re-fetch a page that is complete for what was asked of
        it, and would do so on every future run of the same kind.
        """
        _collect(_StubCrawler({GAME: DEGRADED_DETAIL}, lightweight=True))

        run = _runs(factory)[GAME]
        assert run.status == "success"
        assert run.records_written == 1
        assert _letters(factory) == []

    def test_the_same_payload_under_a_full_request_is_not_a_success(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """The same bytes, asked for in full, are a shortfall worth re-fetching.

        Paired with the test above on purpose. Either one alone could pass for the
        wrong reason -- one because nothing was ever queued, the other because
        everything was. Together they show the outcome follows the request, not
        the payload.
        """
        _collect(_StubCrawler({GAME: DEGRADED_DETAIL}))

        run = _runs(factory)[GAME]
        assert run.status == "partial"
        assert run.records_written == 1
        assert len(_letters(factory)) == 1

    def test_replay_asks_for_full_detail_explicitly(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """A letter is queued because the detail was incomplete.

        If replay inherited a lighter default it would fetch the same reduced page
        and land back on the same partial, so the requirement is stated at the call
        rather than left to whatever the crawler's default happens to be.
        """
        from src.repositories.crawl_execution_repository import CrawlRunSpec
        from src.services.game_collection_service import GameCollectionConfig, replay_single_game_detail

        crawler = _StubCrawler({GAME: FULL_DETAIL}, lightweight=True)
        spec = CrawlRunSpec(
            crawler="game_detail",
            target_type="game",
            target_id=GAME,
            game_id=GAME,
            run_id="replay-full-1",
        )
        with (
            patch("src.services.game_collection_service.save_game_detail", return_value=True),
            patch(
                "src.services.game_collection_service._detail_payload_failure_reason",
                side_effect=_only_a_missing_payload_fails,
            ),
        ):
            asyncio.run(
                replay_single_game_detail(
                    GAME,
                    spec,
                    detail_crawler=crawler,
                    config=GameCollectionConfig(),
                ),
            )

        assert crawler.lightweight_requests == [False]
        assert _NOT_REQUESTED not in crawler.lightweight_requests


class TestAStoredPartialIsStillUnfinished:
    def test_a_partial_is_written_and_queued_at_the_validate_stage(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """The two facts coexist: data worth keeping, completeness unmet.

        Asserted together on purpose. Checking either alone lets a later change
        drop one of them without failing.
        """
        _collect(_StubCrawler({GAME: DEGRADED_DETAIL}))

        run = _runs(factory)[GAME]
        assert run.status == "partial"
        assert run.records_written == 1

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].status == "pending"
        assert letters[0].error_code == FailureCode.VALIDATION_QUALITY.value
        assert letters[0].failure_stage == "validate"
        assert letters[0].target_id == GAME
        assert letters[0].original_run_id == run.run_id

    def test_a_fetch_failure_is_queued_under_its_own_taxonomy(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """A game that never reached the write step is queued as the failure it was."""
        _collect(_StubCrawler({GAME: None}))

        run = _runs(factory)[GAME]
        assert run.status == "failed"
        assert run.records_written == 0

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].error_code == run.error_code
        assert letters[0].original_run_id == run.run_id

    def test_a_write_that_never_landed_is_queued_as_a_persistence_failure(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """The fetch worked, so the letter must blame the write, not the fetch.

        The stage is read off the code rather than assumed, so this also pins the
        rule that the two can never be told apart by inference.
        """
        from src.crawlers.failure_taxonomy import stage_for_code

        _collect(_StubCrawler({GAME: FULL_DETAIL}), save_error=TimeoutError("write timed out"))

        run = _runs(factory)[GAME]
        assert run.status == "failed"
        assert run.error_code == FailureCode.PERSIST_TIMEOUT.value

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].error_code == FailureCode.PERSIST_TIMEOUT.value
        assert letters[0].failure_stage == stage_for_code(FailureCode.PERSIST_TIMEOUT.value).value


class TestReplayingMovesTheExistingLetter:
    def _queue_a_partial(self, factory: sessionmaker) -> CrawlDeadLetter:
        _collect(_StubCrawler({GAME: DEGRADED_DETAIL}))
        return _letters(factory)[0]

    def test_a_complete_replay_resolves_the_original_letter(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """One incident, one letter, closed by the retry that fixed it."""
        original = self._queue_a_partial(factory)
        run_a = original.original_run_id

        with patch(
            "src.services.crawl_replay_dispatcher.GameDetailCrawler",
            return_value=_StubCrawler({GAME: FULL_DETAIL}),
        ):
            _retry(factory, original.dlq_id)

        assert len(_letters(factory)) == 1
        settled = _letters(factory)[0]
        assert settled.dlq_id == original.dlq_id
        assert settled.status == "resolved"
        assert settled.original_run_id == run_a
        assert settled.replay_run_id != run_a

    def test_an_incomplete_replay_never_raises_a_second_letter(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """Retrying an incomplete game must not grow the queue.

        Each retry gets its own run id, and the unique key is
        (crawler, target_type, target_id, original_run_id). A new enqueue during
        replay would therefore add a row, and the letter that tracks the problem
        would stop being the one that tracks it.
        """
        original = self._queue_a_partial(factory)
        run_a = original.original_run_id
        max_retries = original.max_retries

        replay_run_ids = []
        with patch(
            "src.services.crawl_replay_dispatcher.GameDetailCrawler",
            return_value=_StubCrawler({GAME: DEGRADED_DETAIL}),
        ):
            for _ in range(max_retries):
                _retry(factory, original.dlq_id)
                current = _letters(factory)[0]
                replay_run_ids.append(current.replay_run_id)

        assert len(_letters(factory)) == 1
        settled = _letters(factory)[0]
        assert settled.status == "exhausted"
        assert settled.retry_count == max_retries
        # The original incident is still the one being tracked, and the retries
        # are recorded against it rather than beside it.
        assert settled.original_run_id == run_a
        assert replay_run_ids[-1] == settled.replay_run_id
        assert all(run_id is not None for run_id in replay_run_ids)

    def test_a_partial_replay_keeps_the_letter_unresolved(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """The retry ran and stored data, and the problem is still true.

        Reporting this as a success would close an incident that has not been
        fixed, and the next attempt would never be scheduled.
        """
        original = self._queue_a_partial(factory)

        with patch(
            "src.services.crawl_replay_dispatcher.GameDetailCrawler",
            return_value=_StubCrawler({GAME: DEGRADED_DETAIL}),
        ):
            _retry(factory, original.dlq_id)

        settled = _letters(factory)[0]
        assert settled.status == "pending"
        assert settled.retry_count == 1

    def test_the_replay_runs_under_the_letter_identity(
        self,
        factory: sessionmaker,
        wired: None,
    ) -> None:
        """The retry must be traceable back to the run that failed.

        A fresh id would make the recovery look like an unrelated crawl, and the
        lineage from incident to fix would be lost.
        """
        from src.models.crawl_execution import RUN_STATUS_SUCCESS

        original = self._queue_a_partial(factory)

        with patch(
            "src.services.crawl_replay_dispatcher.GameDetailCrawler",
            return_value=_StubCrawler({GAME: FULL_DETAIL}),
        ):
            _retry(factory, original.dlq_id)

        settled = _letters(factory)[0]
        replay_run = _run_by_id(factory, settled.replay_run_id)

        assert replay_run is not None
        assert replay_run.run_id != original.original_run_id
        assert replay_run.replay_of_run_id == original.original_run_id
        assert replay_run.parent_run_id == original.original_run_id
        assert replay_run.status == RUN_STATUS_SUCCESS
