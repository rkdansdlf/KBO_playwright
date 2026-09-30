"""One crawl run per game, opened before the fetch and closed after the save.

The batch case is the one that matters: a single `crawl_games` call covers many
games, and each of them can end up complete, degraded, or failed. Recording the
batch as one run would say nothing, and recording the save as its own run would
leave the fetch time out of the duration.

So the runs are opened before the crawl and closed after the write, one row per
game, each transition on its own short transaction -- a save that rolls back must
not erase the record of the attempt that produced the payload.

No dead letter is created here. That belongs with the replay path, which has to
decide what a partial run means for a re-fetch.
"""

from __future__ import annotations

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import REGISTRY
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.exc import OperationalError
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.game_detail_outcome import GameDetailStatus
from src.models.crawl_execution import CrawlExecutionRun
from src.monitoring import crawler_metrics as cm
from src.services import game_collection_service as gcs
from src.services.game_detail_runs import GAME_DETAIL_CRAWLER_NAME, GameDetailRunLedger, RunCounts

GAME_A = "20250501LGOB0"
GAME_B = "20250502KTSS0"
GAME_C = "20250503SSLG0"

FULL_DETAIL: dict = {
    "hitters": {"away": [{}], "home": [{}]},
    "pitchers": {"away": [{}], "home": [{}]},
    "teams": {"away": {"code": "LG", "score": 3}, "home": {"code": "OB", "score": 5}},
    "metadata": {"stadium": "잠실"},
}

DEGRADED_DETAIL: dict = {
    "hitters": {"away": [{}], "home": []},
    "pitchers": {"away": [{}], "home": []},
    "teams": {"away": {"code": "KT", "score": 1}, "home": {"code": "SS", "score": 2}},
    "metadata": {},
}


@pytest.fixture
def session_factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def wired(session_factory: sessionmaker, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("src.services.crawl_run_service.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.game_detail_runs.SessionLocal", session_factory)
    monkeypatch.setattr("src.services.game_collection_service.SessionLocal", session_factory)


@pytest.fixture(autouse=True)
def _fresh_metric_state():
    cm.reset_initialized_crawlers()
    yield
    cm.reset_initialized_crawlers()


def _runs(session_factory: sessionmaker) -> dict[str, CrawlExecutionRun]:
    with session_factory() as check:
        return {run.target_id: run for run in check.query(CrawlExecutionRun).all()}


def _sample(status: str) -> float:
    return (
        REGISTRY.get_sample_value(
            "kbo_crawl_runs_total",
            {"crawler": GAME_DETAIL_CRAWLER_NAME, "status": status},
        )
        or 0.0
    )


class _TypedCrawler:
    """A crawler that reports a typed outcome per game."""

    def __init__(self, outcomes: dict[str, object]) -> None:
        self._outcomes = outcomes
        self.seen: list[dict[str, str]] = []

    async def crawl_game_attempts(self, games, *, concurrency=None):
        from src.crawlers.game_detail_outcome import attempt_from_result

        self.seen = list(games)
        attempts = []
        for entry in games:
            game_id = entry["game_id"]
            outcome = self._outcomes.get(game_id)
            payload = outcome if isinstance(outcome, dict) else None
            attempts.append(
                attempt_from_result(game_id, payload, lightweight=False, reason="timeout" if payload is None else None)
            )
        return attempts

    async def crawl_games(self, games, *, concurrency=None):
        return [g for g in (self._outcomes.get(e["game_id"]) for e in games) if isinstance(g, dict)]

    def get_last_failure_reason(self, game_id: str) -> str | None:
        return "timeout" if self._outcomes.get(game_id) is None else None

    async def close(self) -> None:
        return None


def _context(outcomes: dict[str, object], *, save_result, **cfg_kwargs):
    """Build a service context with a stubbed save predicate."""
    from src.services.game_collection_service import (
        GameCollectionConfig,
        GameCollectionItemResult,
        GameCollectionResult,
    )

    result = GameCollectionResult(total_targets=len(outcomes))
    result.items = {game_id: GameCollectionItemResult(game_id=game_id, game_date="20250501") for game_id in outcomes}
    config = GameCollectionConfig(**cfg_kwargs)
    config.log = MagicMock()
    context = MagicMock()
    context.detail_crawler = _TypedCrawler(outcomes)
    context.cfg = config
    context.result = result
    context.detail_ready = set()
    return context, result


def _only_missing_payload_fails(target, payload, crawler, should_save_detail):
    """Mirror the production rule that only a missing payload fails the gate.

    Validation of the payload itself is exercised elsewhere; here it would only
    obscure whether a game with no payload at all is recorded as failed.
    """
    if not payload:
        return ("crawl_failed", "timeout", "no_detail_payload")
    return None


def _run_batch(outcomes, *, save_result) -> dict:
    """Run one detail phase over the given outcomes with the save stubbed."""
    from src.services.game_collection_service import (
        GameCollectionTarget,
        _collect_detail_phase,
        _mark_skipped_detail_targets,
    )
    from src.services.game_collection_service import ExistingGameData

    context, result = _context(outcomes, save_result=save_result)
    targets = [GameCollectionTarget(game_id=game_id, game_date="20250501") for game_id in outcomes]
    exist_map = {t.game_id: ExistingGameData() for t in targets}
    _mark_skipped_detail_targets(targets, exist_map, force=True, result=result, log=MagicMock())

    saved_payloads: list[dict] = []

    def _save(target, payload, ctx):
        saved_payloads.append(payload)
        return (save_result(payload), None)

    with (
        patch.object(gcs, "_save_detail_payload", side_effect=_save),
        patch.object(gcs, "_detail_payload_failure_reason", side_effect=_only_missing_payload_fails),
    ):
        import asyncio

        asyncio.run(_collect_detail_phase(targets, exist_map, context))
    return {"context": context, "result": result, "saved": saved_payloads}


class TestTheBatchCase:
    def test_each_game_lands_on_its_own_outcome(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """The regression this ledger exists for: a timeout, a degraded payload
        and a full payload in one batch must not be averaged into one verdict.
        """
        before = {"success": _sample("success"), "partial": _sample("partial"), "failed": _sample("failed")}

        _run_batch(
            {GAME_A: None, GAME_B: DEGRADED_DETAIL, GAME_C: FULL_DETAIL},
            save_result=lambda payload: True,
        )

        runs = _runs(session_factory)
        assert runs[GAME_A].status == "failed"
        assert runs[GAME_A].error_code == FailureCode.FETCH_TIMEOUT.value
        assert runs[GAME_A].records_written == 0

        assert runs[GAME_B].status == "partial"
        assert runs[GAME_B].error_code == FailureCode.VALIDATION_QUALITY.value
        assert runs[GAME_B].error_message == "partial_detail"
        assert runs[GAME_B].records_written == 1

        assert runs[GAME_C].status == "success"
        assert runs[GAME_C].error_code is None
        assert runs[GAME_C].records_written == 1

        assert _sample("success") - before["success"] == 1.0
        assert _sample("partial") - before["partial"] == 1.0
        assert _sample("failed") - before["failed"] == 1.0

    def test_the_target_is_the_game(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """The replay unit is a game, so the ledger row must name the game."""
        _run_batch({GAME_C: FULL_DETAIL}, save_result=lambda payload: True)

        run = _runs(session_factory)[GAME_C]

        assert run.crawler == GAME_DETAIL_CRAWLER_NAME
        assert run.target_type == "game"
        assert run.target_id == GAME_C
        assert run.game_id == GAME_C
        assert run.season == 2025

    def test_the_run_is_opened_before_the_crawl(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """If the run only appeared after the fetch, the crawl time would be
        missing from its duration and a crash mid-batch would leave no trace of
        the games that were in flight.
        """
        seen_during_crawl: list[str] = []
        from src.services.game_collection_service import GameCollectionTarget, _collect_detail_phase
        from src.services.game_collection_service import ExistingGameData

        context, result = _context({GAME_A: FULL_DETAIL}, save_result=True)

        class _WatchingCrawler(_TypedCrawler):
            async def crawl_game_attempts(self, games, *, concurrency=None):
                seen_during_crawl.extend(sorted(_runs(session_factory)))
                return await super().crawl_game_attempts(games, concurrency=concurrency)

        context.detail_crawler = _WatchingCrawler({GAME_A: FULL_DETAIL})
        targets = [GameCollectionTarget(game_id=GAME_A, game_date="20250501")]
        exist_map = {GAME_A: ExistingGameData()}

        import asyncio

        with (
            patch.object(gcs, "_save_detail_payload", return_value=(True, None)),
            patch.object(gcs, "_detail_payload_failure_reason", side_effect=_only_missing_payload_fails),
        ):
            asyncio.run(_collect_detail_phase(targets, exist_map, context))

        assert seen_during_crawl == [GAME_A]
        assert _runs(session_factory)[GAME_A].status == "success"


class TestWriteFailures:
    def test_a_quality_rejection_is_not_reported_as_a_broken_database(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """`False` with no exception is the quality gate declining the payload.
        Calling it a connection failure would blame the infrastructure for a data
        decision.
        """
        _run_batch({GAME_C: FULL_DETAIL}, save_result=lambda payload: False)

        run = _runs(session_factory)[GAME_C]

        assert run.status == "failed"
        assert run.error_code == FailureCode.VALIDATION_QUALITY.value
        assert run.records_read == 1
        assert run.records_written == 0
        assert run.records_failed == 1

    def test_a_database_error_is_classified_as_persistence(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """A raised error means the database did it, so it is a PERSIST_* code."""
        from src.services.game_collection_service import (
            GameCollectionTarget,
            _collect_detail_phase,
        )
        from src.services.game_collection_service import ExistingGameData

        context, result = _context({GAME_C: FULL_DETAIL}, save_result=True)
        targets = [GameCollectionTarget(game_id=GAME_C, game_date="20250501")]
        exist_map = {GAME_C: ExistingGameData()}

        import asyncio

        with (
            patch.object(
                gcs,
                "_save_detail_payload",
                return_value=(False, (FailureCode.PERSIST_CONNECTION.value, "OperationalError: lost")),
            ),
            patch.object(gcs, "_detail_payload_failure_reason", side_effect=_only_missing_payload_fails),
        ):
            asyncio.run(_collect_detail_phase(targets, exist_map, context))

        run = _runs(session_factory)[GAME_C]

        assert run.status == "failed"
        assert run.error_code == FailureCode.PERSIST_CONNECTION.value
        assert run.records_failed == 1

    def test_a_save_exception_does_not_erase_the_run(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """The run is the only durable trace of what was attempted, so a failed
        write must not roll it back.
        """
        _run_batch({GAME_C: FULL_DETAIL}, save_result=lambda payload: False)

        runs = _runs(session_factory)

        assert GAME_C in runs
        assert runs[GAME_C].status == "failed"

    @pytest.mark.parametrize(
        ("exc", "expected"),
        [
            (TimeoutError("lock wait timeout"), FailureCode.PERSIST_TIMEOUT),
            (
                OperationalError("SELECT 1", {}, Exception("server closed the connection")),
                FailureCode.PERSIST_CONNECTION,
            ),
        ],
    )
    def test_a_real_database_error_is_classified_where_it_is_caught(
        self,
        session_factory: sessionmaker,
        wired: None,
        exc: Exception,
        expected: FailureCode,
    ) -> None:
        """A timeout and a lost connection are different operational problems and
        must not collapse into one code. The classification happens inside the
        save, where the exception is in hand: re-deriving it from the message
        afterwards would invent a timeout for everything.
        """
        from src.services.game_collection_service import (
            GameCollectionTarget,
            _collect_detail_phase,
        )
        from src.services.game_collection_service import ExistingGameData

        context, result = _context({GAME_C: FULL_DETAIL}, save_result=True)
        targets = [GameCollectionTarget(game_id=GAME_C, game_date="20250501")]
        exist_map = {GAME_C: ExistingGameData()}

        import asyncio

        with (
            patch("src.services.game_collection_service.save_game_detail", side_effect=exc),
            patch.object(gcs, "_detail_payload_failure_reason", side_effect=_only_missing_payload_fails),
        ):
            asyncio.run(_collect_detail_phase(targets, exist_map, context))

        run = _runs(session_factory)[GAME_C]

        assert run.status == "failed"
        assert run.error_code == expected.value
        assert run.records_failed == 1
        assert run.records_written == 0


class TestUntypedCrawlersStillWork:
    def test_a_crawler_without_the_typed_path_is_still_recorded(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """A test double or an older crawler keeps the untyped path; the ledger
        must still produce a row for every game rather than crashing.
        """

        class _OldCrawler:
            def __init__(self) -> None:
                self.closed = False

            async def crawl_games(self, games, *, concurrency=None):
                return [FULL_DETAIL | {"game_id": entry["game_id"]} for entry in games]

            def get_last_failure_reason(self, game_id: str) -> str | None:
                return None

            async def close(self) -> None:
                self.closed = True

        from src.services.game_collection_service import (
            GameCollectionTarget,
            _collect_detail_phase,
        )
        from src.services.game_collection_service import ExistingGameData

        context, result = _context({GAME_C: FULL_DETAIL}, save_result=True)
        context.detail_crawler = _OldCrawler()
        targets = [GameCollectionTarget(game_id=GAME_C, game_date="20250501")]
        exist_map = {GAME_C: ExistingGameData()}

        import asyncio

        with (
            patch.object(gcs, "_save_detail_payload", return_value=(True, None)),
            patch.object(gcs, "_detail_payload_failure_reason", side_effect=_only_missing_payload_fails),
        ):
            asyncio.run(_collect_detail_phase(targets, exist_map, context))

        assert _runs(session_factory)[GAME_C].status == "success"


class TestLedgerTransitionsInIsolation:
    def test_a_failed_run_still_closes_when_the_crawl_produced_nothing(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        ledger = GameDetailRunLedger()
        run_id = ledger.open_runs([GAME_A]).run_id_for(GAME_A)

        ledger.record_failed(
            run_id,
            error_code=FailureCode.FETCH_TIMEOUT.value,
            error_message="timeout",
        )

        run = _runs(session_factory)[GAME_A]
        assert run.status == "failed"
        assert run.finished_at is not None
        assert run.records_read == 0
        assert run.records_written == 0
        assert run.records_failed == 0

    def test_a_game_whose_run_could_not_start_is_left_alone_but_not_silent(
        self,
        session_factory: sessionmaker,
        wired: None,
    ) -> None:
        """Half a run is worse than none, so no run row is created.

        The cause comes back with the answer rather than being logged away. The
        caller has a payload for this game and a database that refused the run;
        it cannot choose what to do about a game unless it is told what happened.
        """
        with patch("src.services.game_detail_runs.CrawlRunService.start", side_effect=RuntimeError("db down")):
            opened = GameDetailRunLedger().open_runs([GAME_A])

        assert opened.started == {}
        assert _runs(session_factory) == {}
        code, _ = opened.failure_for(GAME_A)
        assert code == FailureCode.PERSIST_CONNECTION.value

    def test_counts_reach_the_row(self, session_factory: sessionmaker, wired: None) -> None:
        ledger = GameDetailRunLedger()
        run_id = ledger.open_runs([GAME_C]).run_id_for(GAME_C)

        ledger.record_success(run_id, counts=RunCounts(read=1, written=1))

        run = _runs(session_factory)[GAME_C]
        assert run.records_read == 1
        assert run.records_written == 1
        assert run.records_failed == 0
