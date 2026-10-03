"""The relay run ledger and the persistence boundary it depends on.

The distinction these hold is that `save_relay_data` returning 0 means two
different things, and only one of them is a database failure. A ledger that
cannot tell them apart records a broken database as a data-quality rejection,
which sends an operator to look at the payload instead of at the connection.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.relay_outcome import AttemptSeed, InningStop, RelayStatus, build_attempt
from src.models.crawl_execution import CrawlExecutionRun
from src.services.crawl_run_ledger import RunCounts
from src.services.game_collection_service import (
    ExistingGameData,
    GameCollectionItemResult,
    GameCollectionResult,
    GameCollectionTarget,
    _collect_relay_phase,
)
from src.services.relay_runs import RELAY_CRAWLER_NAME, RELAY_TARGET_TYPE, RelayRunLedger, season_of

if TYPE_CHECKING:
    from src.services.relay_runs import RelayOutcome

GAME = "20250501LGOB0"
ROWS = {"events": [{"inning": 1}], "raw_pbp_rows": [{"inning": 1}]}


@pytest.fixture
def factory() -> sessionmaker:
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@pytest.fixture
def wired(factory: sessionmaker, monkeypatch: pytest.MonkeyPatch) -> None:
    for module in (
        "src.services.crawl_run_service",
        "src.services.relay_runs",
        "src.services.game_collection_service",
    ):
        monkeypatch.setattr(f"{module}.SessionLocal", factory)


class _RecordingLedger:
    """Captures transitions without a database."""

    def __init__(self, *, open_fails: bool = False) -> None:
        self.transitions: list[tuple[str, str, str | None]] = []
        self.open_fails = open_fails

    def open_runs(self, game_ids: Any) -> Any:
        from src.services.crawl_run_ledger import RunOpenResult

        if self.open_fails:
            return RunOpenResult(failures=dict.fromkeys(game_ids, (FailureCode.PERSIST_TIMEOUT.value, "cannot open")))
        return RunOpenResult(started={gid: f"run-{gid}" for gid in game_ids})

    def _record(self, status: str, run_id: str, error_code: str | None = None) -> bool:
        self.transitions.append((status, run_id, error_code))
        return True

    def record_success(self, run_id: str, *, counts: RunCounts) -> bool:
        return self._record("success", run_id)

    def record_partial(self, run_id: str, *, error_code: str, error_message: str, counts: RunCounts) -> bool:
        return self._record("partial", run_id, error_code)

    def record_failed(self, run_id: str, *, error_code: str, error_message: str, counts: RunCounts) -> bool:
        return self._record("failed", run_id, error_code)


def _context(attempt: Any, ledger: _RecordingLedger | None = None) -> tuple[Any, _RecordingLedger]:
    """Build the relay processing context with a typed attempt already decided."""
    from src.services.game_collection_service import RelayProcessingContext

    ctx = MagicMock()
    ctx.cfg = MagicMock()
    ctx.cfg.force = False
    ctx.cfg.relay_requires_detail = False
    ctx.cfg.pause_every = None
    ctx.contract = MagicMock()
    ctx.relay_crawler = MagicMock()
    ctx.relay_crawler.crawl_relay_attempt = _async(attempt)
    ctx.result = GameCollectionResult()
    ctx.result.items = {GAME: GameCollectionItemResult(game_id=GAME, game_date="20250501")}
    ctx.result.__class__ = GameCollectionResult
    ctx._ledger = ledger or _RecordingLedger()
    return ctx, ctx._ledger


def _async(value: Any) -> Any:
    from unittest.mock import AsyncMock

    return AsyncMock(return_value=value)


def _attempt(
    payload: dict[str, Any] | None,
    *,
    status: RelayStatus = RelayStatus.SUCCESS,
    reason: str | None = None,
    stop: InningStop = InningStop.COMPLETED,
    innings: int = 1,
) -> Any:
    return build_attempt(
        GAME,
        AttemptSeed(
            status=status,
            result=payload,
            reason=reason,
            stop=stop,
            innings_fetched=innings,
        ),
    )


async def _run(ctx: Any, ledger: Any = None, targets: list[GameCollectionTarget] | None = None) -> Any:

    games = targets or [GameCollectionTarget(game_id=GAME, game_date="20250501")]
    recorder = ledger if ledger is not None else ctx._ledger
    with patch("src.services.game_collection_service._relay_run_ledger", lambda: recorder):
        await _collect_relay_phase(games, {GAME: ExistingGameData(has_relay=False)}, set(), ctx)
    return ctx.result


class TestTheLedgerRecordsRelay:
    def test_it_names_relay_and_games(self, factory: sessionmaker, wired: None) -> None:
        ledger = RelayRunLedger()
        run_id = ledger.open_runs([GAME]).run_id_for(GAME)
        assert run_id is not None

        with factory() as session:
            run = session.query(CrawlExecutionRun).one()
            assert run.crawler == RELAY_CRAWLER_NAME
            assert run.target_type == RELAY_TARGET_TYPE
            assert run.target_id == GAME
            assert run.game_id == GAME
            assert run.season == 2025

    def test_a_partial_records_what_stopped_it(self, factory: sessionmaker, wired: None) -> None:
        ledger = RelayRunLedger()
        run_id = ledger.open_runs([GAME]).run_id_for(GAME)
        assert run_id is not None

        assert (
            ledger.record_partial(
                run_id,
                error_code=FailureCode.FETCH_TIMEOUT.value,
                error_message="relay request failed before a payload was parsed",
                counts=RunCounts(read=1, written=1),
            )
            is True
        )

        with factory() as session:
            run = session.query(CrawlExecutionRun).one()
            assert run.status == "partial"
            assert run.error_code == FailureCode.FETCH_TIMEOUT.value
            # The rows that did arrive are still recorded as written.
            assert run.records_written == 1

    def test_a_game_whose_run_could_not_start_reports_the_cause(self, factory: sessionmaker, wired: None) -> None:
        with patch("src.services.relay_runs.CrawlRunService.start", side_effect=RuntimeError("db down")):
            opened = RelayRunLedger().open_runs([GAME])

        assert opened.started == {}
        code, _ = opened.failure_for(GAME)
        assert code == FailureCode.PERSIST_CONNECTION.value

    def test_season_is_read_off_the_game_id(self) -> None:
        assert season_of(GAME) == 2025
        assert season_of("not-a-game") is None


@pytest.mark.asyncio
class TestTerminalMapping:
    async def test_stored_relay_succeeds_with_rows(self) -> None:
        ctx, ledger = _context(_attempt(ROWS))
        with patch("src.services.game_collection_service.save_relay_data", return_value=2):
            result = await _run(ctx)

        assert result.relay_saved_games == 1
        assert [t[0] for t in ledger.transitions] == ["success"]
        assert ledger.transitions[0][2] is None

    async def test_an_unchanged_payload_succeeds_without_writing(self) -> None:
        """Already stored and identical: a successful no-op, not a failure."""
        ctx, ledger = _context(_attempt({"status": "not_modified"}, status=RelayStatus.NOT_MODIFIED))
        with patch("src.services.game_collection_service.save_relay_data") as save:
            await _run(ctx)

        save.assert_not_called()
        assert [t[0] for t in ledger.transitions] == ["success"]
        assert ctx.result.items[GAME].relay_status == "not_modified"

    async def test_a_genuine_absence_succeeds_and_is_never_queued(self) -> None:
        """The source does not carry it, so nothing failed and nothing is retryable."""
        ctx, ledger = _context(_attempt(None, status=RelayStatus.FAILED, reason="relay_not_found"))
        with patch("src.services.game_collection_service.save_relay_data") as save:
            result = await _run(ctx)

        save.assert_not_called()
        assert [t[0] for t in ledger.transitions] == ["success"]
        assert ctx.result.items[GAME].relay_status == "empty"
        assert result.relay_missing == 1

    async def test_a_mid_game_stop_is_partial_with_rows_and_its_code(self) -> None:
        ctx, ledger = _context(
            _attempt(
                ROWS,
                status=RelayStatus.PARTIAL,
                reason="relay_api_error",
                stop=InningStop.FETCH_FAILED,
                innings=8,
            )
        )
        with patch("src.services.game_collection_service.save_relay_data", return_value=3):
            await _run(ctx)

        status, _run_id, code = ledger.transitions[0]
        assert status == "partial"
        assert code == FailureCode.FETCH_HTTP_ERROR.value
        assert ctx.result.items[GAME].relay_status == "partial"

    async def test_a_failed_fetch_fails_with_its_taxonomy(self) -> None:
        ctx, ledger = _context(_attempt(None, status=RelayStatus.FAILED, reason="relay_api_error"))
        with patch("src.services.game_collection_service.save_relay_data") as save:
            await _run(ctx)

        save.assert_not_called()
        status, _run_id, code = ledger.transitions[0]
        assert status == "failed"
        assert code == FailureCode.FETCH_HTTP_ERROR.value

    async def test_a_blocked_crawl_fails_and_is_not_an_absence(self) -> None:
        """Blocked means the crawl never got to ask, which is not the same as
        the source having nothing.
        """
        ctx, ledger = _context(_attempt(None, status=RelayStatus.FAILED, reason="blocked"))
        await _run(ctx)

        status, _run_id, code = ledger.transitions[0]
        assert status == "failed"
        assert code == FailureCode.FETCH_BLOCKED.value


@pytest.mark.asyncio
class TestTheZeroRowSplit:
    async def test_a_declined_payload_is_a_quality_failure(self) -> None:
        """The write ran and returned nothing to store. That is a data decision."""
        ctx, ledger = _context(_attempt(ROWS))
        with patch("src.services.game_collection_service.save_relay_data", return_value=0):
            await _run(ctx)

        status, _run_id, code = ledger.transitions[0]
        assert status == "failed"
        assert code == FailureCode.VALIDATION_QUALITY.value

    async def test_a_database_refusal_is_a_persistence_failure(self) -> None:
        """The same 0 the caller would see, but the exception was real.

        Without `raise_on_error` the exception never leaves the repository, the
        caller sees 0, and the run is filed as a quality rejection -- pointing an
        operator at the payload when the database is what refused.
        """
        ctx, ledger = _context(_attempt(ROWS))
        boom = OperationalError("SELECT 1", {}, Exception("connection refused"))
        with patch("src.services.game_collection_service.save_relay_data", side_effect=boom):
            await _run(ctx)

        status, _run_id, code = ledger.transitions[0]
        assert status == "failed"
        assert code == FailureCode.PERSIST_CONNECTION.value
        assert code != FailureCode.VALIDATION_QUALITY.value

    async def test_a_timeout_is_named_as_a_timeout(self) -> None:
        ctx, ledger = _context(_attempt(ROWS))
        with patch("src.services.game_collection_service.save_relay_data", side_effect=TimeoutError("write timed out")):
            await _run(ctx)

        _status, _run_id, code = ledger.transitions[0]
        assert code == FailureCode.PERSIST_TIMEOUT.value


@pytest.mark.asyncio
class TestARunIsOpenedBeforeTheFetch:
    async def test_an_unopened_run_stops_the_game_being_fetched(self) -> None:
        """Writing rows with no run, no letter and no metric is what the ledger
        exists to prevent, so the fetch does not happen at all.
        """
        ctx = MagicMock()
        ctx.cfg = MagicMock()
        ctx.cfg.force = False
        ctx.cfg.relay_requires_detail = False
        ctx.cfg.pause_every = None
        ctx.contract = MagicMock()
        crawler = MagicMock()
        crawler.crawl_relay_attempt = _async(_attempt(ROWS))
        ctx.relay_crawler = crawler
        ctx.result = GameCollectionResult()
        ctx.result.items = {GAME: GameCollectionItemResult(game_id=GAME, game_date="20250501")}

        ledger = _RecordingLedger(open_fails=True)
        with (
            patch("src.services.game_collection_service._relay_run_ledger", lambda: ledger),
            patch("src.services.game_collection_service.save_relay_data") as save,
        ):
            result = await _run(ctx, ledger)

        crawler.crawl_relay_attempt.assert_not_called()
        save.assert_not_called()
        assert ledger.transitions == []
        assert result.relay_runs_unopened == 1
        assert ctx.result.items[GAME].relay_status == "run_unopened"

    async def test_the_run_is_named_for_the_game(self) -> None:
        ctx, ledger = _context(_attempt(ROWS))
        with patch("src.services.game_collection_service.save_relay_data", return_value=1):
            await _run(ctx)

        assert ledger.transitions[0][1] == f"run-{GAME}"


@pytest.mark.asyncio
class TestAnUnrecordedTerminalIsCounted:
    async def test_a_refused_finalize_is_visible(self) -> None:
        ctx, ledger = _context(_attempt(ROWS))
        ledger.record_success = MagicMock(return_value=False)
        with patch("src.services.game_collection_service.save_relay_data", return_value=2):
            result = await _run(ctx)

        assert result.relay_runs_unfinalized == 1
        assert result.relay_saved_games == 1

    async def test_a_healthy_run_counts_nothing(self) -> None:
        ctx, _ledger = _context(_attempt(ROWS))
        with patch("src.services.game_collection_service.save_relay_data", return_value=2):
            result = await _run(ctx)

        assert result.relay_runs_unopened == 0
        assert result.relay_runs_unfinalized == 0
