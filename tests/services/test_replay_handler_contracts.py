"""What every replay handler must do, regardless of which crawler it replays.

The handlers look like five near-identical functions and that is the point: the
contracts they share are the contracts the retry policy depends on. A replay
that enqueues its own dead letter, or forgets to save, or reports success from
memory instead of from the stored run, does not fail loudly -- it quietly turns
one incident into two, or resolves an incident that is still true.

These assertions are about the call, not the crawl: each crawler is replaced so
that what gets checked is exactly the arguments the handler chose to make.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, patch

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

RUN_A = "RUN-A1"
RUN_B = "RUN-B1"
TARGET_TYPE = "unit"
STAGE = "fetch"

#: The name each crawler class is bound to in the dispatcher's module namespace.
#: The dispatcher imports the classes at module level, so patching the crawler's
#: own module would leave the very reference under test untouched.
SYMBOLS: dict[str, str] = {
    "food": "FoodCrawler",
    "parking": "ParkingCrawler",
    "team_history": "TeamHistoryCrawler",
    "kbo_event": "KboEventCrawler",
    "player_movement": "PlayerMovementCrawler",
    "awards": "AwardCrawler",
}

#: The five handlers this change registered, with the unit each letter names.
PHASE_I: tuple[tuple[str, str], ...] = (
    ("food", "OB"),
    ("parking", "OB"),
    ("team_history", "history"),
    ("kbo_event", "main"),
    ("player_movement", "2026"),
)


def _letter(**overrides: Any) -> CrawlDeadLetter:
    """Build a detached dead letter, as the dispatcher receives one."""
    fields: dict[str, Any] = {
        "dlq_id": "DLQ-1",
        "original_run_id": RUN_A,
        "crawler": "relay",
        "target_type": TARGET_TYPE,
        "target_id": "20250501LGOB0",
        "failure_stage": STAGE,
        "error_code": "FETCH_TIMEOUT",
        "error_message": "timed out",
    }
    fields.update(overrides)
    return CrawlDeadLetter(**fields)


def _letter_for(crawler: str, target_id: str | None) -> CrawlDeadLetter:
    """Build a letter for one of the Phase I crawlers."""
    return _letter(
        crawler=crawler,
        target_id=target_id,
        season=2026,
        source_url="https://www.koreabaseball.com/one",
    )


@contextmanager
def _crawler_stub(crawler: str) -> Iterator[Any]:
    """Replace a crawler class with one whose ``run`` records its arguments."""
    with patch.object(dispatcher_mod, SYMBOLS[crawler]) as cls:
        cls.return_value.run = AsyncMock(return_value=[])
        cls.return_value.close = AsyncMock()
        yield cls


@pytest.fixture
def factory() -> Iterator[sessionmaker]:
    """Provide an in-memory ledger the handlers read their verdict from."""
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    CrawlExecutionRun.__table__.create(engine)
    yield sessionmaker(bind=engine, expire_on_commit=False)
    engine.dispose()


@pytest.fixture(autouse=True)
def wired(monkeypatch: pytest.MonkeyPatch, factory: sessionmaker) -> None:
    """Point the verdict read at the test ledger instead of a real database."""
    monkeypatch.setattr(dispatcher_mod, "SessionLocal", factory)


def _record_run(factory: sessionmaker, *, status: str, run_id: str = RUN_B) -> None:
    with factory() as session:
        session.add(
            CrawlExecutionRun(
                run_id=run_id,
                crawler="relay",
                target_type=TARGET_TYPE,
                target_id="20250501LGOB0",
                status=status,
                started_at=datetime.now(UTC).replace(tzinfo=None),
            ),
        )
        session.commit()


def _replay(crawler: str, target_id: str | None, run_id: str = RUN_B) -> Any:
    """Run one handler with its crawler stubbed, and return what it reported."""
    handler = build_default_dispatcher()._handlers[crawler]
    with _crawler_stub(crawler) as cls:
        outcome = handler(_letter_for(crawler, target_id), run_id)
    return outcome, cls


# ── the contracts ──────────────────────────────────────────────────────────


class TestARetryNeverQueuesAnotherLetter:
    """The retry policy owns the attempt count, not the replay.

    Enqueuing a letter here would double the incident on every attempt, so an
    operator retrying nine times would be counting letters instead of failures.
    """

    @pytest.mark.parametrize(("crawler", "target_id"), PHASE_I)
    def test_the_enqueue_flag_is_off(self, factory: sessionmaker, crawler: str, target_id: str) -> None:
        _record_run(factory, status="success")
        _, cls = _replay(crawler, target_id)

        assert cls.return_value.run.await_args.kwargs["record_dead_letters"] is False

    def test_the_earlier_handlers_obey_it_too(self, factory: sessionmaker) -> None:
        """The refactor into shared helpers did not soften the Phase B handlers."""
        _record_run(factory, status="success")
        _, cls = _replay("awards", "wikipedia")

        assert cls.return_value.run.await_args.kwargs["record_dead_letters"] is False


class TestAReplayActuallyStoresWhatItFetches:
    """``save`` defaults to False on most of these crawlers.

    Inheriting that default would produce a replay that fetched the page, stored
    nothing, and reported success -- the worst possible outcome, because the
    incident would close over an unchanged database.
    """

    @pytest.mark.parametrize(("crawler", "target_id"), [row for row in PHASE_I if row[0] != "player_movement"])
    def test_the_save_flag_is_pinned_on(self, factory: sessionmaker, crawler: str, target_id: str) -> None:
        _record_run(factory, status="success")
        _, cls = _replay(crawler, target_id)

        assert cls.return_value.run.await_args.kwargs["save"] is True

    def test_a_movement_replay_recaptures_its_source(self, factory: sessionmaker) -> None:
        """The crawler persists its snapshots from inside `run`, and its rows outside."""
        _record_run(factory, status="success")
        _, cls = _replay("player_movement", "2026")

        assert cls.return_value.run.await_args.kwargs["save_snapshots"] is True


class TestAReplayAimsAtTheFailingUnit:
    """The letter names one unit. Re-crawling more than that spends goodwill."""

    @pytest.mark.parametrize("crawler", ["food", "parking"])
    def test_a_stadium_replay_asks_for_one_team(self, factory: sessionmaker, crawler: str) -> None:
        _record_run(factory, status="success")
        _, cls = _replay(crawler, "OB")

        assert cls.return_value.run.await_args.kwargs["team_filter"] == "OB"

    def test_an_event_replay_asks_for_one_page(self, factory: sessionmaker) -> None:
        """Handing the letter's URL back as `base_url` narrows the sweep to it."""
        _record_run(factory, status="success")
        _, cls = _replay("kbo_event", "main")

        assert cls.call_args.kwargs["base_url"] == "https://www.koreabaseball.com/one"

    def test_a_year_range_is_replayed_as_that_range(self, factory: sessionmaker) -> None:
        _record_run(factory, status="success")
        _, cls = _replay("player_movement", "2023-2024")

        assert cls.return_value.run.await_args.args == (2023, 2024)

    def test_a_single_year_is_not_treated_as_a_range(self, factory: sessionmaker) -> None:
        _record_run(factory, status="success")
        _, cls = _replay("player_movement", "2026")

        assert cls.return_value.run.await_args.args == (2026, 2026)


class TestTheStoredRunIsTheVerdict:
    """What the database holds is the claim; what the crawl returned is not."""

    @pytest.mark.parametrize(
        ("status", "success"),
        [("success", True), ("partial", False), ("failed", False), ("source_limited", False)],
    )
    def test_the_run_status_decides(self, factory: sessionmaker, status: str, success: bool) -> None:
        _record_run(factory, status=status)
        outcome, _ = _replay("food", "OB")

        assert outcome.success is success
        assert outcome.status == status

    def test_a_crawl_that_raised_is_left_to_the_caller(self, factory: sessionmaker) -> None:
        """An exception propagates rather than being read as a verdict here.

        A crawler that dies may have died before it closed its run, so the stored
        status is not trustworthy on that path. Deciding it here would guess;
        `retry_dead_letter` is the layer that owns turning an exception into an
        outcome, and it errs towards "not successful" when it does.
        """
        _record_run(factory, status="success")
        handler = build_default_dispatcher()._handlers["food"]
        with _crawler_stub("food") as cls:
            cls.return_value.run = AsyncMock(side_effect=RuntimeError("crashed after the write"))

            with pytest.raises(RuntimeError):
                handler(_letter_for("food", "OB"), RUN_B)

    def test_a_run_that_was_never_recorded_is_not_a_success(self, factory: sessionmaker) -> None:
        outcome, _ = _replay("food", "OB", "RUN-never-written")

        assert outcome.success is False
        assert outcome.status == "missing"


class TestALetterWithoutAUnitIsRefusedRatherThanGuessed:
    """Re-crawling "everything" is not a recovery, it is a different job."""

    @pytest.mark.parametrize("crawler", ["food", "parking"])
    def test_a_stadium_letter_without_a_team_is_refused(self, factory: sessionmaker, crawler: str) -> None:
        outcome, cls = _replay(crawler, None)

        assert outcome.success is False
        assert outcome.status == "unaddressable"
        cls.return_value.run.assert_not_awaited()

    def test_an_event_letter_without_a_url_is_refused(self, factory: sessionmaker) -> None:
        """Without the page URL the only alternative is the whole page sweep."""
        handler = build_default_dispatcher()._handlers["kbo_event"]
        with _crawler_stub("kbo_event") as cls:
            outcome = handler(_letter(crawler="kbo_event", source_url=None), RUN_B)

        assert outcome.status == "unaddressable"
        cls.assert_not_called()

    @pytest.mark.parametrize("target_id", [None, "", "abc", "20xx", "2026-"])
    def test_an_unreadable_year_target_is_refused(self, factory: sessionmaker, target_id: str | None) -> None:
        """Crawling the wrong years would resolve an incident for work never done."""
        outcome, cls = _replay("player_movement", target_id)

        assert outcome.success is False
        assert outcome.status == "unaddressable"
        cls.return_value.run.assert_not_awaited()


class TestEveryReplayRunPointsBackAtTheOriginal:
    """Without the lineage the ledger cannot say which failure a retry addressed."""

    @pytest.mark.parametrize(("crawler", "target_id"), PHASE_I)
    def test_the_spec_carries_the_lineage(self, factory: sessionmaker, crawler: str, target_id: str) -> None:
        _record_run(factory, status="success")
        _, cls = _replay(crawler, target_id)

        spec = cls.return_value.run.await_args.kwargs["run_spec"]
        assert spec.run_id == RUN_B
        assert spec.replay_of_run_id == RUN_A
        assert spec.parent_run_id == RUN_A
        assert spec.season == 2026


class TestTheRegistryMatchesTheHandlers:
    """The matrix advertises replay; the dispatcher has to actually provide it."""

    def test_every_advertised_handler_is_registered(self) -> None:
        from src.crawlers.adoption_matrix import REPLAY_HANDLERS

        dispatcher = build_default_dispatcher()

        for crawler in REPLAY_HANDLERS:
            assert crawler in dispatcher._handlers, f"{crawler} advertises a replay handler but none is registered"

    def test_nothing_is_registered_that_the_matrix_does_not_advertise(self) -> None:
        """Otherwise the matrix would under-report what an operator can retry."""
        from src.crawlers.adoption_matrix import REPLAY_HANDLERS

        dispatcher = build_default_dispatcher()

        assert set(dispatcher._handlers) == set(REPLAY_HANDLERS)


class TestAMovementReplayStoresWhatItFound:
    """The one handler that writes domain rows itself.

    Every other crawler persists from inside `run`. `PlayerMovementCrawler`
    deliberately does not -- its caller owns the write so the daily pipeline can
    choose the order. A replay has no such caller, so without a write here the
    incident would close over rows nobody had refreshed.
    """

    def test_the_rows_are_written(self, factory: sessionmaker) -> None:
        _record_run(factory, status="success")
        handler = build_default_dispatcher()._handlers["player_movement"]
        movements = [{"player_id": 1, "season": 2026}]

        with _crawler_stub("player_movement") as cls, patch.object(dispatcher_mod, "PlayerRepository") as repo_cls:
            cls.return_value.run = AsyncMock(return_value=movements)
            handler(_letter_for("player_movement", "2026"), RUN_B)

        repo_cls.return_value.save_player_movements.assert_called_once_with(movements)

    def test_nothing_crawled_means_nothing_written(self, factory: sessionmaker) -> None:
        """An empty crawl is a legitimate answer; writing it would be a no-op."""
        _record_run(factory, status="success")
        handler = build_default_dispatcher()._handlers["player_movement"]

        with _crawler_stub("player_movement") as cls, patch.object(dispatcher_mod, "PlayerRepository") as repo_cls:
            cls.return_value.run = AsyncMock(return_value=[])
            handler(_letter_for("player_movement", "2026"), RUN_B)

        repo_cls.assert_not_called()

    def test_a_write_that_fails_is_not_reported_as_a_resolution(self, factory: sessionmaker) -> None:
        """Our own write failing has to reach the retry policy, not be swallowed.

        Reporting success here would close the incident while the rows are still
        missing, which is the failure mode the ledger exists to prevent.
        """
        _record_run(factory, status="success")
        handler = build_default_dispatcher()._handlers["player_movement"]

        with _crawler_stub("player_movement") as cls, patch.object(dispatcher_mod, "PlayerRepository") as repo_cls:
            cls.return_value.run = AsyncMock(return_value=[{"player_id": 1}])
            repo_cls.return_value.save_player_movements.side_effect = RuntimeError("write refused")

            with pytest.raises(RuntimeError):
                handler(_letter_for("player_movement", "2026"), RUN_B)
