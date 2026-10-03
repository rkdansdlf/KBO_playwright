"""The whole relay reliability chain, end to end, over one game.

Every other relay test stops somewhere. This one starts with a real
`CrawlerHttpClient` classification of a real HTTP response and follows it all the
way through to a queued letter, and then through a replay that resolves it. Only
the socket is replaced: the client's own status handling, the crawler's inning
loop, the run ledger, the queue and the replay dispatcher are all real, because
the bugs this catches live in the seams between them rather than inside any one.

The chain under test:

    CrawlerHttpClient -> RelayAttempt -> RelayStatus -> CrawlExecutionRun
                      -> CrawlDeadLetter -> replay RUN-B -> retry policy
"""

from __future__ import annotations

import asyncio
import socket
from contextlib import asynccontextmanager, contextmanager
from typing import TYPE_CHECKING, Any
from unittest.mock import patch

import httpx
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.crawlers.failure_taxonomy import FailureCode, stage_for_code
from src.crawlers.http_client import CrawlerHttpClient
from src.crawlers.relay_crawler import RelayCrawler
from src.crawlers.relay_outcome import RelayStatus
from src.models.crawl_dead_letter import CrawlDeadLetter
from src.models.crawl_execution import CrawlExecutionRun

if TYPE_CHECKING:
    from collections.abc import Iterator

GAME = "20250501LGOB0"
ROWS = [{"inning": 1}]


# ── scripted transport ────────────────────────────────────────────────────


def _relay_envelope(inning: int = 1) -> dict[str, Any]:
    """One inning carrying a titled segment.

    The title matters: the real parser turns a segment into a play-by-play row
    from its `title`, and a segment without one is skipped. A payload the parser
    discards would come back as `relay_empty`, which is a failure and not the
    outcome any of these tests are about.
    """
    half = "후" if inning % 2 == 0 else "초"
    return {
        "result": {
            "textRelayData": {
                "textRelays": [
                    {"title": f"{inning}회{half}", "textOptions": [{"text": "타이타흘 3루타"}]},
                ],
            },
        },
    }


def _empty_envelope() -> dict[str, Any]:
    return {"result": {"textRelayData": {"textRelays": []}}}


def _transport(script: dict[int, Any], *, default: Any = None) -> httpx.MockTransport:
    """Answer relay requests per inning.

    A value that is an `Exception` instance is raised instead of returned, which
    is how MockTransport produces a timeout without a socket.
    """

    def handle(request: httpx.Request) -> httpx.Response:
        raw_inning = request.url.params.get("inning")
        inning = int(raw_inning) if raw_inning and raw_inning.isdigit() else 0
        answer = script.get(inning, default)
        if answer is None:
            answer = _relay_envelope(inning) if inning else _empty_envelope()
        if isinstance(answer, Exception):
            raise answer
        if isinstance(answer, httpx.Response):
            return answer
        status, body = answer
        return httpx.Response(status, json=body)

    return httpx.MockTransport(handle)


@asynccontextmanager
async def _mocked_client(transport: httpx.MockTransport) -> Any:
    async with httpx.AsyncClient(transport=transport) as client:
        yield client


def _stub(crawler: RelayCrawler) -> RelayCrawler:
    """Remove the two lookups that would reach outside this test.

    The Naver ID is already the KBO ID here, so the mapping is the identity.
    The schedule fallback answers "not carried" directly instead of searching a
    run of dates: when a relay result comes back empty the crawler assumes it
    asked with the wrong ID and goes looking, which in this file would follow
    every empty relay into requests nobody scripted and then, once those fail,
    into a circuit breaker cooldown costing a minute per test. The claim the
    fallback exists to establish -- "the schedule does not carry it either" -- is
    what is asserted by stating it.
    """
    crawler._map_to_naver_id = lambda game_id: game_id  # type: ignore[method-assign]
    crawler._resolve_game_metadata = lambda game_id, stadium, game_time: (stadium, game_time)  # type: ignore[method-assign]

    async def _no_schedule_lookup(game_id: str, **_kwargs: Any) -> None:
        return None

    crawler._resolve_naver_game_id = _no_schedule_lookup  # type: ignore[method-assign]
    return crawler


class _StubbedRelayCrawler(RelayCrawler):
    """A real crawler with its external lookups stubbed out.

    Subclassing rather than substituting a factory function matters: module code
    reaches for this crawler's static parsing helpers on the class itself, so
    replacing the class with a function would break the very parser the canary
    is here to exercise.
    """

    def __init__(self) -> None:
        """Initialize the real crawler, then stub its lookups."""
        super().__init__()
        _stub(self)


# ── fixtures ───────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _isolated_transport_state(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Keep the canary about one game's chain, not about process-global state.

    Three things would otherwise decide the result, and none of them are the
    chain under test:

    * The circuit breaker is a process-global registry keyed by transport name,
      so a test that trips it makes every later test see "circuit OPEN" instead
      of the classification it is asserting.
    * Throttling sleeps. `record_rate_limit` multiplies the delay by 1.5 on
      every retry, so a nine-inning game with three timed-out attempts spends
      minutes asleep. Throttling has its own tests.
    * Every request is SSRF-checked, and that check resolves the host over real
      DNS. On a machine with slow egress one lookup costs seconds, and a retried
      request pays it again. Pinning resolution to a public address keeps the
      check itself running -- only the lookup goes away.
    """
    from src.crawlers.circuit_breaker import circuit_registry
    from src.crawlers.resilience import AdaptiveRateLimiter
    from src.crawlers import http_client as hc

    circuit_registry.reset_all()

    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("8.8.8.8", 443))],
    )

    class _NoWait:
        async def wait(self, host: str) -> float:
            return 0.0

    async def _no_delay(self: AdaptiveRateLimiter) -> float:
        return 0.0

    def _no_backoff(self: CrawlerHttpClient, attempt: int) -> float:
        # The retry loop's own backoff is separate from the throttle, and it
        # doubles: three attempts of a drifted response is 6s of nothing.
        return 0.0

    original_throttle = hc.throttle
    original_acquire = AdaptiveRateLimiter.acquire
    original_backoff = CrawlerHttpClient._backoff
    hc.throttle = _NoWait()
    AdaptiveRateLimiter.acquire = _no_delay
    CrawlerHttpClient._backoff = _no_backoff
    try:
        yield
    finally:
        hc.throttle = original_throttle
        AdaptiveRateLimiter.acquire = original_acquire
        CrawlerHttpClient._backoff = original_backoff  # type: ignore[method-assign]
        circuit_registry.reset_all()


@pytest.fixture
def factory() -> sessionmaker:
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
        "src.services.relay_runs",
        "src.services.game_collection_service",
        "src.services.crawl_dead_letter_service",
        "src.services.crawl_replay_dispatcher",
    ):
        monkeypatch.setattr(f"{module}.SessionLocal", factory)


def _runs(factory: sessionmaker) -> list[CrawlExecutionRun]:
    with factory() as session:
        return list(session.query(CrawlExecutionRun).order_by(CrawlExecutionRun.started_at).all())


def _letters(factory: sessionmaker) -> list[CrawlDeadLetter]:
    with factory() as session:
        return list(session.query(CrawlDeadLetter).order_by(CrawlDeadLetter.id).all())


@contextmanager
def _scripted(script: dict[int, Any], *, default: Any = None, save_rows: int | None = 2, save_error: Any = None) -> Any:
    """Run something against a scripted relay endpoint.

    The crawler's class symbol is patched as well as the socket: the replay
    dispatcher builds its own `RelayCrawler()`, so stubbing only the instance
    would leave half the canary running against the schedule API.
    """
    transport = _transport(script, default=default)
    save_patch = (
        patch("src.services.game_collection_service.save_relay_data", side_effect=save_error)
        if save_error is not None
        else patch("src.services.game_collection_service.save_relay_data", return_value=save_rows)
    )
    with (
        patch("src.crawlers.http_client.CrawlerHttpClient._client", lambda self: _mocked_client(transport)),
        patch("src.crawlers.relay_crawler.RelayCrawler", _StubbedRelayCrawler),
        save_patch,
    ):
        yield


def _collect(
    script: dict[int, Any],
    *,
    default: Any = None,
    save_rows: int | None = 2,
    save_error: Any = None,
) -> dict[str, Any]:
    """Drive one game through the whole chain and report what it produced."""
    from src.services.game_collection_service import (
        ExistingGameData,
        GameCollectionConfig,
        GameCollectionItemResult,
        GameCollectionResult,
        GameCollectionTarget,
        _collect_relay_phase,
    )

    result = GameCollectionResult()
    result.items = {GAME: GameCollectionItemResult(game_id=GAME, game_date="20250501")}

    cfg = GameCollectionConfig()
    # The relay phase normally waits for the detail phase to run first. This
    # canary is about relay's own chain, so it does not depend on that.
    cfg.relay_requires_detail = False
    cfg.log = lambda *_args, **_kwargs: None  # type: ignore[method-assign]

    ctx = type(
        "_Ctx",
        (),
        {
            "cfg": cfg,
            "relay_crawler": _StubbedRelayCrawler(),
            "contract": __import__(
                "src.services.game_write_contract", fromlist=["GameWriteContract"]
            ).GameWriteContract(),
            "result": result,
        },
    )()

    with _scripted(script, default=default, save_rows=save_rows, save_error=save_error):
        asyncio.run(
            _collect_relay_phase(
                [GameCollectionTarget(game_id=GAME, game_date="20250501")],
                {GAME: ExistingGameData(has_relay=False)},
                set(),
                ctx,
            )
        )
    return {"result": result, "item": result.items[GAME]}


def _retry(
    factory: sessionmaker,
    script: dict[int, Any],
    *,
    default: Any = None,
    save_rows: int | None = 2,
) -> Any:
    """Re-queue the one letter a failure left behind and replay it.

    This is the operator's path end to end: `retry_dead_letter` opens RUN-B,
    the dispatcher re-fetches, and `finalize_retry` reads the verdict back off
    the stored run. A new letter must not appear, because the retry policy owns
    the attempt count -- an operator who retries nine times should leave one
    incident behind, not nine.
    """
    from src.services.crawl_dead_letter_service import retry_dead_letter
    from src.services.crawl_replay_dispatcher import build_default_dispatcher

    letter = _letters(factory)[0]
    with _scripted(script, default=default, save_rows=save_rows):
        return retry_dead_letter(letter.dlq_id, build_default_dispatcher(), session_factory=factory)


# ── the chain ──────────────────────────────────────────────────────────────


class TestTheChainEndToEnd:
    """One assertion per outcome the plan requires the chain to distinguish."""

    def test_a_complete_game_succeeds_and_queues_nothing(self, factory: sessionmaker, wired: None) -> None:
        outcome = _collect({1: (200, _relay_envelope(1)), 2: (200, _empty_envelope())})

        assert outcome["item"].relay_status == "saved"
        runs = _runs(factory)
        assert len(runs) == 1
        assert runs[0].status == "success"
        assert runs[0].records_written == 2
        assert _letters(factory) == []

    def test_a_write_that_stores_nothing_is_a_quality_failure(self, factory: sessionmaker, wired: None) -> None:
        """The write ran and declined the payload, so this is a data decision.

        Distinct from a database refusal, which is a persistence failure. Both
        arrive as a zero from the same function, and conflating them sends an
        operator to the payload when the connection is what failed.
        """
        outcome = _collect({1: (200, _relay_envelope(1)), 2: (200, _empty_envelope())}, save_rows=0)

        assert outcome["item"].relay_status == "save_failed"
        runs = _runs(factory)
        assert runs[0].status == "failed"
        assert runs[0].records_written == 0

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].error_code == FailureCode.VALIDATION_QUALITY.value
        assert letters[0].failure_stage == "validate"

    def test_a_source_that_carries_nothing_ends_the_incident(self, factory: sessionmaker, wired: None) -> None:
        """A 404 from the relay endpoint is an absence, not a failure to fetch."""
        outcome = _collect({1: (404, {})}, save_rows=0)

        assert outcome["item"].relay_status == "empty"
        runs = _runs(factory)
        assert runs[0].status == "success"
        assert _letters(factory) == []

    def test_eight_innings_then_a_lost_ninth_is_partial_and_queued(self, factory: sessionmaker, wired: None) -> None:
        """The rows arrived, so they are stored; the ninth never came, so it is queued."""
        script: dict[int, Any] = {i: (200, _relay_envelope(i)) for i in range(1, 9)}
        script[9] = httpx.ReadTimeout("timed out")

        outcome = _collect(script)

        assert outcome["item"].relay_status == "partial"
        runs = _runs(factory)
        assert runs[0].status == "partial"
        assert runs[0].records_written == 2

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].status == "pending"
        assert letters[0].error_code == FailureCode.FETCH_TIMEOUT.value

    def test_a_first_inning_timeout_fails_the_game_and_queues_it(self, factory: sessionmaker, wired: None) -> None:
        outcome = _collect({1: httpx.ReadTimeout("timed out")})

        assert outcome["item"].relay_status == "failed"
        runs = _runs(factory)
        assert runs[0].status == "failed"
        assert runs[0].records_written == 0

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].status == "pending"
        assert letters[0].error_code == FailureCode.FETCH_TIMEOUT.value

    def test_a_blocked_crawl_is_recorded_but_never_retried(self, factory: sessionmaker, wired: None) -> None:
        outcome = _collect({1: (403, {})})

        assert outcome["item"].relay_status == "failed"
        assert _runs(factory)[0].status == "failed"

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].error_code == FailureCode.FETCH_BLOCKED.value
        assert letters[0].status == "ignored"

    def test_a_page_where_json_was_expected_is_drift_not_an_outage(self, factory: sessionmaker, wired: None) -> None:
        """A 200 that is not the payload we asked for is our problem, not the site's.

        Retrying it returns the same unusable body, so it is recorded and left
        alone -- but recorded, because dropping it would be indistinguishable
        from never having run.
        """
        html = httpx.Response(200, text="<html><body>maintenance</body></html>", headers={"Content-Type": "text/html"})
        outcome = _collect({1: html}, default=html)

        assert outcome["item"].relay_status == "failed"
        runs = _runs(factory)
        assert runs[0].status == "failed"

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].error_code == FailureCode.PARSE_INVALID_FORMAT.value
        assert letters[0].status == "ignored"

    def test_a_write_that_times_out_is_a_persistence_failure(self, factory: sessionmaker, wired: None) -> None:
        outcome = _collect(
            {1: (200, _relay_envelope(1)), 2: (200, _empty_envelope())},
            save_error=TimeoutError("write timed out"),
        )

        assert outcome["item"].relay_status == "save_failed"
        runs = _runs(factory)
        assert runs[0].status == "failed"
        assert runs[0].records_written == 0

        letters = _letters(factory)
        assert letters[0].error_code == FailureCode.PERSIST_TIMEOUT.value
        assert letters[0].failure_stage == "persist"

    def test_a_refused_connection_is_named_as_a_connection_failure(self, factory: sessionmaker, wired: None) -> None:
        from sqlalchemy.exc import OperationalError

        outcome = _collect(
            {1: (200, _relay_envelope(1)), 2: (200, _empty_envelope())},
            save_error=OperationalError("SELECT 1", {}, Exception("refused")),
        )

        letters = _letters(factory)
        assert letters[0].error_code == FailureCode.PERSIST_CONNECTION.value
        assert outcome["item"].relay_status == "save_failed"


class TestTheOperatorGetsTheSecondHalf:
    """What an operator does with the letter a failure leaves behind.

    The first half of this file proves an incident is recorded. These prove it
    is a *replayable* incident: the retry re-fetches, the verdict comes off the
    stored run, and the incident either closes or stays open without ever
    multiplying.
    """

    def test_a_replay_that_finishes_the_game_closes_the_incident(self, factory: sessionmaker, wired: None) -> None:
        _collect({1: httpx.ReadTimeout("timed out")})

        result = _retry(factory, {1: (200, _relay_envelope(1)), 2: (200, _empty_envelope())})

        assert result.success is True
        assert result.status == "resolved"

        letters = _letters(factory)
        assert len(letters) == 1, "a retry must not open a second incident"
        assert letters[0].status == "resolved"
        assert letters[0].resolved_at is not None

    def test_a_replay_that_fails_again_leaves_the_incident_open(self, factory: sessionmaker, wired: None) -> None:
        """Still broken is still broken. Closing this would retire a true claim."""
        _collect({1: httpx.ReadTimeout("timed out")})

        result = _retry(factory, {1: httpx.ReadTimeout("timed out")})

        assert result.success is False
        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].status in {"retrying", "pending"}
        assert letters[0].resolved_at is None

    def test_a_replay_that_only_half_finishes_is_not_a_resolution(self, factory: sessionmaker, wired: None) -> None:
        """Eight innings stored still means the ninth is missing.

        This is the case the run's `partial` status exists for, and it is the
        one an operator is most likely to be fooled by: the rows are in the
        database, so the game looks collected.
        """
        _collect({1: httpx.ReadTimeout("timed out")})

        script: dict[int, Any] = {i: (200, _relay_envelope(i)) for i in range(1, 9)}
        script[9] = httpx.ReadTimeout("timed out")
        result = _retry(factory, script)

        assert result.success is False

        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].status in {"retrying", "pending"}

        # RUN-B is the authority, and it says partial.
        runs = _runs(factory)
        replay_run = next(r for r in runs if r.run_id == result.replay_run_id)
        assert replay_run.status == "partial"
        assert replay_run.records_written == 2

    def test_the_retry_count_moves_on_the_one_letter(self, factory: sessionmaker, wired: None) -> None:
        """Backoff has to know how many attempts already failed."""
        _collect({1: httpx.ReadTimeout("timed out")})
        before = _letters(factory)[0].retry_count

        _retry(factory, {1: httpx.ReadTimeout("timed out")})

        assert _letters(factory)[0].retry_count == before + 1

    def test_a_replay_that_finds_nothing_still_closes_the_incident(self, factory: sessionmaker, wired: None) -> None:
        """The source having no game is an answer, so the incident is over.

        A 404 is not a failure to fetch. Reading it as one would leave an
        incident open forever for a game that never existed at the source.
        """
        _collect({1: httpx.ReadTimeout("timed out")})

        result = _retry(factory, {1: (404, {})}, save_rows=0)

        assert result.success is True
        letters = _letters(factory)
        assert len(letters) == 1
        assert letters[0].status == "resolved"

    def test_the_replay_records_its_own_run_against_the_original(self, factory: sessionmaker, wired: None) -> None:
        """RUN-B has to point back at RUN-A, or the incident loses its history."""
        _collect({1: httpx.ReadTimeout("timed out")})
        original = _runs(factory)[0]

        result = _retry(factory, {1: (200, _relay_envelope(1)), 2: (200, _empty_envelope())})

        replay_run = next(r for r in _runs(factory) if r.run_id == result.replay_run_id)
        assert replay_run.run_id != original.run_id
        assert replay_run.replay_of_run_id == original.run_id
        assert replay_run.parent_run_id == original.run_id


class TestTheCodeIsTheSameAtEveryLayer:
    """A run, a queue entry and a Prometheus sample that disagree are unusable.

    Each layer classifies from its own input, so the only thing keeping them
    aligned is that they read the same code off the attempt.
    """

    def test_one_failure_carries_one_code_through_the_whole_chain(self, factory: sessionmaker, wired: None) -> None:
        _collect({1: httpx.ReadTimeout("timed out")})

        run = _runs(factory)[0]
        letter = _letters(factory)[0]

        assert run.error_code == FailureCode.FETCH_TIMEOUT.value
        assert letter.error_code == run.error_code
        assert letter.failure_stage == stage_for_code(letter.error_code).value

    def test_the_failure_metric_agrees(self, factory: sessionmaker, wired: None) -> None:
        from prometheus_client import REGISTRY

        from src.monitoring.crawler_metrics import reset_initialized_crawlers

        reset_initialized_crawlers()
        _collect({1: httpx.ReadTimeout("timed out")})

        runs = REGISTRY.get_sample_value("kbo_crawl_runs_total", {"crawler": "relay", "status": "failed"})
        failures = REGISTRY.get_sample_value(
            "kbo_crawl_failures_total",
            {"crawler": "relay", "error_code": FailureCode.FETCH_TIMEOUT.value, "failure_stage": "fetch"},
        )
        assert runs and runs >= 1
        assert failures and failures >= 1
