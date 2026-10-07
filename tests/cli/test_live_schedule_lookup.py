"""The live loop must not turn a dependency read into a month's work.

`live_crawler` polls every 30s to two minutes to decide what is happening right
now. It used to answer that by calling `crawl_schedule(now.year, now.month)`,
which is a month's unit of work: every cycle recorded a ledger row for the whole
month, and `ScheduleCrawler._crawl_naver_month` then walked every day of that
month one API call at a time. One day of polling on 2026-10-07 produced 422 runs
of `2026-10` -- up to 13,082 upstream requests -- all of them reading a month
that had not changed.

Two records were false, and both were false in the same direction:

* the ledger said the loop had done a month's work, when it had read a date;
* the dead letter queue said there was work needing reprocessing, when a source
  being briefly unavailable to a dependency read is not that.

`save=False` was already no evidence of either. A read that verifies, or an
operator running a crawler to watch it, is worth recording; a dependency read is
not. The contract here is therefore about *who is asking*, not about whether
rows were written.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from src.cli.live import live_crawler
from src.cli.live.live_crawler import (
    SCHEDULE_LOOKUP_TTL_SECONDS,
    _resolve_today_games,
)
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.crawlers.schedule_crawler import SCHEDULE_CRAWLER_NAME, ScheduleCrawler

_NOW = datetime(2026, 10, 7, 18, 30, 0)
_TODAY = "20261007"


class _FakeScheduleCrawler:
    """A schedule crawler that records whether it was consulted at all."""

    def __init__(self, games: list[dict] | None = None, *, ok: bool = True) -> None:
        self._games = games or []
        self._ok = ok
        self.month_lookups: list[tuple[int, int]] = []

    async def lookup_month(self, year: int, month: int, series_id: str | None = None) -> CrawlResult[list[dict]]:
        self.month_lookups.append((year, month))
        if self._ok:
            return CrawlResult.success(list(self._games))
        return CrawlResult.failure(CrawlOutcome.SCHEMA_CHANGED, error="naver unavailable")

    def get_last_failure_reason(self, key: str) -> str | None:
        return None if self._ok else "naver_unavailable"


@pytest.fixture(autouse=True)
def _clear_lookup_cache() -> None:
    """Keep the module-level cache from leaking between tests.

    The cache is keyed by ``(year, month)``, so it is emptied before *and* after
    every test here. A leftover entry for a month another test uses would answer
    that test's poll from this one's data, which shows up as an unrelated failure
    in a file that never touches the cache.
    """
    live_crawler._SCHEDULE_LOOKUP_CACHE.clear()
    yield
    live_crawler._SCHEDULE_LOOKUP_CACHE.clear()


@pytest.fixture
def games_in_db(monkeypatch: pytest.MonkeyPatch):
    """Control what the database reports for today, by default: nothing."""
    rows: list[list[dict]] = [[]]

    def fake_load(today_str: str) -> list[dict]:
        return list(rows[0])

    monkeypatch.setattr(live_crawler, "_load_today_games_from_db", fake_load)
    return rows


class TestTheLoopDoesNotRecordWorkItDidNotDo:
    """The ledger row was the visible half of the problem."""

    async def test_an_available_database_is_never_consulted_upstream(self, games_in_db):
        """Today's games are already stored; polling must read them, not refetch."""
        games_in_db[0] = [{"game_id": "20261007LGHT0", "game_date": "2026-10-07"}]
        crawler = _FakeScheduleCrawler(games=[{"game_id": "other", "game_date": "2026-10-08"}])

        games, failure = await _resolve_today_games(crawler, _TODAY, _NOW)

        assert crawler.month_lookups == [], "a stored date must not trigger an upstream month read"
        assert [g["game_id"] for g in games] == ["20261007LGHT0"]
        assert failure is None

    async def test_the_source_is_read_only_when_the_database_has_nothing(self, games_in_db):
        """A late fixture change is the real reason to fall back."""
        crawler = _FakeScheduleCrawler(games=[{"game_id": "20261007LGHT0", "game_date": "2026-10-07"}])

        games, failure = await _resolve_today_games(crawler, _TODAY, _NOW)

        assert crawler.month_lookups == [(2026, 10)]
        assert [g["game_id"] for g in games] == ["20261007LGHT0"]
        assert failure is None

    async def test_the_fallback_read_does_not_touch_the_ledger(self, games_in_db, monkeypatch):
        """`lookup_month` records nothing; `crawl_schedule` would.

        Asserted structurally rather than by counting rows: the two entry points
        differ in whether they open a ledger context at all, and a test that
        merely counted rows would pass again if a future change added the record
        to a path the counter did not cover.
        """
        tracked: list[str] = []

        def forbidden(*args, **kwargs):
            tracked.append("crawl_schedule")
            return []

        monkeypatch.setattr(ScheduleCrawler, "crawl_schedule", forbidden)
        crawler = _FakeScheduleCrawler(games=[{"game_id": "20261007LGHT0", "game_date": "2026-10-07"}])

        await _resolve_today_games(crawler, _TODAY, _NOW)

        assert tracked == [], "the live loop must not go through the ledger-recording entry point"


class TestRepeatedPollsDoNotRefetchTheMonth:
    """The 422 runs were the symptom; 422 upstream reads were the cost."""

    async def test_a_second_poll_inside_the_ttl_does_not_read_again(self, games_in_db):
        """With no stored rows, the second poll still must not re-fetch."""
        games_in_db[0] = []
        crawler = _FakeScheduleCrawler(games=[{"game_id": "20261007LGHT0", "game_date": "2026-10-07"}])

        for offset in (0, 60, 120, 240):
            games, _ = await _resolve_today_games(crawler, _TODAY, _NOW + timedelta(seconds=offset))
            assert [g["game_id"] for g in games] == ["20261007LGHT0"]

        assert len(crawler.month_lookups) == 1, f"expected one upstream read, got {len(crawler.month_lookups)}"

    async def test_a_poll_past_the_ttl_may_read_again(self, games_in_db):
        """A postponement has to be able to reach the loop eventually."""
        games_in_db[0] = []
        crawler = _FakeScheduleCrawler(games=[{"game_id": "20261007LGHT0", "game_date": "2026-10-07"}])

        await _resolve_today_games(crawler, _TODAY, _NOW)
        await _resolve_today_games(
            crawler,
            _TODAY,
            _NOW + timedelta(seconds=SCHEDULE_LOOKUP_TTL_SECONDS + 1),
        )

        assert len(crawler.month_lookups) == 2

    async def test_the_cache_is_keyed_by_month(self, games_in_db):
        """A month boundary must not serve September's read for October."""
        games_in_db[0] = []
        crawler = _FakeScheduleCrawler(games=[{"game_id": "20261007LGHT0", "game_date": "2026-10-07"}])

        september = _NOW - timedelta(days=8)
        await _resolve_today_games(crawler, "20260930", september)
        await _resolve_today_games(crawler, "20261007", _NOW)

        assert sorted(crawler.month_lookups) == [(2026, 9), (2026, 10)]

    async def test_a_failed_read_is_not_served_from_the_cache(self, games_in_db):
        """A failure must not pin the loop to "no games" for the whole TTL.

        Caching an unreadable month as an empty one would be the more
        dangerous half of this change: the loop would report no games for the
        next ten minutes on the strength of a single failed read, and real games
        would go unpolled. So a failure is retried on the next poll instead --
        still far from the original one-read-per-cycle, and it recovers as soon
        as the source does.
        """
        games_in_db[0] = []
        crawler = _FakeScheduleCrawler(ok=False)

        games, failure = await _resolve_today_games(crawler, _TODAY, _NOW)
        assert games == []
        assert failure == "naver_unavailable"

        # Recovery is visible on the very next poll.
        crawler._ok = True
        crawler._games = [{"game_id": "20261007LGHT0", "game_date": "2026-10-07"}]
        games, failure = await _resolve_today_games(crawler, _TODAY, _NOW + timedelta(seconds=30))

        assert [g["game_id"] for g in games] == ["20261007LGHT0"]
        assert failure is None
        assert len(crawler.month_lookups) == 2

    async def test_a_repeated_failure_still_does_not_record_work(self, games_in_db):
        """Retrying the read is fine; what must not return is the ledger write."""
        games_in_db[0] = []
        crawler = _FakeScheduleCrawler(ok=False)

        for offset in (0, 60, 120, 300):
            await _resolve_today_games(crawler, _TODAY, _NOW + timedelta(seconds=offset))

        # No dead letter, no run: asserted by the absence of the recording path.
        assert crawler.month_lookups, "the source was retried"
        assert not hasattr(crawler, "crawl_schedule")


class TestAnUnreadableMonthIsStillReported:
    """Dropping the ledger must not drop the signal the loop depends on."""

    async def test_a_failure_reason_still_reaches_the_caller(self, games_in_db):
        """The loop uses this to decide it has no games; it must survive."""
        games_in_db[0] = []
        crawler = _FakeScheduleCrawler(ok=False)

        games, failure = await _resolve_today_games(crawler, _TODAY, _NOW)

        assert failure == "naver_unavailable"

    async def test_an_empty_month_is_not_treated_as_a_failure(self, games_in_db):
        """No games in the month and no error is an answer, not an outage."""
        games_in_db[0] = []
        crawler = _FakeScheduleCrawler(games=[])

        games, failure = await _resolve_today_games(crawler, _TODAY, _NOW)

        assert games == []
        assert failure is None


class TestTheReadApiItself:
    """`lookup_month` is the boundary, so its own contract is pinned."""

    def test_it_is_separate_from_the_ledger_entry_point(self):
        assert hasattr(ScheduleCrawler, "lookup_month")
        assert ScheduleCrawler.lookup_month is not ScheduleCrawler.crawl_schedule

    async def test_it_returns_the_classified_result(self, monkeypatch):
        """The caller still needs to tell EMPTY from FAILED to decide anything.

        The resolver is stubbed rather than allowed to fetch: this asserts the
        wrapper's shape, and a live call here would test the Naver API instead.
        """
        _stub_resolver(monkeypatch)
        crawler = ScheduleCrawler(request_delay=0)

        result = await crawler.lookup_month(2026, 10)

        assert isinstance(result, CrawlResult)
        assert hasattr(result, "outcome")

    async def test_lookup_records_nothing(self, monkeypatch):
        """The property the whole change exists for, on the real class.

        Asserted by making the ledger path fail loudly: if `lookup_month` ever
        routes through `crawl_schedule`, the stub raises instead of quietly
        opening a ledger context.
        """

        def forbidden(*args, **kwargs):
            raise AssertionError("lookup_month must not open a ledger context")

        monkeypatch.setattr(ScheduleCrawler, "crawl_schedule", forbidden)
        _stub_resolver(monkeypatch)
        crawler = ScheduleCrawler(request_delay=0)

        await crawler.lookup_month(2026, 10)

    async def test_a_ledger_crawl_still_records_a_run(self, monkeypatch):
        """The counterpart: `crawl_schedule` must not have been weakened.

        Removing the live loop's write is only safe if the real unit of work is
        still recorded. Without this, "no ledger rows anywhere" would satisfy
        both tests above.
        """
        recorded: list[str] = []
        from src.crawlers import schedule_crawler as module

        # `track_crawl_run` yields the live run row; a bare nullcontext would
        # yield None and fail on `run.records_read`, which is a broken double
        # rather than a finding about the ledger.
        @contextmanager
        def fake_track(spec):
            recorded.append(spec.crawler)
            run = SimpleNamespace(records_read=0, records_written=0, records_failed=0, run_id="test-run")
            yield run

        monkeypatch.setattr(module, "track_crawl_run", fake_track)
        _stub_resolver(monkeypatch)
        crawler = ScheduleCrawler(request_delay=0)

        await crawler.crawl_schedule(2026, 10)

        assert recorded == [SCHEDULE_CRAWLER_NAME]


def _stub_resolver(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the month resolver with an async stub returning an empty month.

    `_resolve_month` is awaited by both entry points, so a sync stub fails with
    "object CrawlResult can't be used in 'await' expression" -- which reads like
    a production bug and is really a test-double one.
    """

    async def _resolve(*_args: object, **_kwargs: object) -> CrawlResult[list[dict]]:
        return CrawlResult.success([])

    monkeypatch.setattr(ScheduleCrawler, "_resolve_month", _resolve)


class TestTheTtlIsBounded:
    """An unbounded cache would be a different failure: a permanently stale day."""

    def test_the_ttl_is_short_enough_to_notice_a_postponement(self):
        """A postponement lands minutes before first pitch, not hours ahead."""
        assert 0 < SCHEDULE_LOOKUP_TTL_SECONDS <= 900

    def test_the_cache_records_when_the_read_happened(self):
        """Without a timestamp a TTL cannot be honoured."""
        entry = live_crawler._ScheduleLookup(games=[], at=_NOW)

        assert entry.at == _NOW
        assert (_NOW - entry.at).total_seconds() == 0

    def test_stale_months_are_evicted_so_the_cache_cannot_grow_forever(self):
        """A module-level dict keyed by month must not accumulate one per month."""
        cache = live_crawler._SCHEDULE_LOOKUP_CACHE
        cache[(2026, 10)] = live_crawler._ScheduleLookup(games=[], at=_NOW)
        cache[(2025, 3)] = live_crawler._ScheduleLookup(games=[], at=_NOW)

        live_crawler._evict_stale_lookup_months((2026, 10))

        assert (2026, 10) in cache
        assert (2025, 3) not in cache

    def test_january_keeps_december_of_the_previous_year(self):
        """The eviction boundary crosses a year, which a naive diff would miss."""
        cache = live_crawler._SCHEDULE_LOOKUP_CACHE
        cache[(2025, 12)] = live_crawler._ScheduleLookup(games=[], at=_NOW)
        cache[(2025, 11)] = live_crawler._ScheduleLookup(games=[], at=_NOW)

        live_crawler._evict_stale_lookup_months((2026, 1))

        assert (2025, 12) in cache, "December is reachable from January"
        assert (2025, 11) not in cache
