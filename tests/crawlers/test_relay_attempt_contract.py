"""What `RelayCrawler` now says about a game, and what it still says.

`crawl_game_relay` keeps its original `dict | None` contract; these assert it
does, because the typed entrypoint was added on top of it rather than in place
of it. They also pin the classification the typed entrypoint derives, since that
is the part a ledger will later queue on.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.relay_crawler import RelayCrawler
from src.crawlers.relay_outcome import InningStop, RelayStatus

GAME = "20250501LGOB0"


def _crawler() -> RelayCrawler:
    crawler = RelayCrawler()
    crawler._map_to_naver_id = lambda game_id: game_id  # type: ignore[method-assign]
    return crawler


def _in_relay(text_relays: list[dict[str, Any]] | None) -> dict[str, Any]:
    """One relay inning's envelope, or an empty one when `text_relays` is None."""
    return {"result": {"textRelayData": {"textRelays": text_relays or []}}}


def _entry(options: int = 1) -> dict[str, Any]:
    return {"textOptions": [{}] * options}


@pytest.mark.asyncio
class TestTheLegacyEntryPointIsUnchanged:
    """Every existing caller reads `dict | None` and a failure reason.

    The typed entrypoint is additive. If this drifts, the recovery service and
    the live crawler both change behaviour without anyone deciding that.
    """

    async def test_a_populated_game_still_returns_a_result_dict(self) -> None:
        crawler = _crawler()
        payload = _in_relay([_entry()])
        with (
            patch.object(crawler, "_request_json", AsyncMock(return_value=(payload, None))),
            patch.object(crawler, "_build_relay_result", return_value={"game_id": GAME, "status": "completed"}),
        ):
            result = await crawler.crawl_game_relay(GAME)

        assert result == {"game_id": GAME, "status": "completed"}

    async def test_an_empty_first_inning_still_returns_none(self) -> None:
        crawler = _crawler()
        with patch.object(crawler, "_request_json", AsyncMock(return_value=(_in_relay(None), None))):
            result = await crawler.crawl_game_relay(GAME)

        assert result is None
        assert crawler.get_last_failure_reason(GAME) == "relay_not_found"

    async def test_a_transport_failure_still_records_its_reason(self) -> None:
        crawler = _crawler()
        with patch.object(crawler, "_request_json", AsyncMock(return_value=(None, "relay_api_error"))):
            result = await crawler.crawl_game_relay(GAME)

        assert result is None
        assert crawler.get_last_failure_reason(GAME) == "relay_api_error"


@pytest.mark.asyncio
class TestTheTypedEntryPointClassifies:
    async def test_a_result_is_a_success(self) -> None:
        crawler = _crawler()
        with patch.object(crawler, "crawl_game_relay", AsyncMock(return_value={"status": "completed", "events": [1]})):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.SUCCESS
        assert attempt.error_code is None

    async def test_an_unchanged_payload_is_a_successful_no_op(self) -> None:
        """`not_modified` is content with nothing new: never queued, never rewritten."""
        crawler = _crawler()
        with patch.object(
            crawler, "crawl_game_relay", AsyncMock(return_value={"status": "not_modified", "events": []})
        ):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.NOT_MODIFIED
        assert attempt.error_code is None

    async def test_a_game_the_source_does_not_carry_is_empty_not_failed(self) -> None:
        """The distinction the old `None` could not express.

        Queued as a failure, this game spends the entire retry budget and leaves
        a dead letter for an outcome that cannot change.
        """
        crawler = _crawler()
        crawler._set_failure_reason(GAME, "relay_not_found")
        with patch.object(crawler, "crawl_game_relay", AsyncMock(return_value=None)):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.EMPTY
        assert attempt.error_code is None

    async def test_a_transport_failure_is_failed_with_its_code(self) -> None:
        crawler = _crawler()
        crawler._set_failure_reason(GAME, "relay_api_error")
        crawler._last_fetch_failure_reason = "relay_api_error"
        with patch.object(crawler, "crawl_game_relay", AsyncMock(return_value=None)):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.FAILED
        assert attempt.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert attempt.stop is InningStop.FETCH_FAILED

    async def test_an_unmatched_game_is_failed_and_retryable(self) -> None:
        """Resolution failure is not absence: the schedule may still update."""
        crawler = _crawler()
        crawler._set_failure_reason(GAME, "invalid_relay_match")
        with patch.object(crawler, "crawl_game_relay", AsyncMock(return_value=None)):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.FAILED
        assert attempt.error_code == FailureCode.VALIDATION_QUALITY.value


@pytest.mark.asyncio
class TestTheInningLoopSaysWhyItStopped:
    """Why the loop ended is not recoverable from the payload."""

    async def test_a_404_from_the_relay_endpoint_is_an_absence(self) -> None:
        """The endpoint answered, and the answer was: there is nothing here."""
        crawler = _crawler()
        with patch.object(crawler, "_request_json", AsyncMock(return_value=(None, "http_404"))):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.EMPTY
        assert attempt.error_code is None

    async def test_a_404_while_resolving_is_a_lookup_failure(self) -> None:
        """Same status, opposite consequence: here we never found the game."""
        crawler = _crawler()
        crawler.last_resolved_naver_game_id = "99999999AAA00"
        crawler._set_failure_reason(GAME, "http_404")
        with patch.object(crawler, "crawl_game_relay", AsyncMock(return_value=None)):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.FAILED
        assert attempt.error_code == FailureCode.FETCH_HTTP_ERROR.value

    async def test_a_finished_game_keeps_its_data(self) -> None:
        """An empty ninth inning ends the loop without discarding the first eight."""
        crawler = _crawler()
        responses = [(_in_relay([_entry()]), None), (_in_relay(None), None)]
        with (
            patch.object(crawler, "_request_json", AsyncMock(side_effect=responses)),
            patch.object(crawler, "_build_relay_result", return_value={"status": "completed", "events": [1]}),
        ):
            fetched = await crawler._fetch_with_resolution(
                AsyncMock(),
                GAME,
                GAME,
                None,
                None,
            )

        assert fetched.stop is InningStop.EMPTY_INNING
        assert fetched.innings_fetched == 1
        assert len(fetched.relays) == 1

    async def test_the_end_of_game_marker_is_distinguished_from_an_empty_inning(self) -> None:
        """Entries with no text options is the source saying the game is over."""
        crawler = _crawler()
        responses = [
            (_in_relay([_entry()]), None),
            (_in_relay([_entry(options=0)]), None),
        ]
        with patch.object(crawler, "_request_json", AsyncMock(side_effect=responses)):
            fetched = await crawler._fetch_with_resolution(AsyncMock(), GAME, GAME, None, None)

        assert fetched.stop is InningStop.TERMINAL_MARKER
        assert fetched.innings_fetched == 2

    async def test_a_fetch_failure_mid_game_keeps_what_it_already_had(self) -> None:
        crawler = _crawler()
        responses = [(_in_relay([_entry()]), None), (None, "relay_api_error")]
        with patch.object(crawler, "_request_json", AsyncMock(side_effect=responses)):
            fetched = await crawler._fetch_with_resolution(AsyncMock(), GAME, GAME, None, None)

        assert fetched.stop is InningStop.FETCH_FAILED
        assert fetched.failure_reason == "relay_api_error"
        assert len(fetched.relays) == 1


@pytest.mark.asyncio
class TestAFetchFailureIsNotReportedAsAnAbsence:
    """The resolver may still find nothing while the relay fetch is broken.

    Both produce an empty result, so the reason is the only thing that tells
    them apart. When the reason is lost, an outage is recorded as a permanent
    absence and nothing ever retries it.
    """

    async def test_a_relay_transport_failure_survives_a_resolver_that_answered(self) -> None:
        """The case that separates them.

        When every request fails the resolver records the same reason, so the
        order does not matter and the test proves nothing. Here the relay
        endpoint is broken while the schedule lookup works and finds nothing --
        so the resolver is free to answer "it is not there", and that answer must
        not win.
        """
        crawler = _crawler()

        async def _respond(client: Any, url: str, **kwargs: Any) -> tuple[dict[str, Any] | None, str | None]:
            if "inning=" in url:
                return None, "relay_api_error"
            return {"result": {"games": []}}, None

        with patch.object(crawler, "_request_json", AsyncMock(side_effect=_respond)):
            await crawler.crawl_game_relay(GAME)

        assert crawler.get_last_failure_reason(GAME) == "relay_api_error"

    async def test_it_reaches_the_attempt_as_a_retryable_failure(self) -> None:
        """The consequence, not just the string: it must stay retryable."""
        crawler = _crawler()

        async def _respond(client: Any, url: str, **kwargs: Any) -> tuple[dict[str, Any] | None, str | None]:
            if "inning=" in url:
                return None, "relay_api_error"
            return {"result": {"games": []}}, None

        with patch.object(crawler, "_request_json", AsyncMock(side_effect=_respond)):
            await crawler.crawl_game_relay(GAME)
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.FAILED
        assert attempt.error_code == FailureCode.FETCH_HTTP_ERROR.value

    async def test_a_failed_lookup_does_not_report_absence(self) -> None:
        """The resolver may not conclude absence from a lookup that failed.

        Its schedule query failing is not evidence about the game; reporting
        absence anyway turns an unreachable schedule into a permanent result.
        """
        crawler = _crawler()
        with patch.object(crawler, "_request_json", AsyncMock(return_value=(None, "relay_api_error"))):
            await crawler.crawl_game_relay(GAME)

        assert crawler.get_last_failure_reason(GAME) == "relay_api_error"

    async def test_an_unreachable_schedule_after_an_empty_inning_is_retryable(self) -> None:
        """The one case where the resolver's own guard is the only thing holding.

        The first inning came back empty -- cleanly, with no failure -- and the
        schedule query then failed. With no fetch failure to outrank it, the
        resolver's "not there" would be the last word, and the game would be
        written off as a permanent absence instead of a lookup that broke.
        """
        crawler = _crawler()

        async def _respond(client: Any, url: str, **kwargs: Any) -> tuple[dict[str, Any] | None, str | None]:
            if "inning=" in url:
                return _in_relay(None), None
            return None, "relay_api_error"

        with patch.object(crawler, "_request_json", AsyncMock(side_effect=_respond)):
            await crawler.crawl_game_relay(GAME)

        assert crawler.get_last_failure_reason(GAME) == "relay_api_error"

    async def test_a_genuine_absence_is_still_terminal(self) -> None:
        """The fix must not make every empty result retryable."""
        crawler = _crawler()
        with patch.object(crawler, "_request_json", AsyncMock(return_value=(_in_relay(None), None))):
            attempt = await crawler.crawl_relay_attempt(GAME)

        assert attempt.status is RelayStatus.EMPTY
        assert attempt.error_code is None
