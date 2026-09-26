"""A `CrawlerHttpClient` failure reaches the taxonomy unchanged.

This is contract A of the failure taxonomy: the transport that knows the cause
attaches the code, and `classification_for_result` preserves it rather than
re-deriving it. Re-deriving is how one failure ends up with two names.

Tests drive the real client through `httpx.MockTransport`, so the codes are
observed on the same path a crawler uses.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextlib import asynccontextmanager

import httpx
import pytest

from src.crawlers.circuit_breaker import circuit_registry
from src.crawlers.failure_taxonomy import (
    FailureCode,
    FailureStage,
    classification_for_result,
    failure_code_for_status,
)
from src.crawlers.http_client import (
    THROTTLE_STATUS_CODES,
    CircuitPolicy,
    CrawlerHttpClient,
    HttpPolicy,
)
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.utils.throttle import throttle

Handler = Callable[[httpx.Request], httpx.Response]
_URL = "https://stat.koreabaseball.com/api/taxonomy"


@pytest.fixture(autouse=True)
def _no_throttle(monkeypatch):
    """Keep throttling out of the way so the tests stay fast."""
    monkeypatch.setenv("KBO_REQUEST_DELAY", "0")
    monkeypatch.setenv("KBO_REQUEST_JITTER", "0")
    monkeypatch.setattr(throttle, "default_delay", 0.0)
    monkeypatch.setattr(throttle, "jitter", 0.0)
    monkeypatch.setattr(throttle, "_last_request_times", {})


def _client(
    handler: Handler,
    *,
    name: str = "taxonomy",
    max_attempts: int = 1,
    **policy_kwargs,
) -> CrawlerHttpClient:
    """Build an offline client with a clean circuit."""
    transport = httpx.MockTransport(handler)
    client = CrawlerHttpClient(
        name=name,
        policy=HttpPolicy(
            base_delay_seconds=0.0,
            max_attempts=max_attempts,
            max_backoff_seconds=0.0,
            **policy_kwargs,
        ),
    )

    @asynccontextmanager
    async def _mock_client():
        async with httpx.AsyncClient(
            headers=client.default_headers,
            timeout=client.timeout,
            transport=transport,
            follow_redirects=True,
        ) as raw:
            yield raw

    # The URL validation hook is exercised separately below; replacing the
    # transport keeps every other assertion offline.
    client._client = _mock_client  # type: ignore[method-assign]
    circuit_registry.reset_all()
    return client


def _fetch(handler: Handler, **kwargs) -> CrawlResult[object]:
    return asyncio.run(_client(handler, **kwargs).fetch_json(_URL))


def _raise(exc: Exception) -> Handler:
    def handler(request: httpx.Request) -> httpx.Response:
        raise exc

    return handler


class TestExceptionsCarryTheirOwnCode:
    def test_timeout(self):
        result = _fetch(_raise(httpx.ReadTimeout("slow")))

        assert result.outcome is CrawlOutcome.RETRYABLE_ERROR
        assert result.error_code == FailureCode.FETCH_TIMEOUT.value

    def test_connect_error_is_a_transport_fault(self):
        result = _fetch(_raise(httpx.ConnectError("refused")))

        assert result.outcome is CrawlOutcome.RETRYABLE_ERROR
        assert result.error_code == FailureCode.FETCH_HTTP_ERROR.value

    def test_url_validator_rejection(self):
        """A blocked target never reaches the network, and that is a `fetch`
        refusal rather than a server-side error.
        """

        async def _reject(request: httpx.Request) -> None:
            message = f"Blocked crawler request to {request.url}: blocked host"
            raise ValueError(message)

        @asynccontextmanager
        async def _blocked_client():
            async with httpx.AsyncClient(
                headers={"User-Agent": "test"},
                event_hooks={"request": [_reject]},
                transport=httpx.MockTransport(lambda request: httpx.Response(200, json={"a": 1})),
            ) as raw:
                yield raw

        client = _client(lambda request: httpx.Response(200, json={"a": 1}), name="blocked")
        client._client = _blocked_client  # type: ignore[method-assign]

        result = asyncio.run(client.fetch_json(_URL))

        assert result.outcome is CrawlOutcome.PERMANENT_ERROR
        assert result.error_code == FailureCode.FETCH_BLOCKED.value

    def test_circuit_open(self):
        """The taxonomy has no circuit-specific code yet, so an open circuit
        reports as a fetch error. It stays a `retryable` outcome.
        """
        result = _fetch(
            lambda request: httpx.Response(503),
            name="breaker",
            circuit=CircuitPolicy(failure_threshold=1, recovery_timeout_seconds=600.0),
        )
        assert result.error_code == FailureCode.FETCH_HTTP_ERROR.value

        client = _client(
            lambda request: httpx.Response(503),
            name="breaker-open",
            circuit=CircuitPolicy(failure_threshold=1, recovery_timeout_seconds=600.0),
        )
        asyncio.run(client.fetch_json(_URL))
        second = asyncio.run(client.fetch_json(_URL))

        assert second.outcome is CrawlOutcome.RETRYABLE_ERROR
        assert second.error_code == FailureCode.FETCH_HTTP_ERROR.value


class TestStatusCodesCarryTheirCode:
    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            (408, FailureCode.FETCH_TIMEOUT),
            (429, FailureCode.FETCH_RATE_LIMITED),
            (401, FailureCode.FETCH_BLOCKED),
            (403, FailureCode.FETCH_BLOCKED),
            (500, FailureCode.FETCH_HTTP_ERROR),
            (502, FailureCode.FETCH_HTTP_ERROR),
            (503, FailureCode.FETCH_HTTP_ERROR),
            (504, FailureCode.FETCH_HTTP_ERROR),
            (404, FailureCode.FETCH_HTTP_ERROR),
            (418, FailureCode.FETCH_HTTP_ERROR),
        ],
    )
    def test_status_mapping(self, status, expected):
        result = _fetch(lambda request: httpx.Response(status, text="nope"))

        assert result.http_status == status
        assert result.error_code == expected.value

    def test_503_is_a_throttle_for_transport_but_not_for_meaning(self):
        """The adaptive limiter treats 503 as backoff; the taxonomy must not, or
        a server outage would be indistinguishable from a rate limit.
        """
        assert 503 in THROTTLE_STATUS_CODES
        assert failure_code_for_status(503) is FailureCode.FETCH_HTTP_ERROR
        assert failure_code_for_status(429) is FailureCode.FETCH_RATE_LIMITED

    def test_408_is_a_timeout_like_the_transport_exception(self):
        """The same cause must not get two names depending on how it surfaced."""
        assert failure_code_for_status(408) is FailureCode.FETCH_TIMEOUT
        assert _fetch(lambda r: httpx.Response(408, text="late")).error_code == FailureCode.FETCH_TIMEOUT.value
        assert _fetch(_raise(httpx.ReadTimeout("slow"))).error_code == FailureCode.FETCH_TIMEOUT.value


class TestParseFailuresCarryTheirCode:
    def test_html_instead_of_json(self):
        result = _fetch(
            lambda request: httpx.Response(
                200,
                text="<html><body>maintenance</body></html>",
                headers={"Content-Type": "text/html"},
            ),
        )

        assert result.outcome is CrawlOutcome.SCHEMA_CHANGED
        assert result.error_code == FailureCode.PARSE_INVALID_FORMAT.value

    def test_json_decode_failure(self):
        result = _fetch(
            lambda request: httpx.Response(
                200,
                content=b"{not json",
                headers={"Content-Type": "application/json"},
            ),
        )

        assert result.outcome is CrawlOutcome.SCHEMA_CHANGED
        assert result.error_code == FailureCode.PARSE_INVALID_FORMAT.value


class TestClassificationPreservesTheCode:
    @pytest.mark.parametrize(
        "code",
        [
            FailureCode.FETCH_TIMEOUT,
            FailureCode.FETCH_HTTP_ERROR,
            FailureCode.FETCH_BLOCKED,
            FailureCode.FETCH_RATE_LIMITED,
            FailureCode.PARSE_INVALID_FORMAT,
        ],
    )
    def test_known_code_is_preserved(self, code):
        result = CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="x", error_code=code.value)

        stage, normalized = classification_for_result(result)

        assert normalized is code

    def test_stage_is_derived_not_trusted(self):
        """`failure_stage` is an output, never an input: a result naming a fetch
        code cannot claim to have failed somewhere else in the pipeline.
        """
        code = FailureCode.FETCH_TIMEOUT
        result = CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="timeout", error_code=code.value)

        stage, normalized_code = classification_for_result(result)

        assert normalized_code is code
        assert stage is FailureStage.FETCH

    def test_every_fetch_code_derives_the_fetch_stage(self):
        for code in (
            FailureCode.FETCH_TIMEOUT,
            FailureCode.FETCH_HTTP_ERROR,
            FailureCode.FETCH_BLOCKED,
            FailureCode.FETCH_RATE_LIMITED,
        ):
            result = CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="x", error_code=code.value)

            assert classification_for_result(result) == (FailureStage.FETCH, code)

    def test_an_unknown_code_is_rejected_rather_than_downgraded(self):
        """A typo must fail loudly. Silently becoming `UNKNOWN` is how the
        ledger, the dead letter queue, and the metrics drift apart again.
        """
        result = CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="x", error_code="FETCH_TIMOUT")

        with pytest.raises(ValueError, match="FETCH_TIMOUT"):
            classification_for_result(result)


class TestLegacyFallback:
    def test_legacy_result_with_only_a_status(self):
        """Results built before the taxonomy existed still have to normalize."""
        result = CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="HTTP 429", http_status=429)

        assert classification_for_result(result) == (FailureStage.FETCH, FailureCode.FETCH_RATE_LIMITED)

    def test_legacy_schema_drift(self):
        result = CrawlResult.failure(CrawlOutcome.SCHEMA_CHANGED, error="expected JSON", http_status=200)

        assert classification_for_result(result) == (FailureStage.PARSE, FailureCode.PARSE_INVALID_FORMAT)

    def test_legacy_unclassifiable_failure_is_honestly_unknown(self):
        """No status and no code means the cause genuinely is not known, and
        claiming a fetch failure would be a guess.
        """
        result = CrawlResult.failure(CrawlOutcome.PERMANENT_ERROR, error="something happened")

        assert classification_for_result(result) == (FailureStage.UNKNOWN, FailureCode.UNKNOWN)

    def test_a_successful_status_does_not_imply_a_fetch_failure(self):
        """A 200 with a failure outcome means the response arrived in the wrong
        shape, so the status says nothing about the cause.
        """
        result = CrawlResult.failure(CrawlOutcome.PERMANENT_ERROR, error="bad payload", http_status=200)

        assert classification_for_result(result) == (FailureStage.UNKNOWN, FailureCode.UNKNOWN)


class TestSuccessesAreNotFailures:
    def test_success_has_no_classification(self):
        assert classification_for_result(CrawlResult.success({"a": 1})) is None

    def test_empty_has_no_classification(self):
        assert classification_for_result(CrawlResult.empty()) is None

    def test_empty_response_is_not_a_failure(self):
        result = _fetch(lambda request: httpx.Response(200, json=[]))

        assert result.outcome is CrawlOutcome.EMPTY
        assert classification_for_result(result) is None

    def test_successful_response_has_no_classification(self):
        result = _fetch(lambda request: httpx.Response(200, json={"a": 1}))

        assert result.error_code is None
        assert classification_for_result(result) is None


class TestRetryExhaustion:
    def test_final_result_reports_the_terminal_cause(self):
        """After retries are spent the reported cause must still be the taxonomy
        code, so every downstream layer can aggregate it.
        """
        result = _fetch(_raise(httpx.ReadTimeout("slow")), name="retry", max_attempts=2)

        assert result.outcome is CrawlOutcome.RETRYABLE_ERROR
        assert classification_for_result(result) == (FailureStage.FETCH, FailureCode.FETCH_TIMEOUT)

    def test_retry_then_success_does_not_leak_the_first_error(self):
        """The rebuild in `_fetch` must drop the failed attempt's code, otherwise
        a run that recovered is counted as a failure everywhere downstream.
        """
        attempts = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["n"] += 1
            if attempts["n"] == 1:
                return httpx.Response(503, text="busy")
            return httpx.Response(200, json={"a": 1})

        result = _fetch(handler, name="recover", max_attempts=2)

        assert result.outcome is CrawlOutcome.SUCCESS
        assert result.error_code is None
        assert classification_for_result(result) is None

    def test_retry_moves_from_a_rate_limit_to_a_success(self):
        attempts = {"n": 0}

        def handler(request: httpx.Request) -> httpx.Response:
            attempts["n"] += 1
            if attempts["n"] == 1:
                return httpx.Response(429, text="slow down")
            return httpx.Response(200, json=[1])

        result = _fetch(handler, name="recover-429", max_attempts=2)

        assert result.outcome is CrawlOutcome.SUCCESS
        assert result.error_code is None
