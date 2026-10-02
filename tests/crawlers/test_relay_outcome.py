"""The relay outcome contract, before any crawl or ledger exists.

The distinction these hold is that "nothing came back" is four different
things. Two of them must never be retried -- a game the public source does not
carry, and a response that changed shape -- and reporting either as a failure
spends the whole retry budget on an outcome that cannot change.
"""

from __future__ import annotations

import pytest

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.relay_outcome import (
    BUCKET_API_FAILED,
    BUCKET_EMPTY,
    BUCKET_MATCH_FAILED,
    SOURCE_RELAY,
    SOURCE_SCHEDULE,
    AttemptSeed,
    InningStop,
    RelayStatus,
    build_attempt,
    classify_relay_failure,
    is_terminal,
)

GAME = "20250501LGOB0"


class TestTheSameStatusMeansDifferentThingsPerEndpoint:
    """A 404 from the relay endpoint means the data is absent.

    The same status from the schedule endpoint means the lookup failed. These
    want opposite handling, and the reason string alone cannot tell them apart,
    so the classifier takes the source.
    """

    def test_a_relay_404_is_a_terminal_absence(self) -> None:
        failure = classify_relay_failure("http_404", source=SOURCE_RELAY)

        assert failure.code == FailureCode.PARSE_SELECTOR_MISSING.value
        assert failure.terminal is True
        assert failure.bucket == BUCKET_EMPTY

    def test_a_schedule_404_is_a_retryable_lookup_failure(self) -> None:
        failure = classify_relay_failure("http_404", source=SOURCE_SCHEDULE)

        assert failure.code == FailureCode.FETCH_HTTP_ERROR.value
        assert failure.terminal is False
        assert failure.bucket == BUCKET_API_FAILED

    def test_other_statuses_are_transport_failures_from_either_endpoint(self) -> None:
        for source in (SOURCE_RELAY, SOURCE_SCHEDULE):
            failure = classify_relay_failure("http_500", source=source)

            assert failure.code == FailureCode.FETCH_HTTP_ERROR.value
            assert failure.terminal is False


class TestTerminalOutcomesAreNotRetried:
    """The reason a terminal failure is not a failure at all.

    A game the source does not carry will never acquire relay data. Queued, it
    spends the full retry budget and leaves a dead letter describing a problem
    that cannot be solved.
    """

    @pytest.mark.parametrize(
        "reason",
        ["relay_not_found", "relay_schema_drift", "blocked"],
    )
    def test_these_are_terminal(self, reason: str) -> None:
        assert is_terminal(reason) is True

    @pytest.mark.parametrize(
        "reason",
        ["relay_api_error", "invalid_relay_match", "relay_empty"],
    )
    def test_those_are_retryable(self, reason: str) -> None:
        assert is_terminal(reason) is False

    def test_a_terminal_failure_arrives_as_empty_with_no_error_code(self) -> None:
        attempt = build_attempt(
            GAME,
            AttemptSeed(status=RelayStatus.FAILED, reason="relay_not_found", source=SOURCE_RELAY),
        )

        assert attempt.status is RelayStatus.EMPTY
        # No error code: there is nothing to retry, so nothing to code.
        assert attempt.error_code is None
        assert attempt.reason == "relay_not_found"

    def test_a_retryable_failure_keeps_its_code(self) -> None:
        attempt = build_attempt(
            GAME,
            AttemptSeed(status=RelayStatus.FAILED, reason="invalid_relay_match", source=SOURCE_SCHEDULE),
        )

        assert attempt.status is RelayStatus.FAILED
        assert attempt.error_code == FailureCode.VALIDATION_QUALITY.value


class TestAnUnrecognisedReasonIsNotGuessedAt:
    def test_it_becomes_unknown_and_stays_retryable(self) -> None:
        """Unknown is not a reason to give up.

        A reason string added later must not silently become terminal and stop
        being retried, nor masquerade as a specific cause an operator would act
        on incorrectly.
        """
        failure = classify_relay_failure("something_new")

        assert failure.code == FailureCode.UNKNOWN.value
        assert failure.terminal is False

    def test_a_missing_reason_is_also_unknown(self) -> None:
        assert classify_relay_failure(None).code == FailureCode.UNKNOWN.value

    def test_it_does_not_silently_inherit_a_neighbouring_reason(self) -> None:
        """Substring matching is how `http_404` used to be read as an empty result."""
        assert classify_relay_failure("relay_api_error_v2").code == FailureCode.UNKNOWN.value
        assert classify_relay_failure("not_relay_empty").code == FailureCode.UNKNOWN.value


class TestBucketsMatchTheRecoveryService:
    """`relay_recovery_service` already reports these three names.

    Emitting different ones would make the two systems' reports incomparable
    while both claim to describe the same games.
    """

    @pytest.mark.parametrize(
        ("reason", "expected"),
        [
            ("invalid_relay_match", BUCKET_MATCH_FAILED),
            ("relay_api_error", BUCKET_API_FAILED),
            ("http_500", BUCKET_API_FAILED),
            ("relay_empty", BUCKET_EMPTY),
            ("relay_not_found", BUCKET_EMPTY),
        ],
    )
    def test_bucket_names(self, reason: str, expected: str) -> None:
        assert classify_relay_failure(reason).bucket == expected


class TestNotModifiedIsASuccessfulNoOp:
    """It has content and nothing new, and must not be queued."""

    def test_it_is_not_a_failure(self) -> None:
        attempt = build_attempt(
            GAME,
            AttemptSeed(status=RelayStatus.NOT_MODIFIED, result={"status": "not_modified", "events": []}),
        )

        assert attempt.status is RelayStatus.NOT_MODIFIED
        assert attempt.error_code is None
        assert attempt.result is not None


class TestTheInningStopIsRecorded:
    """Why the loop stopped is not recoverable from the payload.

    An empty inning at the end of a game is normal; an empty first inning is a
    real absence. Without the inning number and the cause, both look like an
    empty payload.
    """

    def test_the_stop_and_innings_survive_into_the_attempt(self) -> None:
        attempt = build_attempt(
            GAME,
            AttemptSeed(
                status=RelayStatus.SUCCESS,
                result={"status": "completed"},
                stop=InningStop.TERMINAL_MARKER,
                innings_fetched=9,
                naver_game_id="20250501LGOB0",
            ),
        )

        assert attempt.stop is InningStop.TERMINAL_MARKER
        assert attempt.innings_fetched == 9
        assert attempt.naver_game_id == "20250501LGOB0"

    def test_resolution_is_recorded_separately_from_absence(self) -> None:
        """'The source has nothing' and 'we could not find it' are different claims."""
        looked = build_attempt(GAME, AttemptSeed(status=RelayStatus.EMPTY, resolution_attempted=True))
        never_looked = build_attempt(GAME, AttemptSeed(status=RelayStatus.EMPTY, resolution_attempted=False))

        assert looked.resolution_attempted is True
        assert never_looked.resolution_attempted is False
