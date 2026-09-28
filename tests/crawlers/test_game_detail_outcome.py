"""What a game-detail attempt means, and why the three outcomes are different.

`GameDetailCrawler` returns a payload or nothing, and a caller that needs to
know which game failed has to diff the target list against the result and read
`_last_failure_reason`. This module pins the vocabulary that makes the durable
per-game outcome possible, before any of it is wired to a ledger.

The contract under test:

    lightweight=True + score/metadata    -> SUCCESS   (degraded, but intended)
    full detail + hitters and pitchers   -> SUCCESS
    full detail + boxscore missing but a
      recovery anchor is present         -> PARTIAL   (storable, re-fetchable)
    no anchor, navigation or validation
    failure                              -> FAILED

The existing collection contract is preserved rather than redesigned: a quality
failure stays retryable, because the same page fetched again usually renders
completely.
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.result import CrawlOutcome, CrawlResult
from src.models.crawl_execution import CrawlExecutionRun
from src.crawlers.game_detail_outcome import (
    PARTIAL_DETAIL_REASON,
    GameDetailStatus,
    attempt_from_result,
    canonical_failure,
    classify_payload,
    error_code_for_reason,
    has_full_detail_rows,
    has_partial_detail_anchor,
)
from src.services.crawl_retry_policy import decide


def _payload(*, boxscore: bool, score: bool = True, metadata: bool = False) -> dict:
    """Build a detail payload with or without the parts that matter."""
    payload: dict = {
        "teams": {
            "away": {"code": "HH", "score": 3 if score else None},
            "home": {"code": "HT", "score": 5 if score else None},
        },
    }
    if boxscore:
        payload["hitters"] = {"away": [{}], "home": [{}]}
        payload["pitchers"] = {"away": [{}], "home": [{}]}
    if metadata:
        payload["metadata"] = {"stadium": "잠실", "attendance": 20000}
    return payload


def _anchorless_payload() -> dict:
    """A payload with no team codes and nothing to anchor a degraded row on."""
    return {"teams": {"away": {"code": None}, "home": {"code": None}}, "metadata": {}}


class TestTheFourStates:
    def test_full_detail_is_a_success(self) -> None:
        assert classify_payload(_payload(boxscore=True), lightweight=False) is GameDetailStatus.SUCCESS

    def test_a_degraded_payload_in_full_mode_is_partial(self) -> None:
        status = classify_payload(_payload(boxscore=False), lightweight=False)

        assert status is GameDetailStatus.PARTIAL

    def test_a_degraded_payload_in_lightweight_mode_is_a_success(self) -> None:
        """The request asked for score and metadata, so this is the answer."""
        status = classify_payload(_payload(boxscore=False), lightweight=True)

        assert status is GameDetailStatus.SUCCESS

    def test_no_payload_is_a_failure_in_either_mode(self) -> None:
        for lightweight in (True, False):
            assert classify_payload(None, lightweight=lightweight) is GameDetailStatus.FAILED

    def test_a_payload_with_no_anchor_is_a_failure_not_a_partial(self) -> None:
        """Without team codes and something to anchor on there is nothing to
        store, so calling it partial would enqueue a row that can never be used.
        """
        assert classify_payload(_anchorless_payload(), lightweight=False) is GameDetailStatus.FAILED

    def test_a_missing_score_on_one_side_still_has_an_anchor(self) -> None:
        """An anchor needs *some* score, not a full scoreboard."""
        payload = _payload(boxscore=False, score=True)
        payload["teams"]["away"]["score"] = None

        assert classify_payload(payload, lightweight=False) is GameDetailStatus.PARTIAL

    def test_it_agrees_with_the_existing_service_predicates(self) -> None:
        """The collection service already decides what is storable. If these two
        ever disagree, a payload the service rejects would be recorded as a
        successful partial, or the other way round.
        """
        from src.services.game_collection_service import (
            _has_full_detail_rows as service_full,
            _has_partial_detail_anchor as service_anchor,
        )

        candidates = [
            _payload(boxscore=True),
            _payload(boxscore=False),
            _payload(boxscore=False, score=False, metadata=True),
            _anchorless_payload(),
            {},
        ]
        for candidate in candidates:
            assert has_full_detail_rows(candidate) == bool(service_full(candidate)), candidate
            assert has_partial_detail_anchor(candidate) == bool(service_anchor(candidate)), candidate

    def test_an_anchor_can_come_from_the_stadium_alone(self) -> None:
        payload = _payload(boxscore=False, score=False, metadata=True)

        assert classify_payload(payload, lightweight=False) is GameDetailStatus.PARTIAL


class TestStructuralPredicates:
    def test_full_detail_needs_both_teams_on_both_sides(self) -> None:
        payload = _payload(boxscore=True)

        assert has_full_detail_rows(payload) is True
        payload["pitchers"]["home"] = []

        assert has_full_detail_rows(payload) is False

    def test_an_anchor_needs_both_team_codes(self) -> None:
        payload = _payload(boxscore=False)
        payload["teams"]["away"] = {"code": None, "score": 3}

        assert has_partial_detail_anchor(payload) is False

    def test_an_empty_payload_is_neither(self) -> None:
        assert has_full_detail_rows(None) is False
        assert has_partial_detail_anchor(None) is False
        assert has_full_detail_rows({}) is False
        assert has_partial_detail_anchor({}) is False


class TestReasonMapping:
    @pytest.mark.parametrize(
        ("reason", "expected"),
        [
            ("timeout", FailureCode.FETCH_TIMEOUT),
            ("navigation_error", FailureCode.FETCH_HTTP_ERROR),
            ("kbo_robots_blocked", FailureCode.FETCH_BLOCKED),
            ("blocked", FailureCode.FETCH_BLOCKED),
            ("incomplete_detail", FailureCode.VALIDATION_QUALITY),
            ("hitter_totals_mismatch", FailureCode.VALIDATION_QUALITY),
            ("inning_score_mismatch", FailureCode.VALIDATION_QUALITY),
            ("exception", FailureCode.UNKNOWN),
        ],
    )
    def test_known_reasons_map_to_a_specific_code(self, reason: str, expected: FailureCode) -> None:
        assert error_code_for_reason(reason) == expected.value

    def test_a_cancelled_game_is_not_retried(self) -> None:
        """The sections do not exist for a cancelled game, so re-fetching cannot
        create them. It maps to a missing-selector code, not to a quality one.
        """
        code = error_code_for_reason("cancelled")

        assert code == FailureCode.PARSE_SELECTOR_MISSING.value
        assert decide(code, retry_count=0).retryable is False

    @pytest.mark.parametrize("reason", [None, "", "  ", "something_new"])
    def test_an_unrecognised_reason_does_not_guess(self, reason: str | None) -> None:
        """An unknown reason must not masquerade as an actionable cause."""
        assert error_code_for_reason(reason) == FailureCode.UNKNOWN.value

    def test_reasons_are_matched_case_insensitively(self) -> None:
        assert error_code_for_reason("TIMEOUT") == FailureCode.FETCH_TIMEOUT.value


class TestRetryPolicyPreservesTheExistingRecoveryContract:
    """The game-detail contract retries quality failures. This must not change."""

    @pytest.mark.parametrize(
        "reason",
        ["incomplete_detail", "hitter_totals_mismatch", "inning_score_mismatch", "timeout", "exception"],
    )
    def test_a_quality_or_transient_failure_is_still_retried(self, reason: str) -> None:
        decision = decide(error_code_for_reason(reason), retry_count=0)

        assert decision.retryable is True, f"{reason} lost its retry behaviour"

    def test_a_schema_failure_is_still_permanent(self) -> None:
        """A payload that does not match the expected shape does not fix itself."""
        assert decide(FailureCode.VALIDATION_SCHEMA.value, retry_count=0).retryable is False

    def test_a_blocked_target_is_still_permanent(self) -> None:
        assert decide(FailureCode.FETCH_BLOCKED.value, retry_count=0).retryable is False


class TestTheCodeAndMessageAgree:
    """A record whose code and message describe two different things cannot be acted on."""

    def test_a_primary_timeout_keeps_its_own_message(self) -> None:
        primary = CrawlResult.failure(
            CrawlOutcome.RETRYABLE_ERROR,
            error="ReadTimeout: slow",
            error_code=FailureCode.FETCH_TIMEOUT.value,
        )

        code, message = canonical_failure(primary, "kbo_robots_blocked")

        assert code == FailureCode.FETCH_TIMEOUT.value
        assert message == "ReadTimeout: slow"

    def test_an_empty_primary_reports_the_fallback_pair(self) -> None:
        code, message = canonical_failure(CrawlResult.empty(), "navigation_error")

        assert code == FailureCode.FETCH_HTTP_ERROR.value
        assert message == "navigation_error"

    def test_no_primary_reports_the_fallback_pair(self) -> None:
        assert canonical_failure(None, "timeout") == (FailureCode.FETCH_TIMEOUT.value, "timeout")

    def test_nothing_at_all_is_an_honest_unknown(self) -> None:
        code, message = canonical_failure(None, None)

        assert code == FailureCode.UNKNOWN.value
        assert message == FailureCode.UNKNOWN.value

    def test_a_partial_names_its_own_cause(self) -> None:
        """The ledger and the stored row must use the same word for this."""
        attempt = attempt_from_result("20260927HTHH0", _payload(boxscore=False), lightweight=False)

        assert attempt.status is GameDetailStatus.PARTIAL
        assert attempt.error_code == FailureCode.VALIDATION_QUALITY.value
        assert attempt.reason == PARTIAL_DETAIL_REASON
        assert attempt.error_message == PARTIAL_DETAIL_REASON

    def test_the_service_uses_the_same_word(self) -> None:
        from src.services.game_collection_service import DETAIL_COLLECTION_FAILURE_REASONS_RETRYABLE

        assert PARTIAL_DETAIL_REASON in DETAIL_COLLECTION_FAILURE_REASONS_RETRYABLE
        assert PARTIAL_DETAIL_REASON == "partial_detail"


class TestPartialRunsCanCarryAReason:
    """A partial run has to say *why* it is short, without becoming a failure."""

    @pytest.fixture
    def session_factory(self) -> sessionmaker:
        engine = create_engine("sqlite:///:memory:")
        CrawlExecutionRun.__table__.create(engine)
        return sessionmaker(bind=engine, expire_on_commit=False)

    def test_a_partial_run_records_its_reason(self, session_factory) -> None:
        from src.repositories.crawl_execution_repository import CrawlExecutionRepository, CrawlRunSpec
        from src.services.crawl_run_service import CrawlRunService

        with session_factory() as session:
            service = CrawlRunService(session)
            run = service.start(CrawlRunSpec(crawler="game_detail", target_type="game", target_id="A"))
            service.partial(
                run,
                records_read=1,
                records_written=1,
                error_code=FailureCode.VALIDATION_QUALITY.value,
                error_message=PARTIAL_DETAIL_REASON,
            )
            session.commit()

            stored = CrawlExecutionRepository(session).get_by_run_id(run.run_id)

        assert stored.status == "partial"
        assert stored.error_code == FailureCode.VALIDATION_QUALITY.value
        assert stored.error_message == PARTIAL_DETAIL_REASON

    def test_a_partial_without_a_reason_leaves_the_row_clean(self, session_factory) -> None:
        from src.repositories.crawl_execution_repository import CrawlRunSpec
        from src.services.crawl_run_service import CrawlRunService

        with session_factory() as session:
            service = CrawlRunService(session)
            run = service.start(CrawlRunSpec(crawler="game_detail", target_type="game", target_id="A"))
            service.partial(run, records_read=1)
            session.commit()

            stored = session.query(CrawlExecutionRun).one()

        assert stored.status == "partial"
        assert stored.error_code is None

    def test_finalizing_never_discards_a_classification_already_on_the_row(
        self,
        session_factory,
    ) -> None:
        """Finalizing writes only what it is given, so a caller that set a reason
        and then reports a different outcome keeps both facts visible rather than
        having one silently overwritten. The caller owns the row it built.
        """
        from src.repositories.crawl_execution_repository import CrawlRunSpec
        from src.services.crawl_run_service import CrawlRunService

        with session_factory() as session:
            service = CrawlRunService(session)
            run = service.start(CrawlRunSpec(crawler="game_detail", target_type="game", target_id="A"))
            run.error_code = "SET_BY_THE_CALLER"
            service.success(run)
            session.commit()

            stored = session.query(CrawlExecutionRun).one()

        assert stored.status == "success"
        assert stored.error_code == "SET_BY_THE_CALLER"


class TestAttemptRecords:
    def test_a_success_carries_no_error_code(self) -> None:
        attempt = attempt_from_result("20260927HTHH0", _payload(boxscore=True), lightweight=False)

        assert attempt.status is GameDetailStatus.SUCCESS
        assert attempt.error_code is None
        assert attempt.ok is True
        assert attempt.needs_refetch is False

    def test_a_partial_carries_a_code_and_needs_a_refetch(self) -> None:
        attempt = attempt_from_result("20260927HTHH0", _payload(boxscore=False), lightweight=False)

        assert attempt.status is GameDetailStatus.PARTIAL
        assert attempt.error_code is not None
        assert attempt.ok is True
        assert attempt.needs_refetch is True

    def test_a_failure_keeps_the_payload_out(self) -> None:
        attempt = attempt_from_result(
            "20260927HTHH0",
            None,
            lightweight=False,
            reason="navigation_error",
        )

        assert attempt.status is GameDetailStatus.FAILED
        assert attempt.payload is None
        assert attempt.error_code == FailureCode.FETCH_HTTP_ERROR.value
        assert attempt.reason == "navigation_error"
        assert attempt.ok is False
        assert attempt.needs_refetch is True

    def test_a_lightweight_degraded_result_needs_no_refetch(self) -> None:
        attempt = attempt_from_result("20260927HTHH0", _payload(boxscore=False), lightweight=True)

        assert attempt.status is GameDetailStatus.SUCCESS
        assert attempt.needs_refetch is False

    def test_a_partial_never_reports_an_unknown_cause(self) -> None:
        """`UNKNOWN` means "could not classify this". A partial is precisely
        classified -- well-shaped but incomplete -- so a vague code would hide a
        known, retryable cause behind a shrug.
        """
        attempt = attempt_from_result("20260927HTHH0", _payload(boxscore=False), lightweight=False)

        assert attempt.error_code == FailureCode.VALIDATION_QUALITY.value
        assert decide(attempt.error_code or "", retry_count=0).retryable is True

    def test_the_underlying_reason_is_preserved(self) -> None:
        """`game_collection_service` keys off the crawler's own reason strings."""
        attempt = attempt_from_result(
            "20260927HTHH0",
            None,
            lightweight=False,
            reason="hitter_totals_mismatch",
        )

        assert attempt.reason == "hitter_totals_mismatch"
        assert attempt.error_code == FailureCode.VALIDATION_QUALITY.value
