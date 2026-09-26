"""`CrawlResult` carries the failure taxonomy code.

The code is attached where the failure is understood, so the ledger, the dead
letter queue, and the metrics can all name the same failure. These tests pin
the contract that makes that possible: only `failure()` accepts a code, a
success can never carry one, and a code survives the retry loop.
"""

from __future__ import annotations

import pytest

from src.crawlers.failure_taxonomy import FailureCode
from src.crawlers.result import CrawlOutcome, CrawlResult


class TestOnlyFailuresCarryACode:
    def test_success_has_no_error_code_argument(self):
        """The absence of the parameter is what stops a success being coded."""
        with pytest.raises(TypeError):
            CrawlResult.success({"a": 1}, error_code=FailureCode.PARSE_INVALID_FORMAT.value)  # type: ignore[call-arg]

    def test_empty_has_no_error_code_argument(self):
        with pytest.raises(TypeError):
            CrawlResult.empty(error_code=FailureCode.PARSE_EMPTY.value)  # type: ignore[call-arg]

    def test_success_builder_leaves_the_code_empty(self):
        result = CrawlResult.success({"a": 1})

        assert result.error_code is None

    def test_empty_builder_leaves_the_code_empty(self):
        result = CrawlResult.empty()

        assert result.error_code is None

    def test_success_result_with_a_code_is_rejected(self):
        with pytest.raises(ValueError, match="cannot carry a failure error_code"):
            CrawlResult(outcome=CrawlOutcome.SUCCESS, error_code="FETCH_TIMEOUT")

    def test_empty_result_with_a_code_is_rejected(self):
        """`EMPTY` is "the target answered and there is no data", not a failure,
        so a code on it would turn an empty season into an alert.
        """
        with pytest.raises(ValueError, match="cannot carry a failure error_code"):
            CrawlResult(outcome=CrawlOutcome.EMPTY, error_code="PARSE_EMPTY")


class TestFailureCarriesTheCode:
    def test_failure_preserves_the_code(self):
        result = CrawlResult.failure(
            CrawlOutcome.RETRYABLE_ERROR,
            error="timed out",
            error_code=FailureCode.FETCH_TIMEOUT.value,
        )

        assert result.error_code == "FETCH_TIMEOUT"

    def test_failure_without_a_code_is_allowed(self):
        """Producers that predate the taxonomy still have to be normalizable,
        so `None` means "the producer did not classify this", not "not a failure".
        """
        result = CrawlResult.failure(CrawlOutcome.RETRYABLE_ERROR, error="timed out")

        assert result.error_code is None
        assert result.should_retry

    def test_failure_still_rejects_non_failure_outcomes(self):
        for outcome in (CrawlOutcome.SUCCESS, CrawlOutcome.EMPTY):
            with pytest.raises(ValueError, match="is not a failure outcome"):
                CrawlResult.failure(outcome, error="x")

    def test_to_dict_includes_the_code(self):
        """Evidence artifacts and logs are how a code is read after the fact."""
        result = CrawlResult.failure(
            CrawlOutcome.SCHEMA_CHANGED,
            error="expected JSON",
            error_code=FailureCode.PARSE_INVALID_FORMAT.value,
        )

        assert result.to_dict()["error_code"] == "PARSE_INVALID_FORMAT"

    def test_to_dict_reports_none_for_an_unclassified_failure(self):
        result = CrawlResult.failure(CrawlOutcome.PERMANENT_ERROR, error="nope")

        assert result.to_dict()["error_code"] is None
