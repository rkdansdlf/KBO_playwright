"""The ledger speaks three dialects, and the cost of that was a wrong conclusion.

`source_limited` was searched for in `status`, found zero times, and read as "the
fix is not deployed" -- when 90 rows carried it under `checkpoint.outcome`. A
second search then hardcoded the retired `FETCH_BLOCKED` and missed the live
spelling. Both failures came from the vocabulary living nowhere.
"""

from __future__ import annotations

import json

from src.crawlers.run_vocabulary import (
    EXPLANATIONS,
    CheckpointOutcome,
    RunStatus,
    SourceLimitReason,
    explain,
    is_refusal,
    is_source_limited,
    parse_checkpoint,
    run_facts,
    vocabulary,
)


class TestRefusalDetection:
    def test_a_source_limited_run_is_a_refusal(self) -> None:
        checkpoint = {"outcome": "source_limited", "reason": "kbo_robots_blocked"}

        assert is_refusal(checkpoint=checkpoint) is True
        assert is_source_limited(checkpoint) is True

    def test_a_checkpoint_stored_as_a_json_string_is_still_read(self) -> None:
        """The column arrives as text on some paths and as a mapping on others."""
        assert is_source_limited(json.dumps({"outcome": "source_limited"})) is True

    def test_a_checkpoint_stored_as_bytes_is_still_read(self) -> None:
        assert is_source_limited(b'{"outcome": "source_limited"}') is True

    def test_the_retired_error_code_is_still_a_refusal(self) -> None:
        """`food_crawler` still writes this one; dropping it would hide the
        disagreement between the report's verdict and the ledger.
        """
        assert is_refusal(error_code="FETCH_BLOCKED") is True

    def test_an_outage_is_not_a_refusal(self) -> None:
        """The remedy differs and conflating them puts a block in the retry queue."""
        assert is_refusal(error_code="FETCH_TIMEOUT") is False
        assert is_refusal(checkpoint={"outcome": "retried"}) is False

    def test_an_empty_or_absent_checkpoint_is_not_a_refusal(self) -> None:
        for value in (None, "", "   ", b"", {}, "not json"):
            assert is_refusal(checkpoint=value) is False, value

    def test_a_success_that_collected_nothing_is_not_a_refusal_by_itself(self) -> None:
        """It could be an empty page, or a silent block this build never named."""
        assert is_refusal(checkpoint={"outcome": None}) is False


class TestExplain:
    def test_every_named_value_explains_itself(self) -> None:
        """A table entry without a sentence is a value nobody can look up."""
        for column, values in vocabulary().items():
            for value, meaning in values.items():
                assert meaning, f"{column}={value}"
                assert explain(column, value) == meaning

    def test_a_success_explains_where_the_payload_is(self) -> None:
        """A reader asking "why did it succeed and collect nothing" gets a pointer."""
        assert "checkpoint" in explain("status", "success")

    def test_an_unknown_value_returns_nothing_rather_than_a_guess(self) -> None:
        """Silence sends the reader looking; a confident wrong answer stops them."""
        assert explain("status", "almost_success") is None
        assert explain("checkpoint.reason", "some_new_reason") is None
        assert explain("no_such_column", "x") is None

    def test_a_missing_value_does_not_crash(self) -> None:
        assert explain("status", None) is None


class TestRunFacts:
    def test_a_refused_success_is_terminal_and_collected_nothing(self) -> None:
        facts = run_facts(
            {
                "status": "success",
                "error_code": None,
                "checkpoint": {"outcome": "source_limited"},
                "records_read": 0,
                "records_written": 0,
            }
        )

        assert facts["refused"] is True
        assert facts["collected"] is False
        assert facts["terminal"] is True

    def test_a_successful_collecting_run_is_not_a_refusal(self) -> None:
        facts = run_facts(
            {"status": "success", "error_code": None, "checkpoint": None, "records_read": 40, "records_written": 40}
        )

        assert facts["refused"] is False
        assert facts["collected"] is True

    def test_a_run_that_read_but_wrote_nothing_still_collected(self) -> None:
        facts = run_facts(
            {"status": "success", "error_code": None, "checkpoint": None, "records_read": 7, "records_written": 0}
        )

        assert facts["collected"] is True


class TestVocabularyIsComplete:
    def test_the_live_refusal_spelling_is_named(self) -> None:
        """The omission that caused the wrong conclusion, pinned."""
        assert CheckpointOutcome.SOURCE_LIMITED.value in EXPLANATIONS["checkpoint.outcome"]

    def test_the_reason_is_named(self) -> None:
        assert SourceLimitReason.KBO_ROBOTS_BLOCKED.value in EXPLANATIONS["checkpoint.reason"]

    def test_every_status_is_named(self) -> None:
        for status in RunStatus:
            assert status.value in EXPLANATIONS["status"], status

    def test_the_terms_are_the_ones_the_database_uses(self) -> None:
        """These strings are a contract with existing rows, so they are asserted
        literally rather than derived from the enum's names.
        """
        assert RunStatus.SUCCESS.value == "success"
        assert CheckpointOutcome.SOURCE_LIMITED.value == "source_limited"
        assert SourceLimitReason.KBO_ROBOTS_BLOCKED.value == "kbo_robots_blocked"
