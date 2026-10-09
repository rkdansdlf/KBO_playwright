"""Tests for the backfill CLI's refusal rules.

The gap size is the one number that can turn this from a 127-chunk backfill
into a 188,000-chunk one that spends real money, and the failure looks exactly
like success: both are "identities the vector store does not have". These pin
the refusals that stand between those two outcomes.
"""

from __future__ import annotations

import pytest
from src.cli.rag.backfill_missing_vectors import (
    DEFAULT_MAX_GAP,
    EXIT_GAP_TOO_LARGE,
    gap_guard_message,
    main,
)


class TestGapGuard:
    """Pin when a gap is too large to embed without being told to."""

    def test_a_small_gap_is_allowed(self) -> None:
        """Let the expected 127-chunk gap through."""
        assert gap_guard_message(127, DEFAULT_MAX_GAP) is None

    def test_a_gap_equal_to_the_limit_is_allowed(self) -> None:
        """Treat the limit itself as allowed rather than exclusive."""
        assert gap_guard_message(DEFAULT_MAX_GAP, DEFAULT_MAX_GAP) is None

    def test_a_gap_beyond_the_limit_is_refused(self) -> None:
        """Refuse the shape a copy still in progress produces."""
        message = gap_guard_message(188014, DEFAULT_MAX_GAP)
        assert message is not None
        assert "188014" in message

    def test_the_refusal_names_the_way_out(self) -> None:
        """Say what to do rather than only what went wrong."""
        message = gap_guard_message(188014, DEFAULT_MAX_GAP)
        assert message is not None
        assert "--allow-large-gap" in message
        assert "--max-gap" in message


class TestWriteRefusals:
    """Pin the exit codes an operator sees."""

    def test_missing_urls_is_a_configuration_error(self) -> None:
        """Refuse to guess which store to talk to."""
        assert main([]) == 1
