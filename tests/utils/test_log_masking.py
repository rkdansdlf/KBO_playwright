"""Contract tests for masking secrets that reach the log through a request URL.

``httpx`` logs the full request URL at INFO, and the Telegram send URL carries
the bot token as a path segment. Every alert therefore wrote the token into the
log, and anyone holding the log held the bot.
"""

from __future__ import annotations

import logging

import pytest

from src.utils.log_masking import (
    SecretMaskingFilter,
    install_secret_masking,
    mask_text,
)

_SEND_URL = "https://api.telegram.org/bot8755432405:AAFjwy00x03IrJG42ZcO3CtF4ZwvIuuCCSo/sendMessage"


class TestMaskingText:
    """The secret is the part after the colon; the id names the bot."""

    def test_a_send_url_is_masked(self) -> None:
        assert mask_text(_SEND_URL) == "https://api.telegram.org/bot8755432405:***/sendMessage"

    def test_the_bot_id_survives(self) -> None:
        # A masked line still has to say which bot was addressed, or an operator
        # cannot tell two bots apart from the log alone.
        assert "bot8755432405" in mask_text(_SEND_URL)

    def test_the_secret_is_gone(self) -> None:
        assert "AAFjwy00x03IrJG42ZcO3CtF4ZwvIuuCCSo" not in mask_text(_SEND_URL)

    def test_another_url_is_untouched(self) -> None:
        url = "https://api-gw.sports.naver.com/schedule/games?fromDate=2026-10-27"

        assert mask_text(url) == url

    def test_text_without_bot_is_returned_unchanged(self) -> None:
        assert mask_text("plain message") == "plain message"


class TestTheRecordIsMaskedWhereverItComesFrom:
    """A filter on the root logger would never see an ``httpx`` record.

    Filters run for records their own logger creates or their handler emits --
    not for records a logger propagates. ``httpx`` creates the record, so the
    mask has to sit upstream of the logger entirely.
    """

    @pytest.fixture(autouse=True)
    def _factories(self) -> None:
        """Keep the global factory from leaking between tests."""
        original = logging.getLogRecordFactory()
        yield
        logging.setLogRecordFactory(original)

    def test_a_propagated_record_is_masked(self, caplog: pytest.LogCaptureFixture) -> None:
        install_secret_masking()

        with caplog.at_level(logging.INFO, logger="httpx"):
            logging.getLogger("httpx").info("HTTP Request: POST %s", _SEND_URL)

        assert "AAFjwy00x03IrJG42ZcO3CtF4ZwvIuuCCSo" not in caplog.text
        assert "bot8755432405:***" in caplog.text

    def test_a_token_in_an_argument_is_masked(self) -> None:
        install_secret_masking()
        record = SecretMaskingFilter()
        built = logging.LogRecord(
            "httpx",
            logging.INFO,
            __file__,
            1,
            "HTTP Request: %s %s",
            (_SEND_URL, "HTTP/1.1"),
            None,
        )

        record.filter(built)

        assert "AAFjwy00x03IrJG42ZcO3CtF4ZwvIuuCCSo" not in built.getMessage()

    def test_a_non_string_argument_is_left_alone(self) -> None:
        # ``%d`` would raise if the tuple were rewritten to strings.
        install_secret_masking()
        record = SecretMaskingFilter()
        built = logging.LogRecord("httpx", logging.INFO, __file__, 1, "HTTP %d for %s", (200, _SEND_URL), None)

        record.filter(built)

        assert built.getMessage() == "HTTP 200 for https://api.telegram.org/bot8755432405:***/sendMessage"

    def test_installing_twice_does_not_stack(self) -> None:
        install_secret_masking()
        first = logging.getLogRecordFactory()

        install_secret_masking()

        assert logging.getLogRecordFactory() is first

    def test_the_mask_survives_a_later_basic_config(self, caplog: pytest.LogCaptureFixture) -> None:
        # The scheduler configures logging after importing its jobs, so a mask
        # that lives on handlers would miss everything from then on.
        install_secret_masking()
        logging.basicConfig(level=logging.INFO, force=True)

        with caplog.at_level(logging.INFO, logger="httpx"):
            logging.getLogger("httpx").info("HTTP Request: POST %s", _SEND_URL)

        assert "AAFjwy00x03IrJG42ZcO3CtF4ZwvIuuCCSo" not in caplog.text
