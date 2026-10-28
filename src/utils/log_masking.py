"""Mask secrets that would otherwise reach the log through a request URL.

``httpx`` logs the full request URL at INFO. The Telegram send URL carries the
bot token as a path segment -- ``/bot<id>:<secret>/sendMessage`` -- so every
alert writes the token into the log, and anyone holding the log holds the bot.

A logging filter is the only place that sees every record regardless of which
logger emitted it or which formatter renders it, so the mask lives there rather
than at each call site. Masking at the call site would also mean teaching the
fourth caller about the rule, and the fourth caller is exactly the one that
forgets.

The id before the colon is left in place: it names the bot and is not the
secret. Only the part after it is replaced, so a masked line still says which
bot was addressed.
"""

from __future__ import annotations

import logging
import re

#: ``/bot<digits>:<secret>`` -> ``/bot<digits>:***``
_TELEGRAM_TOKEN = re.compile(r"/bot(\d+):[A-Za-z0-9_-]+")

_MASKED = r"/bot\1:***"


def mask_text(value: str) -> str:
    """Return ``value`` with any Telegram bot token masked.

    Args:
        value: Text that may contain a send URL.

    Returns:
        The text with the secret part replaced by ``***``.

    """
    return _TELEGRAM_TOKEN.sub(_MASKED, value) if "bot" in value else value


def _mask_argument(value: object) -> object:
    """Mask a logging argument, leaving non-strings untouched.

    Args:
        value: One element of ``record.args``.

    Returns:
        The masked string, or the value unchanged.

    """
    if isinstance(value, str):
        return mask_text(value)
    return value


class SecretMaskingFilter(logging.Filter):
    """Rewrite records so a token in the message or its arguments is masked."""

    def filter(self, record: logging.LogRecord) -> bool:
        """Mask the record in place and keep it.

        Args:
            record: The record about to be emitted.

        Returns:
            Always ``True`` -- this filter rewrites, it never drops.

        """
        record.msg = mask_text(str(record.msg))
        if record.args:
            if isinstance(record.args, dict):
                record.args = {key: _mask_argument(value) for key, value in record.args.items()}
            else:
                record.args = tuple(_mask_argument(value) for value in record.args)
        return True


_FACTORY_MARKER = "_kbo_secret_masking"


def install_secret_masking() -> None:
    """Mask every log record, at the point records are made.

    A filter runs for records its own logger creates, or for records a handler
    emits -- never for records a logger merely propagates. ``httpx`` makes the
    record, so a filter on the root logger would never see it; putting one on
    every handler would miss whatever handler is added next, including by a
    ``basicConfig`` that runs after this call. The record factory is upstream of
    all of it and survives later handler changes.

    Idempotent, because importing the alerting layer more than once -- which
    happens in tests and in the scheduler's long-lived process -- would
    otherwise stack a wrapper per import.
    """
    factory = logging.getLogRecordFactory()
    if getattr(factory, _FACTORY_MARKER, False):
        return

    masking = SecretMaskingFilter()

    def masked_factory(*args: object, **kwargs: object) -> logging.LogRecord:
        record = factory(*args, **kwargs)
        masking.filter(record)
        return record

    setattr(masked_factory, _FACTORY_MARKER, True)
    logging.setLogRecordFactory(masked_factory)
