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

Not every secret has a shape a pattern can recognise. A heartbeat ping URL is a
credential whose only distinguishing feature is that it exists, so callers
register the exact value with :func:`register_secret` and it is replaced
verbatim. That is equality, not inference: the caller knows what it handed to
``httpx``, and guessing which part of an opaque URL is secret is how half of it
stays in the log.
"""

from __future__ import annotations

import logging
import re
import threading

#: ``/bot<digits>:<secret>`` -> ``/bot<digits>:***``
_TELEGRAM_TOKEN = re.compile(r"/bot(\d+):[A-Za-z0-9_-]+")

_MASKED = r"/bot\1:***"

#: Literal substituted for a registered secret.
_MASK = "***"

#: Exact values to replace, longest first.
#:
#: Order matters: replacing ``abc`` before ``abcdef`` leaves ``***def`` in the
#: log, so a secret that is a prefix of another would leak its tail. Sorted at
#: registration rather than on every record, because ``mask_text`` runs for
#: every log line and the registry holds one or two entries.
_REGISTERED_SECRETS: tuple[str, ...] = ()

_REGISTERED_LOCK = threading.Lock()


def register_secret(value: str | None) -> None:
    """Mask ``value`` wherever it appears in a log record from now on.

    Registering the whole value rather than a pattern is the point: a ping URL
    is a credential with no fixed shape, and a pattern would have to guess which
    part is secret. The caller knows exactly what it put in the URL, so the mask
    is an equality substitution.

    Idempotent, because a job that pings every ten minutes would otherwise grow
    the tuple without bound.

    Args:
        value: The exact secret to mask. Empty or ``None`` is ignored, so a
            missing environment variable cannot turn every log line into a mask.

    """
    if not value:
        return
    global _REGISTERED_SECRETS  # noqa: PLW0603
    with _REGISTERED_LOCK:
        if value in _REGISTERED_SECRETS:
            return
        _REGISTERED_SECRETS = tuple(sorted((*_REGISTERED_SECRETS, value), key=len, reverse=True))


def mask_text(value: str) -> str:
    """Return ``value`` with any registered secret or Telegram bot token masked.

    Args:
        value: Text that may contain a secret.

    Returns:
        The text with each secret replaced by ``***``.

    """
    secrets = _REGISTERED_SECRETS
    if secrets:
        for secret in secrets:
            value = value.replace(secret, _MASK)
    if "bot" in value:
        return _TELEGRAM_TOKEN.sub(_MASKED, value)
    return value


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
