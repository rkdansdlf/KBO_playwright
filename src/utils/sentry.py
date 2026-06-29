"""Sentry integration for error reporting in KBO scheduler and CLI."""

from __future__ import annotations

import logging
import os

import sentry_sdk

logger = logging.getLogger(__name__)


def init_sentry() -> None:
    """Initialize the Sentry SDK if SENTRY_DSN is configured in the environment."""
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        logger.info("Sentry DSN not configured. Sentry integration is disabled.")
        return

    env = os.getenv("SENTRY_ENVIRONMENT", "production")

    try:
        sentry_sdk.init(
            dsn=dsn,
            environment=env,
            traces_sample_rate=1.0,
        )
        logger.info("Sentry SDK initialized successfully for environment: %s", env)
    except Exception:
        logger.exception("Failed to initialize Sentry SDK")
