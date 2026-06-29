from __future__ import annotations

import os
from unittest.mock import patch

from src.utils.sentry import init_sentry


def test_init_sentry_disabled_by_default() -> None:
    """Verify that Sentry is not initialized if SENTRY_DSN is not in the environment."""
    with patch.dict(os.environ, {}, clear=True), patch("sentry_sdk.init") as mock_init:
        init_sentry()
        mock_init.assert_not_called()


def test_init_sentry_enabled_with_dsn() -> None:
    """Verify that Sentry is initialized when SENTRY_DSN is configured."""
    mock_env = {
        "SENTRY_DSN": "https://dummy_dsn@o0.ingest.sentry.io/0",
        "SENTRY_ENVIRONMENT": "testing",
    }
    with patch.dict(os.environ, mock_env, clear=True), patch("sentry_sdk.init") as mock_init:
        init_sentry()
        mock_init.assert_called_once_with(
            dsn="https://dummy_dsn@o0.ingest.sentry.io/0",
            environment="testing",
            traces_sample_rate=1.0,
        )
