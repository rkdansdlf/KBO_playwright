from __future__ import annotations

from unittest.mock import patch

from src.utils.sentry import init_sentry


class TestInitSentry:
    def test_skips_init_when_no_dsn(self, monkeypatch):
        monkeypatch.delenv("SENTRY_DSN", raising=False)
        with patch("src.utils.sentry.sentry_sdk") as mock_sdk:
            init_sentry()
            mock_sdk.init.assert_not_called()

    def test_initializes_with_dsn(self, monkeypatch):
        monkeypatch.setenv("SENTRY_DSN", "https://test@sentry.io/123")
        monkeypatch.setenv("SENTRY_ENVIRONMENT", "staging")
        with patch("src.utils.sentry.sentry_sdk") as mock_sdk:
            init_sentry()
            mock_sdk.init.assert_called_once_with(
                dsn="https://test@sentry.io/123",
                environment="staging",
                traces_sample_rate=1.0,
            )

    def test_handles_init_exception(self, monkeypatch):
        monkeypatch.setenv("SENTRY_DSN", "https://test@sentry.io/123")
        with patch("src.utils.sentry.sentry_sdk") as mock_sdk:
            mock_sdk.init.side_effect = RuntimeError("init failed")
            init_sentry()
            mock_sdk.init.assert_called_once()

    def test_default_environment_is_production(self, monkeypatch):
        monkeypatch.setenv("SENTRY_DSN", "https://test@sentry.io/456")
        monkeypatch.delenv("SENTRY_ENVIRONMENT", raising=False)
        with patch("src.utils.sentry.sentry_sdk") as mock_sdk:
            init_sentry()
            mock_sdk.init.assert_called_once_with(
                dsn="https://test@sentry.io/456",
                environment="production",
                traces_sample_rate=1.0,
            )
