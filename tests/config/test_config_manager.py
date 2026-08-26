"""Unit tests for src.config.manager."""

from __future__ import annotations

from typing import TYPE_CHECKING

from src.config.dto import EnvironmentType
from src.config.manager import ConfigManager

if TYPE_CHECKING:
    import pytest


def test_config_manager_load_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ENVIRONMENT", "production")
    monkeypatch.setenv("DATABASE_URL", "sqlite:///:memory:")
    monkeypatch.setenv("KBO_REQUEST_DELAY_MIN", "2.5")

    settings = ConfigManager.load_settings(force_reload=True)
    assert settings.env == EnvironmentType.PRODUCTION
    assert settings.crawler.delay_min_seconds == 2.5


def test_config_manager_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATABASE_URL", raising=False)
    report = ConfigManager.validate_environment(target_env=EnvironmentType.PRODUCTION)
    assert report.is_valid is False
    assert "DATABASE_URL" in report.missing_required_keys


def test_config_manager_feature_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FEATURE_ENABLE_LIVE_CRAWL", "true")
    ConfigManager.load_settings(force_reload=True)
    assert ConfigManager.get_feature_flag("enable_live_crawl") is True


def test_config_manager_mask_secrets() -> None:
    masked = ConfigManager.mask_secrets()
    assert isinstance(masked, dict)
    assert "database" in masked
    assert "crawler" in masked
