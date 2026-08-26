"""Unit tests for src.config.dto."""

from __future__ import annotations

from src.config.dto import (
    AlertingConfig,
    ConfigValidationReport,
    CrawlerConfig,
    DatabaseConfig,
    EnvironmentType,
    ExternalApiConfig,
    PlatformSettings,
)


def test_environment_type_values() -> None:
    assert EnvironmentType.LOCAL == "local"
    assert EnvironmentType.DEVELOPMENT == "development"
    assert EnvironmentType.STAGING == "staging"
    assert EnvironmentType.PRODUCTION == "production"
    assert EnvironmentType.CI == "ci"


def test_database_config_masking() -> None:
    cfg = DatabaseConfig(
        url="oracle+oracledb://admin:secretpass123@db_host:1521/kbo",
        dialect="oracle",
    )
    d = cfg.to_dict(mask=True)
    assert d["url"] == "oracle+oracledb://***@db_host:1521/kbo"
    assert d["dialect"] == "oracle"


def test_alerting_config_masking() -> None:
    cfg = AlertingConfig(
        telegram_bot_token="123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11",
        slack_webhook_url="https://hooks.slack.com/services/T00/B00/X00",
    )
    d = cfg.to_dict(mask=True)
    assert d["telegram_bot_token"] == "***"
    assert d["slack_webhook_url"] == "***"


def test_platform_settings_to_dict() -> None:
    settings = PlatformSettings(
        env=EnvironmentType.PRODUCTION,
        debug=False,
        database=DatabaseConfig(url="sqlite:///:memory:"),
        crawler=CrawlerConfig(delay_min_seconds=1.5),
        alerting=AlertingConfig(),
        external_api=ExternalApiConfig(youtube_api_key="AIzaSyA12345"),
        feature_flags={"enable_live": True},
    )
    d = settings.to_dict(mask=True)
    assert d["env"] == "production"
    assert d["external_api"]["youtube_api_key"] == "***"
    assert d["feature_flags"]["enable_live"] is True


def test_config_validation_report_to_dict() -> None:
    rep = ConfigValidationReport(
        is_valid=False,
        missing_required_keys=["DATABASE_URL"],
        warnings=["No bot token"],
        checked_at="2026-08-27T00:00:00Z",
    )
    d = rep.to_dict()
    assert d["is_valid"] is False
    assert "DATABASE_URL" in d["missing_required_keys"]
