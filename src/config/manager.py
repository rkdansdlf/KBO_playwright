"""Unified Configuration Manager and Environment Validator."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any

from src.config.dto import (
    AlertingConfig,
    ConfigValidationReport,
    CrawlerConfig,
    DatabaseConfig,
    EnvironmentType,
    ExternalApiConfig,
    PlatformSettings,
)


class ConfigManager:
    """Manages application settings, environment validation, and feature flags."""

    _instance: PlatformSettings | None = None

    @classmethod
    def load_settings(
        cls,
        overrides: dict[str, Any] | None = None,
        *,
        force_reload: bool = False,
    ) -> PlatformSettings:
        """Load typed configuration settings from environment variables."""
        if cls._instance is not None and not force_reload and not overrides:
            return cls._instance

        env_str = os.getenv("ENVIRONMENT", "local").lower()
        try:
            env = EnvironmentType(env_str)
        except ValueError:
            env = EnvironmentType.LOCAL

        debug = os.getenv("DEBUG", "false").lower() in ("true", "1", "yes")

        # Database
        db_url = os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
        dialect = "oracle" if "oracle" in db_url else "sqlite"
        tns_admin = os.getenv("TNS_ADMIN")
        db_config = DatabaseConfig(
            url=db_url,
            dialect=dialect,
            tns_admin=tns_admin,
            pool_size=int(os.getenv("DB_POOL_SIZE", "5")),
            max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "10")),
            echo=debug,
        )

        # Crawler
        delay_min = float(os.getenv("KBO_REQUEST_DELAY_MIN", "1.0"))
        delay_max = float(os.getenv("KBO_REQUEST_DELAY_MAX", "3.0"))
        crawler_config = CrawlerConfig(
            delay_min_seconds=delay_min,
            delay_max_seconds=delay_max,
            headless=os.getenv("CRAWLER_HEADLESS", "true").lower() in ("true", "1", "yes"),
            timeout_seconds=int(os.getenv("CRAWLER_TIMEOUT_SECONDS", "30")),
            concurrency=int(os.getenv("CRAWLER_CONCURRENCY", "3")),
        )

        # Alerting
        alert_config = AlertingConfig(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
            telegram_chat_id=os.getenv("TELEGRAM_CHAT_ID"),
            slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL"),
            alert_threshold=int(os.getenv("ALERT_THRESHOLD", "5")),
        )

        # External APIs
        api_config = ExternalApiConfig(
            youtube_api_key=os.getenv("YOUTUBE_API_KEY"),
            naver_client_id=os.getenv("NAVER_CLIENT_ID"),
            naver_client_secret=os.getenv("NAVER_CLIENT_SECRET"),
        )

        # Feature flags
        feature_flags = {
            "enable_live_crawl": os.getenv("FEATURE_ENABLE_LIVE_CRAWL", "true").lower() in ("true", "1"),
            "enable_oci_sync": os.getenv("FEATURE_ENABLE_OCI_SYNC", "true").lower() in ("true", "1"),
            "enable_rag_search": os.getenv("FEATURE_ENABLE_RAG_SEARCH", "true").lower() in ("true", "1"),
        }

        settings = PlatformSettings(
            env=env,
            debug=debug,
            database=db_config,
            crawler=crawler_config,
            alerting=alert_config,
            external_api=api_config,
            feature_flags=feature_flags,
        )

        if overrides:
            for k, v in overrides.items():
                if hasattr(settings, k):
                    setattr(settings, k, v)

        cls._instance = settings
        return settings

    @classmethod
    def validate_environment(
        cls,
        target_env: str | EnvironmentType = EnvironmentType.PRODUCTION,
    ) -> ConfigValidationReport:
        """Validate required secrets and configuration keys for a target environment."""
        target = EnvironmentType(target_env) if isinstance(target_env, str) else target_env
        missing: list[str] = []
        warnings: list[str] = []

        db_url = os.getenv("DATABASE_URL", "")

        if target in (EnvironmentType.PRODUCTION, EnvironmentType.CI):
            if not db_url:
                missing.append("DATABASE_URL")
            elif "sqlite" in db_url and target == EnvironmentType.PRODUCTION:
                warnings.append("DATABASE_URL is configured for SQLite in production environment.")

            if "oracle" in db_url and not os.getenv("TNS_ADMIN"):
                warnings.append("Oracle dialect detected but TNS_ADMIN environment variable is unset.")

            if not os.getenv("TELEGRAM_BOT_TOKEN"):
                warnings.append("TELEGRAM_BOT_TOKEN is unset (notifications will fall back to console).")

        is_valid = len(missing) == 0

        return ConfigValidationReport(
            is_valid=is_valid,
            missing_required_keys=missing,
            warnings=warnings,
            checked_at=datetime.now(UTC).isoformat(),
        )

    @classmethod
    def mask_secrets(cls, settings: PlatformSettings | None = None) -> dict[str, Any]:
        """Return a dictionary of settings with secrets safely masked."""
        s = settings or cls.load_settings()
        return s.to_dict(mask=True)

    @classmethod
    def get_feature_flag(cls, flag_name: str, *, default: bool = False) -> bool:
        """Get the boolean value of a feature flag."""
        settings = cls.load_settings()
        return settings.feature_flags.get(flag_name, default)
