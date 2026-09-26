"""Unified Configuration Management and Validation package."""

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
from src.config.env_loader import (
    ENV_FILE_LOADING_FLAG,
    env_file_loading_enabled,
    load_project_env,
)
from src.config.manager import ConfigManager

__all__ = [
    "ENV_FILE_LOADING_FLAG",
    "AlertingConfig",
    "ConfigManager",
    "ConfigValidationReport",
    "CrawlerConfig",
    "DatabaseConfig",
    "EnvironmentType",
    "ExternalApiConfig",
    "PlatformSettings",
    "env_file_loading_enabled",
    "load_project_env",
]
