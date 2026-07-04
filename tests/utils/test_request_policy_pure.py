"""Unit tests for request_policy."""

from __future__ import annotations

import pytest

from src.utils.request_policy import (
    RequestPolicy,
    RequestPolicyConfig,
)

_ENV_KEYS = ("KBO_REQUEST_DELAY_MIN", "KBO_REQUEST_DELAY_MAX", "KBO_REQUEST_MAX_RETRIES")


class TestRequestPolicyConfig:
    def test_defaults(self) -> None:
        config = RequestPolicyConfig()
        assert config.min_delay is None
        assert config.max_delay is None
        assert config.max_retries is None

    def test_custom_values(self) -> None:
        config = RequestPolicyConfig(min_delay=1.0, max_delay=5.0, max_retries=3)
        assert config.min_delay == 1.0
        assert config.max_delay == 5.0
        assert config.max_retries == 3


class TestRequestPolicy:
    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for key in _ENV_KEYS:
            monkeypatch.delenv(key, raising=False)
        yield

    def test_default_init(self) -> None:
        policy = RequestPolicy()
        assert policy.min_delay >= 0
        assert policy.max_delay >= 0
        assert policy.max_retries >= 0

    def test_config_init(self) -> None:
        config = RequestPolicyConfig(min_delay=1.0, max_delay=5.0, max_retries=3)
        policy = RequestPolicy(config=config)
        assert policy.max_retries == 3

    def test_config_values(self) -> None:
        config = RequestPolicyConfig(max_retries=5)
        policy = RequestPolicy(config=config)
        assert policy.max_retries == 5

    def test_frozen_config(self) -> None:
        config = RequestPolicyConfig(min_delay=1.0)
        with pytest.raises(AttributeError):
            config.min_delay = 2.0
