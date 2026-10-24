"""Unit tests for milestone crawl feature flags."""

from __future__ import annotations

import pytest

from src.config.milestone_flags import MILESTONE_CRAWL_ENABLED_ENV, milestone_crawl_enabled


def test_milestone_crawl_enabled_defaults_to_false(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the environment variable is not set, milestone crawling is disabled."""
    monkeypatch.delenv(MILESTONE_CRAWL_ENABLED_ENV, raising=False)
    assert milestone_crawl_enabled() is False


@pytest.mark.parametrize(
    "falsy_val",
    [
        "0",
        "false",
        "False",
        "no",
        "NO",
        "off",
        "OFF",
        "  0  ",
        "  off  ",
        # QA: an empty/blank value (`MILESTONE_CRAWL_ENABLED=`) used to enable the crawl.
        "",
        "   ",
        "\t",
        # Unknown values are off too (allowlist, not denylist).
        "enabled",
        "2",
        "y",
        "tru",
        "disabled",
    ],
)
def test_milestone_crawl_enabled_falsy_values(monkeypatch: pytest.MonkeyPatch, falsy_val: str) -> None:
    """Explicitly recognized falsy strings disable milestone crawling."""
    monkeypatch.setenv(MILESTONE_CRAWL_ENABLED_ENV, falsy_val)
    assert milestone_crawl_enabled() is False


@pytest.mark.parametrize("truthy_val", ["1", "true", "True", "TRUE", "yes", "YES", "on", "ON", " 1 ", "  true\n"])
def test_milestone_crawl_enabled_truthy_values(monkeypatch: pytest.MonkeyPatch, truthy_val: str) -> None:
    """Truthy strings enable milestone crawling."""
    monkeypatch.setenv(MILESTONE_CRAWL_ENABLED_ENV, truthy_val)
    assert milestone_crawl_enabled() is True
