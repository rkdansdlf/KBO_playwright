"""Tests for key crawler utility functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.crawlers.team_info_crawler import TeamInfoCrawler
from src.utils.team_codes import normalize_kbo_game_id, resolve_team_code


class TestNormalizeKboGameId:
    def test_standard_format(self) -> None:
        result = normalize_kbo_game_id("20260624LGSS0")
        assert result == "20260624LGSS0"

    def test_with_dash(self) -> None:
        result = normalize_kbo_game_id("2026-06-24-LG-SS")
        assert "LG" in result
        assert "SS" in result

    def test_lowercase(self) -> None:
        result = normalize_kbo_game_id("20260624lgss0")
        assert result == "20260624LGSS0"


class TestResolveTeamCode:
    def test_lg_code(self) -> None:
        assert resolve_team_code("LG") == "LG"

    def test_ssg_code(self) -> None:
        assert resolve_team_code("SSG") == "SSG"

    def test_alternative_code(self) -> None:
        assert resolve_team_code("WO") == "WO"

    def test_unknown_code(self) -> None:
        result = resolve_team_code("INVALID")
        assert result is None
