"""Tests for failure_diagnosis and crawler_selector_gate."""

from __future__ import annotations

import pytest

from src.monitoring.crawler_selector_gate import SelectorCheck, SelectorTarget
from src.monitoring.failure_diagnosis import DiagnosisFinding, DiagnosisReport, diagnose_text


class TestDiagnosisFinding:
    def test_to_dict(self) -> None:
        finding = DiagnosisFinding(
            category="selector",
            severity="high",
            message="Selector not found",
            evidence="div.scoreboard",
            suggested_commands=("Re-crawl page", "Check selector"),
        )
        d = finding.to_dict()
        assert d["category"] == "selector"
        assert d["severity"] == "high"
        assert len(d["suggested_commands"]) == 2


class TestDiagnoseText:
    def test_selector_error(self) -> None:
        text = "Timeout waiting for selector 'div.scoreboard'"
        result = diagnose_text(text)
        assert result is not None
        assert len(result.findings) > 0

    def test_auth_error(self) -> None:
        text = "Authentication failed: login required"
        result = diagnose_text(text)
        assert result is not None

    def test_database_error(self) -> None:
        text = "DB connection timeout after 30s"
        result = diagnose_text(text)
        assert result is not None

    def test_normal_line(self) -> None:
        text = "Successfully crawled game 20260624LGSS0"
        result = diagnose_text(text)
        assert result is not None


class TestSelectorCheck:
    def test_dataclass_creation(self) -> None:
        check = SelectorCheck(
            name="scoreboard",
            selector="div.scoreboard",
            min_count=1,
        )
        assert check.name == "scoreboard"
        assert check.selector == "div.scoreboard"
        assert check.min_count == 1
        assert check.max_count is None


class TestSelectorTarget:
    def test_dataclass_creation(self) -> None:
        target = SelectorTarget(
            name="kbo_scoreboard",
            source="https://www.koreabaseball.com",
            checks=[],
        )
        assert target.name == "kbo_scoreboard"
        assert target.source_type == "file"
