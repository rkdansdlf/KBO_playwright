from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src.cli.retry_daily_failures import (
    _dedupe_game_ids,
    _detail_groups,
    build_retry_commands,
    load_daily_summary,
    retry_candidates,
)


class TestLoadDailySummary:
    def test_file_not_found(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError, match="Daily summary not found"):
            load_daily_summary(tmp_path / "nonexistent.json")

    def test_malformed_json(self, tmp_path: Path) -> None:
        (tmp_path / "bad.json").write_text("{invalid", encoding="utf-8")
        with pytest.raises(ValueError, match="Malformed daily summary JSON"):
            load_daily_summary(tmp_path / "bad.json")

    def test_not_dict(self, tmp_path: Path) -> None:
        (tmp_path / "array.json").write_text("[1, 2, 3]", encoding="utf-8")
        with pytest.raises(TypeError, match="must be a JSON object"):
            load_daily_summary(tmp_path / "array.json")

    def test_missing_stability(self, tmp_path: Path) -> None:
        (tmp_path / "nostab.json").write_text('{"data": {}}', encoding="utf-8")
        with pytest.raises(TypeError, match="missing stability payload"):
            load_daily_summary(tmp_path / "nostab.json")

    def test_valid_summary(self, tmp_path: Path) -> None:
        payload = {"stability": {"retry_candidates": {"detail": [], "relay": []}}}
        (tmp_path / "valid.json").write_text(json.dumps(payload), encoding="utf-8")
        result = load_daily_summary(tmp_path / "valid.json")
        assert result == payload


class TestDedupeGameIds:
    def test_empty_list(self) -> None:
        assert _dedupe_game_ids([]) == []

    def test_none_values(self) -> None:
        assert _dedupe_game_ids([None, "", "  "]) == []

    def test_duplicates(self) -> None:
        result = _dedupe_game_ids(["20250401LGSS0", "20250401LGSS0"])
        assert result == ["20250401LGSS0"]

    def test_mixed_valid_invalid(self) -> None:
        result = _dedupe_game_ids(["20250401LGSS0", None, "", "20250402LGSS0"])
        assert "20250401LGSS0" in result
        assert "20250402LGSS0" in result


class TestRetryCandidates:
    def test_non_mapping_stability(self) -> None:
        detail, relay = retry_candidates({"stability": "not_a_dict"})
        assert detail == []
        assert relay == []

    def test_non_mapping_retry(self) -> None:
        detail, relay = retry_candidates({"stability": {"retry_candidates": None}})
        assert detail == []
        assert relay == []

    def test_non_list_detail(self) -> None:
        detail, relay = retry_candidates({"stability": {"retry_candidates": {"detail": "not_list", "relay": []}}})
        assert detail == []
        assert relay == []

    def test_valid_candidates(self) -> None:
        summary = {
            "stability": {
                "retry_candidates": {
                    "detail": ["20250401LGSS0", "20250402LGHH0"],
                    "relay": ["20250401LGSS0"],
                },
            },
        }
        detail, relay = retry_candidates(summary)
        assert "20250401LGSS0" in detail
        assert "20250402LGHH0" in detail
        assert "20250401LGSS0" in relay


class TestDetailGroups:
    def test_invalid_game_id_short(self) -> None:
        with pytest.raises(ValueError, match="Invalid KBO game_id date prefix"):
            _detail_groups(["short"])

    def test_invalid_game_id_non_digit(self) -> None:
        with pytest.raises(ValueError, match="Invalid KBO game_id date prefix"):
            _detail_groups(["ABCDEFGH0"])

    def test_invalid_month_zero(self) -> None:
        with pytest.raises(ValueError, match="Invalid KBO game_id month"):
            _detail_groups(["20250001LGSS0"])

    def test_invalid_month_13(self) -> None:
        with pytest.raises(ValueError, match="Invalid KBO game_id month"):
            _detail_groups(["20251301LGSS0"])

    def test_groups_by_year_month(self) -> None:
        result = _detail_groups(["20250401LGSS0", "20250501LGSS0", "20240401LGSS0"])
        assert (2025, 4) in result
        assert (2025, 5) in result
        assert (2024, 4) in result
        assert result[(2025, 4)] == ["20250401LGSS0"]


class TestBuildRetryCommands:
    def test_empty_no_commands(self) -> None:
        summary: dict[str, Any] = {"stability": {"retry_candidates": {"detail": [], "relay": []}}}
        commands = build_retry_commands(summary)
        assert commands == []

    def test_sync_branch(self) -> None:
        summary = {
            "stability": {
                "retry_candidates": {
                    "detail": ["20250401LGSS0"],
                    "relay": [],
                },
            },
        }
        commands = build_retry_commands(summary, sync=True)
        assert len(commands) == 2
        assert "sync_oci" in commands[1][2]

    def test_relay_command(self) -> None:
        summary = {
            "stability": {
                "retry_candidates": {
                    "detail": [],
                    "relay": ["20250401LGSS0"],
                },
            },
        }
        commands = build_retry_commands(summary)
        assert len(commands) == 1
        assert "fetch_kbo_pbp.py" in commands[0][1]
