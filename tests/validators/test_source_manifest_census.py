"""Tests for Source Manifest & Schedule Census Engine."""

from __future__ import annotations

import pytest

from src.validators.source_manifest_census import (
    SourceCensusReport,
    compare_source_manifest_against_db,
)


class TestSourceManifestCensus:
    def test_perfect_match(self) -> None:
        source = {
            "20240401LGHH0": {"game_id": "20240401LGHH0", "status": "COMPLETED"},
            "20240401NCSS0": {"game_id": "20240401NCSS0", "status": "COMPLETED"},
        }
        db = [
            {"game_id": "20240401LGHH0", "status": "COMPLETED"},
            {"game_id": "20240401NCSS0", "status": "COMPLETED"},
        ]
        report = compare_source_manifest_against_db(source, db, year=2024)
        assert report.ok is True
        assert report.coverage_ratio == 1.0
        assert len(report.missing_in_db) == 0
        assert len(report.unexpected_in_db) == 0
        assert len(report.status_mismatches) == 0

    def test_missing_games_in_db_detected(self) -> None:
        source = {
            "20240401LGHH0": {"game_id": "20240401LGHH0", "status": "COMPLETED"},
            "20240401NCSS0": {"game_id": "20240401NCSS0", "status": "COMPLETED"},
        }
        db = [
            {"game_id": "20240401LGHH0", "status": "COMPLETED"},
        ]
        report = compare_source_manifest_against_db(source, db, year=2024)
        assert report.ok is False
        assert report.coverage_ratio == 0.5
        assert report.missing_in_db == ["20240401NCSS0"]
        assert len(report.unexpected_in_db) == 0

    def test_unexpected_ghost_games_in_db_detected(self) -> None:
        source = {
            "20240401LGHH0": {"game_id": "20240401LGHH0", "status": "COMPLETED"},
        }
        db = [
            {"game_id": "20240401LGHH0", "status": "COMPLETED"},
            {"game_id": "20240401GHOST0", "status": "COMPLETED"},
        ]
        report = compare_source_manifest_against_db(source, db, year=2024)
        assert report.ok is False
        assert report.unexpected_in_db == ["20240401GHOST0"]

    def test_status_mismatch_detected(self) -> None:
        source = {
            "20240401LGHH0": {"game_id": "20240401LGHH0", "status": "CANCELLED"},
        }
        db = [
            {"game_id": "20240401LGHH0", "status": "COMPLETED"},
        ]
        report = compare_source_manifest_against_db(source, db, year=2024)
        assert report.ok is False
        assert len(report.status_mismatches) == 1
        assert report.status_mismatches[0]["game_id"] == "20240401LGHH0"
