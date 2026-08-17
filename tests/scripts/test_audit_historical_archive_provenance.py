"""Tests for historical archive provenance classification."""

from __future__ import annotations

import json

from scripts.converters.convert_kbo_archive_records import generate_season_dataset
from scripts.verification.audit_historical_archive_provenance import audit_archive_provenance, main


def test_generated_archive_marks_all_fields_as_synthetic() -> None:
    """Generated payloads must identify themselves as non-factual fixtures."""
    payload = generate_season_dataset(1982)

    assert payload["provenance"]["data_class"] == "synthetic_fixture"
    assert payload["provenance"]["verified"] is False
    assert all(source.startswith("synthetic:") for source in payload["provenance"]["field_sources"].values())


def test_audit_reports_generated_counts_and_ingest_block() -> None:
    """The audit must expose counts and mark generated output unsafe to ingest."""
    report = audit_archive_provenance(1982, 1982)

    assert report.season_game_counts == {"1982": 240}
    assert report.safe_for_historical_ingest is False
    assert "games.away_score" in report.field_sources
    assert report.findings


def test_audit_cli_emits_json_and_nonzero_for_synthetic_data(capsys) -> None:
    """The verification CLI must make synthetic status machine-readable."""
    assert main(["--start-year", "1982", "--end-year", "1982", "--json"]) == 1

    output = json.loads(capsys.readouterr().out)
    assert output["data_class"] == "synthetic_fixture"
    assert output["safe_for_historical_ingest"] is False
