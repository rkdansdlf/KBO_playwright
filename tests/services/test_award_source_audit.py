"""Tests for the read-only awards source audit."""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

from src.services.award_source_audit import AwardAuditInput, audit_award_rows, build_award_audit


def _source_rows() -> list[SimpleNamespace]:
    return [
        SimpleNamespace(id=1, source_key="kbo_awards_wikipedia", is_active=True, last_success_at=None),
        SimpleNamespace(id=2, source_key="kbo_awards_yagoonara", is_active=True, last_success_at=None),
    ]


def test_empty_registered_sources_are_classified_as_missing_upstream() -> None:
    """Distinguish registered sources with no raw capture from parser failures."""
    report = build_award_audit(
        AwardAuditInput(
            award_rows=[],
            data_sources=_source_rows(),
            snapshots=[],
            player_ids=set(),
            team_codes=set(),
            probe_runs=[],
            current_year=2026,
        ),
    )

    assert report["status"] == "missing_upstream"
    assert report["raw_records"] == 0
    assert report["parsed_records"] is None
    assert report["ok"] is False


def test_raw_pending_snapshots_are_not_reported_as_stored_data() -> None:
    """Report captured but unprocessed snapshots as a downstream gap."""
    report = build_award_audit(
        AwardAuditInput(
            award_rows=[],
            data_sources=_source_rows(),
            snapshots=[
                SimpleNamespace(
                    data_source_id=1,
                    parse_status="pending",
                    fetched_at=datetime(2026, 8, 16, 10, 0),
                ),
            ],
            player_ids=set(),
            team_codes=set(),
            probe_runs=[],
            current_year=2026,
        ),
    )

    assert report["status"] == "parser_or_persistence_missing"
    assert report["source_details"][0]["parse_status_counts"] == {"pending": 1}


def test_probe_records_without_stored_rows_are_persistence_missing() -> None:
    """Use live parsed counts to isolate repository persistence failures."""
    report = build_award_audit(
        AwardAuditInput(
            award_rows=[],
            data_sources=_source_rows(),
            snapshots=[],
            player_ids=set(),
            team_codes=set(),
            probe_runs=[
                SimpleNamespace(
                    source_key="kbo_awards_wikipedia",
                    fetched=True,
                    parsed_records=495,
                    error=None,
                ),
            ],
            current_year=2026,
        ),
    )

    assert report["status"] == "persistence_missing"
    assert report["parsed_records"] == 495


def test_persisted_snapshot_metadata_reports_parsed_counts_without_probe() -> None:
    """Use parser metadata from saved snapshots when no live probe runs."""
    report = build_award_audit(
        AwardAuditInput(
            award_rows=[
                SimpleNamespace(
                    year=2024,
                    award_type="MVP",
                    category=None,
                    player_name="김선수",
                    team_name="LG",
                    player_id=1,
                    team_code="LG",
                ),
            ],
            data_sources=_source_rows(),
            snapshots=[
                SimpleNamespace(
                    data_source_id=1,
                    parse_status="done",
                    capture_metadata={"parsed_records": 12},
                    fetched_at=datetime(2026, 8, 16, 10, 0),
                ),
            ],
            player_ids={1},
            team_codes={"LG"},
            probe_runs=[],
            current_year=2026,
        ),
    )

    assert report["parsed_records"] == 12
    assert report["source_details"][0]["parsed_records"] == 12
    assert report["status"] == "healthy"


def test_award_quality_checks_natural_keys_years_and_links() -> None:
    """Count structural defects without treating nullable links as invalid."""
    rows = [
        SimpleNamespace(
            year=2024,
            award_type="MVP",
            category=None,
            player_name="김선수",
            team_name="LG",
            player_id=1,
            team_code="LG",
        ),
        SimpleNamespace(
            year=2024,
            award_type="MVP",
            category=None,
            player_name="김선수",
            team_name="LG",
            player_id=1,
            team_code="LG",
        ),
        SimpleNamespace(
            year=1981,
            award_type="MVP",
            category=None,
            player_name="오류",
            team_name="Unknown",
            player_id=999,
            team_code="BAD",
        ),
    ]

    quality = audit_award_rows(rows, player_ids={1}, team_codes={"LG"}, current_year=2026)

    assert quality["duplicate_natural_keys"] == 1
    assert quality["invalid_season_rows"] == 1
    assert quality["invalid_player_ids"] == 1
    assert quality["invalid_team_codes"] == 1
    assert quality["quality_ok"] is False
