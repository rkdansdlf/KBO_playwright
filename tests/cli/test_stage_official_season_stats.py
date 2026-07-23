from __future__ import annotations

from types import SimpleNamespace

from src.cli.stage_official_season_stats import StageRows, build_stage_report


def _batting_row(team_id: str, **values: int) -> dict:
    row = {"team_id": team_id, "games": 144}
    row.update(values)
    return row


def _pitching_row(team_id: str, **values: int) -> dict:
    row = {"team_id": team_id, "games": 144, "innings_outs": 10}
    row.update(values)
    return row


def test_stage_report_is_ready_when_sources_reconcile():
    team_batting = [_batting_row("LG", plate_appearances=10, at_bats=8, hits=2)]
    player_batting = [{"team_code": "LG", "plate_appearances": 10, "at_bats": 8, "hits": 2}]
    team_pitching = [_pitching_row("LG", hits_allowed=2, runs_allowed=1, earned_runs=1)]
    player_pitching = [
        SimpleNamespace(
            team_code="LG",
            innings_outs=10,
            hits_allowed=2,
            runs_allowed=1,
            earned_runs=1,
            home_runs_allowed=0,
            walks_allowed=0,
            strikeouts=0,
            era=2.7,
            player_id=1,
        ),
    ]

    report = build_stage_report(
        2025,
        StageRows(
            team_batting=team_batting,
            team_pitching=team_pitching,
            player_batting=player_batting,
            player_pitching=player_pitching,
            expected_team_ids={"LG"},
        ),
        current_year=2026,
    )

    assert report["ready_for_sync"] is True
    assert report["invalid_era_rows"] == []
    assert report["batting"]["global"]["unavailable_fields"] == [
        "runs",
        "doubles",
        "triples",
        "home_runs",
        "rbi",
    ]


def test_stage_report_rejects_missing_team_coverage_and_era_basis():
    report = build_stage_report(
        2025,
        StageRows(
            team_batting=[_batting_row("LG", plate_appearances=10)],
            team_pitching=[_pitching_row("LG", earned_runs=4)],
            player_batting=[],
            player_pitching=[
                SimpleNamespace(
                    team_code="LG",
                    innings_outs=None,
                    innings_pitched=None,
                    era=54.0,
                    player_id=73,
                ),
            ],
            expected_team_ids={"LG", "KT"},
        ),
        current_year=2026,
    )

    assert report["ready_for_sync"] is False
    assert report["team_coverage"] == {"batting": False, "pitching": False}
    assert report["invalid_era_rows"][0]["player_id"] == 73


def test_stage_report_normalizes_team_innings_to_outs():
    team_batting = [_batting_row("LG", plate_appearances=10, at_bats=8, hits=2)]
    team_pitching = _pitching_row("LG", innings_pitched=10.0, hits_allowed=2, runs_allowed=1, earned_runs=1)
    team_pitching.pop("innings_outs")
    player_pitching = [
        SimpleNamespace(
            team_code="LG",
            innings_outs=30,
            hits_allowed=2,
            runs_allowed=1,
            earned_runs=1,
            home_runs_allowed=0,
            walks_allowed=0,
            strikeouts=0,
            era=9.0,
            player_id=1,
        ),
    ]

    report = build_stage_report(
        2025,
        StageRows(
            team_batting=team_batting,
            team_pitching=[team_pitching],
            player_batting=[{"team_code": "LG", "plate_appearances": 10, "at_bats": 8, "hits": 2}],
            player_pitching=player_pitching,
            expected_team_ids={"LG"},
        ),
        current_year=2026,
    )

    assert report["pitching"]["global"]["diff"] == {}
