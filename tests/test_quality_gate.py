import json
from pathlib import Path

from sqlalchemy import create_engine, text

from scripts.maintenance.quality_gate import (
    collect_metrics,
    evaluate_quality_gate,
    fetch_past_missing_game_ids,
    run_quality_gate,
)


BASELINE = {
    "past_missing_runs_max": 717,
    "batting_null_player_id_max": 0,
    "pitching_null_player_id_max": 0,
    "lineups_null_player_id_max": 0,
    "unresolved_missing_max": 0,
    "orphaned_batting_stats_max": 0,
    "orphaned_pitching_stats_max": 0,
    "orphaned_lineups_max": 0,
    "missing_player_profiles_max": 0,
    "game_batting_duplicate_player_groups_max": 0,
    "game_pitching_duplicate_player_groups_max": 0,
    "game_lineups_duplicate_player_team_groups_max": 0,
    "game_batting_player_team_collisions_max": 0,
    "game_pitching_player_team_collisions_max": 0,
    "game_lineups_player_team_collisions_max": 0,
    "batting_hits_gt_at_bats_max": 0,
    "batting_at_bats_gt_plate_appearances_max": 0,
    "pitching_earned_runs_gt_runs_allowed_max": 0,
    "pseudo_player_profiles_max": 0,
}


def _create_quality_gate_tables(conn):
    conn.execute(
        text(
            """
            CREATE TABLE game (
                game_id TEXT PRIMARY KEY,
                game_date DATE NOT NULL,
                home_score INTEGER,
                away_score INTEGER,
                game_status TEXT
            )
            """
        )
    )
    conn.execute(text("CREATE TABLE game_batting_stats (game_id TEXT, player_id INTEGER)"))
    conn.execute(text("CREATE TABLE game_pitching_stats (game_id TEXT, player_id INTEGER)"))
    conn.execute(text("CREATE TABLE game_lineups (game_id TEXT, player_id INTEGER)"))
    conn.execute(text("CREATE TABLE player_season_batting (player_id INTEGER)"))
    conn.execute(text("CREATE TABLE player_season_pitching (player_id INTEGER)"))
    conn.execute(text("CREATE TABLE player_basic (player_id INTEGER, name TEXT)"))
    conn.execute(text("CREATE TABLE game_inning_scores (game_id TEXT)"))
    conn.execute(text("CREATE TABLE game_events (game_id TEXT)"))
    conn.execute(text("CREATE TABLE game_play_by_play (game_id TEXT)"))


def _write_baseline(tmp_path: Path) -> Path:
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(BASELINE), encoding="utf-8")
    return baseline_path


def _create_sqlite_quality_gate_db(path: Path) -> str:
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as conn:
        _create_quality_gate_tables(conn)
    return f"sqlite:///{path}"


def test_quality_gate_passes_when_within_baseline_and_in_sync():
    metrics = {
        "past_missing_runs": 717,
        "batting_null_player_id": 0,
        "pitching_null_player_id": 0,
        "lineups_null_player_id": 0,
        "unresolved_missing": 0,
        "past_scheduled": 0,
    }
    failures = evaluate_quality_gate(
        local_metrics=metrics,
        oci_metrics=dict(metrics),
        baseline=BASELINE,
        local_missing_ids={"A", "B"},
        oci_missing_ids={"A", "B"},
    )
    assert failures == []


def test_quality_gate_fails_when_baseline_exceeded():
    local_metrics = {
        "past_missing_runs": 717,
        "batting_null_player_id": 1,
        "pitching_null_player_id": 0,
        "lineups_null_player_id": 0,
        "unresolved_missing": 0,
        "past_scheduled": 0,
    }
    oci_metrics = {
        "past_missing_runs": 717,
        "batting_null_player_id": 0,
        "pitching_null_player_id": 0,
        "lineups_null_player_id": 0,
        "unresolved_missing": 0,
        "past_scheduled": 0,
    }
    failures = evaluate_quality_gate(
        local_metrics=local_metrics,
        oci_metrics=oci_metrics,
        baseline=BASELINE,
        local_missing_ids={"A"},
        oci_missing_ids={"A"},
    )
    assert any("exceeds baseline" in msg for msg in failures)


def test_quality_gate_strict_zero_fails_for_baseline_debt():
    metrics = {
        "past_missing_runs": 717,
        "batting_null_player_id": 0,
        "pitching_null_player_id": 0,
        "lineups_null_player_id": 0,
        "unresolved_missing": 0,
        "past_scheduled": 0,
        "game_batting_duplicate_player_groups": 1,
    }
    baseline = dict(BASELINE)
    baseline["game_batting_duplicate_player_groups_max"] = 1

    failures = evaluate_quality_gate(
        local_metrics=metrics,
        oci_metrics=dict(metrics),
        baseline=baseline,
        local_missing_ids={"A"},
        oci_missing_ids={"A"},
        strict_zero=True,
    )

    assert any("strict-zero" in msg for msg in failures)


def test_quality_gate_fails_when_local_oci_mismatch_exists():
    local_metrics = {
        "past_missing_runs": 717,
        "batting_null_player_id": 0,
        "pitching_null_player_id": 0,
        "lineups_null_player_id": 0,
        "unresolved_missing": 0,
        "past_scheduled": 0,
    }
    oci_metrics = {
        "past_missing_runs": 718,
        "batting_null_player_id": 0,
        "pitching_null_player_id": 0,
        "lineups_null_player_id": 0,
        "unresolved_missing": 1,
        "past_scheduled": 1,
    }
    failures = evaluate_quality_gate(
        local_metrics=local_metrics,
        oci_metrics=oci_metrics,
        baseline=BASELINE,
        local_missing_ids={"A", "B"},
        oci_missing_ids={"A", "C"},
    )
    assert any("metric mismatch for past_missing_runs" in msg for msg in failures)
    assert any("past_scheduled" in msg for msg in failures)
    assert any("set mismatch" in msg for msg in failures)


def test_collect_metrics_treats_current_date_as_operational_not_past():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_gate_tables(conn)
        conn.execute(
            text(
                """
                INSERT INTO game (game_id, game_date, game_status)
                VALUES
                    ('TODAY0', CURRENT_DATE, 'SCHEDULED'),
                    ('PAST0', date(CURRENT_DATE, '-1 day'), 'SCHEDULED'),
                    ('RAIN0', date(CURRENT_DATE, '-1 day'), 'CANCELLED'),
                    ('POSTPONED0', date(CURRENT_DATE, '-1 day'), 'POSTPONED')
                """
            )
        )

        metrics = collect_metrics(conn)
        missing_ids = fetch_past_missing_game_ids(conn)

    assert metrics["past_missing_runs"] == 1
    assert metrics["past_scheduled"] == 1
    assert missing_ids == {"PAST0"}


def test_collect_metrics_counts_pitching_missing_profiles_and_unknown_stubs():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        _create_quality_gate_tables(conn)
        conn.execute(text("INSERT INTO player_season_batting (player_id) VALUES (1001), (1002)"))
        conn.execute(text("INSERT INTO player_season_pitching (player_id) VALUES (2001), (2002), (1002)"))
        conn.execute(
            text(
                """
                INSERT INTO player_basic (player_id, name)
                VALUES
                    (1001, '정상타자'),
                    (1002, 'Unknown 1002'),
                    (2002, '정상투수')
                """
            )
        )

        metrics = collect_metrics(conn)

    assert metrics["missing_player_profiles"] == 2


def test_run_quality_gate_no_write_skips_artifact_directory(tmp_path):
    baseline_path = _write_baseline(tmp_path)
    db_url = _create_sqlite_quality_gate_db(tmp_path / "gate.sqlite")
    output_dir = tmp_path / "missing" / "artifacts"

    result = run_quality_gate(
        baseline_path=baseline_path,
        output_dir=output_dir,
        oci_url=db_url,
        oci_only=True,
        write_artifacts=False,
    )

    assert result["ok"] is True
    assert result["artifacts_written"] is False
    assert result["local_snapshot"] is None
    assert result["oci_snapshot"] is None
    assert result["set_diff_csv"] is None
    assert not output_dir.exists()


def test_run_quality_gate_writes_csv_snapshots_by_default(tmp_path):
    baseline_path = _write_baseline(tmp_path)
    db_url = _create_sqlite_quality_gate_db(tmp_path / "gate.sqlite")
    output_dir = tmp_path / "artifacts"

    result = run_quality_gate(
        baseline_path=baseline_path,
        output_dir=output_dir,
        oci_url=db_url,
        oci_only=True,
    )

    assert result["ok"] is True
    assert result["artifacts_written"] is True
    assert output_dir.exists()
    for key in ("local_snapshot", "oci_snapshot", "set_diff_csv"):
        path = Path(result[key])
        assert path.exists()
        assert path.read_text(encoding="utf-8").splitlines()[0] in {"metric,count", "scope,game_id"}
