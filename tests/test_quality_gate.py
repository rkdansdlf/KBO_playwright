from sqlalchemy import create_engine, text

from scripts.maintenance.quality_gate import collect_metrics, evaluate_quality_gate, fetch_past_missing_game_ids


BASELINE = {
    "past_missing_runs_max": 717,
    "batting_null_player_id_max": 0,
    "pitching_null_player_id_max": 0,
    "lineups_null_player_id_max": 0,
    "unresolved_missing_max": 0,
    "orphaned_batting_stats_max": 0,
    "orphaned_pitching_stats_max": 0,
    "missing_player_profiles_max": 0,
}


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
        conn.execute(text("CREATE TABLE player_basic (player_id INTEGER)"))
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
