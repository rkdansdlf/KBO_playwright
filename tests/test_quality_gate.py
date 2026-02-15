from scripts.maintenance.quality_gate import evaluate_quality_gate


BASELINE = {
    "past_missing_runs_max": 717,
    "batting_null_player_id_max": 0,
    "pitching_null_player_id_max": 0,
    "lineups_null_player_id_max": 0,
    "unresolved_missing_max": 0,
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
