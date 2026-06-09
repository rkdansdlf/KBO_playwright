import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from scripts.legacy.quality_gate import (
    BASELINE_KEYS,
    collect_metrics,
    evaluate_quality_gate,
    load_baseline,
    run_quality_gate,
)


class TestLoadBaseline:
    def test_success(self):
        data = {k: 0 for k in BASELINE_KEYS}
        with patch("pathlib.Path.read_text", return_value=json.dumps(data)):
            baseline = load_baseline(Path("dummy.json"))
            assert all(k in baseline for k in BASELINE_KEYS)

    def test_missing_keys(self):
        import pytest

        data = {}
        with patch("pathlib.Path.read_text", return_value=json.dumps(data)):
            with pytest.raises(ValueError, match="missing"):
                load_baseline(Path("dummy.json"))


class TestCollectMetrics:
    @patch("scripts.legacy.quality_gate.collect_audit_metrics")
    @patch("scripts.legacy.quality_gate.flatten_gate_metrics")
    def test_returns_dict(self, mock_flatten, mock_audit):
        mock_session = MagicMock()
        mock_session.execute.return_value.scalar.return_value = 0
        mock_audit.return_value = {}
        mock_flatten.return_value = {}

        result = collect_metrics(mock_session)
        assert isinstance(result, dict)


class TestEvaluateQualityGate:
    def test_no_failures(self):
        baseline = {k: 5 for k in BASELINE_KEYS}
        metrics = {key.replace("_max", ""): 0 for key in BASELINE_KEYS}
        failures = evaluate_quality_gate(
            local_metrics={"game_status_column_present": 1, **metrics},
            oci_metrics={"game_status_column_present": 1, **metrics},
            baseline=baseline,
            local_missing_ids=set(),
            oci_missing_ids=set(),
        )
        assert failures == []

    def test_exceeds_baseline(self):
        baseline = {k: 0 for k in BASELINE_KEYS}
        metrics = {"past_missing_runs": 5}
        failures = evaluate_quality_gate(
            local_metrics={"game_status_column_present": 1, **metrics},
            oci_metrics={"game_status_column_present": 1, **metrics},
            baseline=baseline,
            local_missing_ids=set(),
            oci_missing_ids=set(),
        )
        assert len(failures) > 0

    def test_past_scheduled(self):
        baseline = {k: 5 for k in BASELINE_KEYS}
        metrics = {key.replace("_max", ""): 0 for key in BASELINE_KEYS}
        failures = evaluate_quality_gate(
            local_metrics={"game_status_column_present": 1, "past_scheduled": 2, **metrics},
            oci_metrics={"game_status_column_present": 1, "past_scheduled": 0, **metrics},
            baseline=baseline,
            local_missing_ids=set(),
            oci_missing_ids=set(),
        )
        assert any("local past_scheduled" in f for f in failures)

    def test_past_scheduled_zero(self):
        baseline = {k: 5 for k in BASELINE_KEYS}
        metrics = {key.replace("_max", ""): 0 for key in BASELINE_KEYS}
        failures = evaluate_quality_gate(
            local_metrics={"game_status_column_present": 1, "past_scheduled": 0, **metrics},
            oci_metrics={"game_status_column_present": 1, "past_scheduled": 0, **metrics},
            baseline=baseline,
            local_missing_ids=set(),
            oci_missing_ids=set(),
        )
        assert all("past_scheduled" not in f for f in failures)


class TestRunQualityGate:
    @patch("scripts.legacy.quality_gate.SessionLocal")
    @patch("scripts.legacy.quality_gate.load_baseline")
    @patch("scripts.legacy.quality_gate.collect_metrics")
    @patch("scripts.legacy.quality_gate.fetch_past_missing_game_ids")
    def test_skip_oci(self, mock_fetch, mock_collect, mock_load, mock_session_local):
        mock_load.return_value = {k: 5 for k in BASELINE_KEYS}
        mock_session = MagicMock()
        mock_session.__enter__.return_value = mock_session
        mock_session_local.return_value = mock_session
        mock_collect.return_value = {key.replace("_max", ""): 0 for key in BASELINE_KEYS}
        mock_fetch.return_value = set()

        result = run_quality_gate(
            baseline_path=Path("dummy.json"),
            output_dir=Path("/tmp"),
            oci_url=None,
            skip_oci=True,
            write_artifacts=False,
        )
        assert isinstance(result, dict)
        assert result["ok"] is True
