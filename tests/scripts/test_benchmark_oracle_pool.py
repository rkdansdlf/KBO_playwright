"""Tests for Oracle connection pool benchmark utility (Issue #5)."""

from __future__ import annotations

import json
from pathlib import Path

from scripts.verification.benchmark_oracle_pool import (
    OraclePoolBenchmark,
    PoolBenchmarkMetrics,
    main,
)
from src.db.engine import create_engine_for_url


def test_oracle_pool_benchmark_metrics_calculation(tmp_path: Path) -> None:
    db_file = tmp_path / "test_pool.db"
    engine = create_engine_for_url(f"sqlite:///{db_file}")

    runner = OraclePoolBenchmark(engine, pool_size=2, max_overflow=2)
    metrics = runner.run_benchmark(workers=2, iterations_per_worker=5)

    assert isinstance(metrics, PoolBenchmarkMetrics)
    assert metrics.pool_size == 2
    assert metrics.max_overflow == 2
    assert metrics.workers == 2
    assert metrics.iterations_per_worker == 5
    assert metrics.total_operations == 10
    assert metrics.successful_operations == 10
    assert metrics.failed_operations == 0
    assert metrics.timed_out_operations == 0
    assert metrics.timeout_rate_pct == 0.0
    assert metrics.status == "PASS"
    assert metrics.throughput_ops_per_sec > 0
    assert metrics.avg_latency_ms >= 0


def test_benchmark_cli_dry_run_and_json_report(tmp_path: Path, monkeypatch) -> None:
    db_file = tmp_path / "cli_test_pool.db"
    json_report = tmp_path / "report.json"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_file}")

    exit_code = main(["--workers", "2", "--iterations", "3", "--json-out", str(json_report)])

    assert exit_code == 0
    assert json_report.exists()

    data = json.loads(json_report.read_text(encoding="utf-8"))
    assert data["total_operations"] == 6
    assert data["status"] == "PASS"
    assert data["timeout_rate_pct"] == 0.0
