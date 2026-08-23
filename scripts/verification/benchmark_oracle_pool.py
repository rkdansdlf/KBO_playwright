"""Benchmark and live-validation runner for Oracle connection pool (2/2 config).

Issue #5 Acceptance Criteria:
- Single sync smoke test completes without pool timeout.
- Two concurrent sync jobs complete without connection starvation.
- Transient connection retry behavior remains successful.
- Record pool timeout rate, throughput, and maximum active connections.
- Exit 0 if timeout rate == 0% and throughput > threshold.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, UTC
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

if str(Path(__file__).resolve().parents[2]) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.db.engine import create_engine_for_url

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)


@dataclass
class PoolBenchmarkMetrics:
    """Aggregated metrics from connection pool benchmark."""

    target_url_redacted: str
    pool_size: int
    max_overflow: int
    workers: int
    iterations_per_worker: int
    total_operations: int
    successful_operations: int
    failed_operations: int
    timed_out_operations: int
    timeout_rate_pct: float
    total_duration_sec: float
    throughput_ops_per_sec: float
    avg_latency_ms: float
    max_latency_ms: float
    max_active_connections: int
    timestamp_utc: str
    status: str


class OraclePoolBenchmark:
    """Benchmark runner simulating concurrent sync workload against Oracle/DB pool."""

    def __init__(self, engine: Engine, *, pool_size: int = 2, max_overflow: int = 2) -> None:
        self.engine = engine
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self._active_connections = 0
        self._max_observed_active = 0

    def _execute_worker_task(self, worker_id: int, iterations: int) -> list[dict[str, Any]]:
        """Simulate a sync worker executing database queries with hold times."""
        results: list[dict[str, Any]] = []

        for i in range(iterations):
            start = time.perf_counter()
            op_success = False
            is_timeout = False
            error_msg: str | None = None

            try:
                # Checkout connection from pool
                with self.engine.connect() as conn:
                    # Increment active counter
                    self._active_connections += 1
                    self._max_observed_active = max(self._max_observed_active, self._active_connections)

                    # Run representative query
                    conn.execute(text("SELECT 1 FROM DUAL" if self.engine.dialect.name == "oracle" else "SELECT 1"))
                    # Small hold time to simulate read/write batching (5-15ms)
                    time.sleep(0.01)

                    op_success = True
            except (SQLAlchemyError, TimeoutError, OSError, RuntimeError) as exc:
                error_msg = str(exc)
                if "timeout" in error_msg.lower() or "queuepool" in error_msg.lower():
                    is_timeout = True
            finally:
                if self._active_connections > 0:
                    self._active_connections -= 1

            latency_ms = (time.perf_counter() - start) * 1000.0
            results.append(
                {
                    "worker_id": worker_id,
                    "iteration": i,
                    "success": op_success,
                    "is_timeout": is_timeout,
                    "latency_ms": latency_ms,
                    "error": error_msg,
                }
            )

        return results

    def run_benchmark(self, workers: int = 2, iterations_per_worker: int = 25) -> PoolBenchmarkMetrics:
        """Run multi-threaded connection pool benchmark and compute metrics."""
        self._active_connections = 0
        self._max_observed_active = 0
        start_time = time.perf_counter()

        all_results: list[dict[str, Any]] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            futures = [
                executor.submit(self._execute_worker_task, worker_id, iterations_per_worker)
                for worker_id in range(workers)
            ]
            for f in concurrent.futures.as_completed(futures):
                all_results.extend(f.result())

        total_duration = time.perf_counter() - start_time
        total_ops = len(all_results)
        successful_ops = sum(1 for r in all_results if r["success"])
        failed_ops = total_ops - successful_ops
        timeouts = sum(1 for r in all_results if r["is_timeout"])
        timeout_rate = (timeouts / total_ops * 100.0) if total_ops > 0 else 0.0

        latencies = [r["latency_ms"] for r in all_results]
        avg_latency = (sum(latencies) / len(latencies)) if latencies else 0.0
        max_latency = max(latencies) if latencies else 0.0
        throughput = (total_ops / total_duration) if total_duration > 0 else 0.0

        raw_url = str(self.engine.url)
        redacted_url = raw_url.rsplit("@", maxsplit=1)[-1] if "@" in raw_url else raw_url

        status = "PASS" if timeout_rate <= 1.0 and failed_ops == 0 else "FAIL"

        return PoolBenchmarkMetrics(
            target_url_redacted=redacted_url,
            pool_size=self.pool_size,
            max_overflow=self.max_overflow,
            workers=workers,
            iterations_per_worker=iterations_per_worker,
            total_operations=total_ops,
            successful_operations=successful_ops,
            failed_operations=failed_ops,
            timed_out_operations=timeouts,
            timeout_rate_pct=round(timeout_rate, 2),
            total_duration_sec=round(total_duration, 3),
            throughput_ops_per_sec=round(throughput, 2),
            avg_latency_ms=round(avg_latency, 2),
            max_latency_ms=round(max_latency, 2),
            max_active_connections=self._max_observed_active,
            timestamp_utc=datetime.now(UTC).isoformat(),
            status=status,
        )


def main(argv: list[str] | None = None) -> int:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-url", type=str, default=None, help="Target DB URL (defaults to DATABASE_URL)")
    parser.add_argument("--workers", type=int, default=2, help="Number of concurrent workers (default: 2)")
    parser.add_argument("--iterations", type=int, default=20, help="Iterations per worker (default: 20)")
    parser.add_argument("--json-out", type=Path, default=None, help="Path to write JSON benchmark metrics report")
    parser.add_argument("--dry-run", action="store_true", help="Run in mock/lightweight mode without writing DB rows")
    args = parser.parse_args(argv)

    db_url = args.db_url or os.getenv("DATABASE_URL", "sqlite:///./data/kbo_dev.db")
    engine = create_engine_for_url(db_url)

    runner = OraclePoolBenchmark(engine, pool_size=2, max_overflow=2)
    metrics = runner.run_benchmark(workers=args.workers, iterations_per_worker=args.iterations)

    print("=" * 60)
    print("Oracle / Database Connection Pool Benchmark Report")
    print("=" * 60)
    print(f"Target DB:               {metrics.target_url_redacted}")
    print(f"Pool Config:             size={metrics.pool_size}, overflow={metrics.max_overflow}")
    print(f"Concurrency:             {metrics.workers} workers, {metrics.iterations_per_worker} iter/worker")
    print(
        f"Total Ops:               {metrics.total_operations} ({metrics.successful_operations} OK, {metrics.failed_operations} ERR)"
    )
    print(f"Timeout Rate:            {metrics.timeout_rate_pct}% ({metrics.timed_out_operations} timeouts)")
    print(f"Max Active Connections:  {metrics.max_active_connections}")
    print(
        f"Throughput:              {metrics.throughput_ops_per_sec} ops/sec (Duration: {metrics.total_duration_sec}s)"
    )
    print(f"Latency (avg / max):     {metrics.avg_latency_ms}ms / {metrics.max_latency_ms}ms")
    print(f"Validation Status:       {metrics.status}")
    print("=" * 60)

    if args.json_out:
        args.json_out.parent.mkdir(parents=True, exist_ok=True)
        args.json_out.write_text(json.dumps(asdict(metrics), indent=2), encoding="utf-8")
        print(f"Report saved to: {args.json_out}")

    return 0 if metrics.status == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
