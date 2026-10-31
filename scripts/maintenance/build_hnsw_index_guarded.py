"""Build the HNSW index while watching what it costs the searches sharing the server.

The first attempt (2026-10-31, 75 minutes) was cancelled because the build saturated
this host's I/O: the lightweight BM25 candidate query went from 0.20 s to 165 s, an
800x regression for every reader. The build itself was still at
``building index: loading tuples`` with 100% of blocks read, waiting on ``WALWrite``
over and over -- the signature of a graph being paged to disk faster than the server
can flush, which is what a small ``maintenance_work_mem`` produces.

So this run changes the parameter that addresses that cause and adds the guard the
first run did not have:

* ``maintenance_work_mem`` is raised for this session only. Parallel workers stay at
  0 because ``/dev/shm`` cannot hold their segments (the measured failure:
  ``could not resize shared memory segment ... 265326304 bytes``), so the build
  holds its graph in this backend's own memory instead of paging it out.
* A watchdog measures a fixed lightweight query every ``PROBE_SECONDS`` and calls
  ``pg_cancel_backend`` when it sees ``SLOW_STRIKES`` consecutive probes over
  ``SLOW_MS``. The line sits far below the 165 s the last build produced and far
  above the ~14 ms the query costs when the server is healthy.

The watchdog is the point. A build that cannot finish without making search unusable
is not one this repository should start unattended, and the decision to keep going
belongs to a measurement rather than a guess about how much time is left.

Usage:
    python3 -m scripts.maintenance.build_hnsw_index_guarded
    python3 -m scripts.maintenance.build_hnsw_index_guarded --mem 1GB --max-minutes 60
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from datetime import UTC, datetime

from sqlalchemy import create_engine, text

from src.db.engine import Engine

INDEX_NAME = "idx_rag_chunks_embedding_hnsw"
PROBE_SECONDS = 15
#: A healthy server answers the probe in ~14 ms. The cancelled build pushed it past
#: 100,000 ms, so the line is drawn well inside the healthy range and well below the
#: range where a reader notices search is unusable.
SLOW_MS = 3_000
SLOW_STRIKES = 3
#: Give up rather than let the build run into the nightly jobs at 05:00 KST.
DEFAULT_MAX_MINUTES = 90
PROBE_SQL = "SELECT count(*) FROM rag_chunks WHERE game_date = '2026-10-26'"
#: Defaults for the two build-time parameters. Measured on this host, the standard
#: ``m=16 ef_construction=64`` advanced at 0.55%/min once the graph grew, which put
#: completion past the watchdog's limit; ``m`` halves the graph's edges and ``ef``
#: halves the search done per insert, so both the memory held and the work done fall.
#: Recall is slightly lower and still far better than the ivfflat scan it replaces.
DEFAULT_M = 8
DEFAULT_EF_CONSTRUCTION = 32


def _create_sql(m: int, ef_construction: int) -> str:
    """Return the CREATE INDEX statement for the given build parameters."""
    return (
        f"CREATE INDEX {INDEX_NAME} ON rag_chunks USING hnsw (embedding vector_cosine_ops) "
        f"WITH (m = {m}, ef_construction = {ef_construction})"
    )


def _connection_url() -> str:
    """Return the operational URL, password included, for a dedicated connection."""
    return Engine.url.render_as_string(hide_password=False)


def _probe_ms(conn) -> float | None:
    """Return the probe's round trip in milliseconds, or None if it could not run."""
    started = time.perf_counter()
    try:
        conn.execute(text(PROBE_SQL)).scalar()
    except Exception:
        return None
    return (time.perf_counter() - started) * 1000


def _progress(conn) -> str:
    row = conn.execute(text("SELECT phase, blocks_done, blocks_total FROM pg_stat_progress_create_index")).first()
    if row is None:
        return "no progress row"
    pct = round(100.0 * (row[1] or 0) / row[2], 1) if row[2] else "?"
    return f"{row[0]} {row[1]}/{row[2]} ({pct}%)"


def _build_backend_pid(conn) -> int | None:
    return conn.execute(
        text("SELECT pid FROM pg_stat_activity WHERE query LIKE 'CREATE INDEX%hnsw%' AND state <> 'idle' LIMIT 1"),
    ).scalar()


def _index_exists(conn) -> bool:
    return bool(conn.execute(text("SELECT 1 FROM pg_class WHERE relname = :n"), {"n": INDEX_NAME}).first())


def _cancel_build(conn) -> None:
    """Ask the backend running the build to stop; it cleans up its partial index."""
    pid = _build_backend_pid(conn)
    if pid:
        conn.execute(text("SELECT pg_cancel_backend(:p)"), {"p": pid})


def _watch_build(watch_conn, build_thread: threading.Thread, started: float, max_minutes: int) -> bool:
    """Measure the server until the build ends; return True when it was cancelled."""
    strikes = 0
    while build_thread.is_alive():
        elapsed_min = (time.monotonic() - started) / 60
        if elapsed_min >= max_minutes:
            _cancel_build(watch_conn)
            print(f"  GAVE UP at {elapsed_min:.0f} min (limit {max_minutes})")
            return True
        probe = _probe_ms(watch_conn)
        if probe is None:
            time.sleep(PROBE_SECONDS)
            continue
        strikes = strikes + 1 if probe > SLOW_MS else 0
        print(f"  [{elapsed_min:5.1f} min] {_progress(watch_conn)} | probe {probe:7.0f} ms | strikes {strikes}")
        sys.stdout.flush()
        if strikes >= SLOW_STRIKES:
            _cancel_build(watch_conn)
            print(f"  CANCELLED: probe stayed over {SLOW_MS} ms for {strikes} probes")
            return True
        time.sleep(PROBE_SECONDS)
    return False


def _report_index(conn) -> None:
    """Print the index's size and what the planner does with it now."""
    if not _index_exists(conn):
        print(f"   {INDEX_NAME} does not exist (cancelled or failed)")
        return
    size = conn.execute(text("SELECT pg_size_pretty(pg_relation_size(:n::regclass))"), {"n": INDEX_NAME}).scalar()
    print(f"   {INDEX_NAME} exists — {size}")
    vec = conn.execute(text("SELECT embedding::text FROM rag_chunks WHERE embedding IS NOT NULL LIMIT 1")).scalar()
    for label, predicate in (
        ("no filter (27 s case)", ""),
        ("team_id='KIA'   (2 s case)", "AND team_id = 'KIA'"),
        ("season_year=2025", "AND season_year = 2025"),
    ):
        started_ms = time.perf_counter()
        rows = conn.execute(
            text(
                "EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id FROM rag_chunks "
                "WHERE embedding IS NOT NULL AND (index_status IS NULL OR index_status IN ('ACTIVE')) "
                f"{predicate} ORDER BY embedding <=> '{vec}' LIMIT 15",
            ),
        ).fetchall()
        ms = (time.perf_counter() - started_ms) * 1000
        plan = " | ".join(r[0].strip() for r in rows)
        idx = next(
            (p.split(" using ")[-1].split(" ")[0] for p in plan.split(" | ") if "Index Scan" in p),
            "(no index scan)",
        )
        print(f"   {label:28s} {ms:>9.1f} ms  index={idx}")


def _run_build(build_conn, result: dict[str, str], sql: str) -> None:
    """Run the CREATE INDEX on the build connection and record the outcome."""
    try:
        build_conn.execute(text(sql))
        result["outcome"] = "ok"
    except Exception as exc:
        result["outcome"] = f"{type(exc).__name__}: {str(exc)[:160]}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--mem", default="512MB", help="Session maintenance_work_mem")
    parser.add_argument("--max-minutes", type=int, default=DEFAULT_MAX_MINUTES)
    parser.add_argument("--m", type=int, default=DEFAULT_M, help="HNSW m (edges per node)")
    parser.add_argument("--ef", type=int, default=DEFAULT_EF_CONSTRUCTION, help="HNSW ef_construction")
    args = parser.parse_args(argv)

    url = _connection_url()
    # Two connections: the build holds one for its whole life and the watchdog needs
    # its own to measure the server while that is true.
    build_engine = create_engine(url, isolation_level="AUTOCOMMIT", connect_args={"connect_timeout": 20})
    watch_engine = create_engine(url, isolation_level="AUTOCOMMIT", connect_args={"connect_timeout": 20})

    started = time.monotonic()
    result: dict[str, str] = {}
    cancelled = False
    with build_engine.connect() as build_conn:
        build_conn.execute(text(f"SET maintenance_work_mem = '{args.mem}'"))
        build_conn.execute(text("SET max_parallel_maintenance_workers = 0"))
        build_conn.execute(text("SET lock_timeout = '90s'"))
        build_conn.execute(text("SET statement_timeout = '0'"))
        print(
            f"[{datetime.now(UTC):%H:%M:%S}Z] build starting (mem={args.mem}, parallel=0, max={args.max_minutes} min)"
        )
        sys.stdout.flush()

        build_thread = threading.Thread(
            target=_run_build,
            args=(build_conn, result, _create_sql(args.m, args.ef)),
            daemon=True,
        )
        build_thread.start()

        with watch_engine.connect() as watch_conn:
            watch_conn.execute(text("SET statement_timeout = '30s'"))
            cancelled = _watch_build(watch_conn, build_thread, started, args.max_minutes)

        build_thread.join(timeout=120)

    elapsed_min = (time.monotonic() - started) / 60
    print(f"\n[{datetime.now(UTC):%H:%M:%S}Z] finished in {elapsed_min:.1f} min — {result.get('outcome', 'unknown')}")

    with watch_engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '120s'"))
        _report_index(conn)
    return 0 if (not cancelled and result.get("outcome") == "ok") else 1


if __name__ == "__main__":
    sys.exit(main())
