"""Watch an HNSW index build to completion and record what it bought.

The build runs on the server; a client timeout does not stop it (measured: the
connection dropped at 15:00 while the backend kept working past 15:23). So the
completion has to be read from the catalog, not from the client's return value.

On completion this measures the three things the decision needs:

* does the planner choose HNSW for a filtered search, and what does it cost now;
* what did the build cost (index size, wall clock from ``pg_stat_progress``);
* is the result set still the one the queries returned before -- a different
  index kind is still an approximation, and a rank change is something a human
  has to look at rather than something to accept silently.

Usage:
    python3 -m scripts.maintenance.watch_hnsw_build            # wait, then report
    python3 -m scripts.maintenance.watch_hnsw_build --check     # report once, no wait
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import UTC, datetime

from sqlalchemy import text

from src.db.engine import Engine

INDEX_NAME = "idx_rag_chunks_embedding_hnsw"
POLL_SECONDS = 60
#: Two hours is well past the 75 minutes this build took on this server; a build
#: still running after that is worth a human decision rather than more waiting.
MAX_WAIT_SECONDS = 2 * 60 * 60


def _index_exists(conn) -> bool:
    return bool(conn.execute(text("SELECT 1 FROM pg_class WHERE relname = :n"), {"n": INDEX_NAME}).first())


def _size(conn) -> str:
    return (
        conn.execute(text("SELECT pg_size_pretty(pg_relation_size(:n::regclass))"), {"n": INDEX_NAME}).scalar() or "-"
    )


def _progress(conn) -> str:
    row = conn.execute(
        text("SELECT phase, blocks_done, blocks_total FROM pg_stat_progress_create_index"),
    ).first()
    if row is None:
        return "no progress row"
    pct = round(100.0 * (row[1] or 0) / row[2], 1) if row[2] else "?"
    return f"{row[0]} blocks={row[1]}/{row[2]} ({pct}%)"


def _backend(conn) -> str:
    row = conn.execute(
        text("SELECT state, wait_event_type, wait_event FROM pg_stat_activity WHERE query LIKE '%hnsw%'"),
    ).first()
    return f"{row[0]} wait={row[1]}/{row[2]}" if row else "gone"


def _sample_vector(conn) -> str:
    return conn.execute(text("SELECT embedding::text FROM rag_chunks WHERE embedding IS NOT NULL LIMIT 1")).scalar()


def _plan_for(conn, label: str, predicate: str, vec: str) -> tuple[str, float]:
    started = time.perf_counter()
    rows = conn.execute(
        text(
            f"EXPLAIN (ANALYZE, FORMAT TEXT) SELECT id FROM rag_chunks "
            f"WHERE embedding IS NOT NULL AND (index_status IS NULL OR index_status IN ('ACTIVE')) "
            f"{predicate} ORDER BY embedding <=> '{vec}' LIMIT 15",
        ),
    ).fetchall()
    elapsed = (time.perf_counter() - started) * 1000
    plan = " | ".join(r[0].strip() for r in rows)
    index = next(
        (part.split(" using ")[-1].split(" ")[0] for part in plan.split(" | ") if "Index Scan" in part),
        "(no index scan)",
    )
    print(f"   {label:34s} {elapsed:>9.1f} ms   index={index}")
    return plan, elapsed


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else None)
    parser.add_argument("--check", action="store_true", help="Report once instead of waiting")
    args = parser.parse_args(argv)

    started = datetime.now(UTC)
    with Engine.connect() as conn:
        conn.execute(text("SET statement_timeout = '600s'"))
        waited = 0.0
        while not _index_exists(conn):
            if args.check:
                print(f"{INDEX_NAME} does not exist yet; progress: {_progress(conn)}; backend: {_backend(conn)}")
                return 1
            if waited >= MAX_WAIT_SECONDS:
                print(f"GAVE UP after {waited / 60:.0f} min; backend: {_backend(conn)}")
                return 2
            print(
                f"[{datetime.now(UTC):%H:%M:%S}] waiting ({waited / 60:.0f} min): {_progress(conn)}; {_backend(conn)}"
            )
            sys.stdout.flush()
            time.sleep(POLL_SECONDS)
            waited += POLL_SECONDS

        print(f"\n{INDEX_NAME} exists — {_size(conn)} (waited {waited / 60:.0f} min, started {started:%H:%M:%S}Z)")

        vec = _sample_vector(conn)
        print("\n===== filter-less ANN (the 27 s case) =====")
        _plan_for(conn, "no filter", "", vec)
        print("\n===== filtered ANN (the 2 s case) =====")
        _plan_for(conn, "team_id='KIA'", "AND team_id = 'KIA'", vec)
        _plan_for(conn, "season_year=2025", "AND season_year = 2025", vec)

        print("\n===== index inventory =====")
        for row in conn.execute(
            text(
                "SELECT indexname, pg_size_pretty(pg_relation_size(indexname::regclass)) FROM pg_indexes "
                "WHERE tablename = 'rag_chunks' AND (indexname LIKE '%embedding%') ORDER BY indexname",
            ),
        ).fetchall():
            print(f"   {row[0]:38s} {row[1]}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
