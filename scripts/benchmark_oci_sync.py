"""
OCI sync performance benchmark.

Measures throughput of key OCI sync operations:
  1. sync_simple_table — read-side throughput (batch size sweep, record count sweep)
  2. detect_dirty_game_ids — signature comparison scaling
  3. Connection overhead — engine creation cost
  4. _do_bulk_copy_upsert — raw COPY engine (requires --oci-url)

Usage:
  python3 scripts/benchmark_oci_sync.py                          # full benchmark (SQLite)
  python3 scripts/benchmark_oci_sync.py --oci-url postgresql://...  # includes COPY benchmarks
  python3 scripts/benchmark_oci_sync.py --quick                      # quick subset
  python3 scripts/benchmark_oci_sync.py --table-sweep                # only table sync
  python3 scripts/benchmark_oci_sync.py --dirty-sweep                # only dirty detection
  python3 scripts/benchmark_oci_sync.py --connection-sweep           # only connection overhead
  python3 scripts/benchmark_oci_sync.py --copy-sweep --oci-url ...   # only COPY engine
"""

from __future__ import annotations

import itertools
import logging
import os
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Generator

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import Column, Integer, String, Text, create_engine, inspect
from sqlalchemy.orm import Session, declarative_base

from src.sync.oci_sync import OCISync

_BenchBase = declarative_base()
logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# ── test models ────────────────────────────────────────────────────────


class _BenchModel(_BenchBase):
    __tablename__ = "bench_table"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    category = Column(String(50), nullable=True)
    score = Column(Integer, nullable=True)
    payload = Column(Text, nullable=True)
    created_at = Column(String(30), nullable=True)
    updated_at = Column(String(30), nullable=True)


def _make_records(count: int) -> list[dict[str, Any]]:
    return [
        {
            "name": f"player_{i}",
            "category": f"cat_{i % 10}",
            "score": i * 10,
            "payload": None,
            "created_at": None,
            "updated_at": None,
        }
        for i in range(count)
    ]


def _seed_table(engine, model, records: list[dict]):
    with Session(bind=engine) as session:
        for rec in records:
            session.add(model(**rec))
        session.commit()


def _table_exists(engine, table_name: str) -> bool:
    return table_name in inspect(engine).get_table_names()


# ── syncer helpers ─────────────────────────────────────────────────────


def _build_syncer_from_engines(src_engine, tgt_engine):
    syncer = object.__new__(OCISync)
    syncer.sqlite_session = Session(bind=src_engine, autoflush=False, autocommit=False)
    syncer.oci_engine = tgt_engine
    syncer.target_session = Session(bind=tgt_engine, autoflush=False, autocommit=False)
    syncer._temp_table_counter = itertools.count(1)
    syncer._season_map_cache = None
    syncer._franchise_id_mapping_cache = None
    return syncer


def _install_bulk_copy_spy(syncer):
    """Replace _bulk_copy_upsert with a no-op spy for read-side measurement.

    Returns a list that records (table_name, len(records)) per call.
    """
    calls = []
    original = syncer._bulk_copy_upsert

    def _spy(table_name, records, unique_cols, **kwargs):
        calls.append((table_name, len(records)))

    syncer._bulk_copy_upsert = _spy
    return calls, original


def _install_target_exists_spy(syncer, exists: bool = True):
    syncer._target_table_exists = lambda _m: exists


# ── database lifecycle ─────────────────────────────────────────────────


@contextmanager
def _temp_sqlite_pair() -> Generator[tuple[Any, Any], None, None]:
    tmp = tempfile.mkdtemp()
    src_path = os.path.join(tmp, "source.db")
    tgt_path = os.path.join(tmp, "target.db")
    src_engine = create_engine(f"sqlite:///{src_path}")
    tgt_engine = create_engine(f"sqlite:///{tgt_path}")
    _BenchBase.metadata.create_all(bind=src_engine)
    _BenchBase.metadata.create_all(bind=tgt_engine)
    try:
        yield src_engine, tgt_engine
    finally:
        src_engine.dispose()
        tgt_engine.dispose()
        shutil.rmtree(tmp, ignore_errors=True)


@contextmanager
def _oci_target(url: str) -> Generator[Any, None, None]:
    """Create OCI engine for --oci-url runs."""
    engine = create_engine(url)
    _BenchBase.metadata.create_all(bind=engine)
    try:
        yield engine
    finally:
        engine.dispose()


# ── results ────────────────────────────────────────────────────────────


@dataclass
class BenchResult:
    label: str
    elapsed: float
    rows: int = 0
    batch_size: int = 0
    note: str = ""


@dataclass
class BenchSuite:
    title: str = ""
    results: list[BenchResult] = field(default_factory=list)

    def add(self, label: str, elapsed: float, rows: int = 0, batch_size: int = 0, note: str = ""):
        self.results.append(BenchResult(label=label, elapsed=elapsed, rows=rows, batch_size=batch_size, note=note))

    def print_report(self):
        if self.title:
            logger.info(f"\n{'=' * 60}")
            logger.info(f"  {self.title}")
            logger.info(f"{'=' * 60}")
        logger.info(f"  {'Label':<50} {'Rows':>8} {'Batch':>7} {'Time(s)':>10} {'Rows/s':>10}")
        logger.info(f"  {'-' * 50} {'-' * 8} {'-' * 7} {'-' * 10} {'-' * 10}")
        for r in self.results:
            rows_s = f"{r.rows / r.elapsed:>10.0f}" if r.elapsed > 0 else "         -"
            label = r.label
            if r.note:
                label += f" ({r.note})"
            logger.info(f"  {label:<50} {r.rows:>8} {r.batch_size:>7} {r.elapsed:>10.3f} {rows_s}")
        print()


# ── benchmark 1: sync_simple_table read-side throughput ────────────────


def bench_table_sweep(quick: bool = False) -> BenchSuite:
    suite = BenchSuite("Benchmark 1: sync_simple_table Read Throughput")

    batch_sizes = [100, 500, 1000, 5000, 10000, 20000] if not quick else [500, 5000]
    record_counts = [100, 1000, 10000, 50000] if not quick else [1000, 10000]

    # batch size sweep at various record counts
    for n_records in record_counts:
        for batch_size in batch_sizes:
            if batch_size > n_records:
                continue
            with _temp_sqlite_pair() as (src_engine, tgt_engine):
                _seed_table(src_engine, _BenchModel, _make_records(n_records))
                syncer = _build_syncer_from_engines(src_engine, tgt_engine)
                _install_target_exists_spy(syncer)
                calls, _ = _install_bulk_copy_spy(syncer)

                start = time.perf_counter()
                syncer.sync_simple_table(_BenchModel, ["name"], batch_size=batch_size, exclude_cols=["id"])
                elapsed = time.perf_counter() - start

            suite.add(f"n={n_records}", elapsed, rows=n_records, batch_size=batch_size)

    # record count sweep at fixed batch size
    fixed_batch = 5000
    for n_records in [100, 500, 1000, 5000, 10000, 50000, 100000]:
        if quick and n_records > 10000:
            continue
        with _temp_sqlite_pair() as (src_engine, tgt_engine):
            _seed_table(src_engine, _BenchModel, _make_records(n_records))
            syncer = _build_syncer_from_engines(src_engine, tgt_engine)
            _install_target_exists_spy(syncer)
            calls, _ = _install_bulk_copy_spy(syncer)

            start = time.perf_counter()
            syncer.sync_simple_table(_BenchModel, ["name"], batch_size=fixed_batch, exclude_cols=["id"])
            elapsed = time.perf_counter() - start

        suite.add(f"batch={fixed_batch}", elapsed, rows=n_records, batch_size=fixed_batch)

    return suite


# ── benchmark 2: detect_dirty_game_ids ────────────────────────────────


def bench_dirty_detection(quick: bool = False) -> BenchSuite:
    suite = BenchSuite("Benchmark 2: detect_dirty_game_ids Scaling")

    from datetime import date as _date

    game_counts = [10, 100, 1000] if not quick else [100]

    for n in game_counts:
        game_ids = [f"2025101{i:04d}" for i in range(n)]

        with _temp_sqlite_pair() as (src_engine, tgt_engine):
            from src.models.base import Base
            from src.models.game import Game

            Base.metadata.create_all(bind=src_engine)
            Base.metadata.create_all(bind=tgt_engine)

            src_session = Session(bind=src_engine)
            tgt_session = Session(bind=tgt_engine)
            for gid in game_ids:
                day = (int(gid[-3:]) % 28) + 1
                src_session.add(Game(game_id=gid, game_date=_date(2025, 10, day)))
                tgt_session.add(Game(game_id=gid, game_date=_date(2025, 10, day)))
            src_session.commit()
            tgt_session.commit()

            from src.sync.sync_base import detect_dirty_game_ids as _detect_dirty

            start = time.perf_counter()
            try:
                _detect_dirty(src_session, tgt_session, game_ids=game_ids)
            except Exception as e:  # noqa: BLE001
                suite.add(f"n={n}", 0, rows=n, note=f"ERROR: {e}")
                continue
            elapsed = time.perf_counter() - start

            suite.add(f"n={n}", elapsed, rows=n)

    return suite


# ── benchmark 3: connection overhead ───────────────────────────────────


def bench_connection_overhead(quick: bool = False) -> BenchSuite:
    suite = BenchSuite("Benchmark 3: Engine Creation & Session Overhead")
    counts = [1, 5, 10, 20] if not quick else [5, 20]

    for n in counts:
        # source engine + session
        start = time.perf_counter()
        for _ in range(n):
            engine = create_engine("sqlite://")
            session = Session(bind=engine)
            session.close()
            engine.dispose()
        elapsed = time.perf_counter() - start
        suite.add(f"create+dispose {n}x", elapsed, rows=n)

        # OCISync instance creation
        with _temp_sqlite_pair() as (src_engine, tgt_engine):
            start = time.perf_counter()
            for _ in range(n):
                _build_syncer_from_engines(src_engine, tgt_engine)
            elapsed = time.perf_counter() - start
            suite.add(f"OCISync init {n}x", elapsed, rows=n)

    return suite


# ── benchmark 4: _do_bulk_copy_upsert (Postgres only) ──────────────────


def bench_copy_engine(oci_url: str, quick: bool = False) -> BenchSuite:
    suite = BenchSuite("Benchmark 4: _do_bulk_copy_upsert (OCI Postgres)")
    record_counts = [100, 1000, 10000] if not quick else [1000]

    for n in record_counts:
        records = _make_records(n)

        with _temp_sqlite_pair() as (src_engine, _):
            with _oci_target(oci_url) as tgt_engine:
                syncer = _build_syncer_from_engines(src_engine, tgt_engine)
                _install_target_exists_spy(syncer)

                start = time.perf_counter()
                try:
                    syncer._bulk_copy_upsert("bench_table", records, ["name"])
                except Exception as e:  # noqa: BLE001
                    elapsed = time.perf_counter() - start
                    suite.add(f"n={n}", elapsed, rows=n, note=f"ERROR: {e}")
                    continue
                elapsed = time.perf_counter() - start

                suite.add(f"n={n}", elapsed, rows=n)

    return suite


# ── main ───────────────────────────────────────────────────────────────


def main():
    import argparse

    parser = argparse.ArgumentParser(description="OCI Sync Performance Benchmark")
    parser.add_argument("--quick", action="store_true", help="Run smaller sweeps")
    parser.add_argument("--table-sweep", action="store_true", help="Only table read benchmarks")
    parser.add_argument("--dirty-sweep", action="store_true", help="Only dirty detection")
    parser.add_argument("--connection-sweep", action="store_true", help="Only connection overhead")
    parser.add_argument("--copy-sweep", action="store_true", help="Only COPY engine (requires --oci-url)")
    parser.add_argument("--oci-url", help="Real OCI Postgres URL (e.g. postgresql://user:pass@host/db)")
    args = parser.parse_args()

    run_all = not (args.table_sweep or args.copy_sweep or args.dirty_sweep or args.connection_sweep)

    logger.info(f"\n{'#' * 60}")
    logger.info("  OCI Sync Benchmark")
    logger.info(f"  Mode: {'quick' if args.quick else 'full'}")
    logger.info("  Target: SQLite \u2192 SQLite" + (f" \u2192 {args.oci_url[:50]}..." if args.oci_url else ""))
    logger.info(f"  Timestamp: {datetime.now().isoformat()}")
    logger.info(f"{'#' * 60}")

    if run_all or args.table_sweep:
        bench_table_sweep(quick=args.quick).print_report()

    if run_all or args.dirty_sweep:
        bench_dirty_detection(quick=args.quick).print_report()

    if run_all or args.connection_sweep:
        bench_connection_overhead(quick=args.quick).print_report()

    if args.copy_sweep or (run_all and args.oci_url):
        if args.oci_url:
            bench_copy_engine(args.oci_url, quick=args.quick).print_report()
        else:
            logger.info("\n  [SKIP] COPY engine benchmark requires --oci-url")

    logger.info("Done.\n")


if __name__ == "__main__":
    main()
