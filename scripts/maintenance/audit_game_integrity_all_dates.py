"""Read-only driver: run per-date game integrity checks across all game dates.

Wraps ``src.cli.data_integrity_checker.run_integrity_checks`` and loops over every
distinct ``game_date`` so the full 2009-2025 range is audited in one process.

Robustness:
- Resumable: processed dates are persisted to ``<output-dir>/game_integrity_state.json``
  so a killed run can be relaunched and skips already-checked dates.
- Lightweight pool and incremental progress writes (every 20 dates, full summary
  every 100 dates) so progress is observable even if the process is OOM-killed.

Read-only: never writes to the audited database (only the state/summary files).
"""

from __future__ import annotations

import argparse
import json
import os
import time
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from src.cli.data_integrity_checker import run_integrity_checks
from src.db.engine import SessionLocal as _SessionLocal


def _state_path(output_dir: str) -> Path:
    return Path(output_dir) / "game_integrity_state.json"


def _load_processed(output_dir: str) -> set[str]:
    path = _state_path(output_dir)
    if path.exists():
        try:
            return set(json.loads(path.read_text(encoding="utf-8")).get("processed", []))
        except (json.JSONDecodeError, OSError):
            return set()
    return set()


def _save_processed(output_dir: str, processed: set[str]) -> None:
    _state_path(output_dir).write_text(
        json.dumps({"processed": sorted(processed)}, ensure_ascii=False),
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    """Loop integrity checks over all distinct game dates and aggregate results."""
    parser = argparse.ArgumentParser(
        description="Run per-date game integrity checks across all game dates (read-only, resumable)"
    )
    parser.add_argument("--database-url", default=None, help="Local SQLite URL")
    parser.add_argument("--output-dir", default="data/audit")
    parser.add_argument("--year", type=int, default=None, help="Optional year filter")
    args = parser.parse_args(argv)

    database_url = args.database_url or os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("database URL is required via --database-url or DATABASE_URL")
    os.environ["DATABASE_URL"] = database_url
    _SessionLocal.configure(
        bind=create_engine(database_url, pool_size=20, max_overflow=20, pool_timeout=60, pool_pre_ping=True)
    )

    output_dir = args.output_dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    engine = create_engine(
        database_url,
        pool_size=10,
        max_overflow=20,
        pool_timeout=60,
        pool_pre_ping=True,
    )
    with engine.connect() as conn:
        if args.year:
            rows = conn.execute(
                text("SELECT DISTINCT game_date FROM game WHERE game_date LIKE :y ORDER BY game_date"),
                {"y": f"{args.year}-%"},
            ).fetchall()
        else:
            rows = conn.execute(text("SELECT DISTINCT game_date FROM game ORDER BY game_date")).fetchall()
    dates = [r[0] for r in rows]
    total = len(dates)

    processed = _load_processed(output_dir)
    failed_dates: list[dict] = []
    failed_checks_total = 0
    passed_dates = 0
    skipped = 0

    for i, game_date in enumerate(dates):
        if game_date in processed:
            skipped += 1
            continue
        ymd = game_date.replace("-", "")
        try:
            buf = StringIO()
            with redirect_stdout(buf):
                report = run_integrity_checks(ymd)
        except (OSError, RuntimeError, SQLAlchemyError, TypeError, ValueError) as exc:  # keep auditing other dates
            failed_dates.append({"date": game_date, "failed": -1, "detail": [f"ERROR: {exc}"]})
            failed_checks_total += 1
            _save_processed(output_dir, processed | {game_date})
            time.sleep(0.5)
            continue
        if report.overall_passed:
            passed_dates += 1
        else:
            failed_lines = [line for line in buf.getvalue().splitlines() if "❌" in line]
            failed_dates.append(
                {
                    "date": game_date,
                    "failed": report.failed_checks,
                    "detail": failed_lines[:10],
                }
            )
            failed_checks_total += report.failed_checks
        processed.add(game_date)
        if (i + 1) % 20 == 0:
            print(
                f"progress {i + 1}/{total} passed={passed_dates} failed={len(failed_dates)} skipped={skipped}",
                flush=True,
            )
        if (i + 1) % 100 == 0:
            _save_processed(output_dir, processed)
            _write_summary(
                output_dir,
                total,
                passed_dates,
                len(failed_dates),
                failed_checks_total,
                failed_dates,
            )
        time.sleep(0.1)

    _save_processed(output_dir, processed)
    _write_summary(
        output_dir,
        total,
        passed_dates,
        len(failed_dates),
        failed_checks_total,
        failed_dates,
    )
    print(
        f"DONE total={total} passed={passed_dates} "
        f"failed={len(failed_dates)} skipped={skipped} failed_checks={failed_checks_total}"
    )
    return 0


def _write_summary(
    output_dir: str,
    total: int,
    passed: int,
    failed_count: int,
    failed_checks: int,
    failed_dates: list[dict],
) -> None:
    summary = {
        "total_dates": total,
        "passed_dates": passed,
        "failed_dates_count": failed_count,
        "failed_checks_total": failed_checks,
        "failed_dates": failed_dates,
    }
    Path(output_dir).joinpath("game_integrity_all_dates_report.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    raise SystemExit(main())
