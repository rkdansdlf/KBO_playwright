#!/usr/bin/env python3
"""Cleanup temporary crawler artifacts generated during maintenance and debugging.

Usage examples:
  python scripts/maintenance/cleanup_temp_artifacts.py --dry-run
  python scripts/maintenance/cleanup_temp_artifacts.py --days 7 --whatif
  python scripts/maintenance/cleanup_temp_artifacts.py --only refresh_manifests
"""

from __future__ import annotations

import argparse
import fnmatch
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DAYS = 7


@dataclass(frozen=True)
class ArtifactPlan:
    name: str
    paths: list[Path]
    reason: str


def _parse_timestamp_from_name(path: Path, patterns: Iterable[re.Pattern[str]]) -> datetime | None:
    stem = path.name
    for pattern in patterns:
        match = pattern.search(stem)
        if not match:
            continue
        raw = match.group(1)
        for fmt in ("%Y%m%d_%H%M%S", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
    return None


def _is_stale(
    path: Path,
    cutoff: datetime,
    parser_patterns: Iterable[re.Pattern[str]] | None = None,
) -> bool:
    ts = None
    if parser_patterns is not None:
        ts = _parse_timestamp_from_name(path, parser_patterns)
    if ts is None:
        ts = datetime.fromtimestamp(path.stat().st_mtime)
    return ts < cutoff


def _candidate_files(root: Path, patterns: list[str], recursive: bool = False) -> list[Path]:
    files: list[Path] = []
    if not root.exists():
        return files

    if recursive:
        iterator = root.rglob("*")
    else:
        iterator = root.iterdir()

    for item in iterator:
        if not item.is_file():
            continue
        if any(fnmatch.fnmatch(item.name, pattern) for pattern in patterns):
            files.append(item)
    return files


def _is_protected(path: Path, protected_patterns: list[str]) -> bool:
    try:
        rel = path.relative_to(PROJECT_ROOT)
    except ValueError:
        return False

    rel_text = rel.as_posix()

    return any(fnmatch.fnmatch(rel_text, pattern) for pattern in protected_patterns)


def _collect_refresh_manifests(cutoff: datetime) -> list[ArtifactPlan]:
    base = PROJECT_ROOT / "data" / "refresh_manifests"
    patterns = [re.compile(r"\b(\d{8}_\d{6})_[^/]+\.json$")]
    stale = [p for p in _candidate_files(base, ["*.json"]) if _is_stale(p, cutoff, patterns)]
    if not stale:
        return []
    return [ArtifactPlan(name="refresh_manifests", paths=stale, reason="timestamp/mtime older than cutoff")]


def _collect_quality_gate(cutoff: datetime) -> list[ArtifactPlan]:
    base = PROJECT_ROOT / "data"
    pattern = re.compile(r"quality_gate_(?:.*_)?(\d{8}_\d{6})\.csv$")
    files = [p for p in _candidate_files(base, ["quality_gate_*.csv"]) if _is_stale(p, cutoff, [pattern])]
    if not files:
        return []
    return [ArtifactPlan(name="quality_gate", paths=files, reason="timestamp/mtime older than cutoff")]


def _collect_csv_dir(
    cutoff: datetime, base: Path, glob_patterns: list[str], name: str, use_recursive: bool = False
) -> list[ArtifactPlan]:
    files = [p for p in _candidate_files(base, glob_patterns, recursive=use_recursive) if _is_stale(p, cutoff, None)]
    if not files:
        return []
    return [ArtifactPlan(name=name, paths=files, reason="mtime older than cutoff")]


def _collect_snapshots(cutoff: datetime) -> list[ArtifactPlan]:
    base = PROJECT_ROOT / "snapshots"
    files = [p for p in _candidate_files(base, ["*"], recursive=True) if _is_stale(p, cutoff, None)]
    if not files:
        return []
    return [ArtifactPlan(name="snapshots", paths=files, reason="mtime older than cutoff")]


def _collect_scratch(cutoff: datetime) -> list[ArtifactPlan]:
    base = PROJECT_ROOT / "scratch"
    files = [
        p
        for p in _candidate_files(base, ["*.html", "*.json", "*.txt", "*.png", "*.log"], recursive=True)
        if _is_stale(p, cutoff, None)
    ]
    if not files:
        return []
    return [ArtifactPlan(name="scratch", paths=files, reason="mtime older than cutoff")]


def _collect_root_temp_files(cutoff: datetime, include_json: bool = False) -> list[ArtifactPlan]:
    patterns = ["*.png", "*.html"]
    if include_json:
        patterns.append("*.json")

    files = [p for p in _candidate_files(PROJECT_ROOT, patterns, recursive=False) if _is_stale(p, cutoff, None)]
    if not files:
        return []
    return [ArtifactPlan(name="root", paths=files, reason="mtime older than cutoff")]


def _collect_daily_update_summary(cutoff: datetime) -> list[ArtifactPlan]:
    return _collect_csv_dir(
        cutoff=cutoff,
        base=PROJECT_ROOT / "logs" / "daily_update_summary",
        glob_patterns=["*.json"],
        name="daily_update_summary",
    )


def _collect_quality_reports(cutoff: datetime) -> list[ArtifactPlan]:
    return _collect_csv_dir(
        cutoff=cutoff,
        base=PROJECT_ROOT / "logs" / "quality_reports",
        glob_patterns=["*.json"],
        name="quality_reports",
    )


def collect_all_plans(cutoff: datetime, include_root_json: bool = False) -> dict[str, list[ArtifactPlan]]:
    return {
        "refresh_manifests": _collect_refresh_manifests(cutoff),
        "quality_gate": _collect_quality_gate(cutoff),
        "reference_integrity_repair": _collect_csv_dir(
            cutoff,
            base=PROJECT_ROOT / "data" / "reference_integrity_repair",
            glob_patterns=["*.csv"],
            name="reference_integrity_repair",
        ),
        "player_profile_backfill": _collect_csv_dir(
            cutoff,
            base=PROJECT_ROOT / "data" / "player_profile_backfill",
            glob_patterns=["*.csv"],
            name="player_profile_backfill",
        ),
        "daily_update_summary": _collect_daily_update_summary(cutoff),
        "quality_reports": _collect_quality_reports(cutoff),
        "snapshots": _collect_snapshots(cutoff),
        "scratch": _collect_scratch(cutoff),
        "root": _collect_root_temp_files(cutoff, include_json=include_root_json),
    }


def _flatten(plans: list[ArtifactPlan]) -> list[tuple[str, Path, str]]:
    out: list[tuple[str, Path, str]] = []
    for plan in plans:
        for path in sorted(plan.paths):
            out.append((plan.name, path, plan.reason))
    return out


def _human_readable_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _print_summary(
    by_category: dict[str, list[Path]],
    to_remove: list[tuple[str, Path, str]],
    protected: int,
    args: argparse.Namespace,
) -> None:
    print("Temporary artifact cleanup summary")
    print(f"Days: {args.days}")
    print(f"Dry-run: {'yes' if args.dry_run else 'no'}")
    print(f"What-if: {'yes' if args.whatif else 'no'}")
    print(f"Protected skip count: {protected}")
    print(f"Candidate categories: {', '.join(sorted(by_category.keys())) if by_category else 'none'}")
    print("")

    for category in sorted(by_category):
        paths = by_category[category]
        print(f"- {category}: {len(paths)} file(s)")

    total_files = len(to_remove)
    total_bytes = sum(_human_readable_size(path) for _, path, _ in to_remove)
    print(f"\nTotal removable: {total_files} file(s), {total_bytes:,} bytes")

    if args.verbose:
        print("\nDeletion candidates:")
        for category in sorted(by_category):
            for path in sorted(by_category[category]):
                print(f"  [{category}] {path}")


def _run_cleanup(candidates: list[tuple[str, Path, str]], dry_run: bool, verbose: bool) -> tuple[int, int]:
    removed = 0
    failed = 0

    for category, path, _ in candidates:
        try:
            if dry_run:
                if verbose:
                    print(f"skip-delete: {category} -> {path}")
                continue
            path.unlink()
            removed += 1
            if verbose:
                print(f"deleted: {category} -> {path}")
        except OSError:
            failed += 1

    return removed, failed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Remove temporary artifacts generated by maintenance workflows.")
    parser.add_argument(
        "--days",
        type=int,
        default=DEFAULT_DAYS,
        help="Delete artifacts older than this many days (default: 7)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List what would be removed without deleting",
    )
    parser.add_argument(
        "--whatif",
        action="store_true",
        help="Print summary only without file-by-file listing",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Print per-file details",
    )
    parser.add_argument(
        "--only",
        choices=(
            "refresh_manifests",
            "quality_gate",
            "reference_integrity_repair",
            "player_profile_backfill",
            "daily_update_summary",
            "quality_reports",
            "snapshots",
            "scratch",
            "root",
            "all",
        ),
        default="all",
        help="Run only a specific cleanup category or all categories",
    )
    parser.add_argument(
        "--protect",
        action="append",
        default=None,
        help="Extra glob patterns (relative to repo root) to keep even if stale",
    )
    parser.add_argument(
        "--include-root-json",
        action="store_true",
        help="Also remove root-level *.json files",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.days < 0:
        print("ERROR: --days must be 0 or greater")
        return 2

    default_protect = [
        "data/kbo_dev.db",
        "data/kbo_dev.db-shm",
        "data/kbo_dev.db-wal",
        "data/kbo_auth_state.json",
        "data/*overrides*.csv",
        "data/backups/*.db",
    ]
    protected_patterns = list(default_protect)
    if args.protect:
        protected_patterns.extend(args.protect)

    cutoff = datetime.now() - timedelta(days=args.days)
    all_plans = collect_all_plans(cutoff, include_root_json=args.include_root_json)

    if args.only != "all":
        all_plans = {args.only: all_plans[args.only]} if args.only in all_plans else {}

    candidates = _flatten([plan for plans in all_plans.values() for plan in plans])

    eligible = []
    protected = 0
    by_category: dict[str, list[Path]] = {}
    for category, path, reason in candidates:
        if _is_protected(path, protected_patterns):
            protected += 1
            continue
        by_category.setdefault(category, []).append(path)
        eligible.append((category, path, reason))

    if not eligible:
        print("No stale temporary artifacts found.")
        return 0

    _print_summary(
        by_category,
        eligible,
        protected,
        args,
    )

    if args.whatif:
        print(f"What-if summary only: {len(eligible)} file(s) would be removed.")
        return 0

    removed, failed = _run_cleanup(eligible, args.dry_run, args.verbose)

    if args.dry_run:
        print(f"Dry-run finished. Candidates: {len(eligible)}")
        print("No files were deleted.")
        return 0

    print(f"Deleted: {removed}")
    if failed:
        print(f"Failed: {failed}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
