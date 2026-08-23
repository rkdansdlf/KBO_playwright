"""Archive and clean up stale data files from the data/ directory."""

from __future__ import annotations

import argparse
import logging
import shutil
from datetime import datetime
from pathlib import Path

DEFAULT_DATA_DIR = Path("data")
DEFAULT_ARCHIVE_DIR = DEFAULT_DATA_DIR / "archive"
RETENTION_DAYS = 90
MANIFEST_RETENTION_DAYS = 7
INTERMEDIATE_CSV_RETENTION_DAYS = 14

CSV_PATTERNS = [
    "null_player_id_*.csv",
    "quality_gate_*.csv",
    "review_summary_*.csv",
]

BACKUP_PATTERNS = [
    "kbo_dev.db.backup_*",
    "kbo_dev.db.corrupt_bak*",
    "kbo_dev.db.recovered_valid_*",
]

PNG_PATTERNS = [
    "error_*.png",
    "debug_*.png",
    "gamecenter_*.png",
    "hitter_*.png",
    "timeout_*.png",
    "*_debug.png",
]


def _get_file_age_days(path: Path) -> float:
    mtime = path.stat().st_mtime
    age = datetime.now() - datetime.fromtimestamp(mtime)
    return age.total_seconds() / 86400


def _should_archive(pattern: str, age_days: float) -> bool:
    if "null_player_id" in pattern or "quality_gate" in pattern:
        return age_days > RETENTION_DAYS
    if "backup" in pattern:
        return age_days > 7
    return True


def _cleanup_backups(backup_dir: Path, keep_newest: int = 2, *, dry_run: bool = False) -> list[Path]:
    backups: list[Path] = []
    for pat in BACKUP_PATTERNS:
        backups.extend(backup_dir.glob(pat))
    backups = sorted(dict.fromkeys(backups), key=lambda p: p.stat().st_mtime)
    if len(backups) <= keep_newest:
        return []
    to_remove = backups[:-keep_newest]
    removed: list[Path] = []
    for path in to_remove:
        for suffix in ["", "-shm", "-wal"]:
            target = Path(str(path) + suffix) if suffix else path
            if target.exists():
                if not dry_run:
                    target.unlink()
                removed.append(target)
    return removed


def _archive_expired_subdirs(
    archive_dir: Path,
    dry_run: bool,
    results: dict[str, list[Path]],
) -> None:
    for sub_dir_name in ["null_player_id", "quality_gate", "review_summary"]:
        sub_dir = archive_dir / sub_dir_name
        if not sub_dir.exists():
            continue
        for csv_file in sub_dir.glob("*.csv"):
            if _get_file_age_days(csv_file) > RETENTION_DAYS:
                if dry_run:
                    results["archived"].append(csv_file)
                else:
                    dest = archive_dir / f"expired_{sub_dir_name}"
                    dest.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(csv_file), str(dest / csv_file.name))
                    results["archived"].append(csv_file)


def _remove_old_backups(data_dir: Path, dry_run: bool, results: dict[str, list[Path]]) -> None:
    backup_dir = data_dir / "backups"
    if not backup_dir.exists():
        return
    for path in backup_dir.iterdir():
        if _get_file_age_days(path) > RETENTION_DAYS:
            if dry_run:
                results["removed"].append(path)
            else:
                path.unlink()
                results["removed"].append(path)


def _cleanup_patterns(data_dir: Path, dry_run: bool, results: dict[str, list[Path]]) -> None:
    for pattern in PNG_PATTERNS:
        for path in data_dir.glob(pattern):
            age = _get_file_age_days(path)
            if age > 7:
                if dry_run:
                    results["removed"].append(path)
                else:
                    path.unlink()
                    results["removed"].append(path)


def _cleanup_manifests(
    data_dir: Path,
    dry_run: bool,
    results: dict[str, list[Path]],
    max_age_days: int = MANIFEST_RETENTION_DAYS,
) -> None:
    manifest_dir = data_dir / "refresh_manifests"
    if not manifest_dir.exists():
        return
    for path in manifest_dir.glob("*.json"):
        if _get_file_age_days(path) > max_age_days:
            if dry_run:
                results["manifests_cleaned"].append(path)
            else:
                path.unlink()
                results["manifests_cleaned"].append(path)


def _cleanup_empty_logs(data_dir: Path, dry_run: bool, results: dict[str, list[Path]]) -> None:
    for target in [data_dir, data_dir.parent / "logs"]:
        if not target.exists():
            continue
        for log_file in target.glob("*.log"):
            try:
                if log_file.is_file() and log_file.stat().st_size == 0:
                    if dry_run:
                        results["empty_logs_cleaned"].append(log_file)
                    else:
                        log_file.unlink()
                        results["empty_logs_cleaned"].append(log_file)
            except OSError:
                pass


def _cleanup_intermediate_csvs(
    data_dir: Path,
    dry_run: bool,
    results: dict[str, list[Path]],
    max_age_days: int = INTERMEDIATE_CSV_RETENTION_DAYS,
) -> None:
    csv_globs = [
        "null_player_id_conservative_*.csv",
        "player_id_overrides.csv.backup_*",
        "review_summary_backup_*.csv",
        "*_text_relay.csv",
    ]
    for pat in csv_globs:
        for path in data_dir.glob(pat):
            if _get_file_age_days(path) > max_age_days:
                if dry_run:
                    results["temp_csvs_cleaned"].append(path)
                else:
                    path.unlink()
                    results["temp_csvs_cleaned"].append(path)


def archive_data(
    data_dir: Path = DEFAULT_DATA_DIR,
    archive_dir: Path = DEFAULT_ARCHIVE_DIR,
    *,
    dry_run: bool = False,
) -> dict[str, list[Path]]:
    results: dict[str, list[Path]] = {
        "archived": [],
        "removed": [],
        "backups_cleaned": [],
        "manifests_cleaned": [],
        "empty_logs_cleaned": [],
        "temp_csvs_cleaned": [],
    }
    archive_dir.mkdir(parents=True, exist_ok=True)
    _archive_expired_subdirs(archive_dir, dry_run, results)
    _remove_old_backups(data_dir, dry_run, results)
    _cleanup_patterns(data_dir, dry_run, results)
    _cleanup_manifests(data_dir, dry_run, results)
    _cleanup_empty_logs(data_dir, dry_run, results)
    _cleanup_intermediate_csvs(data_dir, dry_run, results)
    results["backups_cleaned"] = _cleanup_backups(data_dir, dry_run=dry_run)
    return results


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Archive and clean up stale data files")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--archive-dir", type=Path, default=DEFAULT_ARCHIVE_DIR)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    results = archive_data(args.data_dir, args.archive_dir, dry_run=args.dry_run)

    for key, items in results.items():
        if items:
            print(f"{key.replace('_', ' ').capitalize()}: {len(items)} files")
    if not any(results.values()):
        print("Nothing to clean up")


if __name__ == "__main__":
    main()
