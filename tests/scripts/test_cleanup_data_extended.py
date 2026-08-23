"""Tests for extended cleanup_data maintenance utility."""

from __future__ import annotations

import time
from pathlib import Path

from scripts.maintenance.cleanup_data import (
    _cleanup_backups,
    _cleanup_empty_logs,
    _cleanup_intermediate_csvs,
    _cleanup_manifests,
    _cleanup_patterns,
    archive_data,
)
from src.utils.refresh_manifest import prune_expired_manifests


def test_cleanup_manifests(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "refresh_manifests"
    manifest_dir.mkdir(parents=True)

    old_manifest = manifest_dir / "20260101_000000_live.json"
    old_manifest.write_text("{}")
    # set mtime to 10 days ago
    old_mtime = time.time() - (10 * 86400)
    import os

    os.utime(old_manifest, (old_mtime, old_mtime))

    new_manifest = manifest_dir / "20260824_000000_live.json"
    new_manifest.write_text("{}")

    results: dict[str, list[Path]] = {"manifests_cleaned": []}
    _cleanup_manifests(tmp_path, dry_run=True, results=results, max_age_days=7)
    assert len(results["manifests_cleaned"]) == 1
    assert old_manifest.exists()

    results_apply: dict[str, list[Path]] = {"manifests_cleaned": []}
    _cleanup_manifests(tmp_path, dry_run=False, results=results_apply, max_age_days=7)
    assert len(results_apply["manifests_cleaned"]) == 1
    assert not old_manifest.exists()
    assert new_manifest.exists()


def test_prune_expired_manifests(tmp_path: Path) -> None:
    old_manifest = tmp_path / "old.json"
    old_manifest.write_text("{}")
    old_mtime = time.time() - (10 * 86400)
    import os

    os.utime(old_manifest, (old_mtime, old_mtime))

    new_manifest = tmp_path / "new.json"
    new_manifest.write_text("{}")

    pruned = prune_expired_manifests(manifest_dir=tmp_path, max_age_days=7, dry_run=True)
    assert len(pruned) == 1
    assert old_manifest.exists()

    pruned_applied = prune_expired_manifests(manifest_dir=tmp_path, max_age_days=7, dry_run=False)
    assert len(pruned_applied) == 1
    assert not old_manifest.exists()
    assert new_manifest.exists()


def test_cleanup_empty_logs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    empty_log = data_dir / "sync_oci_2020.log"
    empty_log.write_text("")
    non_empty_log = data_dir / "sync_oci_2021.log"
    non_empty_log.write_text("hello")

    results: dict[str, list[Path]] = {"empty_logs_cleaned": []}
    _cleanup_empty_logs(data_dir, dry_run=False, results=results)
    assert len(results["empty_logs_cleaned"]) == 1
    assert not empty_log.exists()
    assert non_empty_log.exists()


def test_cleanup_intermediate_csvs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    old_csv = data_dir / "null_player_id_conservative_unresolved_20260101.csv"
    old_csv.write_text("a,b,c")
    old_mtime = time.time() - (20 * 86400)
    import os

    os.utime(old_csv, (old_mtime, old_mtime))

    new_csv = data_dir / "null_player_id_conservative_unresolved_20260824.csv"
    new_csv.write_text("a,b,c")

    results: dict[str, list[Path]] = {"temp_csvs_cleaned": []}
    _cleanup_intermediate_csvs(data_dir, dry_run=False, results=results, max_age_days=14)
    assert len(results["temp_csvs_cleaned"]) == 1
    assert not old_csv.exists()
    assert new_csv.exists()


def test_cleanup_timeout_pngs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    old_png = data_dir / "timeout_20260101.png"
    old_png.write_bytes(b"PNG")
    old_mtime = time.time() - (10 * 86400)
    import os

    os.utime(old_png, (old_mtime, old_mtime))

    results: dict[str, list[Path]] = {"removed": []}
    _cleanup_patterns(data_dir, dry_run=False, results=results)
    assert len(results["removed"]) == 1
    assert not old_png.exists()


def test_cleanup_backups_loose(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    import os

    b1 = data_dir / "kbo_dev.db.backup_1"
    b1.write_bytes(b"db1")
    os.utime(b1, (1000, 1000))

    b2 = data_dir / "kbo_dev.db.backup_2"
    b2.write_bytes(b"db2")
    os.utime(b2, (2000, 2000))

    b3 = data_dir / "kbo_dev.db.backup_3"
    b3.write_bytes(b"db3")
    os.utime(b3, (3000, 3000))

    removed = _cleanup_backups(data_dir, keep_newest=2, dry_run=False)
    assert len(removed) == 1
    assert not b1.exists()
    assert b2.exists()
    assert b3.exists()


def test_archive_data_integration(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    archive_dir = tmp_path / "archive"
    data_dir.mkdir()

    results = archive_data(data_dir=data_dir, archive_dir=archive_dir, dry_run=True)
    assert isinstance(results, dict)
    assert "manifests_cleaned" in results
    assert "empty_logs_cleaned" in results
    assert "temp_csvs_cleaned" in results
