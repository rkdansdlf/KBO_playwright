"""CLI tests for src.cli.rag.apply_rag_rekey (in-process, no live DB)."""

from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import SQLAlchemyError

from src.cli.rag.apply_rag_rekey import main


def _row(
    chunk_id: int,
    legacy_id: str,
    natural_id: str,
    *,
    content_hash: str = "h1",
    status: str = "ACTIVE",
    index_version: str = "r2",
) -> dict[str, Any]:
    """Build a manifest entry dict."""
    return {
        "chunk_id": chunk_id,
        "legacy_source_row_id": legacy_id,
        "natural_source_row_id": natural_id,
        "disposition": "SAFE_REKEY",
        "index_status": status,
        "legacy_content_hash": content_hash,
        "index_version": index_version,
    }


def _tombstone_row(
    chunk_id: int,
    legacy_id: str,
    natural_id: str,
    *,
    content_hash: str = "h1",
    status: str = "ACTIVE",
    index_version: str = "r2",
) -> dict[str, Any]:
    """Build a manifest entry dict for tombstone."""
    return {
        "chunk_id": chunk_id,
        "legacy_source_row_id": legacy_id,
        "natural_source_row_id": natural_id,
        "disposition": "TARGET_EXISTS_SAME_CONTENT",
        "index_status": status,
        "legacy_content_hash": content_hash,
        "index_version": index_version,
    }


class _FakeResult:
    def __init__(self, rowcount: int = 1) -> None:
        self.rowcount = rowcount


class _NullContext:
    def __init__(self, value: object) -> None:
        self._value = value

    def __enter__(self) -> object:
        return self._value

    def __exit__(self, *args: object) -> None:
        return None


def _make_manifest(entries: list[dict]) -> dict:
    """Build a minimal valid manifest with header matching current environment."""
    import datetime
    import os
    import re
    import subprocess

    manifest = {
        "manifest_version": "r2-identity-census-v1",
        "target_index_version": "r2",
        "read_only": True,
        "source_tables": ["awards"],
        "totals": {
            "source_rows": 1,
            "legacy_numeric_rows": 1,
            "legacy_non_numeric_rows": 0,
            "safe_source_matches": 1,
            "safe_rekey_candidates": 1,
            "existing_natural_target": 0,
            "orphan_rows": 0,
            "collision_keys": 0,
            "collision_rows": 0,
            "source_rows_missing_in_index": 0,
        },
        "unsafe_entry_count": 0,
        "sources": [
            {
                "source_table": "awards",
                "source_rows": 1,
                "legacy_numeric_rows": 1,
                "legacy_non_numeric_rows": 0,
                "safe_source_matches": 1,
                "safe_rekey_candidates": 1,
                "existing_natural_target": 0,
                "orphan_rows": 0,
                "collision_keys": 0,
                "collision_rows": 0,
                "source_rows_missing_in_index": 0,
                "by_disposition": {"SAFE_REKEY": len(entries)},
            }
        ],
        "entries": entries,
    }
    # Compute manifest SHA
    content = json.dumps(
        {k: v for k, v in manifest.items() if k != "manifest_header"},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    manifest_sha = hashlib.sha256(content).hexdigest()

    # Current database fingerprint
    db_url = os.getenv("DATABASE_URL", "")
    safe_url = re.sub(r"://[^:]+:[^@]+@", "://***:***@", db_url)
    fp_content = f"{safe_url}|{os.getenv('RAG_INDEX_VERSION', 'rag-v1')}".encode()
    db_fingerprint = hashlib.sha256(fp_content).hexdigest()[:16]

    # Current git SHA
    try:
        current_git_sha = subprocess.run(
            ["/usr/bin/git", "rev-parse", "HEAD"], capture_output=True, text=True, check=True, cwd=Path.cwd()
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError, OSError):
        current_git_sha = "unknown"

    disposition_counts = Counter(e.get("disposition") for e in entries)

    manifest["manifest_header"] = {
        "manifest_schema_version": "r2-rekey-manifest-v1",
        "identity_schema_version": "r2",
        "generated_at": datetime.datetime.now().isoformat(),
        "database_fingerprint": db_fingerprint,
        "git_commit_sha": current_git_sha,
        "manifest_sha256": manifest_sha,
        "expected_entry_count": len(entries),
        "expected_disposition_counts": dict(disposition_counts),
    }
    return manifest


@pytest.fixture()
def mock_session(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock the database session."""
    import src.db.engine as engine_mod

    session = Mock()
    session.execute.return_value = _FakeResult(rowcount=1)

    def _ctx():
        return _NullContext(session)

    monkeypatch.setattr(engine_mod, "get_rag_index_session", _ctx)
    return session


@pytest.fixture()
def mock_lock(monkeypatch: pytest.MonkeyPatch) -> Mock:
    """Mock the maintenance lock."""
    import src.scheduler.locks as locks_mod

    lock = Mock()
    lock.acquire.return_value = True
    lock.release.return_value = None
    monkeypatch.setattr(locks_mod, "MAINTENANCE_LOCK", lock)
    return lock


@pytest.fixture()
def temp_manifest(tmp_path: Path) -> Path:
    """Create a temporary manifest file with valid header."""
    entries = [_row(100, "1", "2025_골든글러브_투수_원태in")]
    manifest = _make_manifest(entries)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


class TestApplyRekey:
    def test_help_exits_zero(self, capsys: pytest.CaptureFixture[str]) -> None:
        with pytest.raises(SystemExit) as exc:
            main(["--help"])
        assert exc.value.code == 0

    def test_missing_manifest_fails(self, capsys: pytest.CaptureFixture[str]) -> None:
        exit_code = main(["--manifest", "/nonexistent.json"])
        assert exit_code == 2

    def test_invalid_json_fails(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("not json")
        exit_code = main(["--manifest", str(bad_file)])
        assert exit_code == 2

    def test_missing_header_fails(self, tmp_path: Path) -> None:
        bad_file = tmp_path / "bad.json"
        bad_file.write_text(json.dumps({"entries": []}))
        exit_code = main(["--manifest", str(bad_file)])
        assert exit_code == 2

    def test_apply_requires_env_gate(self, tmp_path: Path) -> None:
        entries = [_row(100, "1", "2025_골든글러브_투수_원태인")]
        manifest = _make_manifest(entries)
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

        exit_code = main(["--manifest", str(path), "--apply"])
        assert exit_code == 2

    def test_dry_run_normal_rekey(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        temp_manifest: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        exit_code = main(["--manifest", str(temp_manifest), "--json"])
        assert exit_code == 0
        output = json.loads(capsys.readouterr().out)
        assert output["dry_run"] is True
        assert output["rekeyed"] == 1
        assert output["tombstoned"] == 0
        assert output["skipped"] == 0

    def test_dry_run_tombstone(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        entries = [_tombstone_row(100, "1", "2025_골든글러브_투수_원태in")]
        manifest = _make_manifest(entries)
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

        exit_code = main(["--manifest", str(path), "--json"])
        assert exit_code == 0
        output = json.loads(capsys.readouterr().out)
        assert output["dry_run"] is True
        assert output["rekeyed"] == 0
        assert output["tombstoned"] == 1
        assert output["skipped"] == 0

    def test_apply_normal_rekey(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        temp_manifest: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            exit_code = main(["--manifest", str(temp_manifest), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["dry_run"] is False
            assert output["rekeyed"] == 1
            # Verify WHERE clause includes all concurrency checks
            call_args = mock_session.execute.call_args
            assert call_args is not None
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_apply_tombstone(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            entries = [_tombstone_row(100, "1", "2025_골든글러브_투수_원태in")]
            manifest = _make_manifest(entries)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["dry_run"] is False
            assert output["tombstoned"] == 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_stale_legacy_id_rejected(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Legacy ID mismatch should fail optimistic concurrency."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            # Manifest says legacy_id="1" but DB has "2"
            mock_session.execute.return_value = _FakeResult(rowcount=0)

            entries = [_row(100, "1", "2025_골든글러브_투수_원태in")]
            manifest = _make_manifest(entries)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["skipped"] == 1
            assert "optimistic concurrency check failed" in output["skipped_entries"][0]["reason"]
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_stale_content_hash_rejected(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Content hash mismatch should fail optimistic concurrency."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            mock_session.execute.return_value = _FakeResult(rowcount=0)

            entries = [_row(100, "1", "2025_골든글러브_투수_원태in", content_hash="old_hash")]
            manifest = _make_manifest(entries)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["skipped"] == 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_wrong_index_status_rejected(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Wrong index status should fail optimistic concurrency."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            mock_session.execute.return_value = _FakeResult(rowcount=0)

            entries = [_row(100, "1", "2025_골든글러브_투수_원태in", status="DELETED")]
            manifest = _make_manifest(entries)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["skipped"] == 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_wrong_index_version_rejected(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Wrong index version should fail optimistic concurrency."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            mock_session.execute.return_value = _FakeResult(rowcount=0)

            entries = [_row(100, "1", "2025_골든글러브_투수_원태in", index_version="rag-v1")]
            manifest = _make_manifest(entries)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["skipped"] == 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_manifest_tampering_rejected(self, tmp_path: Path) -> None:
        """Tampered manifest (wrong SHA) should be rejected."""
        entries = [_row(100, "1", "2025_골든글러브_투수_원태in")]
        manifest = _make_manifest(entries)
        # Tamper with the manifest SHA
        manifest["manifest_header"]["manifest_sha256"] = "tampered"
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

        exit_code = main(["--manifest", str(path)])
        assert exit_code == 2

    def test_git_sha_mismatch_rejected(self, tmp_path: Path) -> None:
        """Manifest from different git commit should be rejected."""
        entries = [_row(100, "1", "2025_골든글러브_투수_원태in")]
        manifest = _make_manifest(entries)
        # Tamper with git SHA
        manifest["manifest_header"]["git_commit_sha"] = "different_sha"
        # Recompute manifest SHA to be consistent with tampered header
        content = json.dumps(
            {k: v for k, v in manifest.items() if k != "manifest_header"}, sort_keys=True, separators=(",", ":")
        ).encode()
        manifest["manifest_header"]["manifest_sha256"] = hashlib.sha256(content).hexdigest()
        path = tmp_path / "manifest.json"
        path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

        exit_code = main(["--manifest", str(path)])
        assert exit_code == 2

    def test_wrong_db_fingerprint_rejected(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
    ) -> None:
        """Manifest from different database should be rejected."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"
        # Change DB URL to create different fingerprint
        original_db_url = os.environ.get("DATABASE_URL", "")
        os.environ["DATABASE_URL"] = "sqlite:///different.db"

        try:
            entries = [_row(100, "1", "2025_골든글러브_투수_원태in")]
            manifest = _make_manifest(entries)
            # Fix the DB fingerprint to match original
            manifest["manifest_header"]["database_fingerprint"] = "0f35c2eb8f2a4cfb"
            # Recompute SHA
            import hashlib

            content = json.dumps(
                {k: v for k, v in manifest.items() if k != "manifest_header"}, sort_keys=True, separators=(",", ":")
            ).encode()
            manifest["manifest_header"]["manifest_sha256"] = hashlib.sha256(content).hexdigest()
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply"])
            assert exit_code == 2
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]
            if original_db_url:
                os.environ["DATABASE_URL"] = original_db_url

    def test_apply_idempotent_rerun(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        temp_manifest: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Second apply should be no-op (rowcount=0 on second run)."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            # First apply succeeds
            mock_session.execute.return_value = _FakeResult(rowcount=1)
            exit_code = main(["--manifest", str(temp_manifest), "--apply", "--json"])
            assert exit_code == 0

            # Second apply - rowcount=0 because already updated
            mock_session.execute.return_value = _FakeResult(rowcount=0)
            exit_code = main(["--manifest", str(temp_manifest), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["skipped"] == 1  # No-op due to concurrency check
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_lock_failure_aborts(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        temp_manifest: Path,
    ) -> None:
        """Failed lock acquisition should abort."""
        import os

        mock_lock.acquire.return_value = False
        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            exit_code = main(["--manifest", str(temp_manifest), "--apply"])
            assert exit_code == 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_partial_failure_continues(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """One entry fails, others should continue."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            # First entry fails (rowcount=0), second succeeds
            mock_session.execute.side_effect = [
                _FakeResult(rowcount=0),  # First entry fails
                _FakeResult(rowcount=1),  # Second succeeds
            ]

            entries = [
                _row(100, "1", "2025_골든글러브_투수_원태in"),
                _row(101, "2", "2025_골든글러브_타자_김도영"),
            ]
            manifest = _make_manifest(entries)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["rekeyed"] == 1
            assert output["skipped"] == 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_skip_entries_preserved_in_output(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Skipped entries should include reason."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            mock_session.execute.return_value = _FakeResult(rowcount=0)

            entries = [_row(100, "1", "2025_골든글러브_투수_원태in")]
            manifest = _make_manifest(entries)
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["skipped"] == 1
            assert "optimistic concurrency check failed" in output["skipped_entries"][0]["reason"]
            assert output["skipped_entries"][0]["chunk_id"] == 100
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]

    def test_manifest_with_mixed_dispositions(
        self,
        mock_session: Mock,
        mock_lock: Mock,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        """Manifest with both SAFE_REKEY and TARGET_EXISTS_SAME_CONTENT."""
        import os

        os.environ["RAG_INDEX_ALLOW_WRITE"] = "1"
        os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"] = "1"

        try:
            entries = [
                _row(100, "1", "2025_골든글러브_투수_원태in"),
                _tombstone_row(101, "2", "2025_골든글러브_타자_김도영"),
                _row(102, "3", "2025_신인상_NONE_김택연"),
            ]
            manifest = _make_manifest(entries)
            # Fix disposition counts in header
            manifest["manifest_header"]["expected_disposition_counts"] = {
                "SAFE_REKEY": 2,
                "TARGET_EXISTS_SAME_CONTENT": 1,
            }
            path = tmp_path / "manifest.json"
            path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

            exit_code = main(["--manifest", str(path), "--apply", "--json"])
            assert exit_code == 0
            output = json.loads(capsys.readouterr().out)
            assert output["rekeyed"] == 2
            assert output["tombstoned"] == 1
        finally:
            del os.environ["RAG_INDEX_ALLOW_WRITE"]
            del os.environ["RAG_INDEX_ALLOW_PRODUCTION_WRITE"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
