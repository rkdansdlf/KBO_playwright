"""Tests for the shared rekey manifest assembly.

The applier validates three things about this file — the body hash, the commit,
and the entry count — so each of them is pinned here rather than discovered at
apply time.
"""

from __future__ import annotations

from src.services import rag_rekey_manifest as manifest_module
from src.services.rag_rekey_manifest import (
    IDENTITY_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
    build_manifest,
    database_fingerprint,
    manifest_sha256,
)

ENTRY_ONE = {
    "chunk_id": 1,
    "source_table": "awards",
    "legacy_source_row_id": "id=130",
    "natural_source_row_id": "2021_골든글러브_포수_강민호",
    "disposition": "SAFE_REKEY",
}
ENTRY_TWO = {
    "chunk_id": 2,
    "source_table": "awards",
    "legacy_source_row_id": "id=131",
    "natural_source_row_id": None,
    "disposition": "ORPHAN_SOURCE_ROW",
}


def _build(entries: list[dict] | None = None) -> dict:
    return build_manifest(
        manifest_version="legacy-content-match-v1",
        target_index_version="rag-v2",
        source_tables=("awards",),
        totals={"source_rows": 2},
        unsafe_entry_count=0,
        sources=[{"source_table": "awards", "legacy_chunks": len(entries or [])}],
        entries=ENTRIES if (entries := entries or [ENTRY_ONE, ENTRY_TWO]) is None else entries,
    )


def _body_without_header(document: dict) -> dict:
    return {key: value for key, value in document.items() if key != "manifest_header"}


def test_the_header_hash_covers_the_whole_body() -> None:
    """A body edit must change the hash, or the applier's check is decoration."""
    document = build_manifest(
        manifest_version="producer-v1",
        target_index_version="rag-v2",
        source_tables=("awards",),
        totals={"source_rows": 1},
        unsafe_entry_count=0,
        sources=[{"source_table": "awards"}],
        entries=[ENTRY_ONE],
    )

    assert document["manifest_header"]["manifest_sha256"] == manifest_sha256(_body_without_header(document))


def test_tampering_with_an_entry_changes_the_hash() -> None:
    """The applier recomputes the hash from the body it was given."""
    document = _build()
    document["entries"][0]["natural_source_row_id"] = "2021_골든글러브_1루수_누군가"

    assert document["manifest_header"]["manifest_sha256"] != manifest_sha256(_body_without_header(document))


def test_the_header_schema_versions_are_the_applier_expectations() -> None:
    header = _build()["manifest_header"]
    assert header["manifest_schema_version"] == MANIFEST_SCHEMA_VERSION
    assert header["identity_schema_version"] == IDENTITY_SCHEMA_VERSION


def test_the_entry_count_is_the_actual_count() -> None:
    """A header that disagrees with its own body fails the applier's first check."""
    assert _build()["manifest_header"]["expected_entry_count"] == 2
    assert _build([ENTRY_ONE])["manifest_header"]["expected_entry_count"] == 1


def test_the_disposition_counts_are_derived_not_supplied() -> None:
    """Two sources of truth for one number is how a manifest disagrees with itself."""
    counts = _build()["manifest_header"]["expected_disposition_counts"]
    assert counts == {"SAFE_REKEY": 1, "ORPHAN_SOURCE_ROW": 1}


def test_the_commit_is_recorded_from_the_repository() -> None:
    """The applier refuses a manifest built against a different revision."""
    header = _build()["manifest_header"]
    assert header["git_commit_sha"] == manifest_module.current_git_sha()


def test_an_absent_git_yields_unknown_rather_than_a_guess(monkeypatch) -> None:
    monkeypatch.setattr(manifest_module.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(FileNotFoundError))
    assert manifest_module.current_git_sha() == "unknown"


def test_the_database_fingerprint_is_stable_and_masked(monkeypatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:secret@host:5432/db")
    first = database_fingerprint()
    monkeypatch.setenv("DATABASE_URL", "postgresql://user:other-secret@host:5432/db")

    assert first == database_fingerprint(), "two hosts with the same masked shape must not collide"


def test_header_includes_match_schema_version() -> None:
    manifest = _build()
    assert manifest["manifest_header"]["match_schema_version"] == "content-proof-v1"


ENTRIES = [ENTRY_ONE, ENTRY_TWO]
