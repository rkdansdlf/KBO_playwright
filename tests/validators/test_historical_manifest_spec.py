"""Tests for historical archive manifest contract specification (Issue #3)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


def test_historical_manifest_spec_schema_and_checksum_contract() -> None:
    manifest_path = Path("data/manifests/samples/sample_2001_boxscore_manifest.json")
    assert manifest_path.exists(), "Sample manifest must exist"

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["manifest_version"] == "1.0"
    assert manifest["season"] == 2001
    assert "provenance" in manifest
    assert "entries" in manifest
    assert len(manifest["entries"]) > 0

    base_dir = manifest_path.parent
    for entry in manifest["entries"]:
        assert entry["game_id"].startswith(str(manifest["season"]))
        assert len(entry["game_id"]) == 13
        assert entry["status"] in ("ok", "missing", "corrupted")

        payload_file = base_dir / entry["payload_path"]
        assert payload_file.exists(), f"Payload file {payload_file} must exist"

        file_bytes = payload_file.read_bytes()
        computed_sha = hashlib.sha256(file_bytes).hexdigest()
        assert computed_sha == entry["sha256"], "Payload SHA-256 must match manifest entry"
