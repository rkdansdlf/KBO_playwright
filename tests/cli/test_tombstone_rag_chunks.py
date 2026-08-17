"""CLI tests for primary RAG tombstone plans."""

from __future__ import annotations

import json

from src.cli.tombstone_rag_chunks import _load_source_keys


def test_load_source_keys_from_inventory_manifest(tmp_path) -> None:
    """Read deleted identities from all source manifest rows."""
    manifest = tmp_path / "inventory.json"
    manifest.write_text(
        json.dumps(
            {
                "sources": [
                    {"deleted_identities": ["game:g1", "game:g1"]},
                    {"deleted_identities": ["rules:r1"]},
                ],
            },
        ),
        encoding="utf-8",
    )

    assert _load_source_keys(None, manifest) == ("game:g1", "rules:r1")
