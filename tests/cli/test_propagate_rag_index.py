"""Tests for explicit dual-index mutation CLI contracts."""

from __future__ import annotations

import json

from src.cli.propagate_rag_index import main


def test_delete_defaults_to_dry_run(capsys) -> None:
    """Plan a delete without opening either database."""
    assert main(["--source-table", "game", "--source-row-id", "g1", "--delete", "--json"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["operation"] == "delete"
    assert payload["source_key"] == "game:g1"
    assert payload["apply"] is False


def test_update_requires_a_valid_payload(tmp_path, capsys) -> None:
    """Reject an update document that lacks required chunk fields."""
    payload_path = tmp_path / "update.json"
    payload_path.write_text(json.dumps({"title": "Only title"}), encoding="utf-8")

    assert (
        main(
            [
                "--source-table",
                "game",
                "--source-row-id",
                "g1",
                "--payload",
                str(payload_path),
            ]
        )
        == 2
    )
    assert "missing fields" in capsys.readouterr().err
