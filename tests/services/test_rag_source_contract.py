"""Tests for environment-specific RAG source contracts."""

from __future__ import annotations

import json

import pytest

from src.services.rag_source_contract import required_sources_for_profile


def test_required_sources_are_profile_specific(tmp_path) -> None:
    """Require awards only for production and staging profiles."""
    contract = tmp_path / "contract.json"
    contract.write_text(
        json.dumps(
            {
                "rag_sources": {
                    "awards": {
                        "production": "required",
                        "staging": "required",
                        "fixture": "optional",
                        "dev": "optional",
                    },
                },
            },
        ),
        encoding="utf-8",
    )

    assert required_sources_for_profile("production", contract_path=contract) == ("awards",)
    assert required_sources_for_profile("fixture", contract_path=contract) == ()


def test_unknown_profile_is_rejected(tmp_path) -> None:
    """Reject a profile outside the source contract vocabulary."""
    with pytest.raises(ValueError, match="Unsupported"):
        required_sources_for_profile("qa", contract_path=tmp_path / "unused.json")
