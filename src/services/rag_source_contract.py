"""Load environment-specific RAG source requirements."""

from __future__ import annotations

import json
import os
from pathlib import Path

SOURCE_CONTRACT_ENV = "RAG_SOURCE_CONTRACT_PATH"
DEFAULT_SOURCE_CONTRACT_PATH = Path("Docs/references/rag_source_contract.json")
SOURCE_PROFILES = ("production", "staging", "fixture", "dev")


def required_sources_for_profile(
    profile: str | None,
    *,
    contract_path: Path | None = None,
) -> tuple[str, ...]:
    """Return source names required by an optional environment profile."""
    if profile is None:
        return ()
    if profile not in SOURCE_PROFILES:
        error_message = f"Unsupported RAG source profile: {profile}"
        raise ValueError(error_message)
    path = contract_path or Path(os.getenv(SOURCE_CONTRACT_ENV, DEFAULT_SOURCE_CONTRACT_PATH))
    payload = json.loads(path.read_text(encoding="utf-8"))
    sources = payload.get("rag_sources", {})
    if not isinstance(sources, dict):
        error_message = "RAG source contract must contain an object named 'rag_sources'"
        raise TypeError(error_message)
    return tuple(
        sorted(
            str(source)
            for source, statuses in sources.items()
            if isinstance(statuses, dict) and statuses.get(profile) == "required"
        ),
    )
