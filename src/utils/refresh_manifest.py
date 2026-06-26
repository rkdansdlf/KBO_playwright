"""Refresh manifest writer for downstream cache invalidation contracts."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.constants import KST

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping, Sequence

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST_DIR = PROJECT_ROOT / "data" / "refresh_manifests"


def infer_topics(
    datasets: Sequence[str] | None = None,
    derived_refresh: Sequence[str] | None = None,
) -> list[str]:
    """
    Handles the infer topics operation.

    Args:
        datasets: Datasets.
        derived_refresh: Derived Refresh.

    Returns:
        List of results.

    """
    dataset_set = {item for item in (datasets or []) if item}
    derived_set = {item for item in (derived_refresh or []) if item}
    topics: set[str] = set()

    if {"game", "game_events", "game_summary"} & dataset_set or {"game", "game_events"} <= dataset_set:
        topics.add("coach_review")

    if {"game_metadata", "game_lineups"} & dataset_set:
        topics.add("coach_matchup")

    if {"game_summary", "game_play_by_play"} & dataset_set:
        topics.add("search_rag")

    if derived_set & {"standings", "matchups", "stat_rankings"}:
        topics.add("leaderboard")

    return sorted(topics)


@dataclass(frozen=True)
class RefreshManifestSpec:
    """RefreshManifestSpec class."""

    phase: str
    target_date: str
    game_ids: Iterable[str]
    datasets: Sequence[str]
    derived_refresh: Sequence[str] | None = None
    topics: Sequence[str] | None = None
    output_dir: Path | None = None
    stability: Mapping[str, Any] | None = None


def write_refresh_manifest(spec: RefreshManifestSpec | None = None, **kwargs: object) -> Path:
    """
    Writes refresh manifest.

    Args:
        spec: Spec.

    Returns:
        Path object.

    """
    if spec is None:
        spec = RefreshManifestSpec(**kwargs)
    elif kwargs:
        msg = "Pass either RefreshManifestSpec or keyword fields, not both"
        raise TypeError(msg)

    output_path = spec.output_dir or DEFAULT_MANIFEST_DIR
    output_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    payload = {
        "phase": spec.phase,
        "target_date": spec.target_date,
        "game_ids": sorted({gid for gid in spec.game_ids if gid}),
        "datasets": list(dict.fromkeys(spec.datasets)),
        "derived_refresh": list(dict.fromkeys(spec.derived_refresh or [])),
        "topics": list(dict.fromkeys(spec.topics or infer_topics(spec.datasets, spec.derived_refresh))),
        "generated_at": datetime.now(KST).isoformat(),
    }
    if spec.stability is not None:
        payload["stability"] = dict(spec.stability)
    path = output_path / f"{stamp}_{spec.phase}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
