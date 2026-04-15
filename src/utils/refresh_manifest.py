"""Refresh manifest writer for downstream cache invalidation contracts."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Iterable, Sequence


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_MANIFEST_DIR = PROJECT_ROOT / "data" / "refresh_manifests"


def infer_topics(
    datasets: Sequence[str] | None = None,
    derived_refresh: Sequence[str] | None = None,
) -> list[str]:
    dataset_set = {item for item in (datasets or []) if item}
    derived_set = {item for item in (derived_refresh or []) if item}
    topics: set[str] = set()

    if {"game", "game_events", "game_summary"} & dataset_set:
        topics.add("coach_review")
    elif {"game", "game_events"} <= dataset_set:
        topics.add("coach_review")

    if {"game_metadata", "game_lineups"} & dataset_set:
        topics.add("coach_matchup")

    if {"game_summary", "game_play_by_play"} & dataset_set:
        topics.add("search_rag")

    if derived_set & {"standings", "matchups", "stat_rankings"}:
        topics.add("leaderboard")

    return sorted(topics)


def write_refresh_manifest(
    *,
    phase: str,
    target_date: str,
    game_ids: Iterable[str],
    datasets: Sequence[str],
    derived_refresh: Sequence[str] | None = None,
    topics: Sequence[str] | None = None,
    output_dir: Path | None = None,
) -> Path:
    output_path = output_dir or DEFAULT_MANIFEST_DIR
    output_path.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    payload = {
        "phase": phase,
        "target_date": target_date,
        "game_ids": sorted({gid for gid in game_ids if gid}),
        "datasets": list(dict.fromkeys(datasets)),
        "derived_refresh": list(dict.fromkeys(derived_refresh or [])),
        "topics": list(dict.fromkeys(topics or infer_topics(datasets, derived_refresh))),
        "generated_at": datetime.now().isoformat(),
    }
    path = output_path / f"{stamp}_{phase}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path
