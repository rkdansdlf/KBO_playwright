"""
Ranking aggregator that normalizes fielding/baserunning stats.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Any, Iterable, Optional

from src.repositories.ranking_repository import RankingRepository


@dataclass(frozen=True)
class MetricConfig:
    name: str
    source: str
    value_key: str
    descending: bool = True
    entity_type: str = "PLAYER"
    min_games_field: Optional[str] = None
    min_games: Optional[int] = None


FIELDING_METRICS: List[MetricConfig] = [
    MetricConfig(name="fielding_pct", source="FIELDING", value_key="fielding_pct"),
    MetricConfig(name="putouts", source="FIELDING", value_key="putouts"),
    MetricConfig(name="assists", source="FIELDING", value_key="assists"),
    MetricConfig(name="errors", source="FIELDING", value_key="errors", descending=False),
]

BASERUNNING_METRICS: List[MetricConfig] = [
    MetricConfig(name="stolen_bases", source="BASERUNNING", value_key="stolen_bases"),
    MetricConfig(
        name="stolen_base_percentage",
        source="BASERUNNING",
        value_key="stolen_base_percentage",
    ),
    MetricConfig(
        name="caught_stealing", source="BASERUNNING", value_key="caught_stealing", descending=False
    ),
]


class RankingAggregator:
    """Aggregates per-metric rankings across stat categories."""

    def __init__(self, repository: RankingRepository | None = None):
        self.repository = repository or RankingRepository()

    def generate_rankings(
        self,
        season: int,
        *,
        fielding_stats: Iterable[Dict[str, Any]],
        baserunning_stats: Iterable[Dict[str, Any]],
        persist: bool = True,
    ) -> List[Dict[str, Any]]:
        rankings: List[Dict[str, Any]] = []
        rankings.extend(self._build_rankings(season, fielding_stats, FIELDING_METRICS))
        rankings.extend(self._build_rankings(season, baserunning_stats, BASERUNNING_METRICS))

        if persist and rankings:
            self.repository.save_rankings(rankings)
        return rankings

    def _build_rankings(
        self,
        season: int,
        rows: Iterable[Dict[str, Any]],
        metrics: List[MetricConfig],
    ) -> List[Dict[str, Any]]:
        if not rows:
            return []
        rankings: List[Dict[str, Any]] = []
        rows_list = list(rows)
        for config in metrics:
            rankings.extend(self._rank_single_metric(season, rows_list, config))
        return rankings

    def _rank_single_metric(
        self,
        season: int,
        rows: List[Dict[str, Any]],
        config: MetricConfig,
    ) -> List[Dict[str, Any]]:
        processed = []
        for row in rows:
            value = row.get(config.value_key)
            if value is None:
                continue
            if config.min_games_field and config.min_games:
                games = row.get(config.min_games_field)
                if games is None or games < config.min_games:
                    continue
            entity_id = row.get("player_id") or row.get("player_name")
            if not entity_id:
                continue
            processed.append(
                {
                    "entity_id": str(entity_id),
                    "entity_label": row.get("player_name") or str(entity_id),
                    "team_id": row.get("team_id"),
                    "value": float(value),
                    "raw": row,
                }
            )

        processed.sort(key=lambda item: item["value"], reverse=config.descending)
        ranked: List[Dict[str, Any]] = []
        previous_value: Optional[float] = None
        current_rank = 0
        processed_count = 0

        for entry in processed:
            processed_count += 1
            value = entry["value"]
            if previous_value is None or value != previous_value:
                current_rank = processed_count
            ranked.append(
                {
                    "season": season,
                    "metric": config.name,
                    "entity_id": entry["entity_id"],
                    "entity_label": entry["entity_label"],
                    "entity_type": config.entity_type,
                    "team_id": entry["team_id"],
                    "value": value,
                    "rank": current_rank,
                    "is_tie": previous_value is not None and value == previous_value,
                    "source": config.source,
                    "extra": {"raw": entry["raw"]},
                }
            )
            previous_value = value
        return ranked
