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
    # Participation qualifiers
    min_pa: Optional[int] = None
    min_ip_outs: Optional[int] = None


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

BATTING_METRICS: List[MetricConfig] = [
    MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=0),
    MetricConfig(name="obp", source="BATTING", value_key="obp", min_pa=0),
    MetricConfig(name="slg", source="BATTING", value_key="slg", min_pa=0),
    MetricConfig(name="ops", source="BATTING", value_key="ops", min_pa=0),
    MetricConfig(name="iso", source="BATTING", value_key="iso", min_pa=0),
    MetricConfig(name="babip", source="BATTING", value_key="babip", min_pa=0),
    MetricConfig(name="xr", source="BATTING", value_key="xr", min_pa=0),
    MetricConfig(name="home_runs", source="BATTING", value_key="home_runs"),
    MetricConfig(name="rbi", source="BATTING", value_key="rbi"),
]

PITCHING_METRICS: List[MetricConfig] = [
    MetricConfig(name="era", source="PITCHING", value_key="era", descending=False, min_ip_outs=0),
    MetricConfig(name="whip", source="PITCHING", value_key="whip", descending=False, min_ip_outs=0),
    MetricConfig(name="fip", source="PITCHING", value_key="fip", descending=False, min_ip_outs=0),
    MetricConfig(name="k_per_nine", source="PITCHING", value_key="k_per_nine", min_ip_outs=0),
    MetricConfig(name="bb_per_nine", source="PITCHING", value_key="bb_per_nine", descending=False, min_ip_outs=0),
    MetricConfig(name="kbb", source="PITCHING", value_key="kbb", min_ip_outs=0),
    MetricConfig(name="wins", source="PITCHING", value_key="wins"),
    MetricConfig(name="saves", source="PITCHING", value_key="saves"),
    MetricConfig(name="holds", source="PITCHING", value_key="holds"),
]


class RankingAggregator:
    """Aggregates per-metric rankings across stat categories."""

    def __init__(self, repository: RankingRepository | None = None):
        self.repository = repository or RankingRepository()

    def generate_rankings(
        self,
        season: int,
        *,
        fielding_stats: Optional[Iterable[Dict[str, Any]]] = None,
        baserunning_stats: Optional[Iterable[Dict[str, Any]]] = None,
        batting_stats: Optional[Iterable[Dict[str, Any]]] = None,
        pitching_stats: Optional[Iterable[Dict[str, Any]]] = None,
        persist: bool = True,
        min_pa: Optional[int] = None,
        min_ip_outs: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        rankings: List[Dict[str, Any]] = []
        
        if fielding_stats:
            rankings.extend(self._build_rankings(season, fielding_stats, FIELDING_METRICS))
        if baserunning_stats:
            rankings.extend(self._build_rankings(season, baserunning_stats, BASERUNNING_METRICS))
        
        if batting_stats:
            # Inject dynamic min_pa into config copies if provided
            batting_configs = []
            for cfg in BATTING_METRICS:
                if cfg.min_pa is not None and min_pa is not None:
                    batting_configs.append(MetricConfig(
                        name=cfg.name, source=cfg.source, value_key=cfg.value_key,
                        descending=cfg.descending, entity_type=cfg.entity_type,
                        min_pa=min_pa
                    ))
                else:
                    batting_configs.append(cfg)
            rankings.extend(self._build_rankings(season, batting_stats, batting_configs))
            
        if pitching_stats:
            pitching_configs = []
            for cfg in PITCHING_METRICS:
                if cfg.min_ip_outs is not None and min_ip_outs is not None:
                    pitching_configs.append(MetricConfig(
                        name=cfg.name, source=cfg.source, value_key=cfg.value_key,
                        descending=cfg.descending, entity_type=cfg.entity_type,
                        min_ip_outs=min_ip_outs
                    ))
                else:
                    pitching_configs.append(cfg)
            rankings.extend(self._build_rankings(season, pitching_stats, pitching_configs))

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
            
            # 1. Min Games Filter
            if config.min_games_field and config.min_games:
                games = row.get(config.min_games_field)
                if games is None or games < config.min_games:
                    continue
            
            # 2. Min PA Filter
            if config.min_pa is not None:
                pa = row.get("plate_appearances") or 0
                if pa < config.min_pa:
                    continue
            
            # 3. Min IP Outs Filter
            if config.min_ip_outs is not None:
                ip_outs = row.get("innings_outs") or 0
                if ip_outs < config.min_ip_outs:
                    continue

            entity_id = row.get("player_id") or row.get("player_name")
            if not entity_id:
                continue
                
            processed.append(
                {
                    "entity_id": str(entity_id),
                    "entity_label": row.get("player_name") or str(entity_id),
                    "team_id": row.get("team_id") or row.get("team_code"),
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
