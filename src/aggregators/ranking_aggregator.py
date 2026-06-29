"""Ranking aggregator that normalizes fielding/baserunning stats."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from src.repositories.ranking_repository import RankingRepository

if TYPE_CHECKING:
    from collections.abc import Iterable


@dataclass(frozen=True)
class MetricConfig:
    """MetricConfig class."""

    name: str
    source: str
    value_key: str
    descending: bool = True
    entity_type: str = "PLAYER"
    min_games_field: str | None = None
    min_games: int | None = None
    # Participation qualifiers
    min_pa: int | None = None
    min_ip_outs: int | None = None


FIELDING_METRICS: list[MetricConfig] = [
    MetricConfig(name="fielding_pct", source="FIELDING", value_key="fielding_pct"),
    MetricConfig(name="putouts", source="FIELDING", value_key="putouts"),
    MetricConfig(name="assists", source="FIELDING", value_key="assists"),
    MetricConfig(name="errors", source="FIELDING", value_key="errors", descending=False),
]

BASERUNNING_METRICS: list[MetricConfig] = [
    MetricConfig(name="stolen_bases", source="BASERUNNING", value_key="stolen_bases"),
    MetricConfig(
        name="stolen_base_percentage",
        source="BASERUNNING",
        value_key="stolen_base_percentage",
    ),
    MetricConfig(name="caught_stealing", source="BASERUNNING", value_key="caught_stealing", descending=False),
]

SABER_EXTRA_KEY_MAP = {
    "woba": ["extra_stats", "woba"],
    "wrc_plus": ["extra_stats", "wrc_plus"],
    "war": ["extra_stats", "war"],
    "ops_plus": ["extra_stats", "ops_plus"],
    "clutch": ["extra_stats", "clutch"],
    "lob_pct": ["extra_stats", "lob_pct"],
    "wpa_sum": ["extra_stats", "wpa_sum"],
}

BATTING_METRICS: list[MetricConfig] = [
    MetricConfig(name="avg", source="BATTING", value_key="avg", min_pa=0),
    MetricConfig(name="obp", source="BATTING", value_key="obp", min_pa=0),
    MetricConfig(name="slg", source="BATTING", value_key="slg", min_pa=0),
    MetricConfig(name="ops", source="BATTING", value_key="ops", min_pa=0),
    MetricConfig(name="iso", source="BATTING", value_key="iso", min_pa=0),
    MetricConfig(name="babip", source="BATTING", value_key="babip", min_pa=0),
    MetricConfig(name="xr", source="BATTING", value_key="xr", min_pa=0),
    MetricConfig(name="woba", source="BATTING", value_key="woba", min_pa=0),
    MetricConfig(name="wrc_plus", source="BATTING", value_key="wrc_plus", min_pa=0),
    MetricConfig(name="war", source="BATTING", value_key="war", min_pa=0),
    MetricConfig(name="ops_plus", source="BATTING", value_key="ops_plus", min_pa=0),
    MetricConfig(name="home_runs", source="BATTING", value_key="home_runs"),
    MetricConfig(name="rbi", source="BATTING", value_key="rbi"),
    MetricConfig(name="hits", source="BATTING", value_key="hits"),
    MetricConfig(name="doubles", source="BATTING", value_key="doubles"),
    MetricConfig(name="triples", source="BATTING", value_key="triples"),
    MetricConfig(name="runs", source="BATTING", value_key="runs"),
    MetricConfig(name="stolen_bases", source="BATTING", value_key="stolen_bases"),
    MetricConfig(name="clutch", source="BATTING", value_key="clutch", min_pa=0),
    MetricConfig(name="wpa_sum", source="BATTING", value_key="wpa_sum", min_pa=0),
]

PITCHING_METRICS: list[MetricConfig] = [
    MetricConfig(name="era", source="PITCHING", value_key="era", descending=False, min_ip_outs=0),
    MetricConfig(name="whip", source="PITCHING", value_key="whip", descending=False, min_ip_outs=0),
    MetricConfig(name="fip", source="PITCHING", value_key="fip", descending=False, min_ip_outs=0),
    MetricConfig(name="k_per_nine", source="PITCHING", value_key="k_per_nine", min_ip_outs=0),
    MetricConfig(name="bb_per_nine", source="PITCHING", value_key="bb_per_nine", descending=False, min_ip_outs=0),
    MetricConfig(name="kbb", source="PITCHING", value_key="kbb", min_ip_outs=0),
    MetricConfig(name="war", source="PITCHING", value_key="war_pitch", min_ip_outs=0),
    MetricConfig(name="wins", source="PITCHING", value_key="wins"),
    MetricConfig(name="saves", source="PITCHING", value_key="saves"),
    MetricConfig(name="holds", source="PITCHING", value_key="holds"),
]


class RankingAggregator:
    """Aggregate per-metric rankings across stat categories."""

    def __init__(self, repository: RankingRepository | None = None) -> None:
        """
        Initialize a new instance.

        Args:
            repository: Repository.
            repository: Repository.

        """
        self.repository = repository or RankingRepository()

    def generate_rankings(
        self,
        season: int,
        *,
        fielding_stats: Iterable[dict[str, Any]] | None = None,
        baserunning_stats: Iterable[dict[str, Any]] | None = None,
        batting_stats: Iterable[dict[str, Any]] | None = None,
        pitching_stats: Iterable[dict[str, Any]] | None = None,
        persist: bool = True,
        min_pa: int | None = None,
        min_ip_outs: int | None = None,
    ) -> list[dict[str, Any]]:
        """
        Generate rankings.

        Args:
            season: Season year.
            fielding_stats: Fielding Stats.
            baserunning_stats: Baserunning Stats.
            batting_stats: Batting Stats.
            pitching_stats: Pitching Stats.
            persist: Persist.
            min_pa: Min Pa.
            min_ip_outs: Min Ip Outs.
            season: Season year.
            fielding_stats: Fielding Stats.
            baserunning_stats: Baserunning Stats.
            batting_stats: Batting Stats.
            pitching_stats: Pitching Stats.
            persist: Persist.
            min_pa: Min Pa.
            min_ip_outs: Min Ip Outs.
            season: Season year.

        Returns:
            List of results.

        """
        rankings: list[dict[str, Any]] = []

        if fielding_stats:
            rankings.extend(self._build_rankings(season, fielding_stats, FIELDING_METRICS))
        if baserunning_stats:
            rankings.extend(self._build_rankings(season, baserunning_stats, BASERUNNING_METRICS))

        if batting_stats:
            batting_configs = self._build_batting_configs(batting_stats, min_pa)
            rankings.extend(self._build_rankings(season, batting_stats, batting_configs, kbo_min_pa=min_pa))

        if pitching_stats:
            pitching_configs = self._build_pitching_configs(pitching_stats, min_ip_outs)
            rankings.extend(self._build_rankings(season, pitching_stats, pitching_configs, kbo_min_ip_outs=min_ip_outs))

        if persist and rankings:
            self.repository.save_rankings(rankings)
        return rankings

    def _build_batting_configs(
        self,
        batting_stats: Iterable[dict[str, Any]] | None,
        min_pa: int | None,
    ) -> list[MetricConfig]:
        batting_configs: list[MetricConfig] = []
        if not batting_stats:
            return batting_configs
        for cfg in BATTING_METRICS:
            if cfg.min_pa is not None and min_pa is not None:
                batting_configs.append(
                    MetricConfig(
                        name=cfg.name,
                        source=cfg.source,
                        value_key=cfg.value_key,
                        descending=cfg.descending,
                        entity_type=cfg.entity_type,
                        min_pa=min_pa,
                    ),
                )
                batting_configs.append(
                    MetricConfig(
                        name=cfg.name + "_all",
                        source=cfg.source,
                        value_key=cfg.value_key,
                        descending=cfg.descending,
                        entity_type=cfg.entity_type,
                        min_pa=1,
                    ),
                )
            else:
                batting_configs.append(cfg)
        return batting_configs

    def _build_pitching_configs(
        self,
        pitching_stats: Iterable[dict[str, Any]] | None,
        min_ip_outs: int | None,
    ) -> list[MetricConfig]:
        pitching_configs: list[MetricConfig] = []
        if not pitching_stats:
            return pitching_configs
        for cfg in PITCHING_METRICS:
            if cfg.min_ip_outs is not None and min_ip_outs is not None:
                pitching_configs.append(
                    MetricConfig(
                        name=cfg.name,
                        source=cfg.source,
                        value_key=cfg.value_key,
                        descending=cfg.descending,
                        entity_type=cfg.entity_type,
                        min_ip_outs=min_ip_outs,
                    ),
                )
                pitching_configs.append(
                    MetricConfig(
                        name=cfg.name + "_all",
                        source=cfg.source,
                        value_key=cfg.value_key,
                        descending=cfg.descending,
                        entity_type=cfg.entity_type,
                        min_ip_outs=1,
                    ),
                )
            else:
                pitching_configs.append(cfg)
        return pitching_configs

    def _build_rankings(
        self,
        season: int,
        rows: Iterable[dict[str, Any]],
        metrics: list[MetricConfig],
        kbo_min_pa: int | None = None,
        kbo_min_ip_outs: int | None = None,
    ) -> list[dict[str, Any]]:
        if not rows:
            return []
        rankings: list[dict[str, Any]] = []
        rows_list = list(rows)
        for config in metrics:
            rankings.extend(
                self._rank_single_metric(
                    season,
                    rows_list,
                    config,
                    kbo_min_pa=kbo_min_pa,
                    kbo_min_ip_outs=kbo_min_ip_outs,
                ),
            )
        return rankings

    def _resolve_value(self, row: dict[str, Any], config: MetricConfig) -> float | None:
        value_key = config.value_key
        saber_config = SABER_EXTRA_KEY_MAP.get(value_key)
        if saber_config and len(saber_config) == 2:
            extra = row.get(saber_config[0])
            if isinstance(extra, dict):
                return extra.get(saber_config[1])
            return None
        value = row.get(value_key)
        if value is None:
            extra = row.get("extra_stats")
            if isinstance(extra, dict):
                value = extra.get(value_key) or extra.get(value_key.upper())
        return value

    def _rank_single_metric(
        self,
        season: int,
        rows: list[dict[str, Any]],
        config: MetricConfig,
        kbo_min_pa: int | None = None,
        kbo_min_ip_outs: int | None = None,
    ) -> list[dict[str, Any]]:
        processed = [entry for row in rows if (entry := self._ranking_entry(row, config)) is not None]

        processed.sort(key=lambda item: item["value"], reverse=config.descending)
        ranked: list[dict[str, Any]] = []
        previous_value: float | None = None
        current_rank = 0

        for processed_count, entry in enumerate(processed, start=1):
            value = entry["value"]
            if previous_value is None or value != previous_value:
                current_rank = processed_count

            # Build metadata for UI/API consumption
            entity_extra = self._ranking_extra(
                entry,
                config,
                kbo_min_pa=kbo_min_pa,
                kbo_min_ip_outs=kbo_min_ip_outs,
            )

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
                    "extra": entity_extra,
                },
            )
            previous_value = value
        return ranked

    def _ranking_entry(self, row: dict[str, Any], config: MetricConfig) -> dict[str, Any] | None:
        value = self._resolve_value(row, config)
        if value is None or not self._passes_ranking_filters(row, config):
            return None

        entity_id = row.get("player_id") or row.get("player_name")
        if not entity_id:
            return None

        return {
            "entity_id": str(entity_id),
            "entity_label": row.get("player_name") or str(entity_id),
            "team_id": row.get("team_id") or row.get("team_code"),
            "value": float(value),
            "raw": row,
        }

    def _passes_ranking_filters(self, row: dict[str, Any], config: MetricConfig) -> bool:
        if config.min_games_field and config.min_games:
            games = row.get(config.min_games_field)
            if games is None or games < config.min_games:
                return False

        if config.min_pa is not None:
            pa = row.get("plate_appearances") or 0
            if pa < config.min_pa:
                return False

        if config.min_ip_outs is not None:
            ip_outs = row.get("innings_outs") or 0
            if ip_outs < config.min_ip_outs:
                return False

        return True

    def _ranking_extra(
        self,
        entry: dict[str, Any],
        config: MetricConfig,
        *,
        kbo_min_pa: int | None,
        kbo_min_ip_outs: int | None,
    ) -> dict[str, Any]:
        entity_extra = {}
        if config.source == "BATTING":
            pa = entry["raw"].get("plate_appearances") or 0
            min_pa_threshold = kbo_min_pa if kbo_min_pa is not None else (config.min_pa or 0)
            entity_extra.update({"pa": pa, "min_pa": min_pa_threshold, "qualified": pa >= min_pa_threshold})
        elif config.source == "PITCHING":
            ip_outs = entry["raw"].get("innings_outs") or 0
            min_ip_outs_threshold = kbo_min_ip_outs if kbo_min_ip_outs is not None else (config.min_ip_outs or 0)
            entity_extra.update(
                {
                    "innings_outs": ip_outs,
                    "min_ip_outs": min_ip_outs_threshold,
                    "qualified": ip_outs >= min_ip_outs_threshold,
                },
            )

        entity_extra["rank_mode"] = "all" if config.name.endswith("_all") else "qualified"
        entity_extra["raw"] = entry["raw"]
        return entity_extra
