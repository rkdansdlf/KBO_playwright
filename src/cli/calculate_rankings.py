"""Rebuild supported stat_rankings from current season batting/pitching aggregates."""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from src.db.engine import SessionLocal
from src.models.player import PlayerBasic, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.rankings import StatRanking
from src.repositories.ranking_repository import RankingRepository


@dataclass(frozen=True)
class RankingMetric:
    metric: str
    value_attr: str
    descending: bool = True
    min_attr: str | None = None
    min_value: float | int | None = None
    source: str = "SEASON"
    entity_type: str = "PLAYER"


BATTING_METRICS: tuple[RankingMetric, ...] = (
    RankingMetric("batting_avg", "avg", min_attr="at_bats", min_value=1, source="BATTING"),
    RankingMetric("batting_ops", "ops", min_attr="plate_appearances", min_value=1, source="BATTING"),
    RankingMetric("batting_home_runs", "home_runs", min_attr="games", min_value=1, source="BATTING"),
    RankingMetric("batting_hits", "hits", min_attr="games", min_value=1, source="BATTING"),
    RankingMetric("batting_rbi", "rbi", min_attr="games", min_value=1, source="BATTING"),
    RankingMetric("batting_stolen_bases", "stolen_bases", min_attr="games", min_value=1, source="BATTING"),
)

PITCHING_METRICS: tuple[RankingMetric, ...] = (
    RankingMetric("pitching_era", "era", descending=False, min_attr="innings_outs", min_value=1, source="PITCHING"),
    RankingMetric("pitching_wins", "wins", min_attr="games", min_value=1, source="PITCHING"),
    RankingMetric("pitching_saves", "saves", min_attr="games", min_value=1, source="PITCHING"),
    RankingMetric("pitching_holds", "holds", min_attr="games", min_value=1, source="PITCHING"),
    RankingMetric("pitching_strikeouts", "strikeouts", min_attr="innings_outs", min_value=1, source="PITCHING"),
    RankingMetric("pitching_whip", "whip", descending=False, min_attr="innings_outs", min_value=1, source="PITCHING"),
)


def _rank_rows(
    rows: Iterable[object],
    metrics: Sequence[RankingMetric],
    *,
    season: int,
    label_lookup: Dict[int, str],
) -> List[dict]:
    ranked_rows: List[dict] = []
    for metric in metrics:
        processed = []
        for row in rows:
            value = getattr(row, metric.value_attr, None)
            if value is None:
                continue
            if metric.min_attr and metric.min_value is not None:
                min_candidate = getattr(row, metric.min_attr, None)
                if min_candidate is None or min_candidate < metric.min_value:
                    continue
            processed.append(
                {
                    "entity_id": str(row.player_id),
                    "entity_label": label_lookup.get(row.player_id) or str(row.player_id),
                    "team_id": row.canonical_team_code or row.team_code,
                    "value": float(value),
                }
            )

        processed.sort(key=lambda item: item["value"], reverse=metric.descending)
        previous_value = None
        current_rank = 0
        for index, entry in enumerate(processed, start=1):
            if previous_value is None or entry["value"] != previous_value:
                current_rank = index
            ranked_rows.append(
                {
                    "season": season,
                    "metric": metric.metric,
                    "entity_id": entry["entity_id"],
                    "entity_label": entry["entity_label"],
                    "entity_type": metric.entity_type,
                    "team_id": entry["team_id"],
                    "value": entry["value"],
                    "rank": current_rank,
                    "is_tie": previous_value is not None and entry["value"] == previous_value,
                    "source": metric.source,
                    "extra": None,
                }
            )
            previous_value = entry["value"]
    return ranked_rows


def rebuild_rankings(season: int) -> int:
    with SessionLocal() as session:
        batting_rows = session.query(PlayerSeasonBatting).filter(
            PlayerSeasonBatting.season == season,
            PlayerSeasonBatting.league == "REGULAR",
        ).all()
        pitching_rows = session.query(PlayerSeasonPitching).filter(
            PlayerSeasonPitching.season == season,
            PlayerSeasonPitching.league == "REGULAR",
        ).all()
        player_ids = {row.player_id for row in batting_rows}
        player_ids.update(row.player_id for row in pitching_rows)
        label_lookup = {
            row.player_id: row.name
            for row in session.query(PlayerBasic).filter(PlayerBasic.player_id.in_(player_ids)).all()
        } if player_ids else {}

        rankings = []
        rankings.extend(_rank_rows(batting_rows, BATTING_METRICS, season=season, label_lookup=label_lookup))
        rankings.extend(_rank_rows(pitching_rows, PITCHING_METRICS, season=season, label_lookup=label_lookup))

        session.query(StatRanking).filter(StatRanking.season == season).delete(synchronize_session=False)
        session.commit()

    if not rankings:
        print(f"[Rankings] ℹ️ No season stats available for {season}.")
        return 0

    repo = RankingRepository()
    saved = repo.save_rankings(rankings)
    print(f"[Rankings] ✅ Rebuilt {saved} ranking rows for {season}")
    return saved


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Rebuild supported stat_rankings")
    parser.add_argument("--year", type=int, required=True, help="Season year to rebuild")
    args = parser.parse_args(argv)

    rebuild_rankings(args.year)
    return 0


if __name__ == "__main__":
    import sys

    sys.exit(main())
