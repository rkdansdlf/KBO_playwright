"""Hydrate a fresh local runtime SQLite cache from OCI/Postgres."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Dict, Iterable, List, Sequence, Type

from sqlalchemy.orm import Session

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
)
from src.models.player import PlayerBasic, PlayerMovement, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import TeamDailyRoster


@dataclass(frozen=True)
class HydrationSpec:
    label: str
    model: Type
    source_filters: Sequence
    target_filters: Sequence
    replace_scope: bool = True
    exclude_columns: Sequence[str] = ()


class RuntimeHydrator:
    """Copy the minimum operational runtime dataset from OCI into local SQLite."""

    def __init__(self, source_session: Session, target_session: Session):
        self.source_session = source_session
        self.target_session = target_session

    def hydrate_year(self, year: int, *, target_date: date | None = None) -> Dict[str, int]:
        start_of_year = date(year, 1, 1)
        end_of_year = date(year, 12, 31)
        roster_since = start_of_year
        if target_date and target_date.year == year:
            # Pregame/review only need recent roster deltas, but a larger window is still manageable.
            roster_since = max(start_of_year, date.fromordinal(target_date.toordinal() - 45))

        specs = (
            HydrationSpec(
                "player_basic",
                PlayerBasic,
                (),
                (),
                replace_scope=False,
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "player_season_batting",
                PlayerSeasonBatting,
                (PlayerSeasonBatting.season == year, PlayerSeasonBatting.league == "REGULAR"),
                (PlayerSeasonBatting.season == year, PlayerSeasonBatting.league == "REGULAR"),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "player_season_pitching",
                PlayerSeasonPitching,
                (PlayerSeasonPitching.season == year, PlayerSeasonPitching.league == "REGULAR"),
                (PlayerSeasonPitching.season == year, PlayerSeasonPitching.league == "REGULAR"),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "player_movements",
                PlayerMovement,
                (PlayerMovement.movement_date >= start_of_year, PlayerMovement.movement_date <= end_of_year),
                (PlayerMovement.movement_date >= start_of_year, PlayerMovement.movement_date <= end_of_year),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "team_daily_roster",
                TeamDailyRoster,
                (TeamDailyRoster.roster_date >= roster_since, TeamDailyRoster.roster_date <= end_of_year),
                (TeamDailyRoster.roster_date >= roster_since, TeamDailyRoster.roster_date <= end_of_year),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game",
                Game,
                (Game.game_id.like(f"{year}%"),),
                (Game.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_metadata",
                GameMetadata,
                (GameMetadata.game_id.like(f"{year}%"),),
                (GameMetadata.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_inning_scores",
                GameInningScore,
                (GameInningScore.game_id.like(f"{year}%"),),
                (GameInningScore.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_lineups",
                GameLineup,
                (GameLineup.game_id.like(f"{year}%"),),
                (GameLineup.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_batting_stats",
                GameBattingStat,
                (GameBattingStat.game_id.like(f"{year}%"),),
                (GameBattingStat.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_pitching_stats",
                GamePitchingStat,
                (GamePitchingStat.game_id.like(f"{year}%"),),
                (GamePitchingStat.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_events",
                GameEvent,
                (GameEvent.game_id.like(f"{year}%"),),
                (GameEvent.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_summary",
                GameSummary,
                (GameSummary.game_id.like(f"{year}%"),),
                (GameSummary.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "game_play_by_play",
                GamePlayByPlay,
                (GamePlayByPlay.game_id.like(f"{year}%"),),
                (GamePlayByPlay.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
        )

        summary: Dict[str, int] = {}
        for spec in reversed(specs):
            if spec.replace_scope:
                self._delete_scope(spec)
        for spec in specs:
            summary[spec.label] = self._hydrate_spec(spec)
        return summary

    def _delete_scope(self, spec: HydrationSpec) -> None:
        target_query = self.target_session.query(spec.model)
        if spec.target_filters:
            target_query = target_query.filter(*spec.target_filters)
        target_query.delete(synchronize_session=False)
        self.target_session.commit()

    def _hydrate_spec(self, spec: HydrationSpec) -> int:
        source_query = self.source_session.query(spec.model)
        if spec.source_filters:
            source_query = source_query.filter(*spec.source_filters)
        rows = source_query.all()

        if not rows:
            self.target_session.commit()
            return 0

        excluded = {"id", *spec.exclude_columns}
        columns = [column.key for column in spec.model.__table__.columns if column.key not in excluded]
        mappings: List[Dict[str, object]] = []
        for row in rows:
            mappings.append({column: getattr(row, column) for column in columns})

        if not spec.replace_scope:
            for mapping in mappings:
                self.target_session.merge(spec.model(**mapping))
            self.target_session.commit()
            return len(mappings)

        self.target_session.execute(spec.model.__table__.insert(), mappings)
        self.target_session.commit()
        return len(mappings)
