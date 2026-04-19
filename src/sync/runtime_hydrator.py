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

        rows = self._filter_child_rows_with_parent_games(spec, rows)
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

        self._ensure_player_basic_refs(rows)
        self.target_session.execute(spec.model.__table__.insert(), mappings)
        self.target_session.commit()
        return len(mappings)

    def _filter_child_rows_with_parent_games(self, spec: HydrationSpec, rows: List[object]) -> List[object]:
        if spec.model is Game or "game_id" not in spec.model.__table__.columns:
            return rows
        game_ids = sorted({str(getattr(row, "game_id")) for row in rows if getattr(row, "game_id", None)})
        if not game_ids:
            return rows
        existing_ids = {
            str(row[0])
            for row in self.target_session.query(Game.game_id).filter(Game.game_id.in_(game_ids)).all()
        }
        return [row for row in rows if str(getattr(row, "game_id", "")) in existing_ids]

    def _ensure_player_basic_refs(self, rows: Iterable[object]) -> None:
        refs: Dict[int, str] = {}
        for row in rows:
            player_id = getattr(row, "player_id", None)
            if player_id is None:
                continue
            try:
                player_id_int = int(player_id)
            except (TypeError, ValueError):
                continue
            player_name = getattr(row, "player_name", None) or f"Unknown {player_id_int}"
            refs.setdefault(player_id_int, str(player_name))

        if not refs:
            return

        existing_ids = {
            int(row[0])
            for row in self.target_session.query(PlayerBasic.player_id)
            .filter(PlayerBasic.player_id.in_(refs.keys()))
            .all()
        }
        missing_ids = sorted(set(refs) - existing_ids)
        if not missing_ids:
            return

        source_rows = {
            row.player_id: row
            for row in self.source_session.query(PlayerBasic).filter(PlayerBasic.player_id.in_(missing_ids)).all()
        }
        for player_id in missing_ids:
            source_player = source_rows.get(player_id)
            if source_player:
                self.target_session.merge(
                    PlayerBasic(
                        **{
                            column.key: getattr(source_player, column.key)
                            for column in PlayerBasic.__table__.columns
                            if column.key not in {"created_at", "updated_at"}
                        }
                    )
                )
                continue
            self.target_session.merge(
                PlayerBasic(
                    player_id=player_id,
                    name=refs[player_id],
                    status="Unknown/Runtime",
                    status_source="runtime_hydrator",
                )
            )
        self.target_session.flush()
