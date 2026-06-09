"""Hydrate a fresh local runtime SQLite cache from OCI/Postgres."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any
from collections.abc import Iterable, Sequence

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
    GameValidationMetrics,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.models.player import PlayerBasic, PlayerMovement, PlayerSeasonBatting, PlayerSeasonPitching
from src.models.team import TeamDailyRoster


@dataclass(frozen=True)
class HydrationSpec:
    label: str
    model: type
    source_filters: Sequence
    target_filters: Sequence
    replace_scope: bool = True
    exclude_columns: Sequence[str] = ()


class RuntimeHydrator:
    """Copy the minimum operational runtime dataset from OCI into local SQLite."""

    SQLITE_UPSERT_KEYS: dict[type, Sequence[str]] = {
        Game: ("game_id",),
        GameIdAlias: ("alias_game_id",),
        GameMetadata: ("game_id",),
        GameInningScore: ("game_id", "team_side", "inning"),
        GameLineup: ("game_id", "team_side", "appearance_seq"),
        GameBattingStat: ("game_id", "player_id", "appearance_seq"),
        GamePitchingStat: ("game_id", "player_id", "appearance_seq"),
        GameEvent: ("game_id", "event_seq"),
        PlayerBasic: ("player_id",),
        PlayerGameBatting: ("game_id", "player_id"),
        PlayerGamePitching: ("game_id", "player_id"),
    }

    def __init__(self, source_session: Session, target_session: Session):
        self.source_session = source_session
        self.target_session = target_session

    def hydrate_year(
        self,
        year: int,
        *,
        target_date: date | None = None,
        preserve_aliases: bool = False,
    ) -> dict[str, int]:
        start_of_year = date(year, 1, 1)
        end_of_year = date(year, 12, 31)
        roster_since = start_of_year
        if target_date and target_date.year == year:
            # Pregame/review only need recent roster deltas, but a larger window is still manageable.
            roster_since = max(start_of_year, date.fromordinal(target_date.toordinal() - 45))

        specs = [
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
            HydrationSpec(
                "game_validation_metrics",
                GameValidationMetrics,
                (GameValidationMetrics.game_id.like(f"{year}%"),),
                (GameValidationMetrics.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "player_game_batting",
                PlayerGameBatting,
                (PlayerGameBatting.game_id.like(f"{year}%"),),
                (PlayerGameBatting.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
            HydrationSpec(
                "player_game_pitching",
                PlayerGamePitching,
                (PlayerGamePitching.game_id.like(f"{year}%"),),
                (PlayerGamePitching.game_id.like(f"{year}%"),),
                exclude_columns=("created_at", "updated_at"),
            ),
        ]
        if not preserve_aliases:
            game_index = next(index for index, spec in enumerate(specs) if spec.model is Game)
            specs.insert(
                game_index + 1,
                HydrationSpec(
                    "game_id_aliases",
                    GameIdAlias,
                    (GameIdAlias.canonical_game_id.like(f"{year}%"),),
                    (GameIdAlias.canonical_game_id.like(f"{year}%"),),
                    exclude_columns=("created_at", "updated_at"),
                ),
            )

        try:
            summary: dict[str, int] = {}
            preserved_aliases: list[dict[str, Any]] = []
            if preserve_aliases:
                preserved_aliases = self._snapshot_aliases(year)
                self._delete_alias_scope(year)
            for spec in reversed(specs):
                if spec.replace_scope:
                    self._delete_scope(spec)
            all_refs: dict[int, str] = {}
            for spec in specs:
                count, refs = self._hydrate_spec(spec)
                summary[spec.label] = count
                if refs:
                    for k, v in refs.items():
                        all_refs.setdefault(k, v)
                self.target_session.flush()
            if all_refs:
                self._resolve_player_refs(all_refs)
            if preserve_aliases:
                summary["game_id_aliases_preserved"] = self._restore_aliases(preserved_aliases)
            self.target_session.commit()
            return summary
        except Exception:
            self.target_session.rollback()
            raise

    def _snapshot_aliases(self, year: int) -> list[dict[str, Any]]:
        columns = [
            column.key for column in GameIdAlias.__table__.columns if column.key not in {"created_at", "updated_at"}
        ]
        return [
            {column: getattr(row, column) for column in columns}
            for row in self.target_session.query(GameIdAlias)
            .filter(GameIdAlias.canonical_game_id.like(f"{year}%"))
            .all()
        ]

    def _delete_alias_scope(self, year: int) -> None:
        self.target_session.query(GameIdAlias).filter(GameIdAlias.canonical_game_id.like(f"{year}%")).delete(
            synchronize_session=False
        )

    def _restore_aliases(self, aliases: list[dict[str, Any]]) -> int:
        if not aliases:
            return 0
        canonical_ids = sorted({str(alias["canonical_game_id"]) for alias in aliases})
        existing_ids = {
            str(row[0]) for row in self.target_session.query(Game.game_id).filter(Game.game_id.in_(canonical_ids)).all()
        }
        mappings = [a for a in aliases if str(a["canonical_game_id"]) in existing_ids]
        if not mappings:
            return 0
        keys = list(mappings[0].keys())
        stmt = sqlite_insert(GameIdAlias.__table__)
        update_columns = [c for c in keys if c not in ("alias_game_id",)]
        if update_columns:
            stmt = stmt.on_conflict_do_update(
                index_elements=[GameIdAlias.__table__.c.alias_game_id],
                set_={c: getattr(stmt.excluded, c) for c in update_columns},
            )
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=[GameIdAlias.__table__.c.alias_game_id])
        self.target_session.execute(stmt, mappings)
        return len(mappings)

    def _delete_scope(self, spec: HydrationSpec) -> None:
        target_query = self.target_session.query(spec.model)
        if spec.target_filters:
            target_query = target_query.filter(*spec.target_filters)
        target_query.delete(synchronize_session=False)

    def _hydrate_spec(self, spec: HydrationSpec) -> tuple[int, dict[int, str]]:
        source_query = self.source_session.query(spec.model)
        if spec.source_filters:
            source_query = source_query.filter(*spec.source_filters)
        rows = source_query.all()

        if not rows:
            return 0, {}

        rows = self._filter_child_rows_with_parent_games(spec, rows)
        if not rows:
            return 0, {}

        excluded = {"id", *spec.exclude_columns}
        columns = [column.key for column in spec.model.__table__.columns if column.key not in excluded]
        mappings: list[dict[str, object]] = []
        for row in rows:
            mappings.append({column: getattr(row, column) for column in columns})

        if not spec.replace_scope:
            upsert_keys = self.SQLITE_UPSERT_KEYS.get(spec.model)
            if upsert_keys:
                stmt = sqlite_insert(spec.model.__table__)
                update_columns = [c for c in columns if c not in upsert_keys]
                if update_columns:
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[spec.model.__table__.c[c] for c in upsert_keys],
                        set_={c: getattr(stmt.excluded, c) for c in update_columns},
                    )
                else:
                    stmt = stmt.on_conflict_do_nothing(index_elements=[spec.model.__table__.c[c] for c in upsert_keys])
                self.target_session.execute(stmt, mappings)
                return len(mappings), {}
            self.target_session.execute(spec.model.__table__.insert(), mappings)
            return len(mappings), {}

        refs = self._collect_player_refs(rows)
        self._delete_existing_game_id_rows(spec, rows)
        self._insert_mappings(spec, mappings, columns)
        return len(mappings), refs

    def _delete_existing_game_id_rows(self, spec: HydrationSpec, rows: Sequence[object]) -> None:
        table = spec.model.__table__
        if spec.model is Game or "game_id" not in table.columns:
            return
        game_ids = sorted({str(row.game_id) for row in rows if getattr(row, "game_id", None)})
        if not game_ids:
            return
        self.target_session.execute(table.delete().where(table.c.game_id.in_(game_ids)))

    def _insert_mappings(
        self,
        spec: HydrationSpec,
        mappings: list[dict[str, object]],
        columns: Sequence[str],
    ) -> None:
        table = spec.model.__table__
        dialect_name = self.target_session.get_bind().dialect.name
        upsert_keys = self.SQLITE_UPSERT_KEYS.get(spec.model)
        if dialect_name == "sqlite" and upsert_keys:
            stmt = sqlite_insert(table)
            update_columns = [column for column in columns if column not in upsert_keys]
            if update_columns:
                stmt = stmt.on_conflict_do_update(
                    index_elements=[table.c[column] for column in upsert_keys],
                    set_={column: getattr(stmt.excluded, column) for column in update_columns},
                )
            else:
                stmt = stmt.on_conflict_do_nothing(index_elements=[table.c[column] for column in upsert_keys])
            self.target_session.execute(stmt, mappings)
            return
        self.target_session.execute(table.insert(), mappings)

    def _filter_child_rows_with_parent_games(self, spec: HydrationSpec, rows: list[object]) -> list[object]:
        if spec.model is Game or "game_id" not in spec.model.__table__.columns:
            return rows
        game_ids = sorted({str(row.game_id) for row in rows if getattr(row, "game_id", None)})
        if not game_ids:
            return rows
        existing_ids = {
            str(row[0]) for row in self.target_session.query(Game.game_id).filter(Game.game_id.in_(game_ids)).all()
        }
        return [row for row in rows if str(getattr(row, "game_id", "")) in existing_ids]

    @staticmethod
    def _collect_player_refs(rows: Iterable[object]) -> dict[int, str]:
        refs: dict[int, str] = {}
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
        return refs

    def _resolve_player_refs(self, refs: dict[int, str]) -> None:
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

        full_columns = [c.key for c in PlayerBasic.__table__.columns if c.key not in {"created_at", "updated_at"}]
        bulk_mappings: list[dict[str, object]] = []
        for player_id in missing_ids:
            source_player = source_rows.get(player_id)
            if source_player:
                bulk_mappings.append({c: getattr(source_player, c) for c in full_columns})
            else:
                mapping: dict[str, object] = {c: None for c in full_columns}
                mapping.update(
                    player_id=player_id,
                    name=refs[player_id],
                    status="Unknown/Runtime",
                    status_source="runtime_hydrator",
                )
                bulk_mappings.append(mapping)

        stmt = sqlite_insert(PlayerBasic.__table__)
        update_columns = [c for c in full_columns if c != "player_id"]
        stmt = stmt.on_conflict_do_update(
            index_elements=[PlayerBasic.__table__.c.player_id],
            set_={c: getattr(stmt.excluded, c) for c in update_columns},
        )
        self.target_session.execute(stmt, bulk_mappings)
        self.target_session.flush()
