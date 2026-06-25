"""Game-level sync: games, details, PBP, play-by-play, player game stats.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameHighlight,
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

logger = logging.getLogger(__name__)

from src.sync.sync_base import (
    GameSyncEligibility,
    _log_sync_eligibility,
    build_game_sync_eligibility,
    detect_dirty_game_ids,
    filter_game_ids_by_year,
)

_COMPACT_METADATA_SOURCE_PAYLOAD_KEYS = (
    "pbp_validation_status",
    "pbp_validation_error",
    "parser_version",
    "source_schema_version",
    "payload_hash",
)
_DETAIL_REPLACE_CHILD_MODELS = (
    GameMetadata,
    GameInningScore,
    GameLineup,
    GameBattingStat,
    GamePitchingStat,
    GameSummary,
    GameHighlight,
)
_RELAY_REPLACE_CHILD_MODELS = (GameEvent, GamePlayByPlay, GameValidationMetrics)


def _serialized_payload_length(payload: object) -> int:
    if isinstance(payload, (dict, list)):
        return len(json.dumps(payload, ensure_ascii=False))
    return len(str(payload))


def _compact_metadata_source_payload_for_limit(payload: object, limit: int | None) -> object:
    """Keep OCI varchar-backed source_payload values under the target length."""
    if payload is None or not limit or _serialized_payload_length(payload) <= limit:
        return payload

    if not isinstance(payload, dict):
        return str(payload)[:limit]

    compact = {key: payload[key] for key in _COMPACT_METADATA_SOURCE_PAYLOAD_KEYS if payload.get(key) not in (None, "")}
    if not compact:
        compact = {"truncated": True}

    if _serialized_payload_length(compact) <= limit:
        return compact

    for drop_key in ("pbp_validation_error", "source_schema_version", "parser_version", "payload_hash"):
        compact.pop(drop_key, None)
        if _serialized_payload_length(compact) <= limit:
            return compact

    status = compact.get("pbp_validation_status")
    if status not in (None, ""):
        fallback = {"pbp_validation_status": str(status), "truncated": True}
        if _serialized_payload_length(fallback) <= limit:
            return fallback

    return {"truncated": True}


@dataclass(frozen=True)
class GameDetailSyncScope:
    """Groups common sync parameters for game detail chunk operations."""

    scoped_game_ids: list[str]
    filters: list
    target_game_ids: list[str] | None
    year: int | None
    days: int | None
    unsynced_only: bool
    batch_size: int


class GameSyncMixin:
    """Mixin for game-level sync operations."""

    def _game_metadata_source_payload_limit(self) -> int | None:
        if hasattr(self, "_cached_game_metadata_source_payload_limit"):
            return self._cached_game_metadata_source_payload_limit

        limit = None
        if getattr(self, "oci_engine", None):
            try:
                for column in inspect(self.oci_engine).get_columns(GameMetadata.__tablename__):
                    if column["name"] == "source_payload":
                        limit = getattr(column["type"], "length", None)
                        break
            except SQLAlchemyError:
                limit = None

        self._cached_game_metadata_source_payload_limit = limit
        return limit

    def _transform_game_metadata_for_target(self, data: dict) -> dict[str, Any]:
        limit = self._game_metadata_source_payload_limit()
        if limit and "source_payload" in data:
            data["source_payload"] = _compact_metadata_source_payload_for_limit(data["source_payload"], limit)
        return data

    def _transform_game_lineup_for_target(self, data: dict) -> dict[str, Any]:
        batting_order = data.get("batting_order")
        appearance_seq = data.get("appearance_seq")
        if batting_order is None or appearance_seq == batting_order:
            return data

        data["batting_order"] = None
        return data

    def sync_games(self, limit: int | None = None, filters: list | None = None, batch_size: int = 5000) -> int:
        """Sync game detail data from SQLite to OCI using Batched UPSERT or COPY"""
        # Load season map for mapping SQLite season_id (year) to OCI season_id (int)
        season_map = self._get_season_map()

        def transform(data: dict[str, Any]) -> dict[str, Any]:
            # If season_id looks like a year (e.g. > 1900), map it
            raw_sid = data.get("season_id")
            if raw_sid and raw_sid > 1900:
                # Default to Regular season (type 0) for these legacy years
                key = (raw_sid, 0)
                if key in season_map:
                    data["season_id"] = season_map[key]
            return data

        # Exclude columns that don't exist on OCI side
        # We must exclude 'id' to avoid PK conflicts, as SQLite and OCI use different surrogate ID sequences.
        # Business key for deduplication/upsert is 'game_id'.
        exclude_cols = [
            "id",
            "created_at",
            "updated_at",
            "home_franchise_id",
            "away_franchise_id",
            "winning_franchise_id",
        ]

        return self.sync_simple_table(
            Game,
            ["game_id"],
            exclude_cols=exclude_cols,
            filters=filters,
            transform_fn=transform,
            batch_size=batch_size,
        )

    def sync_player_game_batting(self, year: int | None = None, batch_size: int = 5000) -> int:
        """Sync player game batting stats from SQLite to OCI"""
        filters = [PlayerGameBatting.game_id.like(f"{year}%")] if year else None
        return self.sync_simple_table(
            PlayerGameBatting,
            ["game_id", "player_id"],
            filters=filters,
            batch_size=batch_size,
        )

    def sync_player_game_pitching(self, year: int | None = None, batch_size: int = 5000) -> int:
        """Sync player game pitching stats from SQLite to OCI"""
        filters = [PlayerGamePitching.game_id.like(f"{year}%")] if year else None
        return self.sync_simple_table(
            PlayerGamePitching,
            ["game_id", "player_id"],
            filters=filters,
            batch_size=batch_size,
        )

    def sync_all_game_data(self, limit: int | None = None) -> dict[str, int]:
        """Sync all game-related data"""
        return {
            "game_schedules": self.sync_game_schedules(limit=limit),
            "games": self.sync_games(limit=limit),
            "player_game_batting": self.sync_player_game_batting(),
            "player_game_pitching": self.sync_player_game_pitching(),
        }

    def _purge_game_detail_children_for_year(self, year: int) -> None:
        """Delete year-scoped child detail rows on OCI before re-sync.
        This prevents stale duplicates when mutable fields (e.g. player_id) change.
        """
        pattern = f"{year}%"
        child_tables = [
            "game_metadata",
            "game_inning_scores",
            "game_lineups",
            "game_batting_stats",
            "game_pitching_stats",
            "game_play_by_play",
            "game_events",
            "game_validation_metrics",
            "game_summary",
            "game_highlights",
            "player_game_batting",
            "player_game_pitching",
        ]

        def purge_child_rows() -> None:
            for table_name in child_tables:
                self.target_session.execute(
                    text(f"DELETE FROM {table_name} WHERE game_id LIKE :pattern"),
                    {"pattern": pattern},
                )
            self.target_session.commit()

        self._run_target_session_with_retries(
            purge_child_rows,
            label=f"purge_game_detail_children_{year}",
        )
        logger.info("🧹 Purged OCI child game-detail rows for year %s", year)

    def _replace_target_child_rows_for_games(
        self,
        child_models: tuple[type, ...],
        game_ids: list[str],
        *,
        label: str,
    ) -> None:
        target_game_ids = sorted({game_id for game_id in game_ids if game_id})
        if not target_game_ids:
            return

        if getattr(self, "target_session", None) is None:
            logger.info("ℹ️ Skipping OCI %s child row replacement: no target session", label)
            return

        def replace_child_rows() -> None:
            for child_model in child_models:
                if not self._target_table_exists(child_model):
                    logger.info("ℹ️ Skipping delete for missing OCI table: %s", child_model.__tablename__)
                    continue
                self.target_session.query(child_model).filter(child_model.game_id.in_(target_game_ids)).delete(
                    synchronize_session=False,
                )
            self.target_session.commit()

        self._run_target_session_with_retries(
            replace_child_rows,
            label=f"replace_target_child_rows_{label}",
        )
        logger.info("🧹 Replaced OCI %s child rows for %s game(s)", label, len(target_game_ids))

    def get_unsynced_or_modified_game_ids(self) -> list[str]:
        """Detect dirty game_ids by comparing game + child-table signatures across local/OCI."""
        return detect_dirty_game_ids(self.sqlite_session, self.target_session)

    def _game_detail_parent_scope(
        self, days: int | None, year: int | None, *, unsynced_only: bool
    ) -> tuple[list, list[str] | None]:
        filters = []
        target_game_ids = None
        if unsynced_only:
            logger.info("🔍 식별 중: OCI에 없거나 로컬에서 최근에 갱신된 게임 데이터를 검사합니다...")
            target_game_ids = filter_game_ids_by_year(self.get_unsynced_or_modified_game_ids(), year)
            if target_game_ids:
                logger.info("🎯 총 %s개의 변경/누락된 게임을 발견했습니다.", len(target_game_ids))
                filters.append(Game.game_id.in_(target_game_ids))
            return filters, target_game_ids

        if days:
            from datetime import datetime, timedelta

            filters.append(Game.game_date >= (datetime.now(KST) - timedelta(days=days)).date())
        if year:
            filters.append(Game.game_id.like(f"{year}%"))
        return filters, target_game_ids

    def _scoped_game_ids(self, filters: list, target_game_ids: list[str] | None) -> list[str]:
        if target_game_ids is not None:
            return target_game_ids
        scoped_query = self.sqlite_session.query(Game.game_id)
        if filters:
            scoped_query = scoped_query.filter(*filters)
        return [row[0] for row in scoped_query.all()]

    def _sync_parent_games_for_details(
        self,
        results: dict[str, int],
        chunk_parent_filters: list,
        publishable_parent_game_ids: list[str] | None,
        *,
        unsynced_only: bool,
        batch_size: int,
    ) -> None:
        logger.info("⚾ Syncing Parent Game Records...")
        if unsynced_only and publishable_parent_game_ids is not None:
            if publishable_parent_game_ids:
                results["games"] = self.sync_games(
                    filters=[Game.game_id.in_(publishable_parent_game_ids)], batch_size=batch_size
                )
            else:
                results["games"] = 0
                logger.info("ℹ️ No publishable parent game rows beyond schedule-only stubs.")
        else:
            results["games"] = self.sync_games(filters=chunk_parent_filters or None, batch_size=batch_size)

    def _sync_game_id_aliases(
        self,
        results: dict[str, int],
        scope: GameDetailSyncScope,
    ) -> None:
        alias_filters = None
        if scope.target_game_ids:
            alias_filters = [GameIdAlias.canonical_game_id.in_(scope.target_game_ids)]
        elif scope.year:
            alias_filters = [GameIdAlias.canonical_game_id.like(f"{scope.year}%")]
        elif scope.days and scope.filters:
            game_ids = [game.game_id for game in self.sqlite_session.query(Game.game_id).filter(*scope.filters).all()]
            alias_filters = [GameIdAlias.canonical_game_id.in_(game_ids)] if game_ids else []

        if alias_filters != []:
            results["game_id_aliases"] = self.sync_simple_table(
                GameIdAlias,
                ["alias_game_id"],
                exclude_cols=["created_at"],
                filters=alias_filters,
                batch_size=scope.batch_size,
            )

    def _game_detail_child_filters(self, filters: list, year: int | None, days: int | None) -> list | None:
        child_filters = []
        if year:
            child_filters.append(text("game_id LIKE :year_pattern").bindparams(year_pattern=f"{year}%"))
        elif days:
            game_ids = [game.game_id for game in self.sqlite_session.query(Game.game_id).filter(*filters).all()]
            if not game_ids:
                logger.info("ℹ️ No games found for the specified period.")
                return []
            child_filters.append(GameMetadata.game_id.in_(game_ids))
        return child_filters or None

    def _prepare_target_game_detail_children(
        self,
        year: int | None,
        *,
        unsynced_only: bool,
        eligibility: GameSyncEligibility,
    ) -> None:
        if year and not unsynced_only:
            self._purge_game_detail_children_for_year(year)
            return
        self._replace_target_child_rows_for_games(
            _DETAIL_REPLACE_CHILD_MODELS, eligibility.detail_game_ids, label="detail"
        )
        self._replace_target_child_rows_for_games(
            _RELAY_REPLACE_CHILD_MODELS, eligibility.relay_game_ids, label="relay"
        )

    @staticmethod
    def _child_filter_for_model(
        model_cls: type,
        child_filters: list | None,
        scoped_game_ids: list[str],
        eligibility: GameSyncEligibility,
    ) -> list | None:
        if model_cls in {GameEvent, GamePlayByPlay}:
            return [model_cls.game_id.in_(eligibility.relay_game_ids)]
        if model_cls is GameValidationMetrics:
            return [model_cls.game_id.in_(scoped_game_ids)]
        detail_models = {
            GameMetadata,
            GameInningScore,
            GameLineup,
            GameBattingStat,
            GamePitchingStat,
            GameSummary,
            GameHighlight,
            PlayerGameBatting,
            PlayerGamePitching,
        }
        if model_cls in detail_models:
            return [model_cls.game_id.in_(eligibility.detail_game_ids)]
        return child_filters

    def sync_game_details(
        self,
        days: int | None = None,
        year: int | None = None,
        *,
        unsynced_only: bool = False,
        batch_size: int = 5000,
    ) -> dict[str, int]:
        if not self.test_connection():
            logger.error("❌ OCI connection failed. Aborting sync_game_details.")
            return {}

        filters, target_game_ids = self._game_detail_parent_scope(days, year, unsynced_only=unsynced_only)
        if unsynced_only and not target_game_ids:
            year_msg = f" ({year})" if year else ""
            logger.info("🎉 모든 게임 데이터%s가 이미 최신 상태입니다. 동기화를 건너뜁니다.", year_msg)
            return {}

        scoped_game_ids = self._scoped_game_ids(filters, target_game_ids)
        if not scoped_game_ids:
            return {}

        if year and not unsynced_only:
            self._purge_game_detail_children_for_year(year)

        scope = GameDetailSyncScope(
            scoped_game_ids=scoped_game_ids,
            filters=filters,
            target_game_ids=target_game_ids,
            year=year,
            days=days,
            unsynced_only=unsynced_only,
            batch_size=batch_size,
        )
        return self._aggregate_game_detail_chunks(scope)

    def sync_game_details_for_ids(self, game_ids: list[str], batch_size: int = 5000) -> dict[str, int]:
        """Sync completed game details for an explicit list of game IDs."""
        scoped_game_ids = list(dict.fromkeys(game_id for game_id in game_ids if game_id))
        if not scoped_game_ids:
            return {}
        if not self.test_connection():
            logger.error("❌ OCI connection failed. Aborting sync_game_details_for_ids.")
            return {}

        filters = [Game.game_id.in_(scoped_game_ids)]
        scope = GameDetailSyncScope(
            scoped_game_ids=scoped_game_ids,
            filters=filters,
            target_game_ids=scoped_game_ids,
            year=None,
            days=None,
            unsynced_only=False,
            batch_size=batch_size,
        )
        return self._aggregate_game_detail_chunks(scope)

    def _aggregate_game_detail_chunks(
        self,
        scope: GameDetailSyncScope,
    ) -> dict[str, int]:
        game_chunk_size = 20
        chunked_game_ids = [
            scope.scoped_game_ids[i : i + game_chunk_size]
            for i in range(0, len(scope.scoped_game_ids), game_chunk_size)
        ]
        logger.info(
            "📦 Splitting game detail sync into %s chunks (max %s games per chunk)",
            len(chunked_game_ids),
            game_chunk_size,
        )

        results: dict[str, int] = {}
        total_chunks = len(chunked_game_ids)
        for idx, chunk_ids in enumerate(chunked_game_ids, start=1):
            logger.info("🚀 Syncing game detail chunk %s/%s (%s games)...", idx, total_chunks, len(chunk_ids))
            chunk_results = self._sync_game_detail_chunk(
                chunk_ids,
                scope,
                skip_year_purge=True,
            )
            for key, val in chunk_results.items():
                if isinstance(val, int):
                    results[key] = results.get(key, 0) + val
                elif isinstance(val, dict):
                    if key not in results:
                        results[key] = {}
                    for subkey, subval in val.items():
                        results[key][subkey] = results[key].get(subkey, 0) + subval

        logger.info("✅ Game Details Sync Summary: %s", results)
        return results

    def _sync_game_detail_chunk(
        self,
        chunk_ids: list[str],
        scope: GameDetailSyncScope,
        *,
        skip_year_purge: bool = False,
        batch_size: int = 5000,
    ) -> dict[str, Any]:
        results = {}
        eligibility = build_game_sync_eligibility(self.sqlite_session, chunk_ids)
        results.update(eligibility.counts())
        _log_sync_eligibility(eligibility)

        publishable_parent_game_ids = eligibility.parent_game_ids if scope.unsynced_only else None

        # We filter parent game sync to only the game IDs in this chunk
        chunk_parent_filters = [Game.game_id.in_(chunk_ids)]
        self._sync_parent_games_for_details(
            results,
            chunk_parent_filters,
            publishable_parent_game_ids,
            unsynced_only=scope.unsynced_only,
            batch_size=batch_size,
        )

        # Sync aliases scoped to the chunk's games
        self._sync_game_id_aliases(results, scope)

        child_filters = self._game_detail_child_filters(scope.filters, scope.year, scope.days)
        if child_filters == []:
            return results

        if skip_year_purge:
            self._replace_target_child_rows_for_games(
                _DETAIL_REPLACE_CHILD_MODELS, eligibility.detail_game_ids, label="detail"
            )
            self._replace_target_child_rows_for_games(
                _RELAY_REPLACE_CHILD_MODELS, eligibility.relay_game_ids, label="relay"
            )
        else:
            self._prepare_target_game_detail_children(
                scope.year, unsynced_only=scope.unsynced_only, eligibility=eligibility
            )

        def get_child_filters(model_cls: type) -> list | None:
            return self._child_filter_for_model(model_cls, child_filters, chunk_ids, eligibility)

        # 1. Game Metadata
        results["metadata"] = self.sync_simple_table(
            GameMetadata,
            ["game_id"],
            exclude_cols=["created_at"],
            filters=get_child_filters(GameMetadata),
            transform_fn=self._transform_game_metadata_for_target,
            batch_size=batch_size,
        )

        # 2. Inning Scores
        results["inning_scores"] = self.sync_simple_table(
            GameInningScore,
            ["game_id", "team_side", "inning"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameInningScore),
            batch_size=batch_size,
        )

        # 3. Lineups
        results["lineups"] = self.sync_simple_table(
            GameLineup,
            ["game_id", "team_side", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameLineup),
            transform_fn=self._transform_game_lineup_for_target,
            batch_size=batch_size,
            dedupe_keys=["game_id", "player_id"],
        )

        # 4. Batting Stats
        results["batting_stats"] = self.sync_simple_table(
            GameBattingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameBattingStat),
            batch_size=batch_size,
            dedupe_keys=["game_id", "player_id"],
        )

        # 5. Pitching Stats
        results["pitching_stats"] = self.sync_simple_table(
            GamePitchingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GamePitchingStat),
            batch_size=batch_size,
            dedupe_keys=["game_id", "player_id"],
        )

        results["play_by_play"] = self._sync_game_play_by_play(filters=get_child_filters(GamePlayByPlay))

        results["events"] = self.sync_simple_table(
            GameEvent,
            ["game_id", "event_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameEvent),
            batch_size=batch_size,
        )
        results["validation_metrics"] = self.sync_simple_table(
            GameValidationMetrics,
            ["game_id"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameValidationMetrics),
            batch_size=batch_size,
        )

        # 7. Game Summary
        results["summary"] = self._sync_game_summary_rows(filters=get_child_filters(GameSummary), batch_size=batch_size)

        # 8. Game Highlights
        results["highlights"] = self.sync_simple_table(
            GameHighlight,
            ["game_id", "highlight_type", "event_seq"],
            exclude_cols=["id", "created_at"],
            filters=get_child_filters(GameHighlight),
            batch_size=batch_size,
        )

        return results

    def sync_specific_game(self, game_id: str) -> dict[str, int]:
        """Sync all related data for a single game_id"""
        if not self.test_connection():
            logger.error("❌ OCI connection failed. Aborting sync_specific_game.")
            return {}

        results = {}
        eligibility = build_game_sync_eligibility(self.sqlite_session, [game_id])
        results.update(eligibility.counts())
        _log_sync_eligibility(eligibility)
        filters = [Game.game_id == game_id]
        detail_filters = [GameMetadata.game_id.in_(eligibility.detail_game_ids)]
        relay_filters = [GamePlayByPlay.game_id.in_(eligibility.relay_game_ids)]

        # Sync Game record
        results["game"] = self.sync_simple_table(
            Game,
            ["game_id"],
            exclude_cols=["created_at", "updated_at"],
            filters=filters,
        )
        results["game_id_aliases"] = self.sync_simple_table(
            GameIdAlias,
            ["alias_game_id"],
            exclude_cols=["created_at"],
            filters=[GameIdAlias.canonical_game_id == game_id],
        )

        results["player_basic"] = self._sync_referenced_player_basic_for_games([game_id])

        # Player IDs can be repaired after an initial crawl. Because Postgres
        # treats NULL values as distinct in unique constraints, an upsert keyed
        # by player_id would otherwise leave stale NULL-player rows beside the
        # repaired rows. For one-game publishing, replace child snapshots.
        if eligibility.detail_game_ids:
            self._replace_target_child_rows_for_games(
                _DETAIL_REPLACE_CHILD_MODELS,
                eligibility.detail_game_ids,
                label="detail",
            )
        if eligibility.relay_game_ids:
            self._replace_target_child_rows_for_games(
                _RELAY_REPLACE_CHILD_MODELS,
                eligibility.relay_game_ids,
                label="relay",
            )

        # Sync children
        results["metadata"] = self.sync_simple_table(
            GameMetadata,
            ["game_id"],
            exclude_cols=["created_at"],
            filters=detail_filters,
            transform_fn=self._transform_game_metadata_for_target,
        )
        results["inning_scores"] = self.sync_simple_table(
            GameInningScore,
            ["game_id", "team_side", "inning"],
            exclude_cols=["created_at"],
            filters=[GameInningScore.game_id.in_(eligibility.detail_game_ids)],
        )
        results["lineups"] = self.sync_simple_table(
            GameLineup,
            ["game_id", "team_side", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameLineup.game_id.in_(eligibility.detail_game_ids)],
            transform_fn=self._transform_game_lineup_for_target,
            dedupe_keys=["game_id", "player_id"],
        )
        results["batting_stats"] = self.sync_simple_table(
            GameBattingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameBattingStat.game_id.in_(eligibility.detail_game_ids)],
            dedupe_keys=["game_id", "player_id"],
        )
        results["pitching_stats"] = self.sync_simple_table(
            GamePitchingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GamePitchingStat.game_id.in_(eligibility.detail_game_ids)],
            dedupe_keys=["game_id", "player_id"],
        )
        results["play_by_play"] = self._sync_game_play_by_play(filters=relay_filters)
        results["events"] = self.sync_simple_table(
            GameEvent,
            ["game_id", "event_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameEvent.game_id.in_(eligibility.relay_game_ids)],
        )
        if self._target_table_exists(GameValidationMetrics):
            results["validation_metrics"] = self.sync_simple_table(
                GameValidationMetrics,
                ["game_id"],
                exclude_cols=["id", "created_at"],
                filters=[GameValidationMetrics.game_id == game_id],
            )
        else:
            logger.info("ℹ️ Skipping missing OCI table: %s", GameValidationMetrics.__tablename__)
            results["validation_metrics"] = 0
        results["summary"] = self._sync_game_summary_rows(
            filters=[GameSummary.game_id.in_(eligibility.detail_game_ids)],
            replace_game_ids=eligibility.detail_game_ids,
        )
        results["highlights"] = self.sync_simple_table(
            GameHighlight,
            ["game_id", "highlight_type", "event_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameHighlight.game_id.in_(eligibility.detail_game_ids)],
        )

        return results

    def sync_pregame_game(self, game_id: str) -> dict[str, int]:
        """Sync only pregame publish tables for one game without touching completed detail datasets."""
        if not game_id:
            return {}

        results: dict[str, int] = {}
        results["game"] = self.sync_simple_table(
            Game,
            ["game_id"],
            exclude_cols=["created_at", "updated_at"],
            filters=[Game.game_id == game_id],
        )
        results["game_id_aliases"] = self.sync_simple_table(
            GameIdAlias,
            ["alias_game_id"],
            exclude_cols=["created_at"],
            filters=[GameIdAlias.canonical_game_id == game_id],
        )
        results["player_basic"] = self._sync_referenced_player_basic_for_games([game_id])

        def delete_existing_lineups() -> None:
            self.target_session.query(GameLineup).filter(GameLineup.game_id == game_id).delete(
                synchronize_session=False,
            )
            self.target_session.commit()

        self._run_target_session_with_retries(
            delete_existing_lineups,
            label=f"delete_pregame_lineups_{game_id}",
        )

        results["metadata"] = self.sync_simple_table(
            GameMetadata,
            ["game_id"],
            exclude_cols=["created_at"],
            filters=[GameMetadata.game_id == game_id],
            transform_fn=self._transform_game_metadata_for_target,
        )
        results["lineups"] = self.sync_simple_table(
            GameLineup,
            ["game_id", "team_side", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameLineup.game_id == game_id],
            transform_fn=self._transform_game_lineup_for_target,
        )
        results["summary"] = self._sync_game_summary_rows(
            filters=[
                GameSummary.game_id == game_id,
                GameSummary.summary_type == "프리뷰",
            ],
            summary_type="프리뷰",
            replace_game_ids=[game_id],
        )

        return results

    def sync_review_summaries_for_games(
        self,
        game_ids: list[str],
        *,
        summary_type: str = "리뷰_WPA",
    ) -> dict[str, int]:
        """Replace and sync review summary rows for a bounded game_id set."""
        target_game_ids = sorted({game_id for game_id in game_ids if game_id})
        if not target_game_ids:
            return {"summary": 0, "games": 0}

        def delete_existing_summaries() -> None:
            for batch in self._chunked(target_game_ids, 500):
                self.target_session.query(GameSummary).filter(
                    GameSummary.game_id.in_(batch),
                    GameSummary.summary_type == summary_type,
                ).delete(synchronize_session=False)
            self.target_session.commit()

        self._run_target_session_with_retries(
            delete_existing_summaries,
            label=f"delete_review_summaries_{summary_type}",
        )

        synced = self._sync_game_summary_rows(
            filters=[
                GameSummary.game_id.in_(target_game_ids),
                GameSummary.summary_type == summary_type,
            ],
            summary_type=summary_type,
            replace_game_ids=target_game_ids,
        )
        return {"summary": synced, "games": len(target_game_ids)}

    def _sync_game_summary_rows(
        self,
        filters: list | None = None,
        *,
        summary_type: str | None = None,
        replace_game_ids: list[str] | None = None,
        batch_size: int = 5000,
    ) -> int:

        query = self.sqlite_session.query(GameSummary)
        if filters:
            query = query.filter(*filters)
        if summary_type:
            query = query.filter(GameSummary.summary_type == summary_type)

        rows = query.all()
        if not rows:
            logger.info("ℹ️  No records for game_summary")
            return 0

        game_ids = sorted(set(replace_game_ids or [row.game_id for row in rows if row.game_id]))
        if game_ids:

            def delete_existing_game_summaries() -> None:
                for batch in self._chunked(game_ids, 500):
                    delete_query = self.target_session.query(GameSummary).filter(GameSummary.game_id.in_(batch))
                    if summary_type:
                        delete_query = delete_query.filter(GameSummary.summary_type == summary_type)
                    delete_query.delete(synchronize_session=False)
                self.target_session.commit()

            self._run_target_session_with_retries(
                delete_existing_game_summaries,
                label="delete_game_summary_rows",
            )
            self._reset_target_sequence_for_table(GameSummary.__tablename__)

        columns = [c.key for c in GameSummary.__table__.columns if c.key not in {"id", "created_at", "updated_at"}]
        records = []
        seen = set()
        for row in rows:
            key = (row.game_id, row.summary_type, row.player_id, row.player_name or "", row.detail_text)
            if key in seen:
                continue
            seen.add(key)
            records.append({column: getattr(row, column) for column in columns if hasattr(row, column)})

        logger.info("🚚 Syncing game_summary (%s rows, batch=%s)...", len(records), batch_size)
        self._bulk_copy_upsert("game_summary", records, [], update_timestamp=False)
        logger.info("   Synced %s/%s rows via COPY...", len(records), len(records))
        return len(records)

    def _sync_game_play_by_play(self, filters: list | None = None) -> int:

        query = self.sqlite_session.query(GamePlayByPlay.game_id).distinct()
        if filters:
            for filter_clause in filters:
                query = query.filter(filter_clause)

        game_ids = [row[0] for row in query.all()]
        if not game_ids:
            return 0

        self._reset_target_sequence_for_table("game_play_by_play")

        def delete_existing_play_by_play() -> None:
            for batch in self._chunked(game_ids, 500):
                self.target_session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id.in_(batch)).delete(
                    synchronize_session=False,
                )
            self.target_session.commit()

        self._run_target_session_with_retries(
            delete_existing_play_by_play,
            label="delete_game_play_by_play_rows",
        )

        # Use sync_simple_table with empty conflict_keys for blind bulk insert
        return self.sync_simple_table(
            GamePlayByPlay,
            conflict_keys=[],  # Blind insert
            exclude_cols=["id"],
            filters=filters,
            batch_size=20000,
        )
