"""
Game-level sync: games, details, PBP, play-by-play, player game stats.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Callable

from sqlalchemy import inspect, text

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
from src.sync.sync_base import (
    _dedupe_records_for_conflict_keys,
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


class GameSyncMixin:
    """Mixin for game-level sync operations."""

    def _target_table_exists(self, model: type) -> bool:
        if not getattr(self, "oci_engine", None):
            return True
        try:
            return inspect(self.oci_engine).has_table(model.__tablename__)
        except Exception:
            return True

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
            except Exception:
                limit = None

        self._cached_game_metadata_source_payload_limit = limit
        return limit

    def _transform_game_metadata_for_target(self, data: dict) -> dict:
        limit = self._game_metadata_source_payload_limit()
        if limit and "source_payload" in data:
            data["source_payload"] = _compact_metadata_source_payload_for_limit(data["source_payload"], limit)
        return data

    def sync_games(self, limit: int = None, filters: list = None, batch_size: int = 10000) -> int:
        """Sync game detail data from SQLite to OCI using Batched UPSERT or COPY"""

        # Load season map for mapping SQLite season_id (year) to OCI season_id (int)
        season_map = self._get_season_map()

        def transform(data):
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
            Game, ["game_id"], exclude_cols=exclude_cols, filters=filters, transform_fn=transform, batch_size=batch_size
        )

    def sync_player_game_batting(self, limit: int = None) -> int:
        """Sync player game batting stats from SQLite to OCI"""
        return self.sync_simple_table(
            PlayerGameBatting,
            ["game_id", "player_id"],
        )

    def sync_player_game_pitching(self, limit: int = None) -> int:
        """Sync player game pitching stats from SQLite to OCI"""
        return self.sync_simple_table(
            PlayerGamePitching,
            ["game_id", "player_id"],
        )

    def sync_all_game_data(self, limit: int = None) -> dict[str, int]:
        """Sync all game-related data"""
        results = {
            "game_schedules": self.sync_game_schedules(limit=limit),
            "games": self.sync_games(limit=limit),
            "player_game_batting": self.sync_player_game_batting(limit=limit),
            "player_game_pitching": self.sync_player_game_pitching(limit=limit),
        }
        return results

    def _purge_game_detail_children_for_year(self, year: int) -> None:
        """
        Delete year-scoped child detail rows on OCI before re-sync.
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
        for table_name in child_tables:
            self.target_session.execute(
                text(f"DELETE FROM {table_name} WHERE game_id LIKE :pattern"),
                {"pattern": pattern},
            )
        self.target_session.commit()
        print(f"🧹 Purged OCI child game-detail rows for year {year}")

    def get_unsynced_or_modified_game_ids(self) -> list[str]:
        """Detect dirty game_ids by comparing game + child-table signatures across local/OCI."""
        return detect_dirty_game_ids(self.sqlite_session, self.target_session)

    def sync_game_details(
        self, days: int = None, year: int = None, unsynced_only: bool = False, batch_size: int = 10000
    ) -> dict[str, int]:
        """Sync all game detail tables to OCI"""
        results = {}

        if not self.test_connection():
            print("❌ OCI connection failed. Aborting sync_game_details.")
            return results

        filters = []
        target_game_ids = None

        publishable_parent_game_ids = None
        eligibility = None

        if unsynced_only:
            print("🔍 식별 중: OCI에 없거나 로컬에서 최근에 갱신된 게임 데이터를 검사합니다...")
            target_game_ids = self.get_unsynced_or_modified_game_ids()
            target_game_ids = filter_game_ids_by_year(target_game_ids, year)
            if not target_game_ids:
                year_msg = f" ({year})" if year else ""
                print(f"🎉 모든 게임 데이터{year_msg}가 이미 최신 상태입니다. 동기화를 건너뜁니다.")
                return results
            print(f"🎯 총 {len(target_game_ids)}개의 변경/누락된 게임을 발견했습니다.")
            # target_game_ids가 너무 길면 sqlite in_ 절 한도를 초과할 수 있지만, 부분 업데이트라 대개 수십 건 내외임.
            filters.append(Game.game_id.in_(target_game_ids))
        else:
            if days:
                from datetime import datetime, timedelta

                since_date = (datetime.now() - timedelta(days=days)).date()
                filters.append(Game.game_date >= since_date)
            if year:
                filters.append(Game.game_id.like(f"{year}%"))

        scoped_game_ids = target_game_ids
        if scoped_game_ids is None:
            scoped_query = self.sqlite_session.query(Game.game_id)
            if filters:
                scoped_query = scoped_query.filter(*filters)
            scoped_game_ids = [row[0] for row in scoped_query.all()]

        eligibility = build_game_sync_eligibility(self.sqlite_session, scoped_game_ids)
        results.update(eligibility.counts())
        _log_sync_eligibility(eligibility)
        if unsynced_only:
            publishable_parent_game_ids = eligibility.parent_game_ids

        # 0. Sync Parent Games first (Required for Foreign Keys)
        print("⚾ Syncing Parent Game Records...")
        if unsynced_only and target_game_ids is not None:
            if publishable_parent_game_ids:
                results["games"] = self.sync_games(
                    filters=[Game.game_id.in_(publishable_parent_game_ids)], batch_size=batch_size
                )
            else:
                results["games"] = 0
                print("ℹ️ No publishable parent game rows beyond schedule-only stubs.")
        else:
            results["games"] = self.sync_games(filters=filters if filters else None, batch_size=batch_size)

        alias_filters = None
        if unsynced_only and target_game_ids:
            alias_filters = [GameIdAlias.canonical_game_id.in_(target_game_ids)]
        elif year:
            alias_filters = [GameIdAlias.canonical_game_id.like(f"{year}%")]
        elif days and filters:
            game_ids = [g.game_id for g in self.sqlite_session.query(Game.game_id).filter(*filters).all()]
            alias_filters = [GameIdAlias.canonical_game_id.in_(game_ids)] if game_ids else []

        if alias_filters != []:
            results["game_id_aliases"] = self.sync_simple_table(
                GameIdAlias,
                ["alias_game_id"],
                exclude_cols=["created_at"],
                filters=alias_filters,
                batch_size=batch_size,
            )

        # Build filters for child tables (they often use game_id instead of game_date)
        child_filters = []
        if year:
            child_filters.append(text(f"game_id LIKE '{year}%'"))
        elif days:
            game_ids = [g.game_id for g in self.sqlite_session.query(Game.game_id).filter(*filters).all()]
            if game_ids:
                quoted_ids = [f"'{gid}'" for gid in game_ids]
                child_filters.append(text(f"game_id IN ({','.join(quoted_ids)})"))
            else:
                print("ℹ️ No games found for the specified period.")
                return results

        if year and not unsynced_only:
            # Remove existing year-scoped child rows first to avoid stale/null duplicates.
            self._purge_game_detail_children_for_year(year)

        def get_child_filters(model_cls):
            if model_cls in {GameEvent, GamePlayByPlay}:
                return [model_cls.game_id.in_(eligibility.relay_game_ids)]
            if model_cls is GameValidationMetrics:
                return [model_cls.game_id.in_(scoped_game_ids)]
            if model_cls in {GameMetadata, GameInningScore, GameLineup, GameBattingStat, GamePitchingStat, GameSummary, GameHighlight, PlayerGameBatting, PlayerGamePitching}:  # fmt: skip
                return [model_cls.game_id.in_(eligibility.detail_game_ids)]
            return child_filters if child_filters else None

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
            batch_size=batch_size,
        )

        # 4. Batting Stats
        results["batting_stats"] = self.sync_simple_table(
            GameBattingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameBattingStat),
            batch_size=batch_size,
        )

        # 5. Pitching Stats
        results["pitching_stats"] = self.sync_simple_table(
            GamePitchingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GamePitchingStat),
            batch_size=batch_size,
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

        print(f"✅ Game Details Sync Summary: {results}")
        return results

    def sync_specific_game(self, game_id: str) -> dict[str, int]:
        """Sync all related data for a single game_id"""
        if not self.test_connection():
            print("❌ OCI connection failed. Aborting sync_specific_game.")
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
            Game, ["game_id"], exclude_cols=["created_at", "updated_at"], filters=filters
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
        detail_child_models = (
            GameMetadata,
            GameInningScore,
            GameLineup,
            GameBattingStat,
            GamePitchingStat,
            GameSummary,
            GameHighlight,
        )
        relay_child_models = (GameEvent, GamePlayByPlay, GameValidationMetrics)
        if eligibility.detail_game_ids:
            for child_model in detail_child_models:
                if not self._target_table_exists(child_model):
                    print(f"ℹ️ Skipping delete for missing OCI table: {child_model.__tablename__}")
                    continue
                self.target_session.query(child_model).filter(child_model.game_id == game_id).delete(
                    synchronize_session=False
                )
        if eligibility.relay_game_ids:
            for child_model in relay_child_models:
                if not self._target_table_exists(child_model):
                    print(f"ℹ️ Skipping delete for missing OCI table: {child_model.__tablename__}")
                    continue
                self.target_session.query(child_model).filter(child_model.game_id == game_id).delete(
                    synchronize_session=False
                )
        self.target_session.commit()

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
        )
        results["batting_stats"] = self.sync_simple_table(
            GameBattingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameBattingStat.game_id.in_(eligibility.detail_game_ids)],
        )
        results["pitching_stats"] = self.sync_simple_table(
            GamePitchingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GamePitchingStat.game_id.in_(eligibility.detail_game_ids)],
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
            print(f"ℹ️ Skipping missing OCI table: {GameValidationMetrics.__tablename__}")
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

        if not self.test_connection():
            print("❌ OCI connection failed. Aborting sync_pregame_game.")
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

        self.target_session.query(GameLineup).filter(GameLineup.game_id == game_id).delete(synchronize_session=False)
        self.target_session.commit()

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

        for batch in self._chunked(target_game_ids, 500):
            self.target_session.query(GameSummary).filter(
                GameSummary.game_id.in_(batch),
                GameSummary.summary_type == summary_type,
            ).delete(synchronize_session=False)
        self.target_session.commit()

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
        filters: list = None,
        *,
        summary_type: str | None = None,
        replace_game_ids: list[str] | None = None,
        batch_size: int = 10000,
    ) -> int:

        query = self.sqlite_session.query(GameSummary)
        if filters:
            query = query.filter(*filters)
        if summary_type:
            query = query.filter(GameSummary.summary_type == summary_type)

        rows = query.all()
        if not rows:
            print("ℹ️  No records for game_summary")
            return 0

        game_ids = sorted(set(replace_game_ids or [row.game_id for row in rows if row.game_id]))
        if game_ids:
            for batch in self._chunked(game_ids, 500):
                delete_query = self.target_session.query(GameSummary).filter(GameSummary.game_id.in_(batch))
                if summary_type:
                    delete_query = delete_query.filter(GameSummary.summary_type == summary_type)
                delete_query.delete(synchronize_session=False)
            self.target_session.commit()
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

        print(f"🚚 Syncing game_summary ({len(records)} rows, batch={batch_size})...")
        self._bulk_copy_upsert("game_summary", records, [], update_timestamp=False)
        print(f"   Synced {len(records)}/{len(records)} rows via COPY...")
        return len(records)

    def _sync_game_play_by_play(self, filters: list = None) -> int:

        query = self.sqlite_session.query(GamePlayByPlay.game_id).distinct()
        if filters:
            for filter_clause in filters:
                query = query.filter(filter_clause)

        game_ids = [row[0] for row in query.all()]
        if not game_ids:
            return 0

        self._reset_target_sequence_for_table("game_play_by_play")

        for batch in self._chunked(game_ids, 500):
            self.target_session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id.in_(batch)).delete(
                synchronize_session=False
            )
        self.target_session.commit()

        # Use sync_simple_table with empty conflict_keys for blind bulk insert
        return self.sync_simple_table(
            GamePlayByPlay,
            conflict_keys=[],  # Blind insert
            exclude_cols=["id"],
            filters=filters,
            batch_size=20000,
        )

    def sync_simple_table(
        self,
        model: type,
        conflict_keys: list[str],
        exclude_cols: list[str] = None,
        filters: list = None,
        transform_fn: Callable | None = None,
        batch_size: int = 10000,
        update_timestamp: bool | None = None,
    ) -> int:
        """Generic sync parameter for simple tables using Batched UPSERT or COPY"""
        if exclude_cols is None:
            exclude_cols = ["id"]  # Default to exclude ID for auto-inc compatibility
        elif "id" not in exclude_cols:
            exclude_cols.append("id")

        if not self._target_table_exists(model):
            print(f"ℹ️ Skipping missing OCI table: {model.__tablename__}")
            return 0

        # Use all columns except those explicitly excluded and not present in target DB
        if getattr(self, "oci_engine", None) is not None:
            target_column_defs = {c["name"]: c for c in inspect(self.oci_engine).get_columns(model.__tablename__)}
            target_columns = set(target_column_defs)
        else:
            target_column_defs = {}
            target_columns = {c.key for c in model.__table__.columns}

        sqlite_bind = None
        if hasattr(self.sqlite_session, "get_bind"):
            try:
                sqlite_bind = self.sqlite_session.get_bind()
            except Exception:
                pass

        if sqlite_bind is not None:
            local_columns = {c["name"] for c in inspect(sqlite_bind).get_columns(model.__tablename__)}
        else:
            local_columns = {c.key for c in model.__table__.columns}

        columns = [
            c.key
            for c in model.__table__.columns
            if c.key not in exclude_cols and c.key in target_columns and c.key in local_columns
        ]
        if model is GameMetadata and "source_payload" in columns:
            source_payload_type = target_column_defs["source_payload"].get("type")
            source_payload_length = getattr(source_payload_type, "length", None)
            if source_payload_length and source_payload_length <= 255:
                columns.remove("source_payload")
                print("ℹ️ Skipping game_metadata.source_payload for legacy OCI varchar column")
        if not columns:
            print(f"ℹ️ No compatible columns for {model.__tablename__}")
            return 0

        query = self.sqlite_session.query(*[getattr(model, column) for column in columns])
        if filters:
            query = query.filter(*filters)

        total_count = query.count()
        if total_count == 0:
            print(f"ℹ️  No records for {model.__tablename__}")
            return 0

        print(f"🚚 Syncing {model.__tablename__} ({total_count} rows, batch={batch_size})...")

        # Always use Bulk COPY Upsert to be safe from schema mismatches (e.g. created_at/updated_at missing on OCI)
        synced = 0
        for offset in range(0, total_count, batch_size):
            rows = query.offset(offset).limit(batch_size).all()
            records = []
            for row in rows:
                mapping = getattr(row, "_mapping", None)
                if mapping is not None:
                    data = {c: mapping[c] for c in columns if c in mapping}
                else:
                    data = {c: getattr(row, c) for c in columns if hasattr(row, c)}

                # Ensure created_at/updated_at are never null if the table requires them
                now = datetime.now()
                if "created_at" in columns and data.get("created_at") is None:
                    data["created_at"] = now
                if "updated_at" in columns and data.get("updated_at") is None:
                    data["updated_at"] = now

                # Apply transformation if provided
                if transform_fn:
                    data = transform_fn(data)

                # Handle JSON/Dict serialization for COPY
                for k, v in data.items():
                    if isinstance(v, (dict, list)):
                        data[k] = json.dumps(v, ensure_ascii=False)
                records.append(data)

            # Deduplicate records to avoid Postgres "affect row a second time"
            # errors, while preserving SQL's distinct-NULL unique semantics.
            records = _dedupe_records_for_conflict_keys(records, conflict_keys)

            if update_timestamp is None:
                effective_update_timestamp = "updated_at" not in exclude_cols
            else:
                effective_update_timestamp = update_timestamp
            self._bulk_copy_upsert(
                model.__tablename__, records, conflict_keys, update_timestamp=effective_update_timestamp
            )
            synced += len(records)
            print(f"   Synced {synced}/{total_count} rows via COPY...")

        return synced
