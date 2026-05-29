"""
Game-level sync: games, details, PBP, play-by-play, player game stats.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Callable

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
)
from src.sync.sync_base import (
    _dedupe_records_for_conflict_keys,
    _log_sync_eligibility,
    build_game_sync_eligibility,
    detect_dirty_game_ids,
    filter_game_ids_by_year,
)


class GameSyncMixin:
    """Mixin for game-level sync operations."""

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

        return self._sync_simple_table(
            Game, ["game_id"], exclude_cols=exclude_cols, filters=filters, transform_fn=transform, batch_size=batch_size
        )

    def sync_player_game_batting(self, limit: int = None) -> int:
        """Sync player game batting stats from SQLite to OCI"""
        query = self.sqlite_session.query(PlayerGameBatting)  # noqa: F821
        if limit:
            query = query.limit(limit)

        batting_stats = query.all()
        synced = 0

        for stat in batting_stats:
            data = {
                "game_id": stat.game_id,
                "player_id": stat.player_id,
                "player_name": stat.player_name,
                "team_side": stat.team_side,
                "team_code": stat.team_code,
                "batting_order": stat.batting_order,
                "appearance_seq": stat.appearance_seq,
                "position": stat.position,
                "is_starter": bool(stat.is_starter) if stat.is_starter is not None else False,
                "source": stat.source,
                "plate_appearances": stat.plate_appearances,
                "at_bats": stat.at_bats,
                "runs": stat.runs,
                "hits": stat.hits,
                "doubles": stat.doubles,
                "triples": stat.triples,
                "home_runs": stat.home_runs,
                "rbi": stat.rbi,
                "walks": stat.walks,
                "intentional_walks": stat.intentional_walks,
                "hbp": stat.hbp,
                "strikeouts": stat.strikeouts,
                "stolen_bases": stat.stolen_bases,
                "caught_stealing": stat.caught_stealing,
                "sacrifice_hits": stat.sacrifice_hits,
                "sacrifice_flies": stat.sacrifice_flies,
                "gdp": stat.gdp,
                "avg": stat.avg,
                "obp": stat.obp,
                "slg": stat.slg,
                "ops": stat.ops,
                "iso": stat.iso,
                "babip": stat.babip,
                "extras": stat.extras,
            }

            stmt = pg_insert(PlayerGameBatting).values(**data)  # noqa: F821
            update_dict = {k: v for k, v in data.items() if k not in ["game_id", "player_id"]}
            update_dict["updated_at"] = text("CURRENT_TIMESTAMP")
            stmt = stmt.on_conflict_do_update(index_elements=["game_id", "player_id"], set_=update_dict)

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player game batting stats to OCI")
        return synced

    def sync_player_game_pitching(self, limit: int = None) -> int:
        """Sync player game pitching stats from SQLite to OCI"""
        query = self.sqlite_session.query(PlayerGamePitching)  # noqa: F821
        if limit:
            query = query.limit(limit)

        pitching_stats = query.all()
        synced = 0

        for stat in pitching_stats:
            data = {
                "game_id": stat.game_id,
                "player_id": stat.player_id,
                "player_name": stat.player_name,
                "team_side": stat.team_side,
                "team_code": stat.team_code,
                "is_starting": bool(stat.is_starting) if stat.is_starting is not None else False,
                "appearance_seq": stat.appearance_seq,
                "source": stat.source,
                "innings_outs": stat.innings_outs,
                "hits_allowed": stat.hits_allowed,
                "runs_allowed": stat.runs_allowed,
                "earned_runs": stat.earned_runs,
                "home_runs_allowed": stat.home_runs_allowed,
                "walks_allowed": stat.walks_allowed,
                "strikeouts": stat.strikeouts,
                "hit_batters": stat.hit_batters,
                "wild_pitches": stat.wild_pitches,
                "balks": stat.balks,
                "wins": stat.wins,
                "losses": stat.losses,
                "saves": stat.saves,
                "holds": stat.holds,
                "decision": stat.decision,
                "batters_faced": stat.batters_faced,
                "era": stat.era,
                "whip": stat.whip,
                "fip": stat.fip,
                "k_per_nine": stat.k_per_nine,
                "bb_per_nine": stat.bb_per_nine,
                "kbb": stat.kbb,
                "extras": stat.extras,
            }

            stmt = pg_insert(PlayerGamePitching).values(**data)  # noqa: F821
            update_dict = {k: v for k, v in data.items() if k not in ["game_id", "player_id"]}
            update_dict["updated_at"] = text("CURRENT_TIMESTAMP")
            stmt = stmt.on_conflict_do_update(index_elements=["game_id", "player_id"], set_=update_dict)

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player game pitching stats to OCI")
        return synced

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
            "game_summary",
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
            results["game_id_aliases"] = self._sync_simple_table(
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
            if model_cls in {GameMetadata, GameInningScore, GameLineup, GameBattingStat, GamePitchingStat, GameSummary}:
                return [model_cls.game_id.in_(eligibility.detail_game_ids)]
            return child_filters if child_filters else None

        # 1. Game Metadata
        results["metadata"] = self._sync_simple_table(
            GameMetadata,
            ["game_id"],
            exclude_cols=["created_at"],
            filters=get_child_filters(GameMetadata),
            batch_size=batch_size,
        )

        # 2. Inning Scores
        results["inning_scores"] = self._sync_simple_table(
            GameInningScore,
            ["game_id", "team_side", "inning"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameInningScore),
            batch_size=batch_size,
        )

        # 3. Lineups
        results["lineups"] = self._sync_simple_table(
            GameLineup,
            ["game_id", "team_side", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameLineup),
            batch_size=batch_size,
        )

        # 4. Batting Stats
        results["batting_stats"] = self._sync_simple_table(
            GameBattingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameBattingStat),
            batch_size=batch_size,
        )

        # 5. Pitching Stats
        results["pitching_stats"] = self._sync_simple_table(
            GamePitchingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GamePitchingStat),
            batch_size=batch_size,
        )

        results["play_by_play"] = self._sync_game_play_by_play(filters=get_child_filters(GamePlayByPlay))

        results["events"] = self._sync_simple_table(
            GameEvent,
            ["game_id", "event_seq"],
            exclude_cols=["created_at", "id"],
            filters=get_child_filters(GameEvent),
            batch_size=batch_size,
        )

        # 7. Game Summary
        results["summary"] = self._sync_game_summary_rows(filters=get_child_filters(GameSummary), batch_size=batch_size)

        print(f"✅ Game Details Sync Summary: {results}")
        return results

    def sync_specific_game(self, game_id: str) -> dict[str, int]:
        """Sync all related data for a single game_id"""
        # We need Game model for filtering

        results = {}
        eligibility = build_game_sync_eligibility(self.sqlite_session, [game_id])
        results.update(eligibility.counts())
        _log_sync_eligibility(eligibility)
        filters = [Game.game_id == game_id]
        detail_filters = [GameMetadata.game_id.in_(eligibility.detail_game_ids)]
        relay_filters = [GamePlayByPlay.game_id.in_(eligibility.relay_game_ids)]

        # Sync Game record
        results["game"] = self._sync_simple_table(
            Game, ["game_id"], exclude_cols=["created_at", "updated_at"], filters=filters
        )
        results["game_id_aliases"] = self._sync_simple_table(
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
        )
        relay_child_models = (GameEvent, GamePlayByPlay)
        if eligibility.detail_game_ids:
            for child_model in detail_child_models:
                self.target_session.query(child_model).filter(child_model.game_id == game_id).delete(
                    synchronize_session=False
                )
        if eligibility.relay_game_ids:
            for child_model in relay_child_models:
                self.target_session.query(child_model).filter(child_model.game_id == game_id).delete(
                    synchronize_session=False
                )
        self.target_session.commit()

        # Sync children
        results["metadata"] = self._sync_simple_table(
            GameMetadata, ["game_id"], exclude_cols=["created_at"], filters=detail_filters
        )
        results["inning_scores"] = self._sync_simple_table(
            GameInningScore,
            ["game_id", "team_side", "inning"],
            exclude_cols=["created_at"],
            filters=[GameInningScore.game_id.in_(eligibility.detail_game_ids)],
        )
        results["lineups"] = self._sync_simple_table(
            GameLineup,
            ["game_id", "team_side", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameLineup.game_id.in_(eligibility.detail_game_ids)],
        )
        results["batting_stats"] = self._sync_simple_table(
            GameBattingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameBattingStat.game_id.in_(eligibility.detail_game_ids)],
        )
        results["pitching_stats"] = self._sync_simple_table(
            GamePitchingStat,
            ["game_id", "player_id", "appearance_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GamePitchingStat.game_id.in_(eligibility.detail_game_ids)],
        )
        results["play_by_play"] = self._sync_game_play_by_play(filters=relay_filters)
        results["events"] = self._sync_simple_table(
            GameEvent,
            ["game_id", "event_seq"],
            exclude_cols=["id", "created_at"],
            filters=[GameEvent.game_id.in_(eligibility.relay_game_ids)],
        )
        results["summary"] = self._sync_game_summary_rows(
            filters=[GameSummary.game_id.in_(eligibility.detail_game_ids)],
            replace_game_ids=eligibility.detail_game_ids,
        )

        return results

    def sync_pregame_game(self, game_id: str) -> dict[str, int]:
        """Sync only pregame publish tables for one game without touching completed detail datasets."""

        if not game_id:
            return {}

        results: dict[str, int] = {}
        results["game"] = self._sync_simple_table(
            Game,
            ["game_id"],
            exclude_cols=["created_at", "updated_at"],
            filters=[Game.game_id == game_id],
        )
        results["game_id_aliases"] = self._sync_simple_table(
            GameIdAlias,
            ["alias_game_id"],
            exclude_cols=["created_at"],
            filters=[GameIdAlias.canonical_game_id == game_id],
        )
        results["player_basic"] = self._sync_referenced_player_basic_for_games([game_id])

        self.target_session.query(GameLineup).filter(GameLineup.game_id == game_id).delete(synchronize_session=False)
        self.target_session.commit()

        results["metadata"] = self._sync_simple_table(
            GameMetadata,
            ["game_id"],
            exclude_cols=["created_at"],
            filters=[GameMetadata.game_id == game_id],
        )
        results["lineups"] = self._sync_simple_table(
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

        # Use _sync_simple_table with empty conflict_keys for blind bulk insert
        return self._sync_simple_table(
            GamePlayByPlay,
            conflict_keys=[],  # Blind insert
            exclude_cols=["id"],
            filters=filters,
            batch_size=20000,
        )

    def _sync_simple_table(
        self,
        model: type,
        conflict_keys: list[str],
        exclude_cols: list[str] = None,
        filters: list = None,
        transform_fn: Callable | None = None,
        batch_size: int = 10000,
    ) -> int:
        """Generic sync parameter for simple tables using Batched UPSERT or COPY"""
        if exclude_cols is None:
            exclude_cols = ["id"]  # Default to exclude ID for auto-inc compatibility
        elif "id" not in exclude_cols:
            exclude_cols.append("id")

        # Use all columns except those explicitly excluded and not present in target DB
        from sqlalchemy import inspect

        target_columns = {c["name"] for c in inspect(self.oci_engine).get_columns(model.__tablename__)}
        columns = [c.key for c in model.__table__.columns if c.key not in exclude_cols and c.key in target_columns]

        query = self.sqlite_session.query(model)
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

            self._bulk_copy_upsert(
                model.__tablename__, records, conflict_keys, update_timestamp=("updated_at" not in exclude_cols)
            )
            synced += len(records)
            print(f"   Synced {synced}/{total_count} rows via COPY...")

        return synced
