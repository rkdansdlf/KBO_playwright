"""
Player sync: players, identities, basic info, movements, FA contracts.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

from src.models.base import Base
from src.models.crawl import CrawlRun
from src.models.fa_contract import FAContract
from src.models.game import (
    GameBattingStat,
    GameEvent,
    GameLineup,
    GamePitchingStat,
    GameSummary,
)
from src.models.player import (
    Player,
    PlayerBasic,
    PlayerIdentity,
    PlayerMovement,
)


class PlayerSyncMixin:
    """Mixin for player-related sync operations."""

    def sync_players(self) -> int:
        """Sync master player records (players table) from SQLite to OCI using bulk COPY upsert."""
        return self.sync_simple_table(
            Player,
            conflict_keys=["kbo_person_id"],
            exclude_cols=["id"],
            batch_size=5000,
            update_timestamp=True,
        )

    def sync_player_identities(self) -> int:
        """Sync player identities from SQLite to OCI using bulk COPY upsert."""
        player_mapping = self._get_player_id_mapping()
        if not player_mapping:
            return 0

        valid_ids = list(player_mapping.keys())

        def _map_player_id(data: dict) -> dict[str, Any]:
            data["player_id"] = player_mapping.get(data["player_id"], data["player_id"])
            return data

        return self.sync_simple_table(
            PlayerIdentity,
            conflict_keys=[],
            exclude_cols=["created_at", "updated_at", "id"],
            filters=[PlayerIdentity.player_id.in_(valid_ids)],
            transform_fn=_map_player_id,
        )

    def _get_player_id_mapping(self) -> dict[int, int]:
        """Get and cache SQLite player_id → OCI player_id mapping (single batch query)."""
        if hasattr(self, "_player_id_mapping_cache") and self._player_id_mapping_cache is not None:
            return self._player_id_mapping_cache

        sqlite_players = self.sqlite_session.query(Player).all()
        kbo_person_ids = [sp.kbo_person_id for sp in sqlite_players if sp.kbo_person_id]

        if not kbo_person_ids:
            self._player_id_mapping_cache = {}
            return self._player_id_mapping_cache

        oci_rows = self.target_session.query(Player).filter(Player.kbo_person_id.in_(kbo_person_ids)).all()
        oci_by_person = {oci.kbo_person_id: oci.id for oci in oci_rows}

        self._player_id_mapping_cache = {
            sp.id: oci_by_person[sp.kbo_person_id] for sp in sqlite_players if sp.kbo_person_id in oci_by_person
        }
        return self._player_id_mapping_cache

    def sync_all_batting_data(self) -> dict[str, int]:
        """모든 타격 관련 데이터 동기화 (타자 + 투수)"""
        return {
            "pitcher_data": self.sync_pitcher_data(),
            "batting_data": self.sync_batting_data(),
        }

    def sync_player_basic(self, limit: int = None) -> int:
        """Sync player_basic data from SQLite to OCI using fast bulk COPY"""
        return self.sync_simple_table(
            PlayerBasic,
            conflict_keys=["player_id"],
            exclude_cols=[
                "created_at",
                "updated_at",
            ],  # Let OCI handle timestamps if possible, or include them if needed
            filters=None,
            batch_size=5000,
        )

    def sync_player_basic_by_ids(self, player_ids: list[int]) -> int:
        """Sync specific players from SQLite to OCI by their IDs using bulk COPY upsert."""
        target_player_ids = sorted({int(player_id) for player_id in player_ids if player_id is not None})
        if not target_player_ids:
            return 0

        players = self.sqlite_session.query(PlayerBasic).filter(PlayerBasic.player_id.in_(target_player_ids)).all()
        if not players:
            return 0

        records = [
            {
                "player_id": p.player_id,
                "name": p.name,
                "uniform_no": p.uniform_no,
                "team": p.team,
                "position": p.position,
                "birth_date": p.birth_date,
                "birth_date_date": p.birth_date_date,
                "height_cm": p.height_cm,
                "weight_kg": p.weight_kg,
                "career": p.career,
                "status": p.status,
                "staff_role": p.staff_role,
                "status_source": p.status_source,
                "photo_url": p.photo_url,
                "bats": p.bats,
                "throws": p.throws,
                "debut_year": p.debut_year,
                "salary_original": p.salary_original,
                "signing_bonus_original": p.signing_bonus_original,
                "draft_info": p.draft_info,
                "salary_amount": p.salary_amount,
                "salary_currency": p.salary_currency,
                "signing_bonus_amount": p.signing_bonus_amount,
                "signing_bonus_currency": p.signing_bonus_currency,
                "draft_year": p.draft_year,
                "draft_round": p.draft_round,
                "draft_pick_overall": p.draft_pick_overall,
                "draft_type": p.draft_type,
                "education_path": p.education_path,
            }
            for p in players
        ]

        self._bulk_copy_upsert(
            PlayerBasic.__tablename__,
            records,
            unique_cols=["player_id"],
            update_timestamp=True,
        )

        synced = len(records)
        logger.info("✅ Synced %s player_basic records to OCI (by IDs)", synced)
        return synced

    def _sync_referenced_player_basic_for_games(self, game_ids: list[str]) -> int:
        """Sync local player_basic rows referenced by game child tables before FK-bound child sync."""
        target_game_ids = sorted({str(game_id) for game_id in game_ids if game_id})
        if not target_game_ids:
            return 0

        referenced_player_ids: set[int] = set()
        for model in (GameLineup, GameBattingStat, GamePitchingStat, GameSummary):
            rows = (
                self.sqlite_session.query(model.player_id)
                .filter(model.game_id.in_(target_game_ids), model.player_id.isnot(None))
                .distinct()
                .all()
            )
            referenced_player_ids.update(int(row[0]) for row in rows if row[0] is not None)
        for column in (GameEvent.batter_id, GameEvent.pitcher_id):
            rows = (
                self.sqlite_session.query(column)
                .filter(GameEvent.game_id.in_(target_game_ids), column.isnot(None))
                .distinct()
                .all()
            )
            referenced_player_ids.update(int(row[0]) for row in rows if row[0] is not None)

        if not referenced_player_ids:
            return 0

        local_player_ids = {
            int(row[0])
            for row in self.sqlite_session.query(PlayerBasic.player_id)
            .filter(PlayerBasic.player_id.in_(sorted(referenced_player_ids)))
            .all()
        }
        missing_player_ids = sorted(referenced_player_ids - local_player_ids)
        if missing_player_ids:
            game_list = ", ".join(target_game_ids[:5])
            if len(target_game_ids) > 5:
                game_list += f", ... (+{len(target_game_ids) - 5})"
            missing_list = ", ".join(str(player_id) for player_id in missing_player_ids[:20])
            if len(missing_player_ids) > 20:
                missing_list += f", ... (+{len(missing_player_ids) - 20})"
            logger.warning(
                "Skipping %d missing player_ids (not in local player_basic) for games=[%s]: [%s]",
                len(missing_player_ids),
                game_list,
                missing_list,
            )
            referenced_player_ids -= set(missing_player_ids)
            if not referenced_player_ids:
                return 0

        return self.sync_player_basic_by_ids(sorted(referenced_player_ids))

    def sync_player_movements(self) -> int:
        """Sync player_movements from SQLite to OCI using bulk COPY upsert."""

        def _fix_team_code(data: dict) -> dict[str, Any]:
            if not data.get("team_code"):
                data["team_code"] = data.get("canonical_team_id") or "N/A"
            return data

        return self.sync_simple_table(
            PlayerMovement,
            conflict_keys=["movement_date", "team_code", "player_name", "section"],
            exclude_cols=["created_at", "updated_at", "id"],
            transform_fn=_fix_team_code,
            update_timestamp=True,
        )

    def sync_fa_contracts(self) -> int:
        """Sync fa_contracts from SQLite to OCI using bulk COPY upsert."""
        Base.metadata.create_all(self.oci_engine)
        return self.sync_simple_table(
            FAContract,
            conflict_keys=["player_name", "year", "fa_type", "new_team"],
            exclude_cols=["created_at", "updated_at", "id"],
            update_timestamp=True,
        )

    def sync_crawl_runs(self) -> int:
        return self.sync_simple_table(
            CrawlRun,
            conflict_keys=["label", "started_at"],
            exclude_cols=["created_at", "updated_at", "id"],
            update_timestamp=True,
        )
