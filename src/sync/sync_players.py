"""
Player sync: players, identities, basic info, movements, FA contracts.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

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
        """Sync master player records (players table) from SQLite to OCI (batched upsert)"""

        players = self.sqlite_session.query(Player).all()
        synced = 0
        batch_size = 500

        print(f"🚚 Syncing Master Players ({len(players)} rows)...")

        for batch_start in range(0, len(players), batch_size):
            batch = players[batch_start : batch_start + batch_size]
            values_list = [
                {
                    "kbo_person_id": p.kbo_person_id,
                    "player_basic_id": p.player_basic_id,
                    "birth_date": p.birth_date,
                    "birth_place": p.birth_place,
                    "height_cm": p.height_cm,
                    "weight_kg": p.weight_kg,
                    "bats": p.bats,
                    "throws": p.throws,
                    "is_foreign_player": p.is_foreign_player,
                    "debut_year": p.debut_year,
                    "retire_year": p.retire_year,
                    "status": p.status,
                    "notes": p.notes,
                    "photo_url": p.photo_url,
                    "salary_original": p.salary_original,
                    "signing_bonus_original": p.signing_bonus_original,
                    "draft_info": p.draft_info,
                }
                for p in batch
            ]

            if not values_list:
                continue

            stmt = pg_insert(Player).values(values_list)
            update_dict = {
                col: getattr(stmt.excluded, col)
                for col in (
                    "player_basic_id",
                    "birth_date",
                    "birth_place",
                    "height_cm",
                    "weight_kg",
                    "bats",
                    "throws",
                    "is_foreign_player",
                    "debut_year",
                    "retire_year",
                    "status",
                    "notes",
                    "photo_url",
                    "salary_original",
                    "signing_bonus_original",
                    "draft_info",
                )
            }
            update_dict["updated_at"] = text("CURRENT_TIMESTAMP")

            stmt = stmt.on_conflict_do_update(index_elements=["kbo_person_id"], set_=update_dict)

            self.target_session.execute(stmt)
            self.target_session.commit()
            synced += len(values_list)
            print(f"   Synced {synced}/{len(players)} players...")

        print(f"✅ Synced {synced} players to OCI")
        return synced

    def sync_player_identities(self) -> int:
        """Sync player identities from SQLite to OCI"""
        player_mapping = self._get_player_id_mapping()
        identities = self.sqlite_session.query(PlayerIdentity).all()
        synced = 0

        for identity in identities:
            oci_player_id = player_mapping.get(identity.player_id)
            if not oci_player_id:
                continue

            data = {
                "player_id": oci_player_id,
                "name_kor": identity.name_kor,
                "name_eng": identity.name_eng,
                "start_date": identity.start_date,
                "end_date": identity.end_date,
                "is_primary": identity.is_primary,
                "notes": identity.notes,
            }

            stmt = pg_insert(PlayerIdentity).values(**data)
            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player identities to OCI")
        return synced

    def _get_player_id_mapping(self) -> dict[int, int]:
        """Get SQLite player_id → OCI player_id mapping"""
        mapping = {}
        sqlite_players = self.sqlite_session.query(Player).all()

        for sp in sqlite_players:
            if sp.kbo_person_id:
                oci_player = self.target_session.query(Player).filter_by(kbo_person_id=sp.kbo_person_id).first()
                if oci_player:
                    mapping[sp.id] = oci_player.id

        return mapping

    def sync_all_batting_data(self) -> dict[str, int]:
        """모든 타격 관련 데이터 동기화 (타자 + 투수)"""
        results = {
            "pitcher_data": self.sync_pitcher_data(),
            "batting_data": self.sync_batting_data(),
        }
        return results

    def sync_player_basic(self, limit: int = None) -> int:
        """Sync player_basic data from SQLite to OCI using fast bulk COPY"""
        return self._sync_simple_table(
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
        """Sync specific players from SQLite to OCI by their IDs"""
        target_player_ids = sorted({int(player_id) for player_id in player_ids if player_id is not None})
        if not target_player_ids:
            return 0

        players = self.sqlite_session.query(PlayerBasic).filter(PlayerBasic.player_id.in_(target_player_ids)).all()
        if not players:
            return 0

        synced = 0
        for player in players:
            data = {
                "player_id": player.player_id,
                "name": player.name,
                "uniform_no": player.uniform_no,
                "team": player.team,
                "position": player.position,
                "birth_date": player.birth_date,
                "birth_date_date": player.birth_date_date,
                "height_cm": player.height_cm,
                "weight_kg": player.weight_kg,
                "career": player.career,
                "status": player.status,
                "staff_role": player.staff_role,
                "status_source": player.status_source,
                "photo_url": player.photo_url,
                "bats": player.bats,
                "throws": player.throws,
                "debut_year": player.debut_year,
                "salary_original": player.salary_original,
                "signing_bonus_original": player.signing_bonus_original,
                "draft_info": player.draft_info,
            }

            stmt = pg_insert(PlayerBasic).values(**data)
            update_dict = {k: stmt.excluded[k] for k in data if k != "player_id"}
            stmt = stmt.on_conflict_do_update(index_elements=["player_id"], set_=update_dict)

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player_basic records to OCI (by IDs)")
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
            raise ValueError(
                "Cannot sync game child rows because referenced player_id values are missing "
                f"from local player_basic. games=[{game_list}] missing_player_ids=[{missing_list}]"
            )

        return self.sync_player_basic_by_ids(sorted(referenced_player_ids))

    def sync_player_movements(self) -> int:
        """Sync player_movements from SQLite to OCI"""

        movements = self.sqlite_session.query(PlayerMovement).all()
        synced = 0

        if not movements:
            print("ℹ️ No player movement data to sync.")
            return 0

        for m in movements:
            data = {
                "movement_date": m.movement_date,
                "section": m.section,
                "team_code": m.team_code,
                "canonical_team_id": m.canonical_team_id,
                "player_basic_id": m.player_basic_id,
                "resolution_status": m.resolution_status,
                "player_name": m.player_name,
                "remarks": m.remarks,
            }

            stmt = pg_insert(PlayerMovement).values(**data)

            update_dict = {
                "remarks": stmt.excluded.remarks,
                "canonical_team_id": stmt.excluded.canonical_team_id,
                "player_basic_id": stmt.excluded.player_basic_id,
                "resolution_status": stmt.excluded.resolution_status,
                "updated_at": text("CURRENT_TIMESTAMP"),
            }

            stmt = stmt.on_conflict_do_update(constraint="uq_player_movement", set_=update_dict)

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} player movement records to OCI")
        return synced

    def sync_fa_contracts(self) -> int:
        """Sync fa_contracts from SQLite to OCI"""

        # Ensure table exists on target database
        Base.metadata.create_all(self.oci_engine)

        contracts = self.sqlite_session.query(FAContract).all()
        synced = 0

        if not contracts:
            print("ℹ️ No FA contract data to sync.")
            return 0

        for c in contracts:
            data = {
                "player_name": c.player_name,
                "player_basic_id": c.player_basic_id,
                "year": c.year,
                "fa_type": c.fa_type,
                "old_team": c.old_team,
                "new_team": c.new_team,
                "team_code": c.team_code,
                "contract_duration": c.contract_duration,
                "total_amount": c.total_amount,
                "total_amount_krw": c.total_amount_krw,
                "signing_bonus": c.signing_bonus,
                "annual_salary": c.annual_salary,
                "remarks": c.remarks,
                "source_url": c.source_url,
            }

            stmt = pg_insert(FAContract).values(**data)

            update_dict = {
                "player_basic_id": stmt.excluded.player_basic_id,
                "old_team": stmt.excluded.old_team,
                "team_code": stmt.excluded.team_code,
                "contract_duration": stmt.excluded.contract_duration,
                "total_amount": stmt.excluded.total_amount,
                "total_amount_krw": stmt.excluded.total_amount_krw,
                "signing_bonus": stmt.excluded.signing_bonus,
                "annual_salary": stmt.excluded.annual_salary,
                "remarks": stmt.excluded.remarks,
                "source_url": stmt.excluded.source_url,
                "updated_at": text("CURRENT_TIMESTAMP"),
            }

            stmt = stmt.on_conflict_do_update(constraint="uq_fa_contract_record", set_=update_dict)

            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} FA contract records to OCI")
        return synced

    def sync_crawl_runs(self) -> int:
        """Sync crawl runs from SQLite to OCI"""
        from src.cli.sync_oci import clone_row

        runs = self.sqlite_session.query(CrawlRun).all()
        if not runs:
            return 0

        synced = 0
        for run in runs:
            clone = clone_row(run, CrawlRun)
            self.target_session.merge(clone)
            synced += 1
            if synced % 100 == 0:
                self.target_session.commit()

        self.target_session.commit()
        print(f"✅ Synced {synced} crawl run records to OCI")
        return synced
