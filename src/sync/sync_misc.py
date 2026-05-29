"""
Miscellaneous sync: franchises, teams, awards, stadium info, food, ticket, rag, matchup splits, home/away, park factor.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from src.models.award import Award
from src.models.base import Base
from src.models.broadcast import GameBroadcast
from src.models.fan_culture import CheerChant, CheerSong, TeamRivalry
from src.models.foreign_player import ForeignPlayerChange
from src.models.franchise import Franchise
from src.models.game_mvp import GameMvp
from src.models.injury import InjuryEntry
from src.models.manager_change import ManagerChange
from src.models.matchup import (
    BatterSplit,
    BatterStadiumSplit,
    BatterTeamSplit,
    BatterVsStarter,
    MatchupBvP,
    PitcherSplit,
    PitcherTeamSplit,
)
from src.models.rag_chunk import RagChunk
from src.models.stadium_food import StadiumFood
from src.models.stadium_info import StadiumInfo, StadiumRegulation
from src.models.team import Team, TeamCodeMap, TeamDailyRoster
from src.models.team_history import TeamHistory
from src.models.ticket_schedule import TicketSchedule


class MiscSyncMixin:
    """Mixin for misc sync operations."""

    def sync_franchises(self) -> int:
        """Sync franchises from SQLite to OCI"""
        from src.cli.sync_oci import clone_row

        franchises = self.sqlite_session.query(Franchise).all()
        synced = 0

        for f in franchises:
            clone = clone_row(f, Franchise)
            self.target_session.merge(clone)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} franchises to OCI")
        return synced

    def sync_teams(self) -> int:
        """Sync teams from SQLite to OCI"""
        import json

        from src.cli.sync_oci import clone_row

        # Get Franchise ID Mapping (Local ID -> OCI ID)
        franchise_mapping = self._get_franchise_id_mapping()

        teams = self.sqlite_session.query(Team).all()
        synced = 0

        for team in teams:
            # Map Local Franchise ID to OCI Franchise ID
            fid = None
            if team.franchise_id:
                fid = franchise_mapping.get(team.franchise_id)

            clone = clone_row(team, Team)
            clone.franchise_id = fid

            # Ensure aliases is a list for merge to handle it as PG ARRAY
            if clone.aliases is None:
                clone.aliases = []
            elif isinstance(clone.aliases, str):
                try:
                    clone.aliases = json.loads(clone.aliases)
                except Exception:
                    clone.aliases = [clone.aliases]

            self.target_session.merge(clone)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} teams to OCI")
        return synced

    def sync_stadium_info(self) -> int:
        """Sync stadium_info from SQLite to OCI"""
        self._ensure_table(StadiumInfo)
        return self._sync_simple_table(
            StadiumInfo,
            ["stadium_code"],
            exclude_cols=["created_at"],
        )

    def sync_awards(self) -> int:
        """Sync awards from SQLite to OCI"""
        # Ensure table exists
        try:
            migration_path = Path("migrations/oci/019_create_awards.sql")
            if migration_path.exists():
                sql = migration_path.read_text()
                self.target_session.execute(text(sql))
                self.target_session.commit()
                print("✅ Applied awards migration")
        except Exception as e:
            print(f"⚠️ Failed to apply migration: {e}")
            self.target_session.rollback()

        awards = self.sqlite_session.query(Award).all()
        synced = 0
        if not awards:
            print("ℹ️ No awards data to sync.")
            return 0

        for award in awards:
            data = {
                "year": award.year,
                "award_type": award.award_type,
                "category": award.category,
                "player_name": award.player_name,
                "team_name": award.team_name,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
            }
            stmt = pg_insert(Award).values(**data)
            update_dict = {"updated_at": stmt.excluded.updated_at}
            stmt = stmt.on_conflict_do_update(constraint="uq_award_record", set_=update_dict)
            self.target_session.execute(stmt)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} awards to OCI")
        return synced

    def sync_rag_chunks(self, batch_size: int = 1000) -> int:
        """Sync RAG chunks from SQLite to OCI Postgres"""
        print("📁 Ensure RAG chunks table exists on OCI...")
        try:
            Base.metadata.create_all(self.oci_engine)
        except Exception as e:
            print(f"⚠️ Warning: metadata create_all error (might already exist): {e}")

        def transform_rag_chunk(data: dict[str, Any]) -> dict[str, Any]:
            embedding = data.get("embedding")
            if embedding is not None:
                if isinstance(embedding, str):
                    try:
                        import json

                        embedding = json.loads(embedding)
                    except Exception:
                        pass
                if isinstance(embedding, list):
                    target_dim = 256
                    current_dim = len(embedding)
                    if current_dim != target_dim:
                        if current_dim > target_dim:
                            truncated = embedding[:target_dim]
                            import math

                            norm = math.sqrt(sum(x * x for x in truncated))
                            if norm > 1e-9:
                                adjusted = [x / norm for x in truncated]
                            else:
                                adjusted = truncated
                        else:
                            adjusted = embedding + [0.0] * (target_dim - current_dim)
                        data["embedding"] = adjusted
            return data

        return self._sync_simple_table(
            RagChunk,
            ["source_table", "source_row_id"],
            exclude_cols=["created_at", "id"],
            transform_fn=transform_rag_chunk,
            batch_size=batch_size,
        )

    def sync_ticket_schedules(self, batch_size: int = 1000) -> int:
        """Sync ticket schedules from SQLite to OCI Postgres"""
        print("📁 Ensure Ticket schedules table exists on OCI...")
        try:
            Base.metadata.create_all(self.oci_engine)
        except Exception as e:
            print(f"⚠️ Warning: metadata create_all error (might already exist): {e}")

        return self._sync_simple_table(
            TicketSchedule,
            ["game_date", "home_team", "platform"],
            exclude_cols=["created_at", "id"],
            batch_size=batch_size,
        )

    def sync_stadium_foods(self, batch_size: int = 1000) -> int:
        """Sync stadium foods from SQLite to OCI Postgres"""
        print("📁 Ensure Stadium foods table exists on OCI...")
        try:
            Base.metadata.create_all(self.oci_engine)
        except Exception as e:
            print(f"⚠️ Warning: metadata create_all error (might already exist): {e}")

        return self._sync_simple_table(
            StadiumFood,
            ["stadium_name", "restaurant_name", "menu_item"],
            exclude_cols=["created_at", "id"],
            batch_size=batch_size,
        )

    def sync_game_broadcasts(self) -> int:
        """Sync game_broadcasts from SQLite to OCI"""
        self._ensure_table(GameBroadcast)
        return self._sync_simple_table(
            GameBroadcast,
            ["game_id", "broadcaster"],
            exclude_cols=["created_at", "id"],
        )

    def sync_stadium_regulations(self) -> int:
        """Sync stadium_regulations from SQLite to OCI"""
        self._ensure_table(StadiumRegulation)
        return self._sync_simple_table(
            StadiumRegulation,
            ["id"],
            exclude_cols=["created_at"],
        )

    def sync_game_mvps(self) -> int:
        """Sync game_mvps from SQLite to OCI"""
        self._ensure_table(GameMvp)
        return self._sync_simple_table(
            GameMvp,
            ["game_id", "mvp_type", "player_name"],
            exclude_cols=["created_at", "id"],
        )

    def sync_injury_entries(self) -> int:
        """Sync injury_entries from SQLite to OCI"""
        self._ensure_table(InjuryEntry)
        return self._sync_simple_table(
            InjuryEntry,
            ["player_id", "il_placement_date"],
            exclude_cols=["created_at", "id"],
        )

    def sync_foreign_player_changes(self) -> int:
        """Sync foreign_player_changes from SQLite to OCI"""
        self._ensure_table(ForeignPlayerChange)
        return self._sync_simple_table(
            ForeignPlayerChange,
            ["player_name", "team_id", "season", "change_type"],
            exclude_cols=["created_at", "id"],
        )

    def sync_manager_changes(self) -> int:
        """Sync manager_changes from SQLite to OCI"""
        self._ensure_table(ManagerChange)
        return self._sync_simple_table(
            ManagerChange,
            ["team_id", "season", "new_manager"],
            exclude_cols=["created_at", "id"],
        )

    def sync_team_rivalries(self) -> int:
        """Sync team_rivalries from SQLite to OCI"""
        self._ensure_table(TeamRivalry)
        return self._sync_simple_table(
            TeamRivalry,
            ["team_id_a", "team_id_b"],
            exclude_cols=["created_at", "id"],
        )

    def sync_cheer_songs(self) -> int:
        """Sync cheer_songs from SQLite to OCI"""
        self._ensure_table(CheerSong)
        return self._sync_simple_table(
            CheerSong,
            ["team_id", "song_name", "song_type"],
            exclude_cols=["created_at", "id"],
        )

    def sync_cheer_chants(self) -> int:
        """Sync cheer_chants from SQLite to OCI"""
        self._ensure_table(CheerChant)
        return self._sync_simple_table(
            CheerChant,
            ["team_id", "chant_text"],
            exclude_cols=["created_at", "id"],
        )

    def sync_phase1_all(self) -> dict[str, int]:
        """Sync all Phase 1 tables to OCI"""
        results = {}
        results["game_broadcasts"] = self.sync_game_broadcasts()
        results["stadium_info"] = self.sync_stadium_info()
        results["stadium_regulations"] = self.sync_stadium_regulations()
        results["game_mvps"] = self.sync_game_mvps()
        results["injury_entries"] = self.sync_injury_entries()
        results["foreign_player_changes"] = self.sync_foreign_player_changes()
        results["manager_changes"] = self.sync_manager_changes()
        results["team_rivalries"] = self.sync_team_rivalries()
        results["cheer_songs"] = self.sync_cheer_songs()
        results["cheer_chants"] = self.sync_cheer_chants()
        return results

    def sync_daily_rosters(self) -> int:
        """Sync team_daily_roster from SQLite to OCI"""
        from src.utils.team_history import resolve_team_code_for_season

        try:
            rosters = self.sqlite_session.query(TeamDailyRoster).all()
        except Exception:
            print("⚠️ team_daily_roster table likely doesn't exist in local DB yet.")
            return 0

        synced = 0
        if not rosters:
            print("ℹ️ No daily roster data to sync.")
            return 0

        print(f"INFO: Found {len(rosters)} rosters to sync. Resolving team codes and deduplicating...")

        seen_keys = set()
        unique_rosters = []
        for r in rosters:
            season_year = r.roster_date.year if r.roster_date else None
            resolved_code = r.team_code
            if r.team_code and season_year:
                raw = r.team_code.strip().upper()
                if raw == "LOT":
                    raw = "LT"
                elif raw == "KW":
                    raw = "KH"
                resolved = resolve_team_code_for_season(raw, season_year)
                if resolved:
                    resolved_code = resolved
                else:
                    resolved_code = raw

            key = (r.roster_date, resolved_code, r.player_id)
            if key in seen_keys:
                continue
            seen_keys.add(key)
            unique_rosters.append((r, resolved_code))

        print(f"INFO: {len(rosters)} rows deduped to {len(unique_rosters)} unique daily roster records. Syncing...")

        batch_size = 1000
        for i in range(0, len(unique_rosters), batch_size):
            batch = unique_rosters[i : i + batch_size]
            values_list = []

            for r, resolved_code in batch:
                data = {
                    "roster_date": r.roster_date,
                    "team_code": resolved_code,
                    "player_id": r.player_id,
                    "player_basic_id": r.player_basic_id,
                    "person_type": r.person_type,
                    "player_name": r.player_name,
                    "position": r.position,
                    "back_number": r.back_number,
                    "created_at": datetime.now(),
                    "updated_at": datetime.now(),
                }
                values_list.append(data)

            if not values_list:
                continue

            stmt = pg_insert(TeamDailyRoster).values(values_list)

            update_dict = {
                "player_name": stmt.excluded.player_name,
                "player_basic_id": stmt.excluded.player_basic_id,
                "person_type": stmt.excluded.person_type,
                "position": stmt.excluded.position,
                "back_number": stmt.excluded.back_number,
                "updated_at": stmt.excluded.updated_at,
            }

            stmt = stmt.on_conflict_do_update(constraint="uq_team_daily_roster", set_=update_dict)

            self.target_session.execute(stmt)
            self.target_session.commit()
            synced += len(values_list)
            print(f"   Synced batch {i // batch_size + 1} ({len(values_list)} records)")
        print(f"✅ Synced {synced} daily roster records to OCI")
        return synced

    def sync_team_history(self) -> int:
        """Sync team_history table"""
        from src.cli.sync_oci import clone_row

        franchise_mapping = self._get_franchise_id_mapping()
        histories = self.sqlite_session.query(TeamHistory).all()
        synced = 0

        for h in histories:
            sup_fid = franchise_mapping.get(h.franchise_id)
            if not sup_fid:
                continue

            clone = clone_row(h, TeamHistory)
            clone.franchise_id = sup_fid
            self.target_session.merge(clone)
            synced += 1

        self.target_session.commit()
        print(f"✅ Synced {synced} team history records to OCI")
        return synced

    def sync_team_code_map(self) -> int:
        """Sync team_code_map table using PostgreSQL UPSERT"""
        franchise_mapping = self._get_franchise_id_mapping()
        maps = self.sqlite_session.query(TeamCodeMap).all()
        synced = 0
        batch_size = 500

        for i in range(0, len(maps), batch_size):
            batch = maps[i : i + batch_size]
            values_list = []
            for m in batch:
                sup_fid = franchise_mapping.get(m.franchise_id) or m.franchise_id

                data = {
                    "franchise_id": sup_fid,
                    "season": m.season,
                    "curr_code": m.curr_code,
                    "canonical_code": m.canonical_code,
                    "is_canonical": m.is_canonical,
                    "created_at": m.created_at or datetime.now(),
                    "updated_at": m.updated_at or datetime.now(),
                }
                values_list.append(data)

            if not values_list:
                continue

            stmt = pg_insert(TeamCodeMap).values(values_list)
            update_dict = {
                "franchise_id": stmt.excluded.franchise_id,
                "canonical_code": stmt.excluded.canonical_code,
                "is_canonical": stmt.excluded.is_canonical,
                "updated_at": stmt.excluded.updated_at,
            }

            stmt = stmt.on_conflict_do_update(constraint="uq_team_code_map", set_=update_dict)

            self.target_session.execute(stmt)
            self.target_session.commit()
            synced += len(values_list)

        print(f"✅ Synced {synced} team code map records to OCI")
        return synced

    def sync_matchups(self, year: int = None, batch_size: int = 10000) -> dict[str, int]:
        """Sync Matchup Split tables (Batter/Pitcher vs Team, Stadium, Starter, PBP-BvP) to OCI"""
        print("📁 Ensuring matchup tables exist on OCI...")
        Base.metadata.create_all(self.oci_engine)

        results = {}
        filters = [text(f"season_year = {year}")] if year else None

        results["batter_team"] = self._sync_simple_table(
            BatterTeamSplit,
            ["season_year", "league_type_code", "player_id", "team_code", "opponent_team_code"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["pitcher_team"] = self._sync_simple_table(
            PitcherTeamSplit,
            ["season_year", "league_type_code", "player_id", "team_code", "opponent_team_code"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["batter_stadium"] = self._sync_simple_table(
            BatterStadiumSplit,
            ["season_year", "league_type_code", "player_id", "team_code", "stadium_name"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["batter_vs_starter"] = self._sync_simple_table(
            BatterVsStarter,
            ["season_year", "league_type_code", "player_id", "pitcher_name"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["matchup_bvp"] = self._sync_simple_table(
            MatchupBvP, ["batter_id", "pitcher_id"], exclude_cols=["created_at", "id"], batch_size=batch_size
        )

        results["batter_splits"] = self._sync_simple_table(
            BatterSplit,
            ["player_id", "season_year", "split_type"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )
        results["pitcher_splits"] = self._sync_simple_table(
            PitcherSplit,
            ["player_id", "season_year", "split_type"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        print(f"✅ Matchup Splits Sync Summary: {results}")
        return results
