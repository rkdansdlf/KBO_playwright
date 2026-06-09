"""
Miscellaneous sync: franchises, teams, awards, stadium info, food, ticket, rag, matchup splits, home/away, park factor.
Stadium real-time data: transit times, congestion, operation notices.
"""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

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
from src.models.season import KboSeason
from src.models.stadium_congestion import StadiumCongestion
from src.models.stadium_food import StadiumFood
from src.models.stadium_info import StadiumInfo, StadiumRegulation
from src.models.stadium_operation_notice import StadiumOperationNotice
from src.models.stadium_transit_time import StadiumTransitTime
from src.models.team import Team, TeamCodeMap, TeamDailyRoster
from src.models.team_event import TeamEvent
from src.models.team_history import TeamHistory
from src.models.ticket_schedule import TicketSchedule

logger = logging.getLogger(__name__)


def _normalize_daily_roster_date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        for date_format in ("%Y-%m-%d", "%Y%m%d"):
            try:
                return datetime.strptime(raw, date_format).date()
            except ValueError:
                continue
    raise ValueError("daily roster date must be YYYY-MM-DD or YYYYMMDD")


def _format_daily_roster_scope(start_date: date | None, end_date: date | None) -> str:
    if start_date and end_date:
        return f" for {start_date.isoformat()}..{end_date.isoformat()}"
    if start_date:
        return f" from {start_date.isoformat()}"
    if end_date:
        return f" through {end_date.isoformat()}"
    return ""


class MiscSyncMixin:
    """Mixin for misc sync operations."""

    def sync_franchises(self) -> int:
        """Sync franchises from SQLite to OCI"""
        return self.sync_simple_table(Franchise, ["original_code"])

    def sync_kbo_seasons(self) -> int:
        """Sync kbo_seasons reference table from SQLite to OCI"""
        return self.sync_simple_table(KboSeason, ["season_id"])

    def sync_teams(self) -> int:
        """Sync teams from SQLite to OCI"""

        franchise_mapping = self._get_franchise_id_mapping()
        teams = self.sqlite_session.query(Team).all()
        if not teams:
            logger.info("ℹ️ No teams to sync.")
            return 0

        records = []
        for team in teams:
            fid = None
            if team.franchise_id:
                fid = franchise_mapping.get(team.franchise_id)

            aliases = team.aliases
            if aliases is None:
                aliases_list: list = []
            elif isinstance(aliases, str):
                try:
                    aliases_list = json.loads(aliases)
                except json.JSONDecodeError:
                    aliases_list = [aliases]
            elif isinstance(aliases, list):
                aliases_list = aliases
            else:
                aliases_list = []

            aliases_pg = "{" + ",".join(str(a) for a in aliases_list) + "}" if aliases_list else "{}"

            records.append(
                {
                    "team_id": team.team_id,
                    "team_name": team.team_name,
                    "team_short_name": team.team_short_name,
                    "city": team.city,
                    "founded_year": team.founded_year,
                    "stadium_name": team.stadium_name,
                    "franchise_id": fid,
                    "is_active": team.is_active if team.is_active is not None else True,
                    "aliases": aliases_pg,
                    "created_at": team.created_at or datetime.now(),
                    "updated_at": team.updated_at or datetime.now(),
                },
            )

        self._bulk_copy_upsert(Team.__tablename__, records, ["team_id"])
        logger.info(f"✅ Synced {len(records)} teams to OCI")
        return len(records)

    def sync_stadium_info(self) -> int:
        """Sync stadium_info from SQLite to OCI"""
        self._ensure_table(StadiumInfo)
        return self.sync_simple_table(
            StadiumInfo,
            ["stadium_code"],
            exclude_cols=["created_at"],
        )

    def sync_awards(self) -> int:
        """Sync awards from SQLite to OCI"""
        try:
            migration_path = Path("migrations/oci/019_create_awards.sql")
            if migration_path.exists():
                sql = migration_path.read_text()
                self.target_session.execute(text(sql))
                self.target_session.commit()
                logger.info("✅ Applied awards migration")
        except SQLAlchemyError:
            logger.exception("Failed to apply awards migration")
            self.target_session.rollback()

        return self.sync_simple_table(
            Award,
            ["year", "award_type", "category", "player_name", "team_name"],
            exclude_cols=["created_at", "id"],
        )

    def sync_rag_chunks(self, batch_size: int = 1000) -> int:
        """Sync RAG chunks from SQLite to OCI Postgres"""
        logger.info("📁 Ensure RAG chunks table exists on OCI...")
        try:
            Base.metadata.create_all(self.oci_engine)
        except SQLAlchemyError:
            logger.exception("metadata create_all error (might already exist)")

        def transform_rag_chunk(data: dict[str, Any]) -> dict[str, Any]:
            embedding = data.get("embedding")
            if embedding is not None:
                if isinstance(embedding, str):
                    try:
                        import json

                        embedding = json.loads(embedding)
                    except json.JSONDecodeError:
                        logger.debug("Failed to parse embedding JSON")
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

        return self.sync_simple_table(
            RagChunk,
            ["source_table", "source_row_id"],
            exclude_cols=["created_at", "id"],
            transform_fn=transform_rag_chunk,
            batch_size=batch_size,
        )

    def sync_ticket_schedules(self, batch_size: int = 1000) -> int:
        """Sync ticket schedules from SQLite to OCI Postgres"""
        logger.info("📁 Ensure Ticket schedules table exists on OCI...")
        try:
            Base.metadata.create_all(self.oci_engine)
        except SQLAlchemyError:
            logger.exception("metadata create_all error (might already exist)")

        return self.sync_simple_table(
            TicketSchedule,
            ["game_date", "home_team", "platform"],
            exclude_cols=["created_at", "id"],
            batch_size=batch_size,
        )

    def sync_stadium_foods(self, batch_size: int = 1000) -> int:
        """Sync stadium foods from SQLite to OCI Postgres"""
        logger.info("📁 Ensure Stadium foods table exists on OCI...")
        try:
            Base.metadata.create_all(self.oci_engine)
        except SQLAlchemyError:
            logger.exception("metadata create_all error (might already exist)")

        return self.sync_simple_table(
            StadiumFood,
            ["stadium_name", "restaurant_name", "menu_item"],
            exclude_cols=["created_at", "id"],
            batch_size=batch_size,
        )

    def sync_game_broadcasts(self) -> int:
        """Sync game_broadcasts from SQLite to OCI"""
        self._ensure_table(GameBroadcast)
        return self.sync_simple_table(
            GameBroadcast,
            ["game_id", "broadcaster"],
            exclude_cols=["created_at", "id"],
        )

    def sync_stadium_regulations(self) -> int:
        """Sync stadium_regulations from SQLite to OCI"""
        self._ensure_table(StadiumRegulation)
        return self.sync_simple_table(
            StadiumRegulation,
            ["id"],
            exclude_cols=["created_at"],
        )

    def sync_game_mvps(self) -> int:
        """Sync game_mvps from SQLite to OCI"""
        self._ensure_table(GameMvp)
        return self.sync_simple_table(
            GameMvp,
            ["game_id", "mvp_type", "player_name"],
            exclude_cols=["created_at", "id"],
        )

    def sync_injury_entries(self) -> int:
        """Sync injury_entries from SQLite to OCI"""
        self._ensure_table(InjuryEntry)
        return self.sync_simple_table(
            InjuryEntry,
            ["player_id", "il_placement_date"],
            exclude_cols=["created_at", "id"],
        )

    def sync_foreign_player_changes(self) -> int:
        """Sync foreign_player_changes from SQLite to OCI"""
        self._ensure_table(ForeignPlayerChange)
        return self.sync_simple_table(
            ForeignPlayerChange,
            ["player_name", "team_id", "season", "change_type"],
            exclude_cols=["created_at", "id"],
        )

    def sync_manager_changes(self) -> int:
        """Sync manager_changes from SQLite to OCI"""
        self._ensure_table(ManagerChange)
        return self.sync_simple_table(
            ManagerChange,
            ["team_id", "season", "new_manager"],
            exclude_cols=["created_at", "id"],
        )

    def sync_team_rivalries(self) -> int:
        """Sync team_rivalries from SQLite to OCI"""
        self._ensure_table(TeamRivalry)
        return self.sync_simple_table(
            TeamRivalry,
            ["team_id_a", "team_id_b"],
            exclude_cols=["created_at", "id"],
        )

    def sync_cheer_songs(self) -> int:
        """Sync cheer_songs from SQLite to OCI"""
        self._ensure_table(CheerSong)
        return self.sync_simple_table(
            CheerSong,
            ["team_id", "song_name", "song_type"],
            exclude_cols=["created_at", "id"],
        )

    def sync_cheer_chants(self) -> int:
        """Sync cheer_chants from SQLite to OCI"""
        self._ensure_table(CheerChant)
        return self.sync_simple_table(
            CheerChant,
            ["team_id", "chant_text"],
            exclude_cols=["created_at", "id"],
        )

    def sync_team_events(self) -> int:
        """Sync team_events from SQLite to OCI"""
        self._ensure_table(TeamEvent)
        return self.sync_simple_table(
            TeamEvent,
            ["team_id", "title", "source_url"],
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
        results["team_events"] = self.sync_team_events()
        return results

    # ─────────────────────────────────────────────────────────────────────────────
    # Stadium Real-Time Data Sync (이동 시간 · 혼잡도 · 운영 공지)
    # ─────────────────────────────────────────────────────────────────────────────

    def sync_transit_times(
        self,
        game_date: str | None = None,
        batch_size: int = 1000,
    ) -> int:
        """
        Sync stadium_transit_times from SQLite to OCI.

        Args:
            game_date: Filter by YYYYMMDD string. If None, syncs all rows.
            batch_size: Records per UPSERT batch.
        """
        self._ensure_table(StadiumTransitTime)
        filters = None
        if game_date:
            filters = [StadiumTransitTime.game_date == game_date]
        count = self.sync_simple_table(
            StadiumTransitTime,
            ["stadium_code", "origin_label", "transport_mode", "measured_at"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )
        logger.info(f"✅ Synced {count} transit time records to OCI")
        return count

    def sync_congestion(
        self,
        game_date: str | None = None,
        batch_size: int = 1000,
    ) -> int:
        """
        Sync stadium_congestion from SQLite to OCI.

        Args:
            game_date: Filter by YYYYMMDD string. If None, syncs all rows.
            batch_size: Records per UPSERT batch.
        """
        self._ensure_table(StadiumCongestion)
        filters = None
        if game_date:
            filters = [StadiumCongestion.game_date == game_date]
        count = self.sync_simple_table(
            StadiumCongestion,
            ["stadium_code", "location_label", "measured_at"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )
        logger.info(f"✅ Synced {count} congestion records to OCI")
        return count

    def sync_operation_notices(
        self,
        game_date: str | None = None,
        batch_size: int = 500,
    ) -> int:
        """
        Sync stadium_operation_notices from SQLite to OCI.

        Args:
            game_date: Filter by YYYYMMDD string. If None, syncs all rows.
            batch_size: Records per UPSERT batch.
        """
        self._ensure_table(StadiumOperationNotice)
        filters = None
        if game_date:
            filters = [StadiumOperationNotice.game_date == game_date]
        count = self.sync_simple_table(
            StadiumOperationNotice,
            ["stadium_code", "source_name", "external_id"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )
        logger.info(f"✅ Synced {count} operation notice records to OCI")
        return count

    def sync_stadium_realtime_all(
        self,
        game_date: str | None = None,
    ) -> dict[str, int]:
        """Sync all 3 stadium real-time tables to OCI in one call."""
        results = {
            "transit_times": self.sync_transit_times(game_date=game_date),
            "congestion": self.sync_congestion(game_date=game_date),
            "operation_notices": self.sync_operation_notices(game_date=game_date),
        }
        logger.info(f"✅ Stadium Realtime All Sync Summary: {results}")
        return results

    def sync_daily_rosters(
        self,
        start_date: date | datetime | str | None = None,
        end_date: date | datetime | str | None = None,
    ) -> int:
        """Sync team_daily_roster from SQLite to OCI"""
        from src.utils.team_history import resolve_team_code_for_season

        start = _normalize_daily_roster_date(start_date)
        end = _normalize_daily_roster_date(end_date)
        if start and end and start > end:
            raise ValueError("daily roster start_date must be earlier than or equal to end_date")
        scope = _format_daily_roster_scope(start, end)
        logger.info(f"INFO: Syncing daily rosters{scope}...")

        filters = []
        if start:
            filters.append(TeamDailyRoster.roster_date >= start)
        if end:
            filters.append(TeamDailyRoster.roster_date <= end)

        def transform(data: dict) -> dict[str, Any]:
            team_code = data.get("team_code", "")
            roster_date = data.get("roster_date")
            if team_code and roster_date:
                season_year = roster_date.year if hasattr(roster_date, "year") else None
                raw = team_code.strip().upper()
                if raw == "LOT":
                    raw = "LT"
                elif raw == "KW":
                    raw = "KH"
                if season_year:
                    resolved = resolve_team_code_for_season(raw, season_year)
                    if resolved:
                        data["team_code"] = resolved
            return data

        return self.sync_simple_table(
            TeamDailyRoster,
            ["roster_date", "team_code", "player_id"],
            filters=filters or None,
            transform_fn=transform,
            batch_size=1000,
        )

    def sync_team_history(self) -> int:
        """Sync team_history table"""
        if not self._target_table_exists(TeamHistory):
            return 0

        franchise_mapping = self._get_franchise_id_mapping()
        histories = self.sqlite_session.query(TeamHistory).all()
        records = []

        for h in histories:
            sup_fid = franchise_mapping.get(h.franchise_id)
            if not sup_fid:
                continue
            records.append(
                {
                    "id": h.id,
                    "franchise_id": sup_fid,
                    "season": h.season,
                    "team_name": h.team_name,
                    "team_code": h.team_code,
                    "logo_url": h.logo_url,
                    "ranking": h.ranking,
                    "stadium": h.stadium,
                    "city": h.city,
                    "color": h.color,
                    "created_at": h.created_at or datetime.now(),
                    "updated_at": h.updated_at or datetime.now(),
                },
            )

        if not records:
            logger.info("ℹ️ No team history to sync.")
            return 0

        self._bulk_copy_upsert(TeamHistory.__tablename__, records, ["id"])
        logger.info(f"✅ Synced {len(records)} team history records to OCI")
        return len(records)

    def sync_team_code_map(self) -> int:
        """Sync team_code_map table using bulk COPY upsert"""
        franchise_mapping = self._get_franchise_id_mapping()

        def transform(data: dict) -> dict[str, Any]:
            fid = data.get("franchise_id")
            if fid:
                data["franchise_id"] = franchise_mapping.get(fid, fid)
            return data

        return self.sync_simple_table(
            TeamCodeMap,
            ["season", "curr_code"],
            transform_fn=transform,
        )

    def sync_matchups(self, year: int = None, batch_size: int = 10000) -> dict[str, int]:
        """Sync Matchup Split tables (Batter/Pitcher vs Team, Stadium, Starter, PBP-BvP) to OCI"""
        logger.info("📁 Ensuring matchup tables exist on OCI...")
        Base.metadata.create_all(self.oci_engine)

        results = {}
        filters = [text("season_year = :year").bindparams(year=year)] if year else None

        results["batter_team"] = self.sync_simple_table(
            BatterTeamSplit,
            ["season_year", "league_type_code", "player_id", "team_code", "opponent_team_code"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["pitcher_team"] = self.sync_simple_table(
            PitcherTeamSplit,
            ["season_year", "league_type_code", "player_id", "team_code", "opponent_team_code"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["batter_stadium"] = self.sync_simple_table(
            BatterStadiumSplit,
            ["season_year", "league_type_code", "player_id", "team_code", "stadium_name"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["batter_vs_starter"] = self.sync_simple_table(
            BatterVsStarter,
            ["season_year", "league_type_code", "player_id", "pitcher_name"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        results["matchup_bvp"] = self.sync_simple_table(
            MatchupBvP, ["batter_id", "pitcher_id"], exclude_cols=["created_at", "id"], batch_size=batch_size,
        )

        results["batter_splits"] = self.sync_simple_table(
            BatterSplit,
            ["player_id", "season_year", "split_type"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )
        results["pitcher_splits"] = self.sync_simple_table(
            PitcherSplit,
            ["player_id", "season_year", "split_type"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

        logger.info(f"✅ Matchup Splits Sync Summary: {results}")
        return results
