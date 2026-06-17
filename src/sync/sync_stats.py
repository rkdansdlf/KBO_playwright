"""
Season stats sync: batting, pitching, fielding, baserunning, standings, rankings.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from src.models.player import (
    PlayerSeasonBaserunning,
    PlayerSeasonBatting,
    PlayerSeasonFielding,
    PlayerSeasonPitching,
)
from src.models.rankings import StatRanking
from src.models.team_stats import TeamSeasonBatting, TeamSeasonPitching
from src.sync.sync_base import _serialize_scalar

logger = logging.getLogger(__name__)


class StatsSyncMixin:
    """Mixin for season stats sync operations."""

    def _add_existing_player_basic_filter(self, model: type, filters: list) -> list:
        """Avoid FK failures when local season stats contain orphan player IDs."""
        from src.models.player import PlayerBasic

        existing_player_filter = model.player_id.in_(self.sqlite_session.query(PlayerBasic.player_id))
        missing_count = self.sqlite_session.query(model).filter(*filters, ~existing_player_filter).count()
        if missing_count:
            logger.warning(
                "⚠️ Skipping %s %s rows with missing local player_basic references",
                missing_count,
                model.__tablename__,
            )
        return [*filters, existing_player_filter]

    def sync_pitcher_data(self) -> int:
        """새로운 player_season_pitching 테이블의 투수 데이터를 OCI로 동기화 (고속 Batch COPY)"""
        return self.sync_simple_table(
            PlayerSeasonPitching,
            conflict_keys=["player_id", "season", "league", "level"],
            exclude_cols=["id", "created_at"],
        )

    def sync_batting_data(self) -> int:
        """타자 데이터를 OCI로 동기화 (고속 Batch COPY)"""
        return self.sync_simple_table(
            PlayerSeasonBatting,
            conflict_keys=["player_id", "season", "league", "level"],
            exclude_cols=["id", "created_at"],
        )

    def verify_pitcher_sync(self, expected_count: int) -> None:
        """투수 데이터 동기화 결과 검증"""
        try:
            result = self.target_session.execute(
                text("""
                SELECT COUNT(*) as count
                FROM player_season_pitching
            """),
            )

            actual_count = result.fetchone()[0]
            logger.info("🔍 OCI 투수 데이터 확인: %s건 (예상: %s건)", actual_count, expected_count)

            if actual_count >= expected_count:
                logger.info("✅ 투수 데이터 동기화 검증 성공!")
            else:
                logger.warning("⚠️ 동기화된 투수 데이터 수가 예상보다 적습니다.")

        except SQLAlchemyError:
            logger.exception("⚠️ 투수 데이터 동기화 검증 실패")

    def verify_batting_sync(self, expected_count: int) -> None:
        """타자 데이터 동기화 결과 검증"""
        try:
            result = self.target_session.execute(
                text("""
                SELECT COUNT(*) as count
                FROM player_season_batting
            """),
            )

            actual_count = result.fetchone()[0]
            logger.info("🔍 OCI 타자 데이터 확인: %s건 (예상: %s건)", actual_count, expected_count)

            if actual_count >= expected_count:
                logger.info("✅ 타자 데이터 동기화 검증 성공!")
            else:
                logger.warning("⚠️ 동기화된 타자 데이터 수가 예상보다 적습니다.")

        except SQLAlchemyError:
            logger.exception("⚠️ 타자 데이터 동기화 검증 실패")

    def show_oci_data_sample(self) -> None:
        """OCI의 데이터 샘플 표시"""
        try:
            # 투수 데이터 샘플
            pitcher_result = self.target_session.execute(
                text("""
                SELECT player_id, season, games, wins, losses, era, innings_pitched
                FROM player_season_pitching
                LIMIT 3
            """),
            )

            pitcher_rows = pitcher_result.fetchall()
            if pitcher_rows:
                logger.info("\n📊 OCI 투수 데이터 샘플:")
                for i, row in enumerate(pitcher_rows):
                    logger.info("  %s. player_id: %s, season: %s", i + 1, row[0], row[1])
                    logger.info(
                        "     게임수: %s, 승패: %s-%s, ERA: %s, 이닝: %s",
                        row[2],
                        row[3],
                        row[4],
                        row[5],
                        row[6],
                    )

            # 타자 데이터 샘플
            batting_result = self.target_session.execute(
                text("""
                SELECT player_id, season, games, avg, hits, home_runs
                FROM player_season_batting
                LIMIT 3
            """),
            )

            batting_rows = batting_result.fetchall()
            if batting_rows:
                logger.info("\n🏏 OCI 타자 데이터 샘플:")
                for i, row in enumerate(batting_rows):
                    logger.info("  %s. player_id: %s, season: %s", i + 1, row[0], row[1])
                    logger.info("     게임수: %s, 타율: %s, 안타: %s, 홈런: %s", row[2], row[3], row[4], row[5])

        except SQLAlchemyError:
            logger.exception("⚠️ OCI 데이터 조회 실패")

    def _get_table_signature(self, model: type, year: int | None = None, year_col: str = "season") -> dict[str, Any]:
        """
        Calculate a unique signature for a table/year combination to detect changes.
        Signature includes ROW COUNT and MAX(updated_at).
        """

        def get_sig(session) -> dict[str, Any]:
            table_name = model.__tablename__
            where_clause = ""
            params = {}
            if year:
                # Some tables use 'year' instead of 'season'
                col = year_col
                where_clause = f'WHERE "{col}" = :year'
                params = {"year": year}

            sql = f'SELECT COUNT(*), MAX("updated_at") FROM "{table_name}" {where_clause}'
            try:
                row = session.execute(text(sql), params).fetchone()
                return {"count": row[0] or 0, "max_updated_at": _serialize_scalar(row[1])}
            except SQLAlchemyError:
                return {"count": None, "max_updated_at": None}

        local_sig = get_sig(self.sqlite_session)
        remote_sig = get_sig(self.target_session)

        if local_sig["count"] is None or remote_sig["count"] is None:
            return {"local": local_sig, "remote": remote_sig, "match": False}

        # Compare strings but only up to seconds to avoid precision issues
        # Also replace 'T' with space to handle SQLite vs ISO/Postgres format differences
        l_ts = str(local_sig["max_updated_at"])[:19].replace("T", " ") if local_sig["max_updated_at"] else "None"
        r_ts = str(remote_sig["max_updated_at"])[:19].replace("T", " ") if remote_sig["max_updated_at"] else "None"

        match = local_sig["count"] == remote_sig["count"] and l_ts == r_ts

        return {"local": local_sig, "remote": remote_sig, "match": match}

    def sync_player_season_batting(self, year: int | None = None, batch_size: int = 5000, force: bool = False) -> int:
        """Sync player_season_batting data from SQLite to OCI using fast bulk COPY"""
        from src.models.player import PlayerSeasonBatting

        if year and not force:
            sig = self._get_table_signature(PlayerSeasonBatting, year)
            if sig["match"]:
                logger.info("   ⏩ Skipping player_season_batting for %s (No changes detected)", year)
                return 0

        filters = []
        if year:
            filters.append(PlayerSeasonBatting.season == year)
        filters = self._add_existing_player_basic_filter(PlayerSeasonBatting, filters)

        return self.sync_simple_table(
            PlayerSeasonBatting,
            conflict_keys=["player_id", "season", "league", "level"],
            exclude_cols=["id", "created_at"],  # Include updated_at
            filters=filters,
            batch_size=batch_size,
        )

    def sync_player_season_pitching(self, year: int | None = None, batch_size: int = 5000, force: bool = False) -> int:
        """Sync player_season_pitching data from SQLite to OCI using fast bulk COPY"""
        from src.models.player import PlayerSeasonPitching

        if year and not force:
            sig = self._get_table_signature(PlayerSeasonPitching, year)
            if sig["match"]:
                logger.info("   ⏩ Skipping player_season_pitching for %s (No changes detected)", year)
                return 0

        filters = []
        if year:
            filters.append(PlayerSeasonPitching.season == year)
        filters = self._add_existing_player_basic_filter(PlayerSeasonPitching, filters)

        return self.sync_simple_table(
            PlayerSeasonPitching,
            conflict_keys=["player_id", "season", "league", "level"],
            exclude_cols=["id", "created_at"],  # Include updated_at
            filters=filters,
            batch_size=batch_size,
        )

    def sync_all_player_data(self) -> dict[str, int]:
        """Sync all player-related data"""
        return {
            "players": self.sync_players(),
            "player_identities": self.sync_player_identities(),
            "player_season_batting": self.sync_player_season_batting(),
            "player_season_pitching": self.sync_player_season_pitching(),
            "team_season_batting": self.sync_team_season_batting(),
            "team_season_pitching": self.sync_team_season_pitching(),
        }

    def sync_team_season_batting(self, year: int | None = None, batch_size: int = 5000, force: bool = False) -> int:
        """Sync team_season_batting data from SQLite to OCI"""

        if year and not force:
            sig = self._get_table_signature(TeamSeasonBatting, year)
            if sig["match"]:
                logger.info("   ⏩ Skipping team_season_batting for %s (No changes detected)", year)
                return 0

        filters = []
        if year:
            filters.append(TeamSeasonBatting.season == year)

        return self.sync_simple_table(
            TeamSeasonBatting,
            conflict_keys=["team_id", "season", "league"],
            exclude_cols=["id", "created_at"],  # Include updated_at
            filters=filters,
            batch_size=batch_size,
        )

    def sync_team_season_pitching(self, year: int | None = None, batch_size: int = 5000, force: bool = False) -> int:
        """Sync team_season_pitching data from SQLite to OCI"""

        if year and not force:
            sig = self._get_table_signature(TeamSeasonPitching, year)
            if sig["match"]:
                logger.info("   ⏩ Skipping team_season_pitching for %s (No changes detected)", year)
                return 0

        filters = []
        if year:
            filters.append(TeamSeasonPitching.season == year)

        return self.sync_simple_table(
            TeamSeasonPitching,
            conflict_keys=["team_id", "season", "league"],
            exclude_cols=["id", "created_at"],  # Include updated_at
            filters=filters,
            batch_size=batch_size,
        )

    def sync_standings(self, year: int = None, days: int = None, batch_size: int = 10000) -> int:
        """Sync calculated daily standings snapshots to OCI"""
        from src.models.base import Base
        from src.models.standings import TeamStandingsDaily

        logger.info("📁 Ensure Standings table exists on OCI...")
        Base.metadata.create_all(self.oci_engine)  # Ensure table exists

        filters = []
        if year:
            from sqlalchemy import extract

            filters.append(extract("year", TeamStandingsDaily.standings_date) == year)
        if days:
            from datetime import datetime, timedelta

            since_date = (datetime.now() - timedelta(days=days)).date()
            filters.append(TeamStandingsDaily.standings_date >= since_date)

        return self.sync_simple_table(
            TeamStandingsDaily,
            ["standings_date", "team_code"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

    def sync_stat_rankings(self, year: int | None = None, batch_size: int = 5000) -> int:
        """Sync derived stat_rankings rows to OCI."""
        filters = [StatRanking.season == year] if year else None
        return self.sync_simple_table(
            StatRanking,
            ["season", "metric", "entity_id", "entity_type"],
            exclude_cols=["created_at", "id"],
            filters=filters,
            batch_size=batch_size,
        )

    def sync_fielding_stats(self, year: int | None = None, batch_size: int = 10000, force: bool = False) -> int:
        """Sync player fielding stats to OCI."""
        if year and not force:
            sig = self._get_table_signature(PlayerSeasonFielding, year, year_col="year")
            if sig["match"]:
                logger.info("   ⏩ Skipping fielding stats for %s (No changes detected)", year)
                return 0

        filters = [PlayerSeasonFielding.year == year] if year else []
        filters = self._add_existing_player_basic_filter(PlayerSeasonFielding, filters)
        return self.sync_simple_table(
            PlayerSeasonFielding,
            ["player_id", "team_id", "year", "position_id"],
            exclude_cols=["created_at", "id"],  # Include updated_at
            filters=filters,
            batch_size=batch_size,
        )

    def sync_baserunning_stats(self, year: int | None = None, batch_size: int = 10000, force: bool = False) -> int:
        """Sync player baserunning stats to OCI."""
        if year and not force:
            sig = self._get_table_signature(PlayerSeasonBaserunning, year, year_col="year")
            if sig["match"]:
                logger.info("   ⏩ Skipping baserunning stats for %s (No changes detected)", year)
                return 0

        filters = [PlayerSeasonBaserunning.year == year] if year else []
        filters = self._add_existing_player_basic_filter(PlayerSeasonBaserunning, filters)
        return self.sync_simple_table(
            PlayerSeasonBaserunning,
            ["player_id", "team_id", "year"],
            exclude_cols=["created_at", "id"],  # Include updated_at
            filters=filters,
            batch_size=batch_size,
        )

    def sync_team_season_fielding(self, year: int | None = None, batch_size: int = 5000, force: bool = False) -> int:
        """Sync team_season_fielding aggregates from SQLite to OCI."""
        from src.models.team import TeamSeasonFielding

        if year and not force:
            sig = self._get_table_signature(TeamSeasonFielding, year)
            if sig["match"]:
                logger.info("   ⏩ Skipping team_season_fielding for %s (No changes detected)", year)
                return 0

        filters = [TeamSeasonFielding.season == year] if year else None
        return self.sync_simple_table(
            TeamSeasonFielding,
            conflict_keys=["season", "team_code"],
            exclude_cols=["id", "created_at"],
            filters=filters,
            batch_size=batch_size,
        )

    def sync_team_season_baserunning(self, year: int | None = None, batch_size: int = 5000, force: bool = False) -> int:
        """Sync team_season_baserunning aggregates from SQLite to OCI."""
        from src.models.team import TeamSeasonBaserunning

        if year and not force:
            sig = self._get_table_signature(TeamSeasonBaserunning, year)
            if sig["match"]:
                logger.info("   ⏩ Skipping team_season_baserunning for %s (No changes detected)", year)
                return 0

        filters = [TeamSeasonBaserunning.season == year] if year else None
        return self.sync_simple_table(
            TeamSeasonBaserunning,
            conflict_keys=["season", "team_code"],
            exclude_cols=["id", "created_at"],
            filters=filters,
            batch_size=batch_size,
        )

    def purge_season_stats(self, year: int, type: str = "all") -> None:
        """Delete year-scoped stats from OCI to prepare for a clean sync."""
        tables = []
        if type in ("batting", "all"):
            tables.append("player_season_batting")
            tables.append("team_season_batting")
        if type in ("pitching", "all"):
            tables.append("player_season_pitching")
            tables.append("team_season_pitching")
        if type in ("fielding", "all"):
            tables.append("player_season_fielding")
            tables.append("team_season_fielding")
        if type in ("baserunning", "all"):
            tables.append("player_season_baserunning")
            tables.append("team_season_baserunning")

        for table_name in tables:
            # player-level fielding/baserunning use 'year'; team-level and others use 'season'
            use_year_col = table_name in ("player_season_fielding", "player_season_baserunning")
            year_col = "year" if use_year_col else "season"
            self.target_session.execute(text(f'DELETE FROM "{table_name}" WHERE "{year_col}" = :year'), {"year": year})
        self.target_session.commit()
        logger.info("🧹 Purged OCI season stats for %s (type=%s)", year, type)
