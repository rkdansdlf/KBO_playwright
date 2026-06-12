"""
Service to aggregate player season stats into team-level season stats.
Acts as a fallback when KBO's team cumulative record pages are unavailable.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from datetime import date as date_type
from typing import Any

from sqlalchemy import func, or_, text
from sqlalchemy.orm import Session

from src.db.engine import get_database_type
from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.standings import TeamStandingsDaily
from src.models.team import Team
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

logger = logging.getLogger(__name__)

DEFAULT_TEAM_NAMES = {
    "OB": "두산",
    "LT": "롯데",
    "SS": "삼성",
    "WO": "키움",
    "HE": "한화",
    "SK": "SSG",
    "HT": "KIA",
    "LG": "LG",
    "KT": "KT",
    "NC": "NC",
}


class TeamStatAggregator:
    """
    Aggregates individual player season statistics into team-level season statistics.
    Provides pure in-memory aggregation methods for testability and DB-integrated methods for CLI/Crawlers.
    """

    def __init__(self, session: Session | None = None) -> None:
        self.session = session

    @staticmethod
    def _get_team_games(session: Session, team_id: str, year: int) -> int:
        """
        Get the total games played by a team in a season from standings.
        Returns 0 if not found.
        """
        start_date = date_type(year, 1, 1)
        end_date = date_type(year, 12, 31)
        latest_standings = (
            session.query(TeamStandingsDaily)
            .filter(TeamStandingsDaily.team_code == team_id)
            .filter(TeamStandingsDaily.standings_date >= start_date)
            .filter(TeamStandingsDaily.standings_date <= end_date)
            .order_by(TeamStandingsDaily.standings_date.desc())
            .first()
        )
        if latest_standings:
            return latest_standings.games_played
        return 0

    @staticmethod
    def _get_team_record_from_standings(session: Session, team_id: str, year: int) -> dict[str, int]:
        """
        Get the team win/loss/tie record from standings.
        """
        start_date = date_type(year, 1, 1)
        end_date = date_type(year, 12, 31)
        latest_standings = (
            session.query(TeamStandingsDaily)
            .filter(TeamStandingsDaily.team_code == team_id)
            .filter(TeamStandingsDaily.standings_date >= start_date)
            .filter(TeamStandingsDaily.standings_date <= end_date)
            .order_by(TeamStandingsDaily.standings_date.desc())
            .first()
        )
        if latest_standings:
            return {
                "games": latest_standings.games_played,
                "wins": latest_standings.wins,
                "losses": latest_standings.losses,
                "ties": latest_standings.draws,
            }
        return {"games": 0, "wins": 0, "losses": 0, "ties": 0}

    def aggregate_batting(
        self,
        season_or_rows: int | Iterable[PlayerSeasonBatting] | None = None,
        team_id_or_names: str | dict[str, str] | None = None,
        team_games_map: dict[tuple[int, str], int] | None = None,
        rows: Iterable[PlayerSeasonBatting] | None = None,
        team_names: dict[str, str] | None = None,
        dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Dispatches to database-driven aggregation if an integer season is passed,
        or pure in-memory aggregation if an iterable of rows is passed.
        """
        actual_rows = rows if rows is not None else (season_or_rows if not isinstance(season_or_rows, int) else None)
        actual_names = (
            team_names if team_names is not None else (team_id_or_names if isinstance(team_id_or_names, dict) else None)
        )

        if isinstance(season_or_rows, int):
            season = season_or_rows
            team_id = team_id_or_names if isinstance(team_id_or_names, str) else None
            return self._aggregate_batting_db(season, team_id, dry_run=dry_run)
        elif actual_rows is not None:
            return self._aggregate_batting_mem(actual_rows, actual_names, team_games_map)
        else:
            raise ValueError("Either an integer season or rows iterable must be provided")

    def aggregate_pitching(
        self,
        season_or_rows: int | Iterable[PlayerSeasonPitching] | None = None,
        team_id_or_names: str | dict[str, str] | None = None,
        team_games_map: dict[tuple[int, str], int] | None = None,
        rows: Iterable[PlayerSeasonPitching] | None = None,
        team_names: dict[str, str] | None = None,
        dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Dispatches to database-driven aggregation if an integer season is passed,
        or pure in-memory aggregation if an iterable of rows is passed.
        """
        actual_rows = rows if rows is not None else (season_or_rows if not isinstance(season_or_rows, int) else None)
        actual_names = (
            team_names if team_names is not None else (team_id_or_names if isinstance(team_id_or_names, dict) else None)
        )

        if isinstance(season_or_rows, int):
            season = season_or_rows
            team_id = team_id_or_names if isinstance(team_id_or_names, str) else None
            return self._aggregate_pitching_db(season, team_id, dry_run=dry_run)
        elif actual_rows is not None:
            return self._aggregate_pitching_mem(actual_rows, actual_names, team_games_map)
        else:
            raise ValueError("Either an integer season or rows iterable must be provided")

    def aggregate_all(
        self,
        season: int,
        team_id: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, list[dict[str, Any]]]:
        """
        Aggregates and updates both batting and pitching stats.
        """
        batting_results = self.aggregate_batting(season, team_id, dry_run=dry_run)
        pitching_results = self.aggregate_pitching(season, team_id, dry_run=dry_run)
        return {
            "batting": batting_results,
            "pitching": pitching_results,
        }

    def _aggregate_batting_db(
        self,
        season: int,
        team_id: str | None = None,
        dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        if not self.session:
            raise ValueError("Database session is required for database aggregation")

        logger.info("Aggregating player batting stats via database query for season=%s, team_id=%s", season, team_id)
        team_code_expr = func.coalesce(PlayerSeasonBatting.canonical_team_code, PlayerSeasonBatting.team_code)

        query = (
            self.session.query(
                team_code_expr.label("team_id"),
                func.sum(PlayerSeasonBatting.plate_appearances).label("plate_appearances"),
                func.sum(PlayerSeasonBatting.at_bats).label("at_bats"),
                func.sum(PlayerSeasonBatting.runs).label("runs"),
                func.sum(PlayerSeasonBatting.hits).label("hits"),
                func.sum(PlayerSeasonBatting.doubles).label("doubles"),
                func.sum(PlayerSeasonBatting.triples).label("triples"),
                func.sum(PlayerSeasonBatting.home_runs).label("home_runs"),
                func.sum(PlayerSeasonBatting.rbi).label("rbi"),
                func.sum(PlayerSeasonBatting.walks).label("walks"),
                func.sum(PlayerSeasonBatting.intentional_walks).label("intentional_walks"),
                func.sum(PlayerSeasonBatting.hbp).label("hbp"),
                func.sum(PlayerSeasonBatting.strikeouts).label("strikeouts"),
                func.sum(PlayerSeasonBatting.stolen_bases).label("stolen_bases"),
                func.sum(PlayerSeasonBatting.caught_stealing).label("caught_stealing"),
                func.sum(PlayerSeasonBatting.sacrifice_hits).label("sacrifice_hits"),
                func.sum(PlayerSeasonBatting.sacrifice_flies).label("sacrifice_flies"),
                func.sum(PlayerSeasonBatting.gdp).label("gdp"),
                func.max(PlayerSeasonBatting.games).label("max_player_games"),
            )
            .filter(PlayerSeasonBatting.season == season)
            .filter(PlayerSeasonBatting.league == "REGULAR")
            .filter(
                team_code_expr.isnot(None),
                or_(
                    PlayerSeasonBatting.team_code.is_(None),
                    PlayerSeasonBatting.team_code.not_in(["", "합계", "TOTAL", "ALL", "-"]),
                ),
            )
        )
        if team_id:
            query = query.filter(team_code_expr == team_id)

        query = query.group_by(team_code_expr)
        rows = query.all()

        if not rows:
            logger.warning("No player batting stats aggregated for season=%s, team_id=%s", season, team_id)
            return []

        teams = self.session.query(Team).all()
        team_names = {t.team_id: t.team_name for t in teams if t.team_id}

        results = []
        for row in rows:
            tc = row.team_id
            team_games = self._get_team_games(self.session, tc, season)
            if not team_games:
                team_games = row.max_player_games or 0

            data = {
                "team_id": tc,
                "team_name": team_names.get(tc, tc),
                "season": season,
                "league": "REGULAR",
                "games": team_games,
                "plate_appearances": int(row.plate_appearances or 0),
                "at_bats": int(row.at_bats or 0),
                "runs": int(row.runs or 0),
                "hits": int(row.hits or 0),
                "doubles": int(row.doubles or 0),
                "triples": int(row.triples or 0),
                "home_runs": int(row.home_runs or 0),
                "rbi": int(row.rbi or 0),
                "walks": int(row.walks or 0),
                "intentional_walks": int(row.intentional_walks or 0),
                "hbp": int(row.hbp or 0),
                "strikeouts": int(row.strikeouts or 0),
                "stolen_bases": int(row.stolen_bases or 0),
                "caught_stealing": int(row.caught_stealing or 0),
                "sacrifice_hits": int(row.sacrifice_hits or 0),
                "sacrifice_flies": int(row.sacrifice_flies or 0),
                "gdp": int(row.gdp or 0),
                "extra_stats": {"source": "player_rollup"},
            }

            ratios = BattingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            results.append(data)

        if not dry_run:
            self._save_batting_records(results)
        return results

    def _aggregate_pitching_db(
        self,
        season: int,
        team_id: str | None = None,
        dry_run: bool = False,
    ) -> list[dict[str, Any]]:
        if not self.session:
            raise ValueError("Database session is required for database aggregation")

        logger.info("Aggregating player pitching stats via database query for season=%s, team_id=%s", season, team_id)
        team_code_expr = func.coalesce(PlayerSeasonPitching.canonical_team_code, PlayerSeasonPitching.team_code)

        query = (
            self.session.query(
                team_code_expr.label("team_id"),
                func.sum(PlayerSeasonPitching.wins).label("wins"),
                func.sum(PlayerSeasonPitching.losses).label("losses"),
                func.sum(PlayerSeasonPitching.saves).label("saves"),
                func.sum(PlayerSeasonPitching.holds).label("holds"),
                func.sum(PlayerSeasonPitching.innings_outs).label("innings_outs"),
                func.sum(PlayerSeasonPitching.hits_allowed).label("hits_allowed"),
                func.sum(PlayerSeasonPitching.runs_allowed).label("runs_allowed"),
                func.sum(PlayerSeasonPitching.earned_runs).label("earned_runs"),
                func.sum(PlayerSeasonPitching.home_runs_allowed).label("home_runs_allowed"),
                func.sum(PlayerSeasonPitching.walks_allowed).label("walks_allowed"),
                func.sum(PlayerSeasonPitching.intentional_walks).label("intentional_walks"),
                func.sum(PlayerSeasonPitching.hit_batters).label("hit_batters"),
                func.sum(PlayerSeasonPitching.strikeouts).label("strikeouts"),
                func.sum(PlayerSeasonPitching.tbf).label("tbf"),
                func.sum(PlayerSeasonPitching.complete_games).label("complete_games"),
                func.sum(PlayerSeasonPitching.shutouts).label("shutouts"),
                func.sum(PlayerSeasonPitching.wild_pitches).label("wild_pitches"),
                func.sum(PlayerSeasonPitching.balks).label("balks"),
                func.sum(PlayerSeasonPitching.sacrifices_allowed).label("sacrifices_allowed"),
                func.sum(PlayerSeasonPitching.sacrifice_flies_allowed).label("sacrifice_flies_allowed"),
                func.max(PlayerSeasonPitching.games).label("max_player_games"),
            )
            .filter(PlayerSeasonPitching.season == season)
            .filter(PlayerSeasonPitching.league == "REGULAR")
            .filter(
                team_code_expr.isnot(None),
                or_(
                    PlayerSeasonPitching.team_code.is_(None),
                    PlayerSeasonPitching.team_code.not_in(["", "합계", "TOTAL", "ALL", "-"]),
                ),
            )
        )
        if team_id:
            query = query.filter(team_code_expr == team_id)

        query = query.group_by(team_code_expr)
        rows = query.all()

        if not rows:
            logger.warning("No player pitching stats aggregated for season=%s, team_id=%s", season, team_id)
            return []

        teams = self.session.query(Team).all()
        team_names = {t.team_id: t.team_name for t in teams if t.team_id}

        results = []
        for row in rows:
            tc = row.team_id
            rec = self._get_team_record_from_standings(self.session, tc, season)

            # wins/losses are aggregated from player rows, ties from standings
            wins = int(row.wins or 0)
            losses = int(row.losses or 0)
            ties = rec["ties"] if rec["games"] > 0 else 0
            team_games = rec["games"] if rec["games"] > 0 else (row.max_player_games or 0)

            # Opponent Batting Average
            tbf = int(row.tbf or 0)
            bb = int(row.walks_allowed or 0)
            hbp = int(row.hit_batters or 0)
            rec_sac = int(row.sacrifices_allowed or 0)
            rec_sf = int(row.sacrifice_flies_allowed or 0)
            opp_ab = tbf - bb - hbp - rec_sac - rec_sf
            hits = int(row.hits_allowed or 0)
            avg_against = hits / opp_ab if opp_ab > 0 else 0.0

            data = {
                "team_id": tc,
                "team_name": team_names.get(tc, tc),
                "season": season,
                "league": "REGULAR",
                "games": team_games,
                "wins": wins,
                "losses": losses,
                "ties": ties,
                "saves": int(row.saves or 0),
                "holds": int(row.holds or 0),
                "innings_outs": int(row.innings_outs or 0),
                "innings_pitched": int(row.innings_outs or 0) / 3.0,
                "runs_allowed": int(row.runs_allowed or 0),
                "earned_runs": int(row.earned_runs or 0),
                "hits_allowed": hits,
                "home_runs_allowed": int(row.home_runs_allowed or 0),
                "walks_allowed": bb,
                "strikeouts": int(row.strikeouts or 0),
                "intentional_walks": int(row.intentional_walks or 0),
                "hit_batters": int(row.hit_batters or 0),
                "tbf": int(row.tbf or 0),
                "complete_games": int(row.complete_games or 0),
                "shutouts": int(row.shutouts or 0),
                "wild_pitches": int(row.wild_pitches or 0),
                "balks": int(row.balks or 0),
                "sacrifices_allowed": int(row.sacrifices_allowed or 0),
                "sacrifice_flies_allowed": int(row.sacrifice_flies_allowed or 0),
                "extra_stats": {"source": "player_rollup"},
            }

            ratios = PitchingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            data["avg_against"] = avg_against
            results.append(data)

        if not dry_run:
            self._save_pitching_records(results)
        return results

    def _save_batting_records(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        from src.repositories.team_stats_repository import TeamSeasonBattingRepository

        repo = TeamSeasonBattingRepository()
        cleaned = [repo._filter_model_fields(repo._filter_none(r)) for r in records]

        db_type = get_database_type()
        try:
            if db_type == "sqlite":
                self.session.execute(text("PRAGMA foreign_keys = OFF"))
            for payload in cleaned:
                stmt = repo._build_insert_stmt(payload)
                self.session.execute(stmt)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        finally:
            if db_type == "sqlite":
                self.session.execute(text("PRAGMA foreign_keys = ON"))

    def _save_pitching_records(self, records: list[dict[str, Any]]) -> None:
        if not records:
            return
        from src.repositories.team_stats_repository import TeamSeasonPitchingRepository

        repo = TeamSeasonPitchingRepository()
        cleaned = [repo._filter_model_fields(repo._filter_none(r)) for r in records]

        db_type = get_database_type()
        try:
            if db_type == "sqlite":
                self.session.execute(text("PRAGMA foreign_keys = OFF"))
            for payload in cleaned:
                stmt = repo._build_insert_stmt(payload)
                self.session.execute(stmt)
            self.session.commit()
        except Exception:
            self.session.rollback()
            raise
        finally:
            if db_type == "sqlite":
                self.session.execute(text("PRAGMA foreign_keys = ON"))

    def _aggregate_batting_mem(
        self,
        rows: Iterable[PlayerSeasonBatting],
        team_names: dict[str, str] | None = None,
        team_games_map: dict[tuple[int, str], int] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Pure in-memory aggregation of player season batting rows into team batting stats.
        Grouped by (season, team_code).
        """
        if team_names is None:
            team_names = DEFAULT_TEAM_NAMES
        if team_games_map is None:
            team_games_map = {}

        # Group rows by (season, team_code)
        groups: dict[tuple[int, str], list[PlayerSeasonBatting]] = {}
        for r in rows:
            tc = r.canonical_team_code or r.team_code
            if not tc or tc in ("합계", "TOTAL", "ALL", "-"):
                logger.warning(f"[WARN] Skipping PlayerSeasonBatting row ID {r.id}: Invalid team_code '{tc}'")
                continue
            if not r.season:
                logger.warning("[WARN] Skipping PlayerSeasonBatting row ID %s: Missing season", r.id)
                continue

            key = (r.season, tc)
            groups.setdefault(key, []).append(r)

        results = []
        for (season, team_code), p_rows in groups.items():
            # Sum counting stats
            aggregated = {
                "plate_appearances": 0,
                "at_bats": 0,
                "runs": 0,
                "hits": 0,
                "doubles": 0,
                "triples": 0,
                "home_runs": 0,
                "rbi": 0,
                "walks": 0,
                "intentional_walks": 0,
                "hbp": 0,
                "strikeouts": 0,
                "stolen_bases": 0,
                "caught_stealing": 0,
                "sacrifice_hits": 0,
                "sacrifice_flies": 0,
                "gdp": 0,
            }

            max_player_games = 0
            for pr in p_rows:
                max_player_games = max(max_player_games, pr.games or 0)
                aggregated["plate_appearances"] += pr.plate_appearances or 0
                aggregated["at_bats"] += pr.at_bats or 0
                aggregated["runs"] += pr.runs or 0
                aggregated["hits"] += pr.hits or 0
                aggregated["doubles"] += pr.doubles or 0
                aggregated["triples"] += pr.triples or 0
                aggregated["home_runs"] += pr.home_runs or 0
                aggregated["rbi"] += pr.rbi or 0
                aggregated["walks"] += pr.walks or 0
                aggregated["intentional_walks"] += pr.intentional_walks or 0
                aggregated["hbp"] += pr.hbp or 0
                aggregated["strikeouts"] += pr.strikeouts or 0
                aggregated["stolen_bases"] += pr.stolen_bases or 0
                aggregated["caught_stealing"] += pr.caught_stealing or 0
                aggregated["sacrifice_hits"] += pr.sacrifice_hits or 0
                aggregated["sacrifice_flies"] += pr.sacrifice_flies or 0
                aggregated["gdp"] += pr.gdp or 0

            # Determine team games
            team_games = team_games_map.get((season, team_code))
            if not team_games:
                team_games = max_player_games

            # Format result
            data = {
                "team_id": team_code,
                "team_name": team_names.get(team_code, team_code),
                "season": season,
                "league": p_rows[0].league or "REGULAR",
                "games": team_games,
                **aggregated,
            }

            # Calculate ratio metrics
            ratios = BattingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            results.append(data)

        return results

    def _aggregate_pitching_mem(
        self,
        rows: Iterable[PlayerSeasonPitching],
        team_names: dict[str, str] | None = None,
        team_games_map: dict[tuple[int, str], int] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Pure in-memory aggregation of player season pitching rows into team pitching stats.
        Grouped by (season, team_code).
        """
        if team_names is None:
            team_names = DEFAULT_TEAM_NAMES
        if team_games_map is None:
            team_games_map = {}

        # Group rows by (season, team_code)
        groups: dict[tuple[int, str], list[PlayerSeasonPitching]] = {}
        for r in rows:
            tc = r.canonical_team_code or r.team_code
            if not tc or tc in ("합계", "TOTAL", "ALL", "-"):
                logger.warning(f"[WARN] Skipping PlayerSeasonPitching row ID {r.id}: Invalid team_code '{tc}'")
                continue
            if not r.season:
                logger.warning("[WARN] Skipping PlayerSeasonPitching row ID %s: Missing season", r.id)
                continue

            key = (r.season, tc)
            groups.setdefault(key, []).append(r)

        results = []
        for (season, team_code), p_rows in groups.items():
            # Sum counting stats
            aggregated = {
                "wins": 0,
                "losses": 0,
                "saves": 0,
                "holds": 0,
                "innings_outs": 0,
                "hits_allowed": 0,
                "runs_allowed": 0,
                "earned_runs": 0,
                "home_runs_allowed": 0,
                "walks_allowed": 0,
                "intentional_walks": 0,
                "hit_batters": 0,
                "strikeouts": 0,
            }

            max_player_games = 0
            for pr in p_rows:
                max_player_games = max(max_player_games, pr.games or 0)
                aggregated["wins"] += pr.wins or 0
                aggregated["losses"] += pr.losses or 0
                aggregated["saves"] += pr.saves or 0
                aggregated["holds"] += pr.holds or 0
                aggregated["innings_outs"] += pr.innings_outs or 0
                aggregated["hits_allowed"] += pr.hits_allowed or 0
                aggregated["runs_allowed"] += pr.runs_allowed or 0
                aggregated["earned_runs"] += pr.earned_runs or 0
                aggregated["home_runs_allowed"] += pr.home_runs_allowed or 0
                aggregated["walks_allowed"] += pr.walks_allowed or 0
                aggregated["intentional_walks"] += pr.intentional_walks or 0
                aggregated["hit_batters"] += pr.hit_batters or 0
                aggregated["strikeouts"] += pr.strikeouts or 0

            # Determine team games
            team_games = team_games_map.get((season, team_code))
            if not team_games:
                team_games = max_player_games

            # Format result
            data = {
                "team_id": team_code,
                "team_name": team_names.get(team_code, team_code),
                "season": season,
                "league": p_rows[0].league or "REGULAR",
                "games": team_games,
                "ties": 0,
                "innings_pitched": aggregated["innings_outs"] / 3.0,
                **aggregated,
            }

            # Calculate ratio metrics
            ratios = PitchingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            results.append(data)

        return results

    @staticmethod
    def aggregate_team_batting(session: Session, year: int, league: str = "REGULAR", **kwargs) -> list[dict[str, Any]]:
        """
        Aggregate batting stats for all teams in a given season/league using database queries.
        Maintains legacy database-bound compatibility while reusing pure business logic under the hood.
        """
        league = league.upper()

        # Query all player batting rows for target year and league
        rows = (
            session.query(PlayerSeasonBatting)
            .filter(PlayerSeasonBatting.season == year)
            .filter(PlayerSeasonBatting.league == league)
            .all()
        )

        if not rows:
            return []

        # Build Team Name lookup map
        teams = session.query(Team).all()
        team_names = {t.team_id: t.team_name for t in teams if t.team_id}

        # Build Team Games lookup map from Standings
        team_games_map = {}
        unique_teams = {r.canonical_team_code or r.team_code for r in rows if r.canonical_team_code or r.team_code}
        for tc in unique_teams:
            if tc:
                games = TeamStatAggregator._get_team_games(session, tc, year)
                if games > 0:
                    team_games_map[(year, tc)] = games

        aggregator = TeamStatAggregator()
        return aggregator.aggregate_batting(rows=rows, team_names=team_names, team_games_map=team_games_map)

    @staticmethod
    def aggregate_team_pitching(session: Session, year: int, league: str = "REGULAR", **kwargs) -> list[dict[str, Any]]:
        """
        Aggregate pitching stats for all teams in a given season/league using database queries.
        Maintains legacy database-bound compatibility while reusing pure business logic under the hood.
        """
        league = league.upper()

        # Query all player pitching rows for target year and league
        rows = (
            session.query(PlayerSeasonPitching)
            .filter(PlayerSeasonPitching.season == year)
            .filter(PlayerSeasonPitching.league == league)
            .all()
        )

        if not rows:
            return []

        # Build Team Name lookup map
        teams = session.query(Team).all()
        team_names = {t.team_id: t.team_name for t in teams if t.team_id}

        # Build Team Games lookup map from Standings
        team_games_map = {}
        unique_teams = {r.canonical_team_code or r.team_code for r in rows if r.canonical_team_code or r.team_code}
        for tc in unique_teams:
            if tc:
                games = TeamStatAggregator._get_team_games(session, tc, year)
                if games > 0:
                    team_games_map[(year, tc)] = games

        aggregator = TeamStatAggregator()
        results = aggregator.aggregate_pitching(rows=rows, team_names=team_names, team_games_map=team_games_map)

        # Supplement ties from standings
        for r in results:
            tc = r["team_id"]
            if tc:
                rec = TeamStatAggregator._get_team_record_from_standings(session, tc, year)
                if rec["games"] > 0:
                    r["ties"] = rec["ties"]

        return results
