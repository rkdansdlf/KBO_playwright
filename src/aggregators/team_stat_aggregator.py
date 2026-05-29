"""
Service to aggregate player season stats into team-level season stats.
Acts as a fallback when KBO's team cumulative record pages are unavailable.
"""

from typing import Any

from sqlalchemy import func
from sqlalchemy.orm import Session

from src.models.player import PlayerSeasonBatting, PlayerSeasonPitching
from src.models.standings import TeamStandingsDaily
from src.models.team import Team
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator


class TeamStatAggregator:
    """
    Aggregates individual player season statistics into team-level season statistics.
    """

    @staticmethod
    def _get_team_games(session: Session, team_id: str, year: int) -> int:
        """
        Get the total games played by a team in a season from standings,
        falling back to max games by an individual player.
        """
        # Try standings first (latest record in that year)
        latest_standings = (
            session.query(TeamStandingsDaily)
            .filter(TeamStandingsDaily.team_code == team_id)
            .filter(func.strftime("%Y", TeamStandingsDaily.standings_date) == str(year))
            .order_by(TeamStandingsDaily.standings_date.desc())
            .first()
        )
        if latest_standings:
            return latest_standings.games_played
        return 0

    @staticmethod
    def aggregate_team_batting(session: Session, year: int, league: str = "REGULAR") -> list[dict[str, Any]]:
        """
        Aggregate batting stats for all teams in a given season/league.
        """
        league = league.upper()

        # Query counting stats grouped by team_id
        query = (
            session.query(
                PlayerSeasonBatting.team_code.label("team_id"),
                func.max(Team.team_name).label("team_name"),
                func.count(PlayerSeasonBatting.id).label("player_count"),
                func.sum(PlayerSeasonBatting.games).label("games_sum"),
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
            )
            .outerjoin(Team, PlayerSeasonBatting.team_code == Team.team_id)
            .filter(PlayerSeasonBatting.season == year)
            .filter(PlayerSeasonBatting.league == league)
            .group_by(PlayerSeasonBatting.team_code)
        )

        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}

            # Use standings for team games if available
            team_games = TeamStatAggregator._get_team_games(session, data["team_id"], year)
            if team_games == 0:
                # Fallback to max games by a player
                max_player_games = (
                    session.query(func.max(PlayerSeasonBatting.games))
                    .filter(
                        PlayerSeasonBatting.team_code == data["team_id"],
                        PlayerSeasonBatting.season == year,
                        PlayerSeasonBatting.league == league,
                    )
                    .scalar()
                    or 0
                )
                team_games = max_player_games

            data["games"] = team_games

            # Calculate ratios
            ratios = BattingStatCalculator.calculate_ratios(data)
            data.update(ratios)

            # Metadata
            data["season"] = year
            data["league"] = league

            results.append(data)

        return results

    @staticmethod
    def aggregate_team_pitching(session: Session, year: int, league: str = "REGULAR") -> list[dict[str, Any]]:
        """
        Aggregate pitching stats for all teams in a given season/league.
        """
        league = league.upper()

        query = (
            session.query(
                PlayerSeasonPitching.team_code.label("team_id"),
                func.max(Team.team_name).label("team_name"),
                func.sum(PlayerSeasonPitching.games).label("games_sum"),
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
            )
            .outerjoin(Team, PlayerSeasonPitching.team_code == Team.team_id)
            .filter(PlayerSeasonPitching.season == year)
            .filter(PlayerSeasonPitching.league == league)
            .group_by(PlayerSeasonPitching.team_code)
        )

        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}

            team_games = TeamStatAggregator._get_team_games(session, data["team_id"], year)
            if team_games == 0:
                max_player_games = (
                    session.query(func.max(PlayerSeasonPitching.games))
                    .filter(
                        PlayerSeasonPitching.team_code == data["team_id"],
                        PlayerSeasonPitching.season == year,
                        PlayerSeasonPitching.league == league,
                    )
                    .scalar()
                    or 0
                )
                team_games = max_player_games

            data["games"] = team_games

            # IP formatting
            data["innings_pitched"] = data["innings_outs"] / 3.0

            # Calculate ratios
            ratios = PitchingStatCalculator.calculate_ratios(data)
            data.update(ratios)

            # Metadata
            data["season"] = year
            data["league"] = league

            results.append(data)

        return results
