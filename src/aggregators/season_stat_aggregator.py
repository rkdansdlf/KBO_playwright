import logging
from typing import Any

from sqlalchemy import Integer, case, func, or_
from sqlalchemy.orm import Session

from src.models.game import Game, GameBattingStat, GameEvent, GameLineup, GamePitchingStat
from src.models.player import PlayerBasic
from src.models.season import KboSeason
from src.services.stat_calculator import BattingStatCalculator, PitchingStatCalculator

logger = logging.getLogger(__name__)
class SeasonStatAggregator:
    """
    Service to aggregate transactional game stats into season-level cumulative stats.
    Acts as a fallback when KBO's cumulative record pages are unavailable.
    """

    @staticmethod
    def _get_league_name_pattern(series: str) -> str:
        series_map = {
            "regular": "정규시즌",
            "wildcard": "와일드카드",
            "semi_playoff": "준플레이오프",
            "playoff": "플레이오프",
            "korean_series": "한국시리즈",
        }
        return series_map.get(series.lower(), series)

    @staticmethod
    def aggregate_batting_season(
        session: Session, player_id: int, year: int, series: str, source: str = "FALLBACK"
    ) -> dict[str, Any] | None:
        pattern = SeasonStatAggregator._get_league_name_pattern(series)

        query = (
            session.query(
                func.count(GameBattingStat.id).label("games"),
                func.sum(GameBattingStat.plate_appearances).label("plate_appearances"),
                func.sum(GameBattingStat.at_bats).label("at_bats"),
                func.sum(GameBattingStat.runs).label("runs"),
                func.sum(GameBattingStat.hits).label("hits"),
                func.sum(GameBattingStat.doubles).label("doubles"),
                func.sum(GameBattingStat.triples).label("triples"),
                func.sum(GameBattingStat.home_runs).label("home_runs"),
                func.sum(GameBattingStat.rbi).label("rbi"),
                func.sum(GameBattingStat.walks).label("walks"),
                func.sum(GameBattingStat.intentional_walks).label("intentional_walks"),
                func.sum(GameBattingStat.hbp).label("hbp"),
                func.sum(GameBattingStat.strikeouts).label("strikeouts"),
                func.sum(GameBattingStat.stolen_bases).label("stolen_bases"),
                func.sum(GameBattingStat.caught_stealing).label("caught_stealing"),
                func.sum(GameBattingStat.sacrifice_hits).label("sacrifice_hits"),
                func.sum(GameBattingStat.sacrifice_flies).label("sacrifice_flies"),
                func.sum(GameBattingStat.gdp).label("gdp"),
            )
            .join(Game, GameBattingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GameBattingStat.player_id == player_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
        )

        row = query.one_or_none()
        if not row or row.games == 0:
            return None

        data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}

        # Calculate ratios
        ratios = BattingStatCalculator.calculate_ratios(data)
        data.update(ratios)

        # Metadata
        data.update({"player_id": player_id, "season": year, "league": series.upper(), "source": source})

        return data

    @staticmethod
    def aggregate_batting_season_bulk(
        session: Session, year: int, series: str, source: str = "FALLBACK"
    ) -> list[dict[str, Any]]:
        """
        Aggregate batting stats for all players in a season/series in a single query.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        logger.info(f"🚀 [BULK] Aggregating batting stats for {year} {series}...")

        query = (
            session.query(
                GameBattingStat.player_id,
                func.max(GameBattingStat.player_name).label("player_name"),
                func.max(GameBattingStat.team_code).label("team_code"),
                func.count(GameBattingStat.id).label("games"),
                func.sum(GameBattingStat.plate_appearances).label("plate_appearances"),
                func.sum(GameBattingStat.at_bats).label("at_bats"),
                func.sum(GameBattingStat.runs).label("runs"),
                func.sum(GameBattingStat.hits).label("hits"),
                func.sum(GameBattingStat.doubles).label("doubles"),
                func.sum(GameBattingStat.triples).label("triples"),
                func.sum(GameBattingStat.home_runs).label("home_runs"),
                func.sum(GameBattingStat.rbi).label("rbi"),
                func.sum(GameBattingStat.walks).label("walks"),
                func.sum(GameBattingStat.intentional_walks).label("intentional_walks"),
                func.sum(GameBattingStat.hbp).label("hbp"),
                func.sum(GameBattingStat.strikeouts).label("strikeouts"),
                func.sum(GameBattingStat.stolen_bases).label("stolen_bases"),
                func.sum(GameBattingStat.caught_stealing).label("caught_stealing"),
                func.sum(GameBattingStat.sacrifice_hits).label("sacrifice_hits"),
                func.sum(GameBattingStat.sacrifice_flies).label("sacrifice_flies"),
                func.sum(GameBattingStat.gdp).label("gdp"),
            )
            .join(Game, GameBattingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GameBattingStat.player_id is not None)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .group_by(GameBattingStat.player_id)
        )

        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
            ratios = BattingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            data.update({"season": year, "league": series.upper(), "source": source})
            results.append(data)

        return results

    @staticmethod
    def aggregate_pitching_season(
        session: Session, player_id: int, year: int, series: str, source: str = "FALLBACK"
    ) -> dict[str, Any] | None:
        pattern = SeasonStatAggregator._get_league_name_pattern(series)

        query = (
            session.query(
                func.count(GamePitchingStat.id).label("games"),
                func.sum(func.cast(GamePitchingStat.is_starting, Integer)).label("games_started"),
                func.sum(case((GamePitchingStat.decision == "W", 1), else_=0)).label("wins"),
                func.sum(case((GamePitchingStat.decision == "L", 1), else_=0)).label("losses"),
                func.sum(case((GamePitchingStat.decision == "S", 1), else_=0)).label("saves"),
                func.sum(case((GamePitchingStat.decision == "H", 1), else_=0)).label("holds"),
                func.sum(GamePitchingStat.innings_outs).label("innings_outs"),
                func.sum(GamePitchingStat.hits_allowed).label("hits_allowed"),
                func.sum(GamePitchingStat.runs_allowed).label("runs_allowed"),
                func.sum(GamePitchingStat.earned_runs).label("earned_runs"),
                func.sum(GamePitchingStat.home_runs_allowed).label("home_runs_allowed"),
                func.sum(GamePitchingStat.walks_allowed).label("walks_allowed"),
                func.sum(GamePitchingStat.hit_batters).label("hit_batters"),
                func.sum(GamePitchingStat.strikeouts).label("strikeouts"),
                func.sum(GamePitchingStat.wild_pitches).label("wild_pitches"),
                func.sum(GamePitchingStat.balks).label("balks"),
                func.sum(GamePitchingStat.batters_faced).label("tbf"),
                func.sum(GamePitchingStat.pitches).label("np"),
            )
            .join(Game, GamePitchingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GamePitchingStat.player_id == player_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
        )

        row = query.one_or_none()
        if not row or row.games == 0:
            return None

        data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}

        # Calculate derived fields
        data["innings_pitched"] = round(data["innings_outs"] / 3.0, 1)  # Simple representation

        # Calculate ratios
        ratios = PitchingStatCalculator.calculate_ratios(data)
        data.update(ratios)

        # Metadata
        data.update({"player_id": player_id, "season": year, "league": series.upper(), "source": source})

        return data

    @staticmethod
    def aggregate_pitching_season_bulk(
        session: Session, year: int, series: str, source: str = "FALLBACK"
    ) -> list[dict[str, Any]]:
        """
        Aggregate pitching stats for all players in a season/series in a single query.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        logger.info(f"🚀 [BULK] Aggregating pitching stats for {year} {series}...")

        query = (
            session.query(
                GamePitchingStat.player_id,
                func.max(GamePitchingStat.player_name).label("player_name"),
                func.max(GamePitchingStat.team_code).label("team_code"),
                func.count(GamePitchingStat.id).label("games"),
                func.sum(func.cast(GamePitchingStat.is_starting, Integer)).label("games_started"),
                func.sum(case((GamePitchingStat.decision == "W", 1), else_=0)).label("wins"),
                func.sum(case((GamePitchingStat.decision == "L", 1), else_=0)).label("losses"),
                func.sum(case((GamePitchingStat.decision == "S", 1), else_=0)).label("saves"),
                func.sum(case((GamePitchingStat.decision == "H", 1), else_=0)).label("holds"),
                func.sum(GamePitchingStat.innings_outs).label("innings_outs"),
                func.sum(GamePitchingStat.hits_allowed).label("hits_allowed"),
                func.sum(GamePitchingStat.runs_allowed).label("runs_allowed"),
                func.sum(GamePitchingStat.earned_runs).label("earned_runs"),
                func.sum(GamePitchingStat.home_runs_allowed).label("home_runs_allowed"),
                func.sum(GamePitchingStat.walks_allowed).label("walks_allowed"),
                func.sum(GamePitchingStat.hit_batters).label("hit_batters"),
                func.sum(GamePitchingStat.strikeouts).label("strikeouts"),
                func.sum(GamePitchingStat.wild_pitches).label("wild_pitches"),
                func.sum(GamePitchingStat.balks).label("balks"),
                func.sum(GamePitchingStat.batters_faced).label("tbf"),
                func.sum(GamePitchingStat.pitches).label("np"),
            )
            .join(Game, GamePitchingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GamePitchingStat.player_id is not None)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .group_by(GamePitchingStat.player_id)
        )

        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
            data["innings_pitched"] = round(data["innings_outs"] / 3.0, 1)
            ratios = PitchingStatCalculator.calculate_ratios(data)
            data.update(ratios)
            data.update({"season": year, "league": series.upper(), "source": source})
            results.append(data)

        return results

    @staticmethod
    def aggregate_baserunning_season(
        session: Session, player_id: int, year: int, series: str, source: str = "FALLBACK"
    ) -> dict[str, Any] | None:
        """
        Aggregate cumulative baserunning stats from game batting stats.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)

        query = (
            session.query(
                func.count(GameBattingStat.id).label("games"),
                func.sum(GameBattingStat.stolen_bases).label("stolen_bases"),
                func.sum(GameBattingStat.caught_stealing).label("caught_stealing"),
            )
            .join(Game, GameBattingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GameBattingStat.player_id == player_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
        )

        row = query.one_or_none()
        if not row or row.games == 0:
            return None

        data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
        data["stolen_base_attempts"] = data["stolen_bases"] + data["caught_stealing"]
        if data["stolen_base_attempts"] > 0:
            data["stolen_base_percentage"] = round(data["stolen_bases"] / data["stolen_base_attempts"] * 100, 1)
        else:
            data["stolen_base_percentage"] = 0.0

        data.update({"player_id": player_id, "year": year, "league": series.upper(), "source": source})
        return data

    @staticmethod
    def aggregate_baserunning_season_bulk(
        session: Session, year: int, series: str, source: str = "FALLBACK"
    ) -> list[dict[str, Any]]:
        """
        Aggregate baserunning stats for all players in bulk.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        logger.info(f"🚀 [BULK] Aggregating baserunning stats for {year} {series}...")

        query = (
            session.query(
                GameBattingStat.player_id,
                func.count(GameBattingStat.id).label("games"),
                func.sum(GameBattingStat.stolen_bases).label("stolen_bases"),
                func.sum(GameBattingStat.caught_stealing).label("caught_stealing"),
            )
            .join(Game, GameBattingStat.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GameBattingStat.player_id is not None)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .group_by(GameBattingStat.player_id)
        )

        results = []
        for row in query.all():
            data = {k: (v if v is not None else 0) for k, v in row._asdict().items()}
            data["stolen_base_attempts"] = data["stolen_bases"] + data["caught_stealing"]
            if data["stolen_base_attempts"] > 0:
                data["stolen_base_percentage"] = round(data["stolen_bases"] / data["stolen_base_attempts"] * 100, 1)
            else:
                data["stolen_base_percentage"] = 0.0

            data.update({"year": year, "league": series.upper(), "source": source})
            results.append(data)
        return results

    @staticmethod
    def aggregate_fielding_season_bulk(
        session: Session, year: int, series: str, source: str = "FALLBACK"
    ) -> list[dict[str, Any]]:
        """
        Aggregate fielding stats for all players and positions in bulk.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        logger.info(f"🚀 [BULK] Aggregating fielding stats for {year} {series}...")

        # 1. Get all player-position-game counts
        pos_counts_query = (
            session.query(GameLineup.player_id, GameLineup.standard_position, func.count(GameLineup.id).label("games"))
            .join(Game, GameLineup.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GameLineup.player_id is not None)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .group_by(GameLineup.player_id, GameLineup.standard_position)
        )

        counts = pos_counts_query.all()

        # 2. Extract error events for the whole season
        error_events = (
            session.query(GameEvent.game_id, GameEvent.description)
            .join(Game, GameEvent.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .filter(GameEvent.description.like("%실책%"))
            .all()
        )

        players = session.query(PlayerBasic.player_id, PlayerBasic.name).all()
        pid_to_name = {p.player_id: p.name for p in players}

        error_map = {}
        for event_game_id, desc in error_events:
            game_lineups = (
                session.query(GameLineup.player_id, GameLineup.standard_position)
                .filter(GameLineup.game_id == event_game_id)
                .all()
            )

            for pid, pos in game_lineups:
                if not pid or not pos:
                    continue
                name = pid_to_name.get(pid, "")
                if (name and name in desc) or (pos and pos in desc):
                    key = (pid, pos)
                    error_map[key] = error_map.get(key, 0) + 1
                    break

        results = []
        for pid, pos, game_count in counts:
            results.append(
                {
                    "player_id": pid,
                    "year": year,
                    "league": series.upper(),
                    "position_id": pos,
                    "games": game_count,
                    "errors": error_map.get((pid, pos), 0),
                    "source": source,
                }
            )

        return results

    @staticmethod
    def aggregate_fielding_season(
        session: Session, player_id: int, year: int, series: str, source: str = "FALLBACK"
    ) -> list[dict[str, Any]]:
        """
        Aggregate fielding stats (primarily errors) by parsing GameEvents for a single player.
        Returns a list of dicts, one per position played.
        """
        pattern = SeasonStatAggregator._get_league_name_pattern(series)
        player = session.query(PlayerBasic).filter_by(player_id=player_id).first()
        if not player:
            return []

        positions_query = (
            session.query(GameLineup.standard_position, func.count(GameLineup.id).label("games"))
            .join(Game, GameLineup.game_id == Game.game_id)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(GameLineup.player_id == player_id)
            .filter(KboSeason.season_year == year)
            .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
            .group_by(GameLineup.standard_position)
        )

        pos_stats = []
        for pos_row in positions_query.all():
            pos = pos_row.standard_position
            if not pos:
                continue

            error_count = (
                session.query(func.count(GameEvent.id))
                .join(Game, GameEvent.game_id == Game.game_id)
                .join(KboSeason, Game.season_id == KboSeason.season_id)
                .filter(KboSeason.season_year == year)
                .filter(KboSeason.league_type_name.like(f"%{pattern}%"))
                .filter(
                    GameEvent.game_id.in_(
                        session.query(GameLineup.game_id)
                        .filter(GameLineup.player_id == player_id)
                        .filter(GameLineup.standard_position == pos)
                    )
                )
                .filter(GameEvent.description.like("%실책%"))
                .filter(or_(GameEvent.description.like(f"%{player.name}%"), GameEvent.description.like(f"%{pos}%")))
                .scalar()
                or 0
            )

            pos_stats.append(
                {
                    "player_id": player_id,
                    "year": year,
                    "league": series.upper(),
                    "position_id": pos,
                    "games": pos_row.games,
                    "errors": error_count,
                    "source": source,
                }
            )

        return pos_stats
