"""Repository for Futures League Schedule and Standings."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import select

from src.constants import KST
from src.models.futures_schedule import FuturesGameSchedule, FuturesTeamStandings

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


class FuturesScheduleRepository:
    """Repository for managing Futures League schedule and standings."""

    def __init__(self, session: Session) -> None:
        """Initialize repository.

        Args:
            session: DB Session.

        """
        self.session = session

    def save_game_schedule(self, data: dict[str, Any]) -> FuturesGameSchedule:
        """Save or update a futures game schedule record.

        Args:
            data: Game schedule dictionary.

        Returns:
            Saved ORM instance.

        """
        game_id = data["game_id"]
        stmt = select(FuturesGameSchedule).where(FuturesGameSchedule.game_id == game_id)
        existing = self.session.execute(stmt).scalar_one_or_none()

        g_date = data["game_date"]
        if isinstance(g_date, str):
            try:
                g_date = datetime.strptime(g_date, "%Y-%m-%d").replace(tzinfo=KST).date()
            except ValueError:
                g_date = datetime.now(KST).date()

        if existing:
            existing.stadium = data.get("stadium", existing.stadium)
            existing.away_score = data.get("away_score", existing.away_score)
            existing.home_score = data.get("home_score", existing.home_score)
            existing.game_status = data.get("game_status", existing.game_status)
            return existing

        record = FuturesGameSchedule(
            season=data["season"],
            game_date=g_date,
            game_id=game_id,
            away_team=data["away_team"],
            home_team=data["home_team"],
            stadium=data.get("stadium"),
            away_score=data.get("away_score"),
            home_score=data.get("home_score"),
            game_status=data.get("game_status", "SCHEDULED"),
            cancel_reason=data.get("cancel_reason"),
        )
        self.session.add(record)
        self.session.flush()
        return record

    def save_team_standings(self, data: dict[str, Any]) -> FuturesTeamStandings:
        """Save or update a futures team standing record.

        Args:
            data: Team standing dictionary.

        Returns:
            Saved ORM instance.

        """
        season = data["season"]
        division = data["division"]
        team_code = data["team_code"]

        stmt = select(FuturesTeamStandings).where(
            FuturesTeamStandings.season == season,
            FuturesTeamStandings.division == division,
            FuturesTeamStandings.team_code == team_code,
        )
        existing = self.session.execute(stmt).scalar_one_or_none()

        if existing:
            existing.games_played = data.get("games_played", existing.games_played)
            existing.wins = data.get("wins", existing.wins)
            existing.losses = data.get("losses", existing.losses)
            existing.draws = data.get("draws", existing.draws)
            existing.win_pct = data.get("win_pct", existing.win_pct)
            existing.rank = data.get("rank", existing.rank)
            return existing

        record = FuturesTeamStandings(
            season=season,
            division=division,
            team_code=team_code,
            games_played=data.get("games_played", 0),
            wins=data.get("wins", 0),
            losses=data.get("losses", 0),
            draws=data.get("draws", 0),
            win_pct=data.get("win_pct", 0.0),
            games_behind=data.get("games_behind", 0.0),
            rank=data.get("rank", 0),
        )
        self.session.add(record)
        self.session.flush()
        return record
