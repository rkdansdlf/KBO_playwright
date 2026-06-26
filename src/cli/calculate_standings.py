"""
KBO 순위 자동 연산 엔진 (Standings Calculator)
경기 결과 데이터로부터 일일 순위, 5강, 최근10경기, 홈/원정, 주차별 승률추이 계산.
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from sqlalchemy import extract
from sqlalchemy.exc import SQLAlchemyError

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game
from src.models.season import KboSeason
from src.models.standings import TeamStandingsDaily
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

STANDINGS_CALC_EXCEPTIONS = (SQLAlchemyError, RuntimeError, ValueError, TypeError, KeyError, ZeroDivisionError)


def calculate_games_behind(wins: int, losses: int, leader_wins: int, leader_losses: int) -> float:
    """
    Calculates games behind.

    Args:
        wins: Wins.
        losses: Losses.
        leader_wins: Leader Wins.
        leader_losses: Leader Losses.

    Returns:
        float instance.

    """
    return ((leader_wins - wins) + (losses - leader_losses)) / 2.0


def iso_week_number(d: date) -> str:
    """
    Handles the iso week number operation.

    Args:
        d: D.

    Returns:
        String result.

    """
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


@dataclass(frozen=True)
class GameResultData:
    """GameResultData class."""

    is_win: bool
    is_loss: bool
    is_draw: bool
    runs_for: int
    runs_against: int
    is_home: bool
    game_date: date


class TeamState:
    """TeamState class."""

    def __init__(self, team_code: str) -> None:
        """Initializes a new instance."""
        self.team_code = team_code
        self.wins = 0
        self.losses = 0
        self.draws = 0
        self.runs_scored = 0
        self.runs_allowed = 0
        self.current_streak = 0
        self.home_wins = 0
        self.home_losses = 0
        self.away_wins = 0
        self.away_losses = 0
        self.recent_games: deque[str] = deque(maxlen=10)
        self.weekly_wins: dict[str, int] = defaultdict(int)
        self.weekly_losses: dict[str, int] = defaultdict(int)

    @property
    def games_played(self) -> int:
        """
        Handles the games played operation.

        Returns:
            Integer result.

        """
        return self.wins + self.losses + self.draws

    @property
    def win_pct(self) -> float:
        """
        Handles the win pct operation.

        Returns:
            float instance.

        """
        total = self.wins + self.losses
        return self.wins / total if total > 0 else 0.0

    @property
    def recent_10_wins(self) -> int:
        """
        Handles the recent 10 wins operation.

        Returns:
            Integer result.

        """
        return sum(1 for r in self.recent_games if r == "W")

    @property
    def recent_10_losses(self) -> int:
        """
        Handles the recent 10 losses operation.

        Returns:
            Integer result.

        """
        return sum(1 for r in self.recent_games if r == "L")

    @property
    def recent_10_draws(self) -> int:
        """
        Handles the recent 10 draws operation.

        Returns:
            Integer result.

        """
        return sum(1 for r in self.recent_games if r == "D")

    def add_game(self, result: GameResultData) -> None:
        """
        Adds game.

        Args:
            result: Result.

        """
        self.runs_scored += result.runs_for
        self.runs_allowed += result.runs_against

        if result.is_win:
            self._add_win(is_home=result.is_home)
        elif result.is_loss:
            self._add_loss(is_home=result.is_home)
        elif result.is_draw:
            self.draws += 1
            self.recent_games.append("D")

        week_key = iso_week_number(result.game_date)
        if result.is_win:
            self.weekly_wins[week_key] += 1
        elif result.is_loss:
            self.weekly_losses[week_key] += 1

    def _add_win(self, *, is_home: bool) -> None:
        self.wins += 1
        self.current_streak = self.current_streak + 1 if self.current_streak > 0 else 1
        self.recent_games.append("W")
        if is_home:
            self.home_wins += 1
        else:
            self.away_wins += 1

    def _add_loss(self, *, is_home: bool) -> None:
        self.losses += 1
        self.current_streak = self.current_streak - 1 if self.current_streak < 0 else -1
        self.recent_games.append("L")
        if is_home:
            self.home_losses += 1
        else:
            self.away_losses += 1


def _group_games_by_date(games: list[Game]) -> dict[date, list[Game]]:
    games_by_date = defaultdict(list)
    for game in games:
        games_by_date[game.game_date].append(game)
    return games_by_date


def _team_state_for(teams: dict[str, TeamState], team_code: str) -> TeamState:
    if team_code not in teams:
        teams[team_code] = TeamState(team_code)
    return teams[team_code]


def _apply_game_to_standings(game: Game, teams: dict[str, TeamState], game_date: date) -> None:
    home = game.home_team
    away = game.away_team
    home_score = game.home_score if game.home_score is not None else 0
    away_score = game.away_score if game.away_score is not None else 0

    home_state = _team_state_for(teams, home)
    away_state = _team_state_for(teams, away)

    if home_score > away_score:
        home_state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=home_score,
                runs_against=away_score,
                is_home=True,
                game_date=game_date,
            ),
        )
        away_state.add_game(
            GameResultData(
                is_win=False,
                is_loss=True,
                is_draw=False,
                runs_for=away_score,
                runs_against=home_score,
                is_home=False,
                game_date=game_date,
            ),
        )
    elif away_score > home_score:
        home_state.add_game(
            GameResultData(
                is_win=False,
                is_loss=True,
                is_draw=False,
                runs_for=home_score,
                runs_against=away_score,
                is_home=True,
                game_date=game_date,
            ),
        )
        away_state.add_game(
            GameResultData(
                is_win=True,
                is_loss=False,
                is_draw=False,
                runs_for=away_score,
                runs_against=home_score,
                is_home=False,
                game_date=game_date,
            ),
        )
    else:
        home_state.add_game(
            GameResultData(
                is_win=False,
                is_loss=False,
                is_draw=True,
                runs_for=home_score,
                runs_against=away_score,
                is_home=True,
                game_date=game_date,
            ),
        )
        away_state.add_game(
            GameResultData(
                is_win=False,
                is_loss=False,
                is_draw=True,
                runs_for=away_score,
                runs_against=home_score,
                is_home=False,
                game_date=game_date,
            ),
        )


def _weekly_win_pcts(team: TeamState) -> dict[str, float | None] | None:
    weekly_pcts = {}
    all_weeks = sorted(set(team.weekly_wins.keys()) | set(team.weekly_losses.keys()))
    for week in all_weeks:
        wins = team.weekly_wins.get(week, 0)
        losses = team.weekly_losses.get(week, 0)
        total = wins + losses
        weekly_pcts[week] = round(wins / total, 3) if total > 0 else None
    return weekly_pcts or None


def _build_snapshot(
    standings_date: date,
    team: TeamState,
    rank_idx: int,
    leader_wins: int,
    leader_losses: int,
) -> TeamStandingsDaily:
    games_behind = calculate_games_behind(team.wins, team.losses, leader_wins, leader_losses)
    if games_behind < 0:
        games_behind = 0.0

    return TeamStandingsDaily(
        standings_date=standings_date,
        team_code=team.team_code,
        games_played=team.games_played,
        wins=team.wins,
        losses=team.losses,
        draws=team.draws,
        win_pct=team.win_pct,
        games_behind=games_behind,
        current_streak=team.current_streak,
        runs_scored=team.runs_scored,
        runs_allowed=team.runs_allowed,
        run_differential=team.runs_scored - team.runs_allowed,
        rank=rank_idx,
        top_5=1 if rank_idx <= 5 else 0,
        recent_10_wins=team.recent_10_wins,
        recent_10_losses=team.recent_10_losses,
        recent_10_draws=team.recent_10_draws,
        weekly_win_pcts=_weekly_win_pcts(team),
        home_wins=team.home_wins,
        home_losses=team.home_losses,
        away_wins=team.away_wins,
        away_losses=team.away_losses,
    )


def _build_daily_snapshots(games: list[Game]) -> list[TeamStandingsDaily]:
    teams: dict[str, TeamState] = {}
    daily_snapshots = []

    for standings_date, day_games in sorted(_group_games_by_date(games).items()):
        for game in day_games:
            _apply_game_to_standings(game, teams, standings_date)

        sorted_teams = sorted(teams.values(), key=lambda t: (t.win_pct, t.wins), reverse=True)
        leader = sorted_teams[0] if sorted_teams else None
        leader_wins = leader.wins if leader else 0
        leader_losses = leader.losses if leader else 0

        for rank_idx, team in enumerate(sorted_teams, start=1):
            daily_snapshots.append(_build_snapshot(standings_date, team, rank_idx, leader_wins, leader_losses))

    return daily_snapshots


class StandingsCalculator:
    """StandingsCalculator class."""

    def __init__(self, session: Session) -> None:
        """Initializes a new instance."""
        self.session = session

    def _load_completed_games(self, year: int) -> list[Game]:
        return (
            self.session.query(Game)
            .join(KboSeason, Game.season_id == KboSeason.season_id)
            .filter(
                KboSeason.season_year == year,
                KboSeason.league_type_name.in_(["정규시즌", "Regular Season"]),
                Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
            )
            .order_by(Game.game_date, Game.game_id)
            .all()
        )

    def _save_snapshots(self, year: int, daily_snapshots: list[TeamStandingsDaily]) -> None:
        logger.info("[Standings] %s건 스냅샷 DB 저장 중...", len(daily_snapshots))
        self.session.query(TeamStandingsDaily).filter(
            extract("year", TeamStandingsDaily.standings_date) == year,
        ).delete(synchronize_session=False)
        self.session.bulk_save_objects(daily_snapshots)
        self.session.commit()

    def calculate_year(self, year: int) -> None:
        """
        Calculates year.

        Args:
            year: Season year.

        """
        games = self._load_completed_games(year)

        if not games:
            logger.info("[Standings] %s 시즌 완료 경기 없음.", year)
            return

        logger.info("[Standings] %s년 %s경기 로드. 순위 연산 시작...", year, len(games))
        self._save_snapshots(year, _build_daily_snapshots(games))
        logger.info("[Standings] %s 시즌 순위표 계산 완료!", year)

    def print_report(self, year: int, target_date: date | None = None) -> None:
        """
        Prints print report.

        Args:
            year: Season year.
            target_date: Target Date.

        """
        query = self.session.query(TeamStandingsDaily).filter(
            extract("year", TeamStandingsDaily.standings_date) == year,
        )
        if target_date:
            query = query.filter(TeamStandingsDaily.standings_date <= target_date)

        latest_date = query.order_by(TeamStandingsDaily.standings_date.desc()).first()
        if not latest_date:
            logger.info("[Report] %s년 순위 데이터 없음.", year)
            return

        d = latest_date.standings_date
        rows = (
            self.session.query(TeamStandingsDaily)
            .filter(TeamStandingsDaily.standings_date == d)
            .order_by(TeamStandingsDaily.rank)
            .all()
        )

        logger.info("\n%s", "=" * 70)
        logger.info("  KBO %s년 순위표 (기준: %s)", year, d)
        logger.info("%s", "=" * 70)
        logger.info(
            "%-4s %-6s %4s %4s %3s %7s %5s %8s %4s %8s %8s",
            "순위",
            "팀",
            "승",
            "패",
            "무",
            "승률",
            "승차",
            "최근10",
            "연속",
            "홈",
            "원정",
        )
        logger.info("%s", "-" * 70)

        top_5_rows = [r for r in rows if r.top_5]
        bottom_5_rows = [r for r in rows if not r.top_5]

        for r in top_5_rows:
            recent = f"{r.recent_10_wins}승{r.recent_10_losses}패"
            streak_s = f"{abs(r.current_streak)}{'연승' if r.current_streak >= 0 else '연패'}"
            home_s = f"{r.home_wins}승{r.home_losses}패"
            away_s = f"{r.away_wins}승{r.away_losses}패"
            logger.info(
                "  ★%2d  %-6s %4d %4d %3d  %.3f  %4.1f  %8s %4s %8s %8s",
                r.rank,
                r.team_code,
                r.wins,
                r.losses,
                r.draws,
                r.win_pct,
                r.games_behind,
                recent,
                streak_s,
                home_s,
                away_s,
            )

        if bottom_5_rows:
            logger.info("  %s", "─" * 68)
            for r in bottom_5_rows:
                recent = f"{r.recent_10_wins}승{r.recent_10_losses}패"
                streak_s = f"{abs(r.current_streak)}{'연승' if r.current_streak >= 0 else '연패'}"
                home_s = f"{r.home_wins}승{r.home_losses}패"
                away_s = f"{r.away_wins}승{r.away_losses}패"
                logger.info(
                    "    %2d  %-6s %4d %4d %3d  %.3f  %4.1f  %8s %4s %8s %8s",
                    r.rank,
                    r.team_code,
                    r.wins,
                    r.losses,
                    r.draws,
                    r.win_pct,
                    r.games_behind,
                    recent,
                    streak_s,
                    home_s,
                    away_s,
                )

        logger.info("%s", "=" * 70)
        logger.info("  ★ 상위 5팀 (5강)" if top_5_rows else "")

    def print_trend(self, year: int, team_code: str | None = None) -> None:
        """
        Prints trend.

        Args:
            year: Season year.
            team_code: Team Code.

        """
        rows = (
            self.session.query(TeamStandingsDaily)
            .filter(extract("year", TeamStandingsDaily.standings_date) == year)
            .order_by(TeamStandingsDaily.standings_date)
            .all()
        )

        team_rows = defaultdict(list)
        for r in rows:
            team_rows[r.team_code].append(r)

        teams_to_show = [team_code] if team_code else sorted(team_rows.keys())

        for tc in teams_to_show:
            if tc not in team_rows:
                continue
            data = team_rows[tc]
            logger.info("\n[%s] 승률 추이 (%s)", tc, year)
            logger.info("%-12s %3s %3s %7s %4s %8s", "날짜", "승", "패", "승률", "순위", "최근10")
            logger.info("%s", "-" * 45)
            step = max(1, len(data) // 15)
            for r in data[::step]:
                recent = f"{r.recent_10_wins}승{r.recent_10_losses}패"
                logger.info(
                    "  %s  %3s %3s  %.3f  %3s위  %8s",
                    r.standings_date,
                    r.wins,
                    r.losses,
                    r.win_pct,
                    r.rank,
                    recent,
                )
            if data:
                last = data[-1]
                recent = f"{last.recent_10_wins}승{last.recent_10_losses}패"
                logger.info(
                    "  %s  %3d %3d  %.3f  %d위  %s",
                    last.standings_date,
                    last.wins,
                    last.losses,
                    last.win_pct,
                    last.rank,
                    recent,
                )


def main() -> int:
    """Main entry point for this CLI command."""
    parser = argparse.ArgumentParser(description="KBO Standings Calculator")
    parser.add_argument("--year", type=int, default=datetime.now(KST).year, help="대상 년도")
    parser.add_argument("--all", action="store_true", help="전체 년도 재계산")
    parser.add_argument("--report", action="store_true", help="순위 리포트 출력")
    parser.add_argument("--trend", type=str, nargs="?", const="__all__", help="승률추이 (팀코드 지정 또는 전체)")
    parser.add_argument("--date", type=str, help="리포트 기준일 (YYYY-MM-DD)")

    args = parser.parse_args()

    session = SessionLocal()
    try:
        calc = StandingsCalculator(session)

        if args.report:
            target_date = date.fromisoformat(args.date) if args.date else None
            calc.print_report(args.year, target_date)
        elif args.trend:
            team_code = None if args.trend == "__all__" else args.trend
            calc.print_trend(args.year, team_code)
        elif args.all:
            years = [y[0] for y in session.query(KboSeason.season_year).distinct().all()]
            for y in sorted(years):
                calc.calculate_year(y)
        else:
            calc.calculate_year(args.year)

    except STANDINGS_CALC_EXCEPTIONS:
        logger.exception("오류 발생")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    main()
