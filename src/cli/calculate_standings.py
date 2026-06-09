"""
KBO 순위 자동 연산 엔진 (Standings Calculator)
경기 결과 데이터로부터 일일 순위, 5강, 최근10경기, 홈/원정, 주차별 승률추이 계산.
"""

from __future__ import annotations

import argparse
import logging
from collections import defaultdict, deque
from datetime import date, datetime

from sqlalchemy import extract

from src.db.engine import SessionLocal
from src.models.game import Game
from src.models.season import KboSeason
from src.models.standings import TeamStandingsDaily
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

logger = logging.getLogger(__name__)


def calculate_games_behind(wins: int, losses: int, leader_wins: int, leader_losses: int) -> float:
    return ((leader_wins - wins) + (losses - leader_losses)) / 2.0


def iso_week_number(d: date) -> str:
    iso = d.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


class StandingsCalculator:
    def __init__(self, session) -> None:
        self.session = session

    def calculate_year(self, year: int) -> None:
        games = (
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

        if not games:
            logger.info(f"[Standings] {year} 시즌 완료 경기 없음.")
            return

        logger.info(f"[Standings] {year}년 {len(games)}경기 로드. 순위 연산 시작...")

        class TeamState:
            def __init__(self, team_code: str) -> None:
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
                self.recent_games: deque = deque(maxlen=10)
                self.weekly_wins: dict[str, int] = defaultdict(int)
                self.weekly_losses: dict[str, int] = defaultdict(int)

            @property
            def games_played(self) -> int:
                return self.wins + self.losses + self.draws

            @property
            def win_pct(self) -> float:
                total = self.wins + self.losses
                return self.wins / total if total > 0 else 0.0

            @property
            def recent_10_wins(self) -> int:
                return sum(1 for r in self.recent_games if r == "W")

            @property
            def recent_10_losses(self) -> int:
                return sum(1 for r in self.recent_games if r == "L")

            @property
            def recent_10_draws(self) -> int:
                return sum(1 for r in self.recent_games if r == "D")

            def add_game(self, is_win: bool, is_loss: bool, is_draw: bool, runs_for: int, runs_against: int, is_home: bool, game_date: date) -> None:
                self.runs_scored += runs_for
                self.runs_allowed += runs_against

                if is_win:
                    self.wins += 1
                    self.current_streak = self.current_streak + 1 if self.current_streak > 0 else 1
                    self.recent_games.append("W")
                    if is_home:
                        self.home_wins += 1
                    else:
                        self.away_wins += 1
                elif is_loss:
                    self.losses += 1
                    self.current_streak = self.current_streak - 1 if self.current_streak < 0 else -1
                    self.recent_games.append("L")
                    if is_home:
                        self.home_losses += 1
                    else:
                        self.away_losses += 1
                elif is_draw:
                    self.draws += 1
                    self.recent_games.append("D")

                week_key = iso_week_number(game_date)
                if is_win:
                    self.weekly_wins[week_key] += 1
                elif is_loss:
                    self.weekly_losses[week_key] += 1

        games_by_date = defaultdict(list)
        for g in games:
            games_by_date[g.game_date].append(g)

        dates = sorted(games_by_date.keys())
        teams: dict[str, TeamState] = {}
        daily_snapshots = []

        for d in dates:
            day_games = games_by_date[d]

            for g in day_games:
                home = g.home_team
                away = g.away_team
                h_score = g.home_score if g.home_score is not None else 0
                a_score = g.away_score if g.away_score is not None else 0

                if home not in teams:
                    teams[home] = TeamState(home)
                if away not in teams:
                    teams[away] = TeamState(away)

                if h_score > a_score:
                    teams[home].add_game(True, False, False, h_score, a_score, True, d)
                    teams[away].add_game(False, True, False, a_score, h_score, False, d)
                elif a_score > h_score:
                    teams[home].add_game(False, True, False, h_score, a_score, True, d)
                    teams[away].add_game(True, False, False, a_score, h_score, False, d)
                else:
                    teams[home].add_game(False, False, True, h_score, a_score, True, d)
                    teams[away].add_game(False, False, True, a_score, h_score, False, d)

            sorted_teams = sorted(teams.values(), key=lambda t: (t.win_pct, t.wins), reverse=True)
            leader = sorted_teams[0] if sorted_teams else None
            leader_wins = leader.wins if leader else 0
            leader_losses = leader.losses if leader else 0

            for rank_idx, t in enumerate(sorted_teams, start=1):
                gb = calculate_games_behind(t.wins, t.losses, leader_wins, leader_losses)
                if gb < 0:
                    gb = 0.0

                weekly_pcts = {}
                all_weeks = sorted(set(t.weekly_wins.keys()) | set(t.weekly_losses.keys()))
                for wk in all_weeks:
                    wk_w = t.weekly_wins.get(wk, 0)
                    wk_l = t.weekly_losses.get(wk, 0)
                    total = wk_w + wk_l
                    weekly_pcts[wk] = round(wk_w / total, 3) if total > 0 else None

                snapshot = TeamStandingsDaily(
                    standings_date=d,
                    team_code=t.team_code,
                    games_played=t.games_played,
                    wins=t.wins,
                    losses=t.losses,
                    draws=t.draws,
                    win_pct=t.win_pct,
                    games_behind=gb,
                    current_streak=t.current_streak,
                    runs_scored=t.runs_scored,
                    runs_allowed=t.runs_allowed,
                    run_differential=t.runs_scored - t.runs_allowed,
                    rank=rank_idx,
                    top_5=1 if rank_idx <= 5 else 0,
                    recent_10_wins=t.recent_10_wins,
                    recent_10_losses=t.recent_10_losses,
                    recent_10_draws=t.recent_10_draws,
                    weekly_win_pcts=weekly_pcts if weekly_pcts else None,
                    home_wins=t.home_wins,
                    home_losses=t.home_losses,
                    away_wins=t.away_wins,
                    away_losses=t.away_losses,
                )
                daily_snapshots.append(snapshot)

        logger.info(f"[Standings] {len(daily_snapshots)}건 스냅샷 DB 저장 중...")
        self.session.query(TeamStandingsDaily).filter(
            extract("year", TeamStandingsDaily.standings_date) == year
        ).delete(synchronize_session=False)
        self.session.bulk_save_objects(daily_snapshots)
        self.session.commit()
        logger.info(f"[Standings] {year} 시즌 순위표 계산 완료!")

    def print_report(self, year: int, target_date: date | None = None) -> None:
        query = self.session.query(TeamStandingsDaily).filter(
            extract("year", TeamStandingsDaily.standings_date) == year
        )
        if target_date:
            query = query.filter(TeamStandingsDaily.standings_date <= target_date)

        latest_date = query.order_by(TeamStandingsDaily.standings_date.desc()).first()
        if not latest_date:
            logger.info(f"[Report] {year}년 순위 데이터 없음.")
            return

        d = latest_date.standings_date
        rows = (
            self.session.query(TeamStandingsDaily)
            .filter(TeamStandingsDaily.standings_date == d)
            .order_by(TeamStandingsDaily.rank)
            .all()
        )

        logger.info(f"\n{'=' * 70}")
        logger.info(f"  KBO {year}년 순위표 (기준: {d})")
        logger.info(f"{'=' * 70}")
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

        logger.info(f"{'=' * 70}")
        logger.info("  ★ 상위 5팀 (5강)" if top_5_rows else "")

    def print_trend(self, year: int, team_code: str | None = None) -> None:
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
            logger.info(f"\n[{tc}] 승률 추이 ({year})")
            logger.info(f"{'날짜':<12} {'승':>3} {'패':>3} {'승률':>7} {'순위':>4} {'최근10':>8}")
            logger.info(f"{'-' * 45}")
            step = max(1, len(data) // 15)
            for r in data[::step]:
                recent = f"{r.recent_10_wins}승{r.recent_10_losses}패"
                logger.info(
                    f"  {r.standings_date}  {r.wins:>3} {r.losses:>3}  {r.win_pct:.3f}  {r.rank:>3}위  {recent:>8}"
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
    parser = argparse.ArgumentParser(description="KBO Standings Calculator")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="대상 년도")
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

    except Exception:
        logger.exception("오류 발생")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    main()
