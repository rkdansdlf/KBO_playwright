"""
KBO 순위 자동 연산 엔진 (Standings Calculator)
크롤링된 경기 결과(Game) 데이터를 시간순으로 읽어들이며, 매일매일의 
승률, 승차(Games Behind), 연승/연패(Streak) 등을 계산해 DB 물리 테이블에 저장합니다.
"""

import argparse
from datetime import datetime
from collections import defaultdict
from sqlalchemy import extract

from src.db.engine import SessionLocal
from src.models.game import Game
from src.models.season import KboSeason
from src.models.standings import TeamStandingsDaily
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

def calculate_games_behind(target_wins, target_losses, leader_wins, leader_losses):
    """승차 계산 공식: {(1위 승수 - 내 승수) + (내 패수 - 1위 패수)} / 2.0"""
    return ((leader_wins - target_wins) + (target_losses - leader_losses)) / 2.0

class StandingsCalculator:
    def __init__(self, session):
        self.session = session
        
    def calculate_year(self, year: int):
        games = self.session.query(Game).join(
            KboSeason, Game.season_id == KboSeason.season_id
        ).filter(
            KboSeason.season_year == year,
            KboSeason.league_type_name.in_(["정규시즌", "Regular Season"]),
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES))
        ).order_by(Game.game_date, Game.game_id).all()
        
        if not games:
            print(f"[Standings] {year} 시즌에 완료된 정규시즌 경기가 없습니다.")
            return
            
        print(f"[Standings] 📊 {year}년 정규시즌 총 {len(games)}경기 로드 완료. 순위 연산 시작...")
        
        class TeamState:
            def __init__(self, team_code):
                self.team_code = team_code
                self.wins = 0
                self.losses = 0
                self.draws = 0
                self.runs_scored = 0
                self.runs_allowed = 0
                self.current_streak = 0
                
            @property
            def games_played(self):
                return self.wins + self.losses + self.draws
                
            @property
            def win_pct(self):
                total = self.wins + self.losses
                return self.wins / total if total > 0 else 0.0

            def add_game(self, is_win, is_loss, is_draw, runs_for, runs_against):
                self.runs_scored += runs_for
                self.runs_allowed += runs_against
                
                if is_win:
                    self.wins += 1
                    self.current_streak = self.current_streak + 1 if self.current_streak > 0 else 1
                elif is_loss:
                    self.losses += 1
                    self.current_streak = self.current_streak - 1 if self.current_streak < 0 else -1
                elif is_draw:
                    self.draws += 1
                    # KBO 룰: 무승부는 연승/연패를 끊지 않음 (그대로 유지)
                
        games_by_date = defaultdict(list)
        for g in games:
            games_by_date[g.game_date].append(g)
            
        dates = sorted(list(games_by_date.keys()))
        teams = {}
        daily_snapshots = []
        
        for d in dates:
            day_games = games_by_date[d]
            
            # 당일 경기 결과 반영
            for g in day_games:
                home = g.home_team
                away = g.away_team
                h_score = g.home_score if g.home_score is not None else 0
                a_score = g.away_score if g.away_score is not None else 0
                
                if home not in teams: teams[home] = TeamState(home)
                if away not in teams: teams[away] = TeamState(away)
                
                if h_score > a_score:
                    teams[home].add_game(True, False, False, h_score, a_score)
                    teams[away].add_game(False, True, False, a_score, h_score)
                elif a_score > h_score:
                    teams[home].add_game(False, True, False, h_score, a_score)
                    teams[away].add_game(True, False, False, a_score, h_score)
                else:
                    teams[home].add_game(False, False, True, h_score, a_score)
                    teams[away].add_game(False, False, True, a_score, h_score)
                    
            # 해당일 종료 시점의 순위 산출
            # 승률 > 승수 순서로 정렬 (KBO 기준)
            sorted_teams = sorted(teams.values(), key=lambda t: (t.win_pct, t.wins), reverse=True)
            leader_wins = sorted_teams[0].wins if sorted_teams else 0
            leader_losses = sorted_teams[0].losses if sorted_teams else 0
                
            for t in sorted_teams:
                gb = calculate_games_behind(t.wins, t.losses, leader_wins, leader_losses)
                if gb < 0: gb = 0.0 
                
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
                    run_differential=t.runs_scored - t.runs_allowed
                )
                daily_snapshots.append(snapshot)
                
        print(f"[Standings] 💾 {year}년도 일자별 스냅샷 {len(daily_snapshots)}건을 로컬 DB에 병합합니다...")
        
        # 기존 데이터 폭파 및 재충전 (안전한 Replace 전략)
        self.session.query(TeamStandingsDaily).filter(
            extract('year', TeamStandingsDaily.standings_date) == year
        ).delete(synchronize_session=False)
        
        self.session.bulk_save_objects(daily_snapshots)
        self.session.commit()
        print(f"[Standings] ✅ {year} 시즌 순위표 계산 완료!")

def main():
    parser = argparse.ArgumentParser(description="KBO Standings / Win Percentage Calculator")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="계산할 타겟 년도 지정")
    parser.add_argument("--all", action="store_true", help="수집된 모든 년도 전체 스냅샷 재계산")
    args = parser.parse_args()
    
    session = SessionLocal()
    try:
        calc = StandingsCalculator(session)
        if args.all:
            years = [y[0] for y in session.query(KboSeason.season_year).distinct().all()]
            for y in sorted(years):
                calc.calculate_year(y)
        else:
            calc.calculate_year(args.year)
    except Exception as e:
        print(f"❌ 계산 중 오류 발생: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    main()
