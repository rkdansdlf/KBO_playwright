"""
Context Aggregator Service
Calculates derived metrics (Head-to-head, Streaks, Trends, WPA moments) 
to provide rich context for LLM analysis.
"""
from __future__ import annotations
from typing import List, Dict, Any, Optional
from sqlalchemy import text, func, or_, and_, desc
from datetime import datetime, date

from src.models.game import Game, GameEvent, GameBattingStat, GamePitchingStat
from src.models.player import PlayerSeasonPitching
from src.models.season import KboSeason

class ContextAggregator:
    def __init__(self, session):
        self.session = session

    def get_team_l10_summary(self, team_code: str, target_date: date) -> Dict[str, Any]:
        """최근 10경기 승패 및 연승/연패 흐름 계산"""
        games = self.session.query(Game).filter(
            or_(Game.home_team == team_code, Game.away_team == team_code),
            Game.game_status == 'COMPLETED',
            Game.game_date < target_date
        ).order_by(desc(Game.game_date)).limit(10).all()

        wins, losses, draws = 0, 0, 0
        streak_type = None
        streak_count = 0
        
        results = []
        for g in games:
            is_home = (g.home_team == team_code)
            my_score = g.home_score if is_home else g.away_score
            opp_score = g.away_score if is_home else g.home_score
            
            if my_score > opp_score:
                wins += 1
                results.append('W')
            elif my_score < opp_score:
                losses += 1
                results.append('L')
            else:
                draws += 1
                results.append('D')

        # 연승/연패 계산 (가장 최근 경기부터 역순)
        if results:
            streak_type = results[0]
            for r in results:
                if r == streak_type:
                    streak_count += 1
                else:
                    break
        
        return {
            "team_code": team_code,
            "wins": wins,
            "losses": losses,
            "draws": draws,
            "l10_text": f"{wins}승 {losses}패 {draws}무",
            "streak": f"{streak_count}{'연승' if streak_type == 'W' else '연패' if streak_type == 'L' else '무'}" if streak_type else "-"
        }

    def get_head_to_head_summary(self, team_a: str, team_b: str, season_year: int, target_date: date) -> Dict[str, Any]:
        """올 시즌 두 팀간의 맞대결 전적 계산"""
        games = self.session.query(Game).filter(
            or_(
                and_(Game.home_team == team_a, Game.away_team == team_b),
                and_(Game.home_team == team_b, Game.away_team == team_a)
            ),
            Game.game_status == 'COMPLETED',
            Game.game_date < target_date,
            func.substr(Game.game_id, 1, 4) == str(season_year)
        ).all()

        a_wins, b_wins, draws = 0, 0, 0
        for g in games:
            # team_a 기준 승패
            if g.home_team == team_a:
                if g.home_score > g.away_score: a_wins += 1
                elif g.home_score < g.away_score: b_wins += 1
                else: draws += 1
            else:
                if g.away_score > g.home_score: a_wins += 1
                elif g.away_score < g.home_score: b_wins += 1
                else: draws += 1

        superior = team_a if a_wins > b_wins else team_b if b_wins > a_wins else "동률"
        
        return {
            "matchup": f"{team_a} vs {team_b}",
            "a_wins": a_wins,
            "b_wins": b_wins,
            "draws": draws,
            "summary_text": f"{a_wins}승 {b_wins}패 {draws}무 ({superior} 우세)"
        }

    def get_crucial_moments(self, game_id: str, limit: int = 3) -> List[Dict[str, Any]]:
        """WPA 기반 승부처(하이라이트) 추출"""
        events = self.session.query(GameEvent).filter(
            GameEvent.game_id == game_id,
            GameEvent.wpa.isnot(None)
        ).order_by(desc(func.abs(GameEvent.wpa))).limit(limit).all()

        moments = []
        for e in events:
            moments.append({
                "inning": f"{e.inning}회{'초' if e.inning_half == 'top' else '말'}",
                "description": e.description,
                "wpa": e.wpa,
                "score": f"{e.away_score}:{e.home_score}",
                "batter": e.batter_name,
                "pitcher": e.pitcher_name
            })
        return moments

    def get_team_recent_metrics(self, team_code: str, target_date: date, limit_games: int = 10) -> Dict[str, Any]:
        """최근 N경기 동안의 팀 타격/투구 지표 요약"""
        # 최근 경기들 ID 확보
        game_ids = [r[0] for r in self.session.query(Game.game_id).filter(
            or_(Game.home_team == team_code, Game.away_team == team_code),
            Game.game_status == 'COMPLETED',
            Game.game_date < target_date
        ).order_by(desc(Game.game_date)).limit(limit_games).all()]

        if not game_ids:
            return {}

        # 팀 타율 계산
        batting = self.session.query(
            func.sum(GameBattingStat.hits).label('hits'),
            func.sum(GameBattingStat.at_bats).label('ab')
        ).filter(
            GameBattingStat.game_id.in_(game_ids),
            GameBattingStat.team_code == team_code
        ).first()

        avg = round(batting.hits / batting.ab, 3) if batting and batting.ab else 0

        # 팀 평균자책점(ERA) 계산
        pitching = self.session.query(
            func.sum(GamePitchingStat.earned_runs).label('er'),
            func.sum(GamePitchingStat.innings_outs).label('outs')
        ).filter(
            GamePitchingStat.game_id.in_(game_ids),
            GamePitchingStat.team_code == team_code
        ).first()

        era = round((pitching.er * 27) / pitching.outs, 2) if pitching and pitching.outs else 0

        # 불펜 평균자책점(ERA) 및 이닝당 출루허용률(WHIP) 계산
        bullpen = self.session.query(
            func.sum(GamePitchingStat.earned_runs).label('er'),
            func.sum(GamePitchingStat.hits_allowed).label('hits'),
            func.sum(GamePitchingStat.walks_allowed).label('walks'),
            func.sum(GamePitchingStat.innings_outs).label('outs')
        ).filter(
            GamePitchingStat.game_id.in_(game_ids),
            GamePitchingStat.team_code == team_code,
            GamePitchingStat.is_starting == False
        ).first()

        bp_era = round((bullpen.er * 27) / bullpen.outs, 2) if bullpen and bullpen.outs else 0
        bp_whip = round(((bullpen.hits or 0) + (bullpen.walks or 0)) * 3 / bullpen.outs, 2) if bullpen and bullpen.outs else 0

        return {
            "avg": avg,
            "era": era,
            "bullpen_era": bp_era,
            "bullpen_whip": bp_whip,
            "sample_games": len(game_ids)
        }

    def get_postseason_series_summary(self, team_a: str, team_b: str, season_year: int, target_date: date) -> Optional[Dict[str, Any]]:
        """포스트시즌 시리즈 전적(예: 준플레이오프 1승 2패) 계산"""
        # 현재 경기의 시리즈 유형 파악을 위해 1경기 조회
        sample_game = self.session.query(Game).filter(
            or_(
                and_(Game.home_team == team_a, Game.away_team == team_b),
                and_(Game.home_team == team_b, Game.away_team == team_a)
            ),
            Game.game_date <= target_date,
            func.substr(Game.game_id, 1, 4) == str(season_year)
        ).first()

        if not sample_game or not sample_game.season_id:
            return None

        # season_id가 정규시즌(보통 코드 0)이 아닌 경우만 처리
        # kbo_seasons 테이블에서 league_type_code 확인 필요 (보통 2:와일드카드, 3:준PO, 4:PO, 5:한국시리즈)
        # 여기서는 단순하게 정규시즌 ID가 아닌 경우를 포스트시즌으로 간주하거나 
        # season_id 범위를 통해 필터링 가능 (프로젝트 규칙에 따라)
        
        # OCI DB 기준: 정규시즌은 대개 season_year와 동일하거나 별도 매핑됨. 
        # 여기서는 해당 season_id의 모든 맞대결을 합산.
        games = self.session.query(Game).filter(
            Game.season_id == sample_game.season_id,
            or_(
                and_(Game.home_team == team_a, Game.away_team == team_b),
                and_(Game.home_team == team_b, Game.away_team == team_a)
            ),
            Game.game_status == 'COMPLETED',
            Game.game_date < target_date
        ).all()

        if not games:
            return None

        a_wins, b_wins, draws = 0, 0, 0
        for g in games:
            if g.home_team == team_a:
                if g.home_score > g.away_score: a_wins += 1
                elif g.home_score < g.away_score: b_wins += 1
                else: draws += 1
            else:
                if g.away_score > g.home_score: a_wins += 1
                elif g.away_score < g.home_score: b_wins += 1
                else: draws += 1

        return {
            "season_id": sample_game.season_id,
            "team_a": team_a,
            "team_b": team_b,
            "a_wins": a_wins,
            "b_wins": b_wins,
            "draws": draws,
            "series_text": f"시리즈 성적: {a_wins}승 {b_wins}패 {draws}무"
        }

    def get_pitcher_season_stats(self, player_id: int, season_year: int) -> Optional[Dict[str, Any]]:
        """선발 투수의 해당 시즌 성적 조회"""
        if not player_id:
            return None
            
        stats = self.session.query(PlayerSeasonPitching).filter(
            PlayerSeasonPitching.player_id == player_id,
            PlayerSeasonPitching.season == season_year,
            PlayerSeasonPitching.league == 'REGULAR'
        ).first()
        
        if not stats:
            return None
            
        return {
            "player_id": player_id,
            "season": season_year,
            "era": stats.era,
            "wins": stats.wins,
            "losses": stats.losses,
            "saves": stats.saves,
            "holds": stats.holds,
            "games": stats.games,
            "innings": stats.innings_pitched,
            "summary_text": f"{stats.wins}승 {stats.losses}패 {stats.era}ERA"
        }
