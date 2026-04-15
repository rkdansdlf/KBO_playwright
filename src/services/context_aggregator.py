"""
Context Aggregator Service
Calculates derived metrics (Head-to-head, Streaks, Trends, WPA moments) 
to provide rich context for LLM analysis.
"""
from __future__ import annotations
from typing import List, Dict, Any, Optional
from sqlalchemy import text, func, or_, and_, desc
from datetime import datetime, date, timedelta

from src.models.game import Game, GameEvent, GameBattingStat, GamePitchingStat
from src.models.player import PlayerSeasonPitching, PlayerMovement
from src.models.team import TeamDailyRoster
from src.models.season import KboSeason
from src.utils.game_status import COMPLETED_LIKE_GAME_STATUSES

class ContextAggregator:
    def __init__(self, session):
        self.session = session

    def get_team_l10_summary(self, team_code: str, target_date: date) -> Dict[str, Any]:
        """최근 10경기 승패 및 연승/연패 흐름 계산"""
        games = self.session.query(Game).filter(
            or_(Game.home_team == team_code, Game.away_team == team_code),
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
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
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
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
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
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

        bp_era = round((bullpen.er * 27) / bullpen.outs, 2) if bullpen and bullpen.outs and bullpen.outs > 0 else None
        bp_whip = round(((bullpen.hits or 0) + (bullpen.walks or 0)) * 3 / bullpen.outs, 2) if bullpen and bullpen.outs and bullpen.outs > 0 else None
        bp_ip = round(bullpen.outs / 3.0, 1) if bullpen and bullpen.outs else 0

        return {
            "avg": avg,
            "era": era,
            "bullpen_era": bp_era,
            "bullpen_whip": bp_whip,
            "bullpen_ip": bp_ip,
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
            Game.game_status.in_(tuple(COMPLETED_LIKE_GAME_STATUSES)),
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

    def get_recent_player_movements(self, team_code: str, target_date: Any, days: int = 7) -> List[Dict[str, Any]]:
        """최근 N일간 해당 팀의 선수 이동 현황(부상, 트레이드 등) 조회"""
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").date()
            
        from src.utils.team_codes import resolve_team_code
        # HT -> KIA, LT -> 롯데 등 한국어 이름으로도 검색 가능하도록 확장
        # (테이블에 한국어 이름과 영문 코드가 섞여 있을 수 있음)
        possible_names = [team_code]
        # KIA, 롯데 등 한국어 이름을 포함시키기 위해 resolve_team_code 활용 (역방향 필요할 수도 있음)
        # 우선 수동 매핑 추가 (가장 확실한 방법)
        team_name_map = {
            "HT": "KIA", "LT": "롯데", "SS": "삼성", "OB": "두산", "HH": "한화",
            "KT": "KT", "NC": "NC", "SK": "SSG", "WO": "키움", "KH": "키움",
            "KIA": "KIA", "롯데": "롯데", "삼성": "삼성", "두산": "두산", "한화": "한화", "SSG": "SSG", "키움": "키움"
        }
        if team_code in team_name_map:
            possible_names.append(team_name_map[team_code])
        
        # 롯데/NC/KT 등은 그대로 쓰거나 영문 코드를 추가
        reverse_map = {v: k for k, v in team_name_map.items()}
        if team_code in reverse_map:
            possible_names.append(reverse_map[team_code])

        start_date = target_date - timedelta(days=days)
        movements = self.session.query(PlayerMovement).filter(
            PlayerMovement.team_code.in_(possible_names),
            PlayerMovement.movement_date >= start_date,
            PlayerMovement.movement_date <= target_date
        ).order_by(desc(PlayerMovement.movement_date)).all()
        
        # DEBUG: print(f"🔍 Found {len(movements)} movements for {possible_names} from {start_date} to {target_date}")
        
        results = []
        for m in movements:
            results.append({
                "date": m.movement_date.isoformat(),
                "section": m.section,
                "player": m.player_name,
                "remarks": m.remarks
            })
        return results

    def get_daily_roster_changes(self, team_code: str, target_date: Any) -> Dict[str, List[str]]:
        """해당 날짜의 1군 등록/말소 현황 비교 (어제와 비교)"""
        if isinstance(target_date, str):
            target_date = datetime.strptime(target_date.replace("-", ""), "%Y%m%d").date()
            
        prev_date = target_date - timedelta(days=1)
        
        curr_roster = self.session.query(TeamDailyRoster).filter(
            TeamDailyRoster.team_code == team_code,
            TeamDailyRoster.roster_date == target_date
        ).all()
        
        prev_roster = self.session.query(TeamDailyRoster).filter(
            TeamDailyRoster.team_code == team_code,
            TeamDailyRoster.roster_date == prev_date
        ).all()
        
        # 만약 전날 데이터가 없으면 '추가/삭제'를 판단할 수 없음
        if not curr_roster or not prev_roster:
            return {"added": [], "removed": []}
            
        curr_ids = {r.player_id: r.player_name for r in curr_roster}
        prev_ids = {r.player_id: r.player_name for r in prev_roster}
        
        added = [name for pid, name in curr_ids.items() if pid not in prev_ids]
        removed = [name for pid, name in prev_ids.items() if pid not in curr_ids]
        
        return {
            "added": added,
            "removed": removed
        }
