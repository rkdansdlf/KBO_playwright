"""
KBO 투수 기록 저장 (타자 크롤러 방식과 동일한 단순 구조)
외래키 제약조건 없이 직접 저장
"""
from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import text

from src.db.engine import SessionLocal


def save_pitching_stats(pitching_stats: List[Dict[str, Any]]) -> int:
    """
    투수 기록을 player_season_batting 테이블에 저장 (타자 크롤러와 동일한 방식)
    """
    if not pitching_stats:
        return 0

    with SessionLocal() as session:
        try:
            saved_count = 0
            for stats_data in pitching_stats:
                # 투수 데이터를 타자 테이블 구조에 맞게 변환
                data = {
                    'player_id': stats_data.get('player_id'),
                    'season': stats_data.get('season'),
                    'league': stats_data.get('league', 'REGULAR'),
                    'level': stats_data.get('level', 'KBO1'),
                    'source': f"PITCHER_{stats_data.get('source', 'CRAWLER')}",  # 투수임을 구분
                    'team_code': stats_data.get('team_code'),
                    # 투수 스탯을 타자 필드에 매핑
                    'games': stats_data.get('games'),
                    'plate_appearances': stats_data.get('innings_pitched'),  # 이닝을 PA에 저장
                    'at_bats': stats_data.get('hits_allowed'),  # 피안타를 AB에 저장
                    'runs': stats_data.get('runs_allowed'),
                    'hits': stats_data.get('strikeouts'),  # 삼진을 H에 저장
                    'home_runs': stats_data.get('home_runs_allowed'),
                    'walks': stats_data.get('walks_allowed'),
                    'intentional_walks': stats_data.get('intentional_walks'),
                    'hbp': stats_data.get('hit_batters'),
                    'strikeouts': stats_data.get('wild_pitches'),  # 폭투를 SO에 저장
                    'extra_stats': stats_data.get('extra_stats', {}),
                }

                # None 값 제거
                data = {k: v for k, v in data.items() if v is not None}

                # UPSERT 수행
                _upsert_pitching_data(session, data)
                saved_count += 1

            session.commit()
            return saved_count

        except Exception as e:
            session.rollback()
            print(f"[ERROR] 투수 기록 저장 실패: {e}")
            raise


def _upsert_pitching_data(session: Session, data: Dict[str, Any]):
    """투수 데이터를 player_season_batting 테이블에 UPSERT"""
    
    # SQLite UPSERT 구문
    upsert_sql = """
    INSERT INTO player_season_batting (
        player_id, season, league, level, source, team_code,
        games, plate_appearances, at_bats, runs, hits, home_runs,
        walks, intentional_walks, hbp, strikeouts, extra_stats,
        created_at, updated_at
    ) VALUES (
        :player_id, :season, :league, :level, :source, :team_code,
        :games, :plate_appearances, :at_bats, :runs, :hits, :home_runs,
        :walks, :intentional_walks, :hbp, :strikeouts, :extra_stats,
        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
    )
    ON CONFLICT (player_id, season, league, level) DO UPDATE SET
        source = excluded.source,
        team_code = excluded.team_code,
        games = excluded.games,
        plate_appearances = excluded.plate_appearances,
        at_bats = excluded.at_bats,
        runs = excluded.runs,
        hits = excluded.hits,
        home_runs = excluded.home_runs,
        walks = excluded.walks,
        intentional_walks = excluded.intentional_walks,
        hbp = excluded.hbp,
        strikeouts = excluded.strikeouts,
        extra_stats = excluded.extra_stats,
        updated_at = CURRENT_TIMESTAMP
    """
    
    # extra_stats를 JSON 문자열로 변환
    if 'extra_stats' in data and isinstance(data['extra_stats'], dict):
        import json
        data['extra_stats'] = json.dumps(data['extra_stats'], ensure_ascii=False)

    session.execute(text(upsert_sql), data)
