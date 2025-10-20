"""
PlayerSeasonPitching 전용 리포지토리
투수 시즌 데이터를 player_season_pitching 테이블에 올바르게 저장
"""
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from src.db.engine import SessionLocal, get_database_type
from src.models.player import PlayerSeasonPitching


def save_pitching_stats_to_db(payloads: List[Dict[str, Any]]) -> int:
    """
    투수 시즌 통계를 player_season_pitching 테이블에 UPSERT 저장
    
    Args:
        payloads: 투수 데이터 딕셔너리 리스트
        
    Returns:
        저장된 레코드 수
    """
    if not payloads:
        return 0
    
    with SessionLocal() as session:
        db_type = get_database_type()
        saved_count = 0
        
        for payload in payloads:
            # extra_stats에서 확장 통계 추출하여 정규 컬럼으로 승격
            extra_stats = payload.get('extra_stats', {})
            metrics = extra_stats.get('metrics', {}) if isinstance(extra_stats, dict) else {}
            
            # 기본 필드들 매핑 (크롤러 PitcherStats.to_repository_payload()와 일치)
            data = {
                'player_id': payload.get('player_id'),
                'season': payload.get('season'),
                'league': payload.get('league'),
                'level': payload.get('level', 'KBO1'),
                'source': payload.get('source', 'CRAWLER'),
                'team_code': payload.get('team_code'),
                
                # 기본 투수 통계
                'games': payload.get('games'),
                'games_started': payload.get('games_started'),
                'wins': payload.get('wins'),
                'losses': payload.get('losses'),
                'saves': payload.get('saves'),
                'holds': payload.get('holds'),
                
                # 이닝 관련
                'innings_pitched': payload.get('innings_pitched'),
                'innings_outs': payload.get('innings_outs'),
                
                # 피칭 결과
                'hits_allowed': payload.get('hits_allowed'),
                'runs_allowed': payload.get('runs_allowed'),
                'earned_runs': payload.get('earned_runs'),
                'home_runs_allowed': payload.get('home_runs_allowed'),
                'walks_allowed': payload.get('walks_allowed'),
                'intentional_walks': payload.get('intentional_walks'),
                'hit_batters': payload.get('hit_batters'),
                'strikeouts': payload.get('strikeouts'),
                'wild_pitches': payload.get('wild_pitches'),
                'balks': payload.get('balks'),
                
                # 고급 통계
                'era': payload.get('era'),
                'whip': payload.get('whip'),
                'fip': payload.get('fip'),
                'k_per_nine': payload.get('k_per_nine'),
                'bb_per_nine': payload.get('bb_per_nine'),
                'kbb': payload.get('kbb'),
                
                # Basic2에서 수집한 확장 통계를 정규 컬럼으로 승격
                'complete_games': metrics.get('complete_games'),
                'shutouts': metrics.get('shutouts'),
                'quality_starts': metrics.get('quality_starts'),
                'blown_saves': metrics.get('blown_saves'),
                'tbf': metrics.get('tbf'),
                'np': metrics.get('np'),
                'avg_against': metrics.get('avg_against'),
                'doubles_allowed': metrics.get('doubles_allowed'),
                'triples_allowed': metrics.get('triples_allowed'),
                'sacrifices_allowed': metrics.get('sacrifices_allowed'),
                'sacrifice_flies_allowed': metrics.get('sacrifice_flies_allowed'),
                
                # 나머지는 extra_stats에 보관
                'extra_stats': extra_stats
            }
            
            # None 값 제거
            data = {k: v for k, v in data.items() if v is not None}
            
            # UPSERT 수행 (DB 종류별로 다른 문법)
            if db_type == 'sqlite':
                stmt = sqlite_insert(PlayerSeasonPitching).values(**data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['player_id', 'season', 'league', 'level'],
                    set_={k: stmt.excluded[k] for k in data.keys() if k not in ['player_id', 'season', 'league', 'level']}
                )
            elif db_type == 'mysql':
                stmt = mysql_insert(PlayerSeasonPitching).values(**data)
                stmt = stmt.on_duplicate_key_update({
                    k: stmt.inserted[k] for k in data.keys() if k not in ['player_id', 'season', 'league', 'level']
                })
            elif db_type == 'postgresql':
                stmt = postgresql_insert(PlayerSeasonPitching).values(**data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['player_id', 'season', 'league', 'level'],
                    set_={k: stmt.excluded[k] for k in data.keys() if k not in ['player_id', 'season', 'league', 'level']}
                )
            else:
                # Fallback: 단순 merge
                existing = session.query(PlayerSeasonPitching).filter_by(
                    player_id=data['player_id'],
                    season=data['season'],
                    league=data['league'],
                    level=data['level']
                ).first()
                
                if existing:
                    for k, v in data.items():
                        setattr(existing, k, v)
                else:
                    new_record = PlayerSeasonPitching(**data)
                    session.add(new_record)
                
                saved_count += 1
                continue
            
            try:
                session.execute(stmt)
                saved_count += 1
            except Exception as e:
                print(f"⚠️ UPSERT 실패 (player_id={data.get('player_id')}): {e}")
                session.rollback()
                continue
        
        try:
            session.commit()
            print(f"✅ 투수 데이터 {saved_count}건 저장 완료 (player_season_pitching 테이블)")
        except Exception as e:
            session.rollback()
            print(f"❌ 커밋 실패: {e}")
            return 0
        
        return saved_count


def get_pitching_stats_count(session: Optional[Session] = None) -> int:
    """투수 테이블의 레코드 수 조회"""
    if session:
        return session.query(PlayerSeasonPitching).count()
    else:
        with SessionLocal() as new_session:
            return new_session.query(PlayerSeasonPitching).count()


def get_pitching_stats_by_season(season: int, session: Optional[Session] = None) -> List[PlayerSeasonPitching]:
    """시즌별 투수 데이터 조회"""
    if session:
        return session.query(PlayerSeasonPitching).filter_by(season=season).all()
    else:
        with SessionLocal() as new_session:
            return new_session.query(PlayerSeasonPitching).filter_by(season=season).all()


def cleanup_invalid_pitching_data(session: Optional[Session] = None) -> int:
    """잘못된 투수 데이터 정리 (예: 필수 필드 누락)"""
    cleanup_session = session or SessionLocal()
    
    try:
        # player_id나 season이 없는 레코드 삭제
        deleted = cleanup_session.query(PlayerSeasonPitching).filter(
            (PlayerSeasonPitching.player_id.is_(None)) |
            (PlayerSeasonPitching.season.is_(None))
        ).delete()
        
        if not session:  # 외부 세션이 아닌 경우만 커밋
            cleanup_session.commit()
        
        return deleted
        
    except Exception as e:
        if not session:
            cleanup_session.rollback()
        print(f"⚠️ 투수 데이터 정리 실패: {e}")
        return 0
    finally:
        if not session:
            cleanup_session.close()