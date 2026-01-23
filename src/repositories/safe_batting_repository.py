"""
Safe batting data repository with foreign key constraint bypass
타자 데이터를 외래키 제약조건 우회하여 안전하게 저장
"""
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert
from sqlalchemy.dialects.postgresql import insert as postgresql_insert

from src.db.engine import SessionLocal, get_database_type
from src.models.player import PlayerSeasonBatting


def save_batting_stats_safe(payloads: List[Dict[str, Any]]) -> int:
    """
    타자 시즌 통계를 player_season_batting 테이블에 안전하게 UPSERT 저장
    외래키 제약조건을 임시로 비활성화하여 데이터 저장
    
    Args:
        payloads: 타자 데이터 딕셔너리 리스트
        
    Returns:
        저장된 레코드 수
    """
    if not payloads:
        return 0
    
    with SessionLocal() as session:
        db_type = get_database_type()
        saved_count = 0
        
        try:
            # SQLite의 경우 외래키 제약조건 임시 비활성화
            if db_type == 'sqlite':
                session.execute(text("PRAGMA foreign_keys = OFF"))
                print("⚙️ SQLite 외래키 제약조건 임시 비활성화")
            
            unique_payloads = {}
            for payload in payloads:
                key = (
                    payload.get("player_id"),
                    payload.get("season"),
                    payload.get("league"),
                    payload.get("level", "KBO1"),
                )
                if key[0] is None or key[1] is None:
                    continue
                unique_payloads[key] = payload

            rows = []
            for payload in unique_payloads.values():
                rows.append({
                    'player_id': payload.get('player_id'),
                    'season': payload.get('season'),
                    'league': payload.get('league'),
                    'level': payload.get('level', 'KBO1'),
                    'source': payload.get('source', 'CRAWLER'),
                    'team_code': payload.get('team_code'),
                    'games': payload.get('games'),
                    'plate_appearances': payload.get('plate_appearances'),
                    'at_bats': payload.get('at_bats'),
                    'runs': payload.get('runs'),
                    'hits': payload.get('hits'),
                    'doubles': payload.get('doubles'),
                    'triples': payload.get('triples'),
                    'home_runs': payload.get('home_runs'),
                    'rbi': payload.get('rbi'),
                    'walks': payload.get('walks'),
                    'intentional_walks': payload.get('intentional_walks'),
                    'hbp': payload.get('hbp'),
                    'strikeouts': payload.get('strikeouts'),
                    'stolen_bases': payload.get('stolen_bases'),
                    'caught_stealing': payload.get('caught_stealing'),
                    'sacrifice_hits': payload.get('sacrifice_hits'),
                    'sacrifice_flies': payload.get('sacrifice_flies'),
                    'gdp': payload.get('gdp'),
                    'avg': payload.get('avg'),
                    'obp': payload.get('obp'),
                    'slg': payload.get('slg'),
                    'ops': payload.get('ops'),
                    'iso': payload.get('iso'),
                    'babip': payload.get('babip'),
                    'extra_stats': payload.get('extra_stats'),
                })

            if not rows:
                return 0

            conflict_keys = ['player_id', 'season', 'league', 'level']

            if db_type == 'sqlite':
                stmt = sqlite_insert(PlayerSeasonBatting).values(rows)
                update_dict = {
                    k: func.coalesce(stmt.excluded[k], getattr(PlayerSeasonBatting, k))
                    for k in rows[0].keys()
                    if k not in conflict_keys
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_keys,
                    set_=update_dict
                )
                try:
                    session.execute(stmt)
                    saved_count = len(rows)
                except Exception as e:
                    session.rollback()
                    print(f"⚠️ 배치 UPSERT 실패, 개별 처리로 전환합니다: {e}")
                    for data in rows:
                        row_stmt = sqlite_insert(PlayerSeasonBatting).values(**data)
                        row_update = {
                            k: func.coalesce(row_stmt.excluded[k], getattr(PlayerSeasonBatting, k))
                            for k in rows[0].keys()
                            if k not in conflict_keys
                        }
                        row_stmt = row_stmt.on_conflict_do_update(
                            index_elements=conflict_keys,
                            set_=row_update
                        )
                        try:
                            session.execute(row_stmt)
                            saved_count += 1
                        except Exception as row_exc:
                            print(f"⚠️ UPSERT 실패 (player_id={data.get('player_id')}): {row_exc}")
                            session.rollback()
            elif db_type == 'mysql':
                stmt = mysql_insert(PlayerSeasonBatting).values(rows)
                update_dict = {
                    k: func.coalesce(stmt.inserted[k], getattr(PlayerSeasonBatting, k))
                    for k in rows[0].keys()
                    if k not in conflict_keys
                }
                stmt = stmt.on_duplicate_key_update(update_dict)
                try:
                    session.execute(stmt)
                    saved_count = len(rows)
                except Exception as e:
                    session.rollback()
                    print(f"⚠️ 배치 UPSERT 실패, 개별 처리로 전환합니다: {e}")
                    for data in rows:
                        row_stmt = mysql_insert(PlayerSeasonBatting).values(**data)
                        row_update = {
                            k: func.coalesce(row_stmt.inserted[k], getattr(PlayerSeasonBatting, k))
                            for k in rows[0].keys()
                            if k not in conflict_keys
                        }
                        row_stmt = row_stmt.on_duplicate_key_update(row_update)
                        try:
                            session.execute(row_stmt)
                            saved_count += 1
                        except Exception as row_exc:
                            print(f"⚠️ UPSERT 실패 (player_id={data.get('player_id')}): {row_exc}")
                            session.rollback()
            elif db_type == 'postgresql':
                stmt = postgresql_insert(PlayerSeasonBatting).values(rows)
                update_dict = {
                    k: func.coalesce(stmt.excluded[k], getattr(PlayerSeasonBatting, k))
                    for k in rows[0].keys()
                    if k not in conflict_keys
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=conflict_keys,
                    set_=update_dict
                )
                try:
                    session.execute(stmt)
                    saved_count = len(rows)
                except Exception as e:
                    session.rollback()
                    print(f"⚠️ 배치 UPSERT 실패, 개별 처리로 전환합니다: {e}")
                    for data in rows:
                        row_stmt = postgresql_insert(PlayerSeasonBatting).values(**data)
                        row_update = {
                            k: func.coalesce(row_stmt.excluded[k], getattr(PlayerSeasonBatting, k))
                            for k in rows[0].keys()
                            if k not in conflict_keys
                        }
                        row_stmt = row_stmt.on_conflict_do_update(
                            index_elements=conflict_keys,
                            set_=row_update
                        )
                        try:
                            session.execute(row_stmt)
                            saved_count += 1
                        except Exception as row_exc:
                            print(f"⚠️ UPSERT 실패 (player_id={data.get('player_id')}): {row_exc}")
                            session.rollback()
            else:
                for data in rows:
                    existing = session.query(PlayerSeasonBatting).filter_by(
                        player_id=data['player_id'],
                        season=data['season'],
                        league=data['league'],
                        level=data['level']
                    ).first()

                    if existing:
                        for k, v in data.items():
                            if v is not None:
                                setattr(existing, k, v)
                    else:
                        new_record = PlayerSeasonBatting(**data)
                        session.add(new_record)

                    saved_count += 1
            
            # 커밋 전에 외래키 제약조건 복원
            if db_type == 'sqlite':
                session.execute(text("PRAGMA foreign_keys = ON"))
                print("⚙️ SQLite 외래키 제약조건 복원")
            
            session.commit()
            print(f"✅ 타자 데이터 {saved_count}건 저장 완료 (player_season_batting 테이블)")
            
        except Exception as e:
            session.rollback()
            print(f"❌ 타자 데이터 저장 실패: {e}")
            return 0
        
        return saved_count


def get_batting_stats_count(session: Optional[Session] = None) -> int:
    """타자 테이블의 레코드 수 조회"""
    if session:
        return session.query(PlayerSeasonBatting).count()
    else:
        with SessionLocal() as new_session:
            return new_session.query(PlayerSeasonBatting).count()


def get_batting_stats_by_season(season: int, session: Optional[Session] = None) -> List[PlayerSeasonBatting]:
    """시즌별 타자 데이터 조회"""
    if session:
        return session.query(PlayerSeasonBatting).filter_by(season=season).all()
    else:
        with SessionLocal() as new_session:
            return new_session.query(PlayerSeasonBatting).filter_by(season=season).all()


def cleanup_invalid_batting_data(session: Optional[Session] = None) -> int:
    """잘못된 타자 데이터 정리 (예: 필수 필드 누락)"""
    cleanup_session = session or SessionLocal()
    
    try:
        # player_id나 season이 없는 레코드 삭제
        deleted = cleanup_session.query(PlayerSeasonBatting).filter(
            (PlayerSeasonBatting.player_id.is_(None)) |
            (PlayerSeasonBatting.season.is_(None))
        ).delete()
        
        if not session:  # 외부 세션이 아닌 경우만 커밋
            cleanup_session.commit()
        
        return deleted
        
    except Exception as e:
        if not session:
            cleanup_session.rollback()
        print(f"⚠️ 타자 데이터 정리 실패: {e}")
        return 0
    finally:
        if not session:
            cleanup_session.close()
