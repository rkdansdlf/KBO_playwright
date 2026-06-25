#!/usr/bin/env python3
"""선수 기본정보를 먼저 Supabase에 동기화하는 스크립트
시즌 기록 동기화 전에 player_basic 테이블을 먼저 채움
"""

import logging

logger = logging.getLogger(__name__)

import os

from sqlalchemy import create_engine, text

from src.db.engine import SessionLocal


def get_supabase_connection():
    """Supabase 연결 생성"""
    supabase_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_url:
        raise ValueError("SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")

    return create_engine(supabase_url, echo=False)


def sync_player_basic():
    """SQLite player_basic 데이터를 Supabase로 동기화"""
    # SQLite에서 데이터 읽기
    with SessionLocal() as sqlite_session:
        logger.info("📥 SQLite에서 선수 기본정보 가져오는 중...")

        players = sqlite_session.execute(
            text("""
            SELECT player_id, name, uniform_no, team, position,
                   birth_date, birth_date_date, height_cm, weight_kg, career
            FROM player_basic
            ORDER BY player_id
        """)
        ).fetchall()

        logger.info(f"📊 SQLite 선수 기본정보: {len(players)}명")

    # Supabase에 저장
    supabase_engine = get_supabase_connection()

    with supabase_engine.begin() as conn:
        logger.info("📤 Supabase로 선수 기본정보 동기화 중...")

        synced_count = 0

        for player in players:
            player_id, name, uniform_no, team, position, birth_date, birth_date_date, height_cm, weight_kg, career = (
                player
            )

            # Supabase에 UPSERT (created_at, updated_at 컬럼 없음)
            conn.execute(
                text("""
                INSERT INTO player_basic (
                    player_id, name, uniform_no, team, position,
                    birth_date, birth_date_date, height_cm, weight_kg, career
                ) VALUES (
                    :player_id, :name, :uniform_no, :team, :position,
                    :birth_date, :birth_date_date, :height_cm, :weight_kg, :career
                )
                ON CONFLICT (player_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    uniform_no = EXCLUDED.uniform_no,
                    team = EXCLUDED.team,
                    position = EXCLUDED.position,
                    birth_date = EXCLUDED.birth_date,
                    birth_date_date = EXCLUDED.birth_date_date,
                    height_cm = EXCLUDED.height_cm,
                    weight_kg = EXCLUDED.weight_kg,
                    career = EXCLUDED.career
            """),
                {
                    "player_id": player_id,
                    "name": name,
                    "uniform_no": uniform_no,
                    "team": team,
                    "position": position,
                    "birth_date": birth_date,
                    "birth_date_date": birth_date_date,
                    "height_cm": height_cm,
                    "weight_kg": weight_kg,
                    "career": career,
                },
            )

            synced_count += 1

            if synced_count % 100 == 0:
                logger.info(f"   📝 {synced_count}명 동기화 중...")

        logger.info(f"✅ {synced_count}명의 선수 기본정보 동기화 완료")


def verify_sync():
    """동기화 결과 확인"""
    logger.info("\n🔍 동기화 결과 확인 중...")

    with SessionLocal() as sqlite_session:
        sqlite_count = sqlite_session.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()

    supabase_engine = get_supabase_connection()
    with supabase_engine.connect() as conn:
        supabase_count = conn.execute(text("SELECT COUNT(*) FROM player_basic")).scalar()

    logger.info(f"📊 SQLite: {sqlite_count}명")
    logger.info(f"📊 Supabase: {supabase_count}명")

    if sqlite_count == supabase_count:
        logger.info("✅ 동기화 성공: 데이터 수가 일치합니다!")
    else:
        logger.warning(f"⚠️ 데이터 수 불일치: SQLite {sqlite_count}명 vs Supabase {supabase_count}명")


def main():
    try:
        logger.info("🚀 선수 기본정보 Supabase 동기화")
        logger.info("=" * 50)

        sync_player_basic()
        verify_sync()

        logger.info("\n💡 다음 단계:")
        logger.info("   ./venv/bin/python3 -m src.sync.supabase_sync")

    except Exception:
        logger.exception("\n❌ 오류 발생")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
