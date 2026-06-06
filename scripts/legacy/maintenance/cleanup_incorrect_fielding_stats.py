import os
import sqlite3

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def cleanup_sqlite(db_path="data/kbo_dev.db"):
    print(f"🧹 SQLite 클린업 시작: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 잘못 들어간 잘못된 매핑 삭제 조건들
    targets = [
        # (시작연도, 종료연도, 잘못 매핑된 레거시 코드)
        (2020, 2026, "WO"),  # 키움 히어로즈는 2020년 이후 WO가 아님 (KH가 정식)
        (2021, 2026, "SK"),  # SSG 랜더스는 2021년 이후 SK가 아님 (SSG가 정식)
        (1999, 2026, "OB"),  # 두산 베어스는 1999년 이후 OB가 아님 (DB가 정식)
        (2001, 2026, "HT"),  # KIA 타이거즈는 2001년 이후 HT가 아님 (KIA가 정식)
    ]

    total_deleted = 0
    for start_yr, end_yr, legacy_code in targets:
        cursor.execute(
            """
            DELETE FROM player_season_fielding
            WHERE team_id = ? AND year BETWEEN ? AND ?
        """,
            (legacy_code, start_yr, end_yr),
        )
        deleted = cursor.rowcount
        total_deleted += deleted
        print(f"   - {legacy_code} ({start_yr}~{end_yr}): {deleted}건 삭제 완료")

    conn.commit()
    conn.close()
    print(f"✅ SQLite 클린업 완료! 총 {total_deleted}건 삭제됨.\n")
    return total_deleted


def cleanup_oci():
    load_dotenv()
    oci_url = os.getenv("OCI_DB_URL") or os.getenv("TARGET_DATABASE_URL")
    if not oci_url:
        print("⚠️ OCI DB URL이 설정되지 않아 원격 DB 클린업은 건너뜁니다.")
        return

    print("🧹 OCI PostgreSQL 클린업 시작...")
    engine = create_engine(oci_url)

    targets = [(2020, 2026, "WO"), (2021, 2026, "SK"), (1999, 2026, "OB"), (2001, 2026, "HT")]

    total_deleted = 0
    with engine.connect() as conn:
        transaction = conn.begin()
        try:
            for start_yr, end_yr, legacy_code in targets:
                query = text("""
                    DELETE FROM player_season_fielding
                    WHERE team_id = :legacy_code AND year BETWEEN :start_yr AND :end_yr
                """)
                result = conn.execute(query, {"legacy_code": legacy_code, "start_yr": start_yr, "end_yr": end_yr})
                deleted = result.rowcount
                total_deleted += deleted
                print(f"   - {legacy_code} ({start_yr}~{end_yr}): {deleted}건 삭제 완료")
            transaction.commit()
            print(f"✅ OCI PostgreSQL 클린업 완료! 총 {total_deleted}건 삭제됨.\n")
        except Exception as e:
            transaction.rollback()
            print(f"❌ OCI PostgreSQL 클린업 실패: {e}")


if __name__ == "__main__":
    cleanup_sqlite()
    cleanup_oci()
