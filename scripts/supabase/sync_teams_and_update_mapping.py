#!/usr/bin/env python3
"""
Supabase teams 데이터를 SQLite로 가져와서 팀 매핑 업데이트
team_name이나 team_short_name으로 강제 매핑하여 데이터 정리
"""

import os

import psycopg2
from sqlalchemy import text

from src.db.engine import SessionLocal


def get_supabase_connection():
    """Supabase 연결 생성"""
    supabase_url = os.getenv("SUPABASE_DB_URL")
    if not supabase_url:
        raise ValueError("SUPABASE_DB_URL 환경변수가 설정되지 않았습니다.")

    return psycopg2.connect(supabase_url)


def fetch_teams_from_supabase() -> list[dict]:
    """Supabase에서 teams 데이터 가져오기"""
    with get_supabase_connection() as conn:
        cursor = conn.cursor()

        print("📥 Supabase에서 teams 데이터 가져오는 중...")

        # teams 테이블과 team_history 테이블 조인으로 완전한 데이터 가져오기
        cursor.execute("""
            SELECT DISTINCT
                th.team_code,
                th.team_name,
                th.city,
                th.start_season,
                th.end_season,
                th.franchise_id,
                CASE
                    WHEN th.team_name LIKE '%트윈스%' OR th.team_name LIKE '%청룡%' THEN 'LG'
                    WHEN th.team_name LIKE '%타이거즈%' OR th.team_name LIKE '%해태%' THEN 'KIA'
                    WHEN th.team_name LIKE '%베어스%' OR th.team_name LIKE '%OB%' THEN 'DOOSAN'
                    WHEN th.team_name LIKE '%랜더스%' OR th.team_name LIKE '%와이번스%' THEN 'SSG'
                    WHEN th.team_name LIKE '%자이언츠%' OR th.team_name LIKE '%롯데%' THEN 'LOTTE'
                    WHEN th.team_name LIKE '%라이온즈%' OR th.team_name LIKE '%삼성%' THEN 'SAMSUNG'
                    WHEN th.team_name LIKE '%이글스%' OR th.team_name LIKE '%한화%' OR th.team_name LIKE '%빙그레%' THEN 'HANWHA'
                    WHEN th.team_name LIKE '%위즈%' OR th.team_name LIKE '%KT%' THEN 'KT'
                    WHEN th.team_name LIKE '%다이노스%' OR th.team_name LIKE '%NC%' THEN 'NC'
                    WHEN th.team_name LIKE '%히어로즈%' OR th.team_name LIKE '%키움%' OR th.team_name LIKE '%넥센%' OR th.team_name LIKE '%우리%' THEN 'KIWOOM'
                    WHEN th.team_name LIKE '%돌핀스%' OR th.team_name LIKE '%태평양%' THEN 'PACIFIC'
                    WHEN th.team_name LIKE '%유니콘스%' OR th.team_name LIKE '%현대%' THEN 'HYUNDAI'
                    WHEN th.team_name LIKE '%핀토스%' OR th.team_name LIKE '%청보%' THEN 'CHUNGBO'
                    ELSE th.team_code
                END as normalized_code
            FROM team_history th
            WHERE th.team_code IS NOT NULL
            ORDER BY th.start_season, th.team_code;
        """)

        teams_data = []
        for row in cursor.fetchall():
            team_code, team_name, city, start_season, end_season, franchise_id, normalized_code = row
            teams_data.append(
                {
                    "team_code": team_code,
                    "team_name": team_name,
                    "city": city,
                    "start_season": start_season,
                    "end_season": end_season,
                    "franchise_id": franchise_id,
                    "normalized_code": normalized_code,
                }
            )

        print(f"✅ {len(teams_data)}개 팀 데이터 가져오기 완료")
        return teams_data


def analyze_sqlite_team_mapping() -> dict[str, list[dict]]:
    """SQLite 데이터의 팀 매핑 분석"""
    with SessionLocal() as session:
        print("\n🔍 SQLite 데이터 팀 매핑 분석 중...")

        # 타자 데이터 팀 분포
        batting_teams = session.execute(
            text("""
            SELECT
                team_id,
                COUNT(*) as count,
                MIN(season) as first_year,
                MAX(season) as last_year,
                GROUP_CONCAT(DISTINCT
                    CASE
                        WHEN json_extract(extra_stats, '$.team_name') IS NOT NULL
                        THEN json_extract(extra_stats, '$.team_name')
                        ELSE NULL
                    END
                ) as team_names
            FROM player_season_batting
            WHERE team_id IS NOT NULL
            GROUP BY team_id
            ORDER BY first_year, team_id
        """)
        ).fetchall()

        # 투수 데이터 팀 분포 (team_code 컬럼 사용)
        pitching_teams = session.execute(
            text("""
            SELECT
                team_code,
                COUNT(*) as count,
                MIN(season) as first_year,
                MAX(season) as last_year,
                GROUP_CONCAT(DISTINCT
                    CASE
                        WHEN json_extract(extra_stats, '$.team_name') IS NOT NULL
                        THEN json_extract(extra_stats, '$.team_name')
                        ELSE NULL
                    END
                ) as team_names
            FROM player_season_pitching
            WHERE team_code IS NOT NULL
            GROUP BY team_code
            ORDER BY first_year, team_code
        """)
        ).fetchall()

        print(f"📊 SQLite 타자 데이터: {len(batting_teams)}개 팀")
        for team_id, count, first_year, last_year, team_names in batting_teams:
            print(f"  - {team_id}: {count}명 ({first_year}-{last_year}) → {team_names}")

        print(f"\n📊 SQLite 투수 데이터: {len(pitching_teams)}개 팀")
        for team_code, count, first_year, last_year, team_names in pitching_teams:
            print(f"  - {team_code}: {count}명 ({first_year}-{last_year}) → {team_names}")

        return {"batting": batting_teams, "pitching": pitching_teams}


def create_team_mapping_rules(teams_data: list[dict]) -> dict[str, str]:
    """팀 매핑 규칙 생성"""
    print("\n🗺️ 팀 매핑 규칙 생성 중...")

    # SQLite team_id → Supabase team_code 매핑
    mapping_rules = {
        # 기본 매핑 (역사적 순서 고려)
        "LG": "LG",  # LG 트윈스 (1990-현재)
        "MBC": "LG",  # MBC 청룡 (1982-1989) → LG
        "KIA": "KIA",  # KIA 타이거즈 (2002-현재)
        "HT": "KIA",  # 해태 타이거즈 (1982-2001) → KIA
        "DOOSAN": "DOOSAN",  # 두산 베어스 (1999-현재)
        "OB": "DOOSAN",  # OB 베어스 (1982-1998) → 두산
        "SSG": "SSG",  # SSG 랜더스 (2021-현재)
        "SK": "SSG",  # SK 와이번스 (2000-2020) → SSG
        "LOTTE": "LOTTE",  # 롯데 자이언츠 (1982-현재)
        "LT": "LOTTE",  # 롯데 (축약)
        "SAMSUNG": "SAMSUNG",  # 삼성 라이온즈 (1982-현재)
        "SM": "SAMSUNG",  # 삼성 (축약)
        "HANWHA": "HANWHA",  # 한화 이글스 (1986-현재)
        "HH": "HANWHA",  # 한화 (축약)
        "BINGGRAE": "HANWHA",  # 빙그레 이글스 (1986-1993) → 한화
        "KT": "KT",  # KT 위즈 (2015-현재)
        "NC": "NC",  # NC 다이노스 (2013-현재)
        "KIWOOM": "KIWOOM",  # 키움 히어로즈 (2019-현재)
        "NEXEN": "KIWOOM",  # 넥센 히어로즈 (2008-2018) → 키움
        "WOORI": "KIWOOM",  # 우리 히어로즈 (2007) → 키움
        "WO": "KIWOOM",  # 우리/넥센/키움 계열
        "우리": "KIWOOM",  # 우리 (한글)
        # 역사적 팀들
        "PC": "PACIFIC",  # 태평양 돌핀스 (1988-1995)
        "PACIFIC": "PACIFIC",
        "CB": "CHUNGBO",  # 청보 핀토스 (1982-1985)
        "CHUNGBO": "CHUNGBO",
        "HYUNDAI": "HYUNDAI",  # 현대 유니콘스 (1982-2007, 해체)
        "SW": "SAMSUNG",  # SW삼성전자 등 → 삼성 계열로 분류
    }

    print(f"📋 생성된 매핑 규칙: {len(mapping_rules)}개")
    for sqlite_id, supabase_code in mapping_rules.items():
        print(f"  {sqlite_id} → {supabase_code}")

    return mapping_rules


def update_sqlite_team_mapping(mapping_rules: dict[str, str], dry_run: bool = True) -> dict[str, int]:
    """SQLite 데이터의 team_id를 Supabase team_code로 업데이트"""
    with SessionLocal() as session:
        print(f"\n🔄 SQLite 팀 매핑 업데이트 {'(시뮬레이션)' if dry_run else '(실제 적용)'}")
        print("-" * 50)

        # 외래키 제약조건 비활성화 (실제 업데이트시에만)
        if not dry_run:
            session.execute(text("PRAGMA foreign_keys = OFF"))
            print("🔓 SQLite 외래키 제약조건 비활성화")

        results = {"batting_updated": 0, "pitching_updated": 0, "unmapped": []}

        # 타자 데이터 업데이트
        print("1️⃣ 타자 데이터 업데이트 중...")
        for sqlite_id, supabase_code in mapping_rules.items():
            if dry_run:
                # 시뮬레이션: 업데이트될 행 수만 확인
                result = session.execute(
                    text("""
                    SELECT COUNT(*)
                    FROM player_season_batting
                    WHERE team_id = :sqlite_id
                """),
                    {"sqlite_id": sqlite_id},
                ).scalar()

                if result > 0:
                    print(f"  📊 {sqlite_id} → {supabase_code}: {result}명")
                    results["batting_updated"] += result
            else:
                # 실제 업데이트
                result = session.execute(
                    text("""
                    UPDATE player_season_batting
                    SET team_id = :supabase_code
                    WHERE team_id = :sqlite_id
                """),
                    {"sqlite_id": sqlite_id, "supabase_code": supabase_code},
                )

                if result.rowcount > 0:
                    print(f"  ✅ {sqlite_id} → {supabase_code}: {result.rowcount}명 업데이트")
                    results["batting_updated"] += result.rowcount

        # 투수 데이터 업데이트
        print("\n2️⃣ 투수 데이터 업데이트 중...")
        for sqlite_id, supabase_code in mapping_rules.items():
            if dry_run:
                # 시뮬레이션
                result = session.execute(
                    text("""
                    SELECT COUNT(*)
                    FROM player_season_pitching
                    WHERE team_code = :sqlite_id
                """),
                    {"sqlite_id": sqlite_id},
                ).scalar()

                if result > 0:
                    print(f"  📊 {sqlite_id} → {supabase_code}: {result}명")
                    results["pitching_updated"] += result
            else:
                # 실제 업데이트
                result = session.execute(
                    text("""
                    UPDATE player_season_pitching
                    SET team_code = :supabase_code
                    WHERE team_code = :sqlite_id
                """),
                    {"sqlite_id": sqlite_id, "supabase_code": supabase_code},
                )

                if result.rowcount > 0:
                    print(f"  ✅ {sqlite_id} → {supabase_code}: {result.rowcount}명 업데이트")
                    results["pitching_updated"] += result

        # 매핑되지 않은 팀 확인
        print("\n3️⃣ 매핑되지 않은 팀 확인...")
        unmapped_batting = session.execute(
            text(
                """
            SELECT DISTINCT team_id, COUNT(*) as count
            FROM player_season_batting
            WHERE team_id NOT IN ({})
            GROUP BY team_id
        """.format(",".join([f"'{code}'" for code in mapping_rules.values()]))
            )
        ).fetchall()

        unmapped_pitching = session.execute(
            text(
                """
            SELECT DISTINCT team_code, COUNT(*) as count
            FROM player_season_pitching
            WHERE team_code NOT IN ({})
            GROUP BY team_code
        """.format(",".join([f"'{code}'" for code in mapping_rules.values()]))
            )
        ).fetchall()

        if unmapped_batting:
            print("  📊 매핑되지 않은 타자 팀:")
            for team_id, count in unmapped_batting:
                print(f"    - {team_id}: {count}명")
                results["unmapped"].append(f"batting:{team_id}({count})")

        if unmapped_pitching:
            print("  📊 매핑되지 않은 투수 팀:")
            for team_code, count in unmapped_pitching:
                print(f"    - {team_code}: {count}명")
                results["unmapped"].append(f"pitching:{team_code}({count})")

        if not dry_run:
            session.commit()
            print("\n✅ 업데이트 완료 및 커밋")

        return results


def main():
    try:
        print("🚀 Supabase teams 데이터 기반 SQLite 팀 매핑 업데이트")
        print("=" * 60)

        # 1. Supabase에서 teams 데이터 가져오기
        teams_data = fetch_teams_from_supabase()

        # 2. SQLite 팀 매핑 현황 분석
        sqlite_analysis = analyze_sqlite_team_mapping()

        # 3. 매핑 규칙 생성
        mapping_rules = create_team_mapping_rules(teams_data)

        # 4. 시뮬레이션 실행
        print("\n🔍 업데이트 시뮬레이션 실행...")
        sim_results = update_sqlite_team_mapping(mapping_rules, dry_run=True)

        print("\n📊 시뮬레이션 결과:")
        print(f"  - 타자 업데이트 예정: {sim_results['batting_updated']:,}명")
        print(f"  - 투수 업데이트 예정: {sim_results['pitching_updated']:,}명")
        print(f"  - 매핑되지 않은 데이터: {len(sim_results['unmapped'])}개")

        if sim_results["unmapped"]:
            print(f"  - 미매핑: {', '.join(sim_results['unmapped'])}")

        # 5. 사용자 확인
        print("\n❓ 실제 업데이트를 진행하시겠습니까?")
        choice = input("y/N: ").strip().lower()

        if choice == "y":
            print("\n🔄 실제 업데이트 실행...")
            real_results = update_sqlite_team_mapping(mapping_rules, dry_run=False)

            print("\n🎉 업데이트 완료!")
            print(f"  - 타자: {real_results['batting_updated']:,}명")
            print(f"  - 투수: {real_results['pitching_updated']:,}명")

            print("\n💡 다음 단계:")
            print("  1. ./venv/bin/python3 -m src.sync.supabase_sync")
            print("  2. 데이터 동기화 확인")
        else:
            print("\n❌ 사용자가 취소했습니다.")

    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
