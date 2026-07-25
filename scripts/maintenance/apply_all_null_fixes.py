"""
누적 NULL player_id 일괄 해소 스크립트.
아래 3단계를 순서대로 실행:
  1. resolve_null_player_ids_conservative --year 2026
  2. backfill_player_ids (전체 시즌)
  3. fix_2020_pitching_nulls (2키 매칭)
  4. roster 기반 overrides 직접 적용 (2020/2026 확인된 매핑)

Usage:
    python3 scripts/maintenance/apply_all_null_fixes.py           # dry-run
    python3 scripts/maintenance/apply_all_null_fixes.py --apply   # 실제 적용
"""

from __future__ import annotations

import sys
import sqlite3
from pathlib import Path

DB_PATH = Path("data/kbo_dev.db")
TABLES = ("game_pitching_stats", "game_batting_stats", "game_lineups")

# roster 기반으로 확인된 명확한 (name, team_code, year) → player_id 매핑
ROSTER_OVERRIDES: list[tuple[str, str, str, int]] = [
    # 2026년
    ("김건우", "SSG", "2026", 51867),
    ("데이비슨", "KH", "2026", 54944),
    ("테일러", "NC", "2026", 56966),
    ("안우진", "KH", "2026", 68341),
    ("양현종", "KIA", "2026", 77637),
    ("이승민", "SS", "2026", 50464),
    # 2026년 추가 확인분 (roster 기반)
    ("고준휘", "NC", "2026", 56949),
    ("박건우", "LT", "2026", 55509),
    ("김강현", "LT", "2026", 65522),
    ("박상원", "HH", "2026", 67703),
    ("이민석", "LT", "2026", 52530),
    ("김영우", "LG", "2026", 55167),
    ("김종수", "HH", "2026", 63765),
    ("박정훈", "KH", "2026", 55394),
    ("김진욱", "LT", "2026", 51516),
    ("박시원", "LG", "2026", 55121),
    ("이태양", "KIA", "2026", 60768),
    ("정현우", "KH", "2026", 55313),
    ("최민준", "SSG", "2026", 68856),
    ("에르난데스", "HH", "2026", 56712),
    ("오러클린", "SS", "2026", 56464),
    ("이병헌", "DB", "2026", 52204),
    # 2026년 EA/WE 올스타전 (단일 매칭)
    ("김건우", "EA", "2026", 51867),
    ("안우진", "WE", "2026", 68341),
    ("이승민", "EA", "2026", 50464),
    ("최원준", "EA", "2026", 66606),  # DB 최원준(66606) 기준
    ("김태훈", "EA", "2026", 62360),  # KH 기준 (SS는 중복)
    ("이승현", "SS", "2026", 60146),  # SS 이승현 (roster 2건 중 기존 활성 선수)
    # 2026년 추가 확인분 (2026-07-25 로스터 재수집)
    ("김종수", "HH", "2026", 63765),
    ("김태훈", "SS", "2026", 62360),
    ("박상원", "HH", "2026", 67703),
    ("박정훈", "KH", "2026", 55394),
    ("양현종", "KIA", "2026", 77637),
    ("이민석", "LT", "2026", 52530),
    ("이태양", "KIA", "2026", 60768),
    ("정현우", "KH", "2026", 55313),
    ("테일러", "NC", "2026", 56966),
    ("이승민", "SS", "2026", 50464),
    # 2001년 확인 선수 (player_basic team + retire_year 기반)
    ("김민범", "HU", "2001", 93367),  # 현대 유니콘스 소속
    ("송진우", "HH", "2001", 89770),  # 한화 이글스 소속
    ("오상민", "SK", "2001", 5007),  # 쌍방울 출신 SK 이적
    # 2020년 — roster로 확인된 단일 매칭
    ("박진형", "LT", "2020", 63512),
    ("김종수", "HH", "2020", 63765),
    ("최원준", "DB", "2020", 67263),
    ("이승현", "SS", "2020", 60146),
    ("라이트", "NC", "2020", 50912),
    ("박세웅", "LT", "2020", 64021),
    ("브룩스", "KIA", "2020", 50636),
    ("이민호", "LG", "2020", 50126),
    ("이태양", "SK", "2020", 60768),
    ("윌슨", "LG", "2020", 68135),
    ("김대우", "SS", "2020", 61365),
    ("김민규", "DB", "2020", 68200),
    ("켈리", "LG", "2020", 69103),
    ("양현종", "KIA", "2020", 77637),
    ("김태훈", "KH", "2020", 62360),
    ("김현준", "KIA", "2020", 66630),
    ("샘슨", "LT", "2020", 50524),
    ("이승민", "SS", "2020", 50464),
    ("김대유", "LG", "2020", 60337),
    ("문대원", "DB", "2020", 67259),
    ("안우진", "KH", "2020", 68341),
    ("이승헌", "NC", "2020", 68910),
    ("이승호", "KH", "2020", 67603),
]

# fix_2020_pitching_nulls: player_basic team+name 2키 매칭 결과
TWO_KEY_OVERRIDES: list[tuple[str, str, str, int]] = [
    ("이상화", "KT", "2020", 77563),
    ("박진우", "NC", "2020", 63991),
    ("김대우", "LT", "2020", 3707),
    ("김성훈", "KT", "2020", 2670),
    ("김대현", "LG", "2020", 2207),
    ("김도현", "HH", "2020", 2772),
    ("박상원", "HH", "2020", 2467),
    ("이민호", "LG", "2020", 50126),
    ("장지훈", "SS", "2020", 2433),
    ("강동호", "LT", "2020", 2450),
    ("김현수", "KIA", "2020", 69516),
    ("김민", "KT", "2020", 2514),
    ("김태훈", "SK", "2020", 3857),
    ("이원준", "SK", "2020", 2490),
    ("고우석", "LG", "2020", 2383),
    ("김진욱", "HH", "2020", 2612),
    ("양승철", "KIA", "2020", 2756),
    ("김민우", "HH", "2020", 2145),
    ("한승혁", "LT", "2020", 2277),
    ("이승헌", "LT", "2020", 2593),
]


def apply_overrides(
    cur: sqlite3.Cursor,
    overrides: list[tuple[str, str, str, int]],
    label: str,
    apply: bool,
) -> int:
    total = 0
    for name, team, year, pid in overrides:
        for tbl in TABLES:
            cur.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE player_id IS NULL "
                f"AND player_name=? AND team_code=? AND substr(game_id,1,4)=?",
                (name, team, year),
            )
            cnt: int = cur.fetchone()[0]
            if cnt == 0:
                continue
            total += cnt
            tag = "[APPLY]" if apply else "[DRY-RUN]"
            print(f"  {tag} [{label}] {tbl}: {name}({team},{year}) -> {pid} ({cnt}행)")
            if apply:
                cur.execute(
                    f"UPDATE {tbl} SET player_id=? WHERE player_id IS NULL "
                    f"AND player_name=? AND team_code=? AND substr(game_id,1,4)=?",
                    (pid, name, team, year),
                )
    return total


def main(apply: bool = False) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    print("=== Step 1: roster 기반 overrides ===")
    t1 = apply_overrides(cur, ROSTER_OVERRIDES, "roster", apply)

    print()
    print("=== Step 2: player_basic 2키 매칭 overrides ===")
    t2 = apply_overrides(cur, TWO_KEY_OVERRIDES, "2key", apply)

    if apply:
        conn.commit()

    total = t1 + t2
    tag = "완료" if apply else "예상(dry-run)"
    print()
    print(f"{'✅' if apply else '📋'} {tag}: 총 {total}행 업데이트")
    if not apply:
        print("  --apply 플래그를 추가하면 실제로 적용됩니다.")

    conn.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
