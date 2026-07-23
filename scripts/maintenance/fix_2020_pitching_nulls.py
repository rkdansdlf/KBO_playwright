"""
2020년 game_pitching_stats / game_batting_stats / game_lineups의
uniform_no=NULL 선수를 name + team(canonical) 2키 매칭으로 player_id 보완.
유일 매칭(정확히 1건)인 경우에만 적용.

Usage:
    python3 -m scripts.maintenance.fix_2020_pitching_nulls           # dry-run
    python3 -m scripts.maintenance.fix_2020_pitching_nulls --apply   # 실제 적용
"""

from __future__ import annotations

import sys
from pathlib import Path

import sqlite3

DB_PATH = Path("data/kbo_dev.db")
TARGET_YEAR = "2020"
TARGET_TABLES = ("game_pitching_stats", "game_batting_stats", "game_lineups")

# 특정 팀코드의 경우 player_basic.team 에서 여러 코드로 검색
TEAM_ALIASES: dict[str, list[str]] = {
    "SK": ["SK", "SSG"],
    "SSG": ["SK", "SSG"],
}


def main(apply: bool = False) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 2020년 NULL (uniform_no NULL) 선수 distinct 목록 수집
    cur.execute(
        """
        SELECT DISTINCT player_name, team_code
        FROM game_pitching_stats
        WHERE player_id IS NULL
          AND uniform_no IS NULL
          AND substr(game_id,1,4) = ?
        """,
        (TARGET_YEAR,),
    )
    candidates = cur.fetchall()

    resolved: list[tuple[str, str, int]] = []
    skipped_ambig: list[tuple[str, str, int]] = []
    skipped_nomatch: list[tuple[str, str]] = []

    for row in candidates:
        name: str = row["player_name"]
        team: str = row["team_code"]
        team_list = TEAM_ALIASES.get(team, [team])

        placeholders = ",".join("?" * len(team_list))
        cur.execute(
            f"SELECT DISTINCT player_id, name, team FROM player_basic WHERE name=? AND team IN ({placeholders})",
            [name, *team_list],
        )
        matches = cur.fetchall()

        if len(matches) == 1:
            pid: int = matches[0]["player_id"]
            resolved.append((name, team, pid))
        elif len(matches) == 0:
            skipped_nomatch.append((name, team))
        else:
            skipped_ambig.append((name, team, len(matches)))

    print(f"=== {TARGET_YEAR} uniform_no NULL 2키 매칭 결과 ===")
    print(f"  유일 매칭(적용 대상): {len(resolved)}명")
    print(f"  동명이인(스킵):       {len(skipped_ambig)}명")
    print(f"  미매칭(스킵):         {len(skipped_nomatch)}명")
    print()

    total_rows = 0
    for name, team, pid in resolved:
        for tbl in TARGET_TABLES:
            cur.execute(
                f"SELECT COUNT(*) FROM {tbl} WHERE player_id IS NULL "
                f"AND player_name=? AND team_code=? AND substr(game_id,1,4)=?",
                (name, team, TARGET_YEAR),
            )
            cnt: int = cur.fetchone()[0]
            if cnt == 0:
                continue
            total_rows += cnt
            tag = "[APPLY]" if apply else "[DRY-RUN]"
            print(f"  {tag} {tbl}: {name}({team}) → player_id={pid} ({cnt}행)")
            if apply:
                cur.execute(
                    f"UPDATE {tbl} SET player_id=? "
                    f"WHERE player_id IS NULL AND player_name=? AND team_code=? "
                    f"AND substr(game_id,1,4)=?",
                    (pid, name, team, TARGET_YEAR),
                )

    print()
    if apply:
        conn.commit()
        print(f"✅ 완료: 총 {total_rows}행 업데이트")
    else:
        print(f"  (dry-run) 예상 업데이트 총 {total_rows}행")
        print("  --apply 플래그를 추가하면 실제로 적용됩니다.")

    conn.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
