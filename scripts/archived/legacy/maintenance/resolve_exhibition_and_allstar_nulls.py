#!/usr/bin/env python3
"""Resolve exhibition, All-Star, and historical NULL player_ids in game stats."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from sqlalchemy import text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.db.engine import SessionLocal


def run_resolution():
    with SessionLocal() as session:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 1. 2026-03-13 Exhibition Game Batting/Lineups
        ex_batting = {
            "현원회": 50840,
            "이승민": 54806,
            "문상준": 50007,
            "정해원": 53659,
            "김석환": 67610,
            "한준희": 56619,
            "김태훈": 65040,
            "오재원": 56754,
            "최유빈": 56793,
            "브룩스": 56322,
            "최재영": 56338,
            "김지석": 56326,
            "양현종": 77637,
            "김대한": 69238,
            "김주오": 56266,
            "박지훈": 50204,
            "김민석": 53554,
        }
        for name, pid in ex_batting.items():
            session.execute(
                text("""
                    UPDATE game_batting_stats
                    SET player_id = :pid, updated_at = :now
                    WHERE player_name = :name AND game_id LIKE '20260313%' AND player_id IS NULL
                """),
                {"pid": pid, "name": name, "now": now_str},
            )
            session.execute(
                text("""
                    UPDATE game_lineups
                    SET player_id = :pid, updated_at = :now
                    WHERE player_name = :name AND game_id LIKE '20260313%' AND player_id IS NULL
                """),
                {"pid": pid, "name": name, "now": now_str},
            )

        # 2. 2026-03-13 Exhibition Game Pitching
        ex_pitching = {
            "타케다": 56823,
            "김민": 68043,
            "박준영": 52731,
            "김성진": 51301,
        }
        for name, pid in ex_pitching.items():
            session.execute(
                text("""
                    UPDATE game_pitching_stats
                    SET player_id = :pid, updated_at = :now
                    WHERE player_name = :name AND game_id LIKE '20260313%' AND player_id IS NULL
                """),
                {"pid": pid, "name": name, "now": now_str},
            )

        # 이승현 (SS) -> 선발/구원 구분
        session.execute(
            text("""
                UPDATE game_pitching_stats
                SET player_id = 51454, updated_at = :now
                WHERE player_name = '이승현' AND team_code = 'SS' AND game_id LIKE '20260313%' AND is_starting = 1 AND player_id IS NULL
            """),
            {"now": now_str},
        )
        session.execute(
            text("""
                UPDATE game_pitching_stats
                SET player_id = 60146, updated_at = :now
                WHERE player_name = '이승현' AND team_code = 'SS' AND game_id LIKE '20260313%' AND is_starting = 0 AND player_id IS NULL
            """),
            {"now": now_str},
        )

        # 3. 2026-05-23 game_pitching_stats 김성진
        session.execute(
            text("""
                UPDATE game_pitching_stats
                SET player_id = 51301, updated_at = :now
                WHERE player_name = '김성진' AND team_code = 'KH' AND game_id = '20260523WOLG0' AND player_id IS NULL
            """),
            {"now": now_str},
        )

        # 4. 2019-07-21 All-Star Game (20190721EAWE0)
        session.execute(
            text("""
                UPDATE game_batting_stats
                SET player_id = 76290, updated_at = :now
                WHERE player_name = '김현수' AND game_id = '20190721EAWE0' AND player_id IS NULL
            """),
            {"now": now_str},
        )
        session.execute(
            text("""
                UPDATE game_lineups
                SET player_id = 76290, updated_at = :now
                WHERE player_name = '김현수' AND game_id = '20190721EAWE0' AND player_id IS NULL
            """),
            {"now": now_str},
        )

        as_pitching = {
            "알칸타라": 2661,
            "장시환": 77318,
            "김태훈": 79847,
            "루친스키": 69940,
            "하준영": 68639,
        }
        for name, pid in as_pitching.items():
            session.execute(
                text("""
                    UPDATE game_pitching_stats
                    SET player_id = :pid, updated_at = :now
                    WHERE player_name = :name AND game_id = '20190721EAWE0' AND player_id IS NULL
                """),
                {"pid": pid, "name": name, "now": now_str},
            )

        # 5. 2010 Korean Series 이승호 (SK)
        session.execute(
            text("""
                UPDATE game_pitching_stats
                SET player_id = 70820, updated_at = :now
                WHERE player_name = '이승호' AND team_code = 'SK' AND game_id LIKE '2010%' AND player_id IS NULL
            """),
            {"now": now_str},
        )

        # 6. 2018 Postseason 김태혁 (20181102NXSK0)
        session.execute(
            text("""
                UPDATE game_pitching_stats
                SET player_id = 76430, updated_at = :now
                WHERE player_name = '김태혁' AND team_code = 'NX' AND game_id = '20181102NXSK0' AND player_id IS NULL
            """),
            {"now": now_str},
        )

        # 7. 2021-10-07 이성곤 (20211007SKHH0)
        session.execute(
            text("""
                UPDATE game_batting_stats
                SET player_id = 64266, updated_at = :now
                WHERE player_name = '이성곤' AND team_code = 'HH' AND game_id = '20211007SKHH0' AND player_id IS NULL
            """),
            {"now": now_str},
        )
        session.execute(
            text("""
                UPDATE game_lineups
                SET player_id = 64266, updated_at = :now
                WHERE player_name = '이성곤' AND team_code = 'HH' AND game_id = '20211007SKHH0' AND player_id IS NULL
            """),
            {"now": now_str},
        )

        # 7.5 Additional All-Star and exhibition game pitchers
        session.execute(
            text("""
                UPDATE game_pitching_stats
                SET player_id = 74556, updated_at = :now
                WHERE player_name = '허준혁' AND team_code = 'LT' AND game_id = '20100320OBLT0' AND player_id IS NULL
            """),
            {"now": now_str},
        )
        as_2010 = {"봉중근": 77147, "양현종": 77637, "금민철": 75258}
        for name, pid in as_2010.items():
            session.execute(
                text("""
                    UPDATE game_pitching_stats
                    SET player_id = :pid, updated_at = :now
                    WHERE player_name = :name AND game_id = '20100724WEEA0' AND player_id IS NULL
                """),
                {"pid": pid, "name": name, "now": now_str},
            )
        as_2012 = {"류현진": 76715, "주키치": 61154, "앤서니": 62644, "유원상": 76757, "김혁민": 77748, "이용찬": 77211}
        for name, pid in as_2012.items():
            session.execute(
                text("""
                    UPDATE game_pitching_stats
                    SET player_id = :pid, updated_at = :now
                    WHERE player_name = :name AND game_id = '20120721WEEA0' AND player_id IS NULL
                """),
                {"pid": pid, "name": name, "now": now_str},
            )
        as_2011 = {"김선우": 78232, "니퍼트": 61240, "주키치": 61154}
        for name, pid in as_2011.items():
            session.execute(
                text("""
                    UPDATE game_pitching_stats
                    SET player_id = :pid, updated_at = :now
                    WHERE player_name = :name AND game_id = '20110723EAWE0' AND player_id IS NULL
                """),
                {"pid": pid, "name": name, "now": now_str},
            )

        # 8. game 테이블 자체도 이 날짜들의 updated_at 갱신해 주어 sync가 잡히도록 함
        affected_games = [
            "20260313SKHT0",
            "20260313SSHH0",
            "20260313WOOB0",
            "20260523WOLG0",
            "20190721EAWE0",
            "20101016SSSK0",
            "20101018SKSS0",
            "20101019SKSS0",
            "20181102NXSK0",
            "20211007SKHH0",
            "20100320OBLT0",
            "20100724WEEA0",
            "20120721WEEA0",
            "20110723EAWE0",
        ]
        gids_str = ",".join(f"'{gid}'" for gid in affected_games)
        session.execute(
            text(f"""
                UPDATE game
                SET updated_at = :now
                WHERE game_id IN ({gids_str})
            """),
            {"now": now_str},
        )

        session.commit()

        print("🎉 Exhibition, All-Star, and historical player_ids resolved successfully!")


if __name__ == "__main__":
    run_resolution()
