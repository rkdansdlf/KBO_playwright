"""
데이터베이스 상태를 점검하는 CLI 스크립트.

환경 변수에 설정된 `DATABASE_URL`을 사용하여 데이터베이스에 연결하고,
연결 성공 여부, 테이블 목록, 주요 테이블의 레코드 수를 출력하여
데이터베이스의 현재 상태를 빠르게 진단할 수 있도록 돕습니다.

사용법:
  python -m src.cli.db_healthcheck
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from typing import List

from sqlalchemy import text, inspect

from src.db.engine import Engine, DATABASE_URL
from src.utils.safe_print import safe_print as print


def main(argv: List[str] | None = None) -> None:
    """데이터베이스 상태 점검을 수행하는 메인 함수."""
    url = os.getenv("DATABASE_URL", DATABASE_URL)
    dialect = Engine.url.get_backend_name()

    print("\n=== DB Healthcheck ===")
    print(f"URL: {url}")
    print(f"Dialect: {dialect}")

    # 1. 데이터베이스 연결 테스트
    try:
        with Engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("Connectivity: OK")
    except Exception as e:
        print(f"Connectivity: FAILED -> {e}")
        return

    # 2. 테이블 목록 조회
    try:
        insp = inspect(Engine)
        tables = insp.get_table_names()
        print(f"Tables: {len(tables)} found")
        if tables:
            # 최대 10개의 테이블 이름 출력
            for t in tables[:10]:
                print(f"  - {t}")
    except Exception as e:
        print(f"Introspection failed: {e}")

    # 3. 주요 테이블의 레코드 수 집계
    for table in [
        "players",
        "teams",
        "game_schedules",
        "player_season_batting",
        "player_game_batting",
        "player_game_pitching",
        "game_play_by_play",
    ]:
        try:
            with Engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar_one()
                print(f"{table}: {count}")
        except Exception:
            # 테이블이 존재하지 않으면 조용히 넘어감
            continue

    print("\nHealthcheck complete.\n")


if __name__ == "__main__":
    main()

