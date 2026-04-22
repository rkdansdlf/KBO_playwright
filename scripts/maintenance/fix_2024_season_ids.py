"""Patch 2024 postseason/preseason game season_id values.

Defaults to dry-run and requires --apply before committing updates.
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Callable

from dotenv import load_dotenv
from sqlalchemy import text


sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from src.db.engine import SessionLocal


SEASON_RULES = (
    ("2024-03-09", "2024-03-19", "시범경기"),
    ("2024-07-06", "2024-07-06", "올스타전"),
    ("2024-10-02", "2024-10-03", "와일드카드"),
    ("2024-10-05", "2024-10-11", "준플레이오프"),
    ("2024-10-13", "2024-10-19", "플레이오프"),
    ("2024-10-21", "2024-10-30", "한국시리즈"),
)


def fix_2024_seasons(*, apply: bool = False, log: Callable[[str], None] = print) -> dict[str, int]:
    updated_by_type: dict[str, int] = {}
    with SessionLocal() as session:
        try:
            for start, end, type_name in SEASON_RULES:
                season_id = session.execute(
                    text(
                        """
                        SELECT season_id
                        FROM kbo_seasons
                        WHERE season_year = 2024
                          AND league_type_name = :name
                        LIMIT 1
                        """
                    ),
                    {"name": type_name},
                ).scalar()

                if not season_id:
                    log(f"[WARN] Could not find season_id for {type_name}")
                    updated_by_type[type_name] = 0
                    continue

                result = session.execute(
                    text(
                        """
                        UPDATE game
                        SET season_id = :season_id
                        WHERE game_date BETWEEN :start_date AND :end_date
                          AND (season_id IS NULL OR season_id != :season_id)
                        """
                    ),
                    {
                        "season_id": season_id,
                        "start_date": start,
                        "end_date": end,
                    },
                )
                updated_by_type[type_name] = int(result.rowcount or 0)
                log(f"[PATCH] {type_name} {start}..{end}: {updated_by_type[type_name]} game(s)")

            if apply:
                session.commit()
            else:
                session.rollback()
        except Exception:
            session.rollback()
            raise
    return updated_by_type


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fix 2024 season_id mappings for special date ranges.")
    parser.add_argument("--apply", action="store_true", help="Commit updates. Default is dry-run rollback.")
    return parser


def main() -> int:
    load_dotenv()
    args = build_arg_parser().parse_args()
    if not args.apply:
        print("[DRY-RUN] No changes will be committed. Pass --apply to update rows.")

    fix_2024_seasons(apply=args.apply)
    if args.apply:
        print("[DONE] 2024 season_id cleanup committed.")
    else:
        print("[DONE] Dry-run rolled back.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
