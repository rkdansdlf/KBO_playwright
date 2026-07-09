"""Seed script to populate stadium_seat_sections table with known seat section data.
This data should be updated when new seat configurations are announced.
"""

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from typing import Any

from src.db.engine import SessionLocal
from src.repositories.stadium_seat_section_repository import StadiumSeatSectionRepository

SEAT_DATA: list[dict[str, Any]] = [
    # === 잠실 (LG / 두산 공동 사용) ===
    {
        "stadium_id": "JAMSIL",
        "section_name": "블루석",
        "seat_grade": "블루석",
        "base_side": "first_base",
        "is_home_cheering": False,
    },
    {
        "stadium_id": "JAMSIL",
        "section_name": "오렌지석",
        "seat_grade": "오렌지석",
        "base_side": "first_base",
        "is_home_cheering": False,
    },
    {
        "stadium_id": "JAMSIL",
        "section_name": "레드석",
        "seat_grade": "레드석",
        "base_side": "third_base",
        "is_away_cheering": False,
    },
    {"stadium_id": "JAMSIL", "section_name": "네이비석", "seat_grade": "네이비석", "base_side": "center"},
    {
        "stadium_id": "JAMSIL",
        "section_name": "그린응원석",
        "seat_grade": "그린응원석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {
        "stadium_id": "JAMSIL",
        "section_name": "그린석",
        "seat_grade": "그린석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    {
        "stadium_id": "JAMSIL",
        "section_name": "프리미엄석",
        "seat_grade": "프리미엄석",
        "base_side": "first_base",
        "is_table_seat": False,
    },
    {
        "stadium_id": "JAMSIL",
        "section_name": "테이블석",
        "seat_grade": "테이블석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {
        "stadium_id": "JAMSIL",
        "section_name": "익사이팅존",
        "seat_grade": "익사이팅존",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    # === 문학 (SSG) ===
    {
        "stadium_id": "MUNHAK",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {
        "stadium_id": "MUNHAK",
        "section_name": "테이블석",
        "seat_grade": "테이블석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {"stadium_id": "MUNHAK", "section_name": "스카이석", "seat_grade": "스카이석", "base_side": "first_base"},
    {"stadium_id": "MUNHAK", "section_name": "블루석", "seat_grade": "블루석", "base_side": "first_base"},
    {"stadium_id": "MUNHAK", "section_name": "오렌지석", "seat_grade": "오렌지석", "base_side": "third_base"},
    {
        "stadium_id": "MUNHAK",
        "section_name": "외야잔디석",
        "seat_grade": "외야잔디석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    # === 사직 (롯데) ===
    {
        "stadium_id": "SAJIK",
        "section_name": "테이블석",
        "seat_grade": "테이블석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {
        "stadium_id": "SAJIK",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {"stadium_id": "SAJIK", "section_name": "네이비석", "seat_grade": "네이비석", "base_side": "first_base"},
    {"stadium_id": "SAJIK", "section_name": "레드석", "seat_grade": "레드석", "base_side": "third_base"},
    {"stadium_id": "SAJIK", "section_name": "오렌지석", "seat_grade": "오렌지석", "base_side": "third_base"},
    {"stadium_id": "SAJIK", "section_name": "블루석", "seat_grade": "블루석", "base_side": "third_base"},
    {
        "stadium_id": "SAJIK",
        "section_name": "외야잔디석",
        "seat_grade": "외야잔디석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    {"stadium_id": "SAJIK", "section_name": "외야그린석", "seat_grade": "외야그린석", "base_side": "outfield"},
    # === 대구 (삼성) ===
    {
        "stadium_id": "DAEGU",
        "section_name": "프리미엄석",
        "seat_grade": "프리미엄석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {
        "stadium_id": "DAEGU",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {"stadium_id": "DAEGU", "section_name": "블루석", "seat_grade": "블루석", "base_side": "first_base"},
    {"stadium_id": "DAEGU", "section_name": "레드석", "seat_grade": "레드석", "base_side": "third_base"},
    {
        "stadium_id": "DAEGU",
        "section_name": "외야잔디석",
        "seat_grade": "외야잔디석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    # === 대전 (한화) ===
    {
        "stadium_id": "HANBAT",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {"stadium_id": "HANBAT", "section_name": "레드석", "seat_grade": "레드석", "base_side": "first_base"},
    {"stadium_id": "HANBAT", "section_name": "블루석", "seat_grade": "블루석", "base_side": "first_base"},
    {"stadium_id": "HANBAT", "section_name": "네이비석", "seat_grade": "네이비석", "base_side": "third_base"},
    {"stadium_id": "HANBAT", "section_name": "오렌지석", "seat_grade": "오렌지석", "base_side": "third_base"},
    {
        "stadium_id": "HANBAT",
        "section_name": "외야잔디석",
        "seat_grade": "외야잔디석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    # === 광주 (KIA) ===
    {
        "stadium_id": "GWANGJU",
        "section_name": "프리미엄석",
        "seat_grade": "프리미엄석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {
        "stadium_id": "GWANGJU",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {"stadium_id": "GWANGJU", "section_name": "블루석", "seat_grade": "블루석", "base_side": "first_base"},
    {"stadium_id": "GWANGJU", "section_name": "레드석", "seat_grade": "레드석", "base_side": "third_base"},
    {
        "stadium_id": "GWANGJU",
        "section_name": "외야잔디석",
        "seat_grade": "외야잔디석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    # === 창원 (NC) ===
    {
        "stadium_id": "CHANGWON",
        "section_name": "테이블석",
        "seat_grade": "테이블석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {
        "stadium_id": "CHANGWON",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {"stadium_id": "CHANGWON", "section_name": "레드석", "seat_grade": "레드석", "base_side": "first_base"},
    {"stadium_id": "CHANGWON", "section_name": "블루석", "seat_grade": "블루석", "base_side": "third_base"},
    {"stadium_id": "CHANGWON", "section_name": "네이비석", "seat_grade": "네이비석", "base_side": "third_base"},
    {
        "stadium_id": "CHANGWON",
        "section_name": "외야잔디석",
        "seat_grade": "외야잔디석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    # === 수원 (KT) ===
    {
        "stadium_id": "SUWON",
        "section_name": "테이블석",
        "seat_grade": "테이블석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {
        "stadium_id": "SUWON",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {"stadium_id": "SUWON", "section_name": "블루석", "seat_grade": "블루석", "base_side": "first_base"},
    {"stadium_id": "SUWON", "section_name": "레드석", "seat_grade": "레드석", "base_side": "third_base"},
    {
        "stadium_id": "SUWON",
        "section_name": "외야잔디석",
        "seat_grade": "외야잔디석",
        "base_side": "outfield",
        "is_home_cheering": True,
    },
    # === 고척 (키움) ===
    {
        "stadium_id": "GOCHEOK",
        "section_name": "테이블석",
        "seat_grade": "테이블석",
        "base_side": "first_base",
        "is_table_seat": True,
    },
    {
        "stadium_id": "GOCHEOK",
        "section_name": "익사이팅석",
        "seat_grade": "익사이팅석",
        "base_side": "first_base",
        "is_home_cheering": True,
    },
    {"stadium_id": "GOCHEOK", "section_name": "레드석", "seat_grade": "레드석", "base_side": "first_base"},
    {"stadium_id": "GOCHEOK", "section_name": "블루석", "seat_grade": "블루석", "base_side": "third_base"},
    {"stadium_id": "GOCHEOK", "section_name": "네이비석", "seat_grade": "네이비석", "base_side": "third_base"},
    {"stadium_id": "GOCHEOK", "section_name": "오렌지석", "seat_grade": "오렌지석", "base_side": "center"},
]


def run(dry_run: bool = False) -> None:
    with SessionLocal() as session:
        repo = StadiumSeatSectionRepository(session)
        created = 0
        for data in SEAT_DATA:
            if not dry_run:
                repo.save(data)
            created += 1
        if not dry_run:
            session.flush()
            session.commit()
        logger.info("[SEED] Seat sections: %s seed rows (dry_run=%s)", created, dry_run)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
