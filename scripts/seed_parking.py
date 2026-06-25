"""Seed script to populate parking_lots and parking_fee_rules tables.
Sources: official stadium/team parking information pages.
"""

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.engine import SessionLocal
from src.repositories.parking_lot_repository import ParkingFeeRuleRepository, ParkingLotRepository

PARKING_DATA = [
    {
        "stadium_id": "JAMSIL",
        "name": "잠실종합운동장 주차장",
        "lot_type": "official",
        "address": "서울특별시 송파구 올림픽로 25",
        "capacity": 3500,
        "walking_minutes": 5,
        "latitude": 37.5114,
        "longitude": 127.0734,
        "operating_hours": "경기 시작 2시간 전 ~ 경기 종료 1시간 후",
        "fee_rules": [
            {
                "vehicle_type": "compact",
                "base_fee": 1000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "free_exit_minutes": 15,
            },
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 15000,
                "free_exit_minutes": 15,
            },
            {
                "vehicle_type": "van",
                "base_fee": 3000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 20000,
                "free_exit_minutes": 15,
            },
            {
                "vehicle_type": "bus",
                "base_fee": 5000,
                "base_minutes": 30,
                "additional_fee": 1000,
                "additional_minutes": 15,
                "daily_max_fee": 30000,
                "free_exit_minutes": 15,
            },
        ],
    },
    {
        "stadium_id": "JAMSIL",
        "name": "잠실롯데월드 주차장",
        "lot_type": "public",
        "capacity": 2000,
        "walking_minutes": 10,
        "address": "서울특별시 송파구 올림픽로 240",
        "latitude": 37.5118,
        "longitude": 127.1000,
        "fee_rules": [
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 12000,
            },
        ],
    },
    {
        "stadium_id": "MUNHAK",
        "name": "문학경기장 주차장",
        "lot_type": "official",
        "capacity": 4002,
        "walking_minutes": 3,
        "address": "인천광역시 미추홀구 매소홀로 618",
        "operating_hours": "경기 시작 2시간 전 ~ 경기 종료 1시간 후",
        "fee_rules": [
            {
                "vehicle_type": "compact",
                "base_fee": 1000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "free_exit_minutes": 15,
            },
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 10000,
                "free_exit_minutes": 15,
            },
            {
                "vehicle_type": "bus",
                "base_fee": 5000,
                "base_minutes": 30,
                "additional_fee": 1000,
                "additional_minutes": 15,
                "daily_max_fee": 20000,
                "free_exit_minutes": 15,
            },
        ],
    },
    {
        "stadium_id": "SAJIK",
        "name": "사직야구장 주차장",
        "lot_type": "official",
        "capacity": 1500,
        "walking_minutes": 3,
        "address": "부산광역시 동래구 사직동 65",
        "operating_hours": "경기 시작 2시간 전 ~ 경기 종료 1시간 후",
        "fee_rules": [
            {
                "vehicle_type": "compact",
                "base_fee": 1000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "free_exit_minutes": 15,
            },
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 12000,
                "free_exit_minutes": 15,
            },
            {
                "vehicle_type": "bus",
                "base_fee": 5000,
                "base_minutes": 30,
                "additional_fee": 1000,
                "additional_minutes": 15,
                "daily_max_fee": 25000,
                "free_exit_minutes": 15,
            },
        ],
    },
    {
        "stadium_id": "DAEGU",
        "name": "라이온즈파크 주차장",
        "lot_type": "official",
        "capacity": 3000,
        "walking_minutes": 5,
        "address": "대구광역시 수성구 야구전설로 1",
        "fee_rules": [
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 10000,
            },
        ],
    },
    {
        "stadium_id": "HANBAT",
        "name": "한화생명이글스파크 주차장",
        "lot_type": "official",
        "capacity": 1800,
        "walking_minutes": 5,
        "address": "대전광역시 중구 대종로 373",
        "fee_rules": [
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 10000,
            },
        ],
    },
    {
        "stadium_id": "GWANGJU",
        "name": "기아챔피언스필드 주차장",
        "lot_type": "official",
        "capacity": 2500,
        "walking_minutes": 5,
        "address": "광주광역시 북구 설죽로 24",
        "fee_rules": [
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 10000,
            },
        ],
    },
    {
        "stadium_id": "CHANGWON",
        "name": "창원NC파크 주차장",
        "lot_type": "official",
        "capacity": 2000,
        "walking_minutes": 5,
        "address": "경상남도 창원시 마산회원구 야구장길 44",
        "fee_rules": [
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 10000,
            },
        ],
    },
    {
        "stadium_id": "SUWON",
        "name": "KT위즈파크 주차장",
        "lot_type": "official",
        "capacity": 2000,
        "walking_minutes": 5,
        "address": "경기도 수원시 장안구 경수대로 333",
        "fee_rules": [
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 10000,
            },
        ],
    },
    {
        "stadium_id": "GOCHEOK",
        "name": "고척돔 주차장",
        "lot_type": "official",
        "capacity": 1200,
        "walking_minutes": 3,
        "address": "서울특별시 구로구 경인로 430",
        "fee_rules": [
            {
                "vehicle_type": "sedan",
                "base_fee": 2000,
                "base_minutes": 30,
                "additional_fee": 500,
                "additional_minutes": 15,
                "daily_max_fee": 12000,
            },
        ],
    },
]


def run(dry_run: bool = False) -> None:
    with SessionLocal() as session:
        lot_repo = ParkingLotRepository(session)
        fee_repo = ParkingFeeRuleRepository(session)

        lot_count = 0
        fee_count = 0

        for park_data in PARKING_DATA:
            fee_rows = list(park_data.get("fee_rules", []))
            lot = lot_repo.save(park_data)
            lot_count += 1
            for fee_data in fee_rows:
                fee_data["parking_lot_id"] = lot.id
                fee_repo.save(fee_data)
                fee_count += 1

        if not dry_run:
            session.commit()
        logger.info("[SEED] Parking: %s lots, %s fee rules (dry_run=%s)", lot_count, fee_count, dry_run)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
