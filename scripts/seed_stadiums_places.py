"""Seed script for the requested stadiums/places master tables.

Column names follow the production tables (stadium_name/team/open_year/
lat/lng/homerun_park_factor, places description/open_time/close_time).
Sources: Wikipedia infoboxes (capacity/opened/dimensions/turf), team
stadium pages (heroes amenity/skydome, hanwha ballpark, champions field),
KBO rule book (fence minima). ``homerun_park_factor`` stays NULL here and
is filled by ``--park-factor-year`` via ParkFactorCalculator.
Phone numbers are filled only where a published stadium contact exists;
the rest stay NULL (most venues publish only the club front number).
Places rows fall back to the stadium coordinates (lat/lng are NOT NULL).
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.engine import SessionLocal
from src.repositories.place_repository import PlaceRepository
from src.repositories.stadium_repository import StadiumRepository

logger = logging.getLogger(__name__)

STADIUM_DATA: list[dict[str, Any]] = [
    {
        "stadium_id": "JAMSIL",
        "stadium_name": "잠실야구장",
        "city": "서울특별시 송파구",
        "team": "LG,OB",
        "capacity": 23750,
        "seating_capacity": None,
        "open_year": 1982,
        "left_fence_m": 100.0,
        "center_fence_m": 125.0,
        "fence_height_m": 2.6,
        "turf_type": "천연",
        "bullpen_type": "외야",
        "homerun_park_factor": None,
        "notes": "우측 100m(대칭). 2026시즌 후 철거, 2027~2030 주경기장 임시사용.",
        "lat": 37.5114,
        "lng": 127.0734,
        "address": "서울특별시 송파구 올림픽로 25",
        "phone": None,
    },
    {
        "stadium_id": "MUNHAK",
        "stadium_name": "인천SSG랜더스필드",
        "city": "인천광역시 미추홀구",
        "team": "SSG",
        "capacity": 23600,
        "seating_capacity": None,
        "open_year": 2002,
        "left_fence_m": 95.0,
        "center_fence_m": 120.0,
        "fence_height_m": 2.4,
        "turf_type": "천연",
        "bullpen_type": "외야",
        "homerun_park_factor": None,
        "notes": "우측 95m(대칭).",
        "lat": 37.4351,
        "lng": 126.6908,
        "address": "인천광역시 미추홀구 매소홀로 618",
        "phone": None,
    },
    {
        "stadium_id": "GOCHEOK",
        "stadium_name": "고척스카이돔",
        "city": "서울특별시 구로구",
        "team": "WO",
        "capacity": 16700,
        "seating_capacity": None,
        "open_year": 2015,
        "left_fence_m": 99.0,
        "center_fence_m": 122.0,
        "fence_height_m": 4.0,
        "turf_type": "인조",
        "bullpen_type": "지하",
        "homerun_park_factor": None,
        "notes": "우측 99m(대칭). 돔구장, 우천 취소 없음.",
        "lat": 37.4981,
        "lng": 126.8670,
        "address": "서울특별시 구로구 경인로 430",
        "phone": "02-2128-2337",
    },
    {
        "stadium_id": "SUWON",
        "stadium_name": "수원케이티위즈파크",
        "city": "경기도 수원시 장안구",
        "team": "KT",
        "capacity": 20600,
        "seating_capacity": None,
        "open_year": 2014,
        "left_fence_m": 98.0,
        "center_fence_m": 120.0,
        "fence_height_m": 4.0,
        "turf_type": "천연",
        "bullpen_type": "외야",
        "homerun_park_factor": None,
        "notes": "우측 98m(대칭).",
        "lat": 37.2990,
        "lng": 126.9990,
        "address": "경기도 수원시 장안구 경수대로 893",
        "phone": None,
    },
    {
        "stadium_id": "SAJIK",
        "stadium_name": "부산사직야구장",
        "city": "부산광역시 동래구",
        "team": "LT",
        "capacity": 22990,
        "seating_capacity": None,
        "open_year": 1985,
        "left_fence_m": 95.0,
        "center_fence_m": 118.0,
        "fence_height_m": 4.8,
        "turf_type": "천연",
        "bullpen_type": "덕아웃옆",
        "homerun_park_factor": None,
        "notes": "우측 95m. 2022~2024 보조펜스 6m 운영 후 2025 철거·원복.",
        "lat": 35.1942,
        "lng": 129.0618,
        "address": "부산광역시 동래구 사직로 45",
        "phone": None,
    },
    {
        "stadium_id": "DAEGU",
        "stadium_name": "대구삼성라이온즈파크",
        "city": "대구광역시 수성구",
        "team": "SS",
        "capacity": 24000,
        "seating_capacity": None,
        "open_year": 2016,
        "left_fence_m": 99.5,
        "center_fence_m": 122.5,
        "fence_height_m": 3.6,
        "turf_type": "천연",
        "bullpen_type": "외야",
        "homerun_park_factor": None,
        "notes": "우측 99.5m(대칭).",
        "lat": 35.8261,
        "lng": 128.6789,
        "address": "대구광역시 수성구 야구전설로 1",
        "phone": None,
    },
    {
        "stadium_id": "CHANGWON",
        "stadium_name": "창원NC파크",
        "city": "경상남도 창원시 마산회원구",
        "team": "NC",
        "capacity": 22000,
        "seating_capacity": None,
        "open_year": 2019,
        "left_fence_m": 101.0,
        "center_fence_m": 121.0,
        "fence_height_m": 3.3,
        "turf_type": "천연",
        "bullpen_type": "외야",
        "homerun_park_factor": None,
        "notes": "우측 101m(대칭).",
        "lat": 35.2222,
        "lng": 128.5673,
        "address": "경상남도 창원시 마산회원구 삼호로 77",
        "phone": None,
    },
    {
        "stadium_id": "GWANGJU",
        "stadium_name": "광주기아챔피언스필드",
        "city": "광주광역시 북구",
        "team": "KIA",
        "capacity": 20500,
        "seating_capacity": None,
        "open_year": 2014,
        "left_fence_m": 99.0,
        "center_fence_m": 121.0,
        "fence_height_m": 2.6,
        "turf_type": "천연",
        "bullpen_type": "외야",
        "homerun_park_factor": None,
        "notes": "우측 99m(대칭).",
        "lat": 35.1683,
        "lng": 126.8860,
        "address": "광주광역시 북구 서양로 50",
        "phone": "070-7686-8000",
    },
    {
        "stadium_id": "HANBAT",
        "stadium_name": "대전한화생명볼파크",
        "city": "대전광역시 중구",
        "team": "HH",
        "capacity": 20000,
        "seating_capacity": None,
        "open_year": 2025,
        "left_fence_m": 99.0,
        "center_fence_m": 122.0,
        "fence_height_m": 2.4,
        "turf_type": "천연",
        "bullpen_type": "외야",
        "homerun_park_factor": None,
        "notes": "비대칭: 우측 95m, 우측펜스 높이 8m. 2025 개장 신구장.",
        "lat": 36.3165,
        "lng": 127.4290,
        "address": "대전광역시 중구 대종로 373",
        "phone": "042-630-8200",
    },
]

PLACE_DATA: list[dict[str, Any]] = [
    {
        "stadium_id": "GOCHEOK",
        "category": "음식점",
        "name": "멕시카나강정",
        "description": "내야 2층 2번 통로 맞은편",
        "lat": 37.4981,
        "lng": 126.8670,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "GOCHEOK",
        "category": "음식점",
        "name": "올리브떡볶이",
        "description": "내야 2층 4번 통로 맞은편",
        "lat": 37.4981,
        "lng": 126.8670,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "GOCHEOK",
        "category": "매점",
        "name": "편의점",
        "description": "내야 2층 2번 통로 맞은편",
        "lat": 37.4981,
        "lng": 126.8670,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "GOCHEOK",
        "category": "용품점",
        "name": "히어로즈 용품점",
        "description": "내야 2층 7번 통로 맞은편",
        "lat": 37.4981,
        "lng": 126.8670,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "GOCHEOK",
        "category": "화장실",
        "name": "장애인 화장실",
        "description": "지상 2층 내야 (남 8, 여 8개소)",
        "lat": 37.4981,
        "lng": 126.8670,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "JAMSIL",
        "category": "음식점",
        "name": "더진순대",
        "description": "1루측 1층 매점",
        "lat": 37.5114,
        "lng": 127.0734,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "JAMSIL",
        "category": "매점",
        "name": "GS25 잠실야구장점",
        "description": "1루측 1층 / 3루측 1층",
        "lat": 37.5114,
        "lng": 127.0734,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "SAJIK",
        "category": "음식점",
        "name": "맛찬들 사직점",
        "description": "1루측 1층",
        "lat": 35.1942,
        "lng": 129.0618,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "HANBAT",
        "category": "매점",
        "name": "성심당",
        "description": "1루측 1층",
        "lat": 36.3165,
        "lng": 127.4290,
        "address": None,
        "phone": None,
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
    {
        "stadium_id": "GWANGJU",
        "category": "기타",
        "name": "KIA타이거즈 사무실",
        "description": "챔피언스필드 내 2층",
        "lat": 35.1683,
        "lng": 126.8860,
        "address": None,
        "phone": "070-7686-8000",
        "rating": None,
        "open_time": None,
        "close_time": None,
    },
]

#: Game.stadium venue label -> stadiums.stadium_id (best effort; labels vary).
VENUE_TO_STADIUM_ID: dict[str, str] = {
    "잠실": "JAMSIL",
    "문학": "MUNHAK",
    "인천": "MUNHAK",
    "고척": "GOCHEOK",
    "수원": "SUWON",
    "사직": "SAJIK",
    "부산": "SAJIK",
    "대구": "DAEGU",
    "창원": "CHANGWON",
    "마산": "CHANGWON",
    "광주": "GWANGJU",
    "대전": "HANBAT",
    "한화생명볼파크": "HANBAT",
}


def _seed_rows(repo: Any, rows: list[dict[str, Any]], dry_run: bool) -> int:
    """Save seed rows unless dry-running.

    Args:
        repo: Repository with a save method.
        rows: Rows.
        dry_run: Dry run.

    Returns:
        Row count.

    """
    count = 0
    for data in rows:
        if not dry_run:
            repo.save(data)
        count += 1
    return count


def _import_food_vendors(session: Any, place_repo: PlaceRepository) -> int:
    """Import food vendors as 음식점 places, falling back to stadium coords.

    Args:
        session: Session.
        place_repo: Place repository.

    Returns:
        Imported row count.

    """
    from sqlalchemy import select

    from src.models.stadium_food_vendor import StadiumFoodVendor
    from src.models.stadium import Stadium

    imported = 0
    vendors = list(session.execute(select(StadiumFoodVendor)).scalars().all())
    for vendor in vendors:
        stadium = session.get(Stadium, vendor.stadium_id)
        if stadium is None or stadium.lat is None or stadium.lng is None:
            logger.warning("[SEED] No stadium coords for vendor %r, skipped", vendor.vendor_name)
            continue
        place_repo.save(
            {
                "stadium_id": vendor.stadium_id,
                "category": "음식점",
                "name": vendor.vendor_name,
                "description": vendor.location_text,
                "lat": stadium.lat,
                "lng": stadium.lng,
                "address": None,
                "phone": None,
                "rating": None,
                "open_time": None,
                "close_time": None,
            }
        )
        imported += 1
    return imported


def _backfill_park_factors(session: Any, stadium_repo: StadiumRepository, year: int) -> int:
    """Backfill HR park factors from game data for one season.

    Args:
        session: Session.
        stadium_repo: Stadium repository.
        year: Season year.

    Returns:
        Updated row count.

    """
    from src.aggregators.park_factor_calculator import ParkFactorCalculator

    calc = ParkFactorCalculator(session)
    updated = 0
    for row in calc.calculate(year):
        venue = str(row.get("stadium", ""))
        stadium_id = next(
            (sid for label, sid in VENUE_TO_STADIUM_ID.items() if label in venue),
            None,
        )
        if stadium_id is None:
            logger.warning("[SEED] Unmapped venue %r, skipped", venue)
            continue
        if stadium_repo.update_park_factor(stadium_id, float(row["park_factor"])):
            updated += 1
    return updated


def run(dry_run: bool = False, park_factor_year: int | None = None, from_food: bool = False) -> None:
    """Seed stadiums/places and optionally backfill HR park factors.

    Args:
        dry_run: Preview without writing.
        park_factor_year: Season year for park factor backfill.
        from_food: Also import stadium_food_vendors rows as 음식점 places.

    """
    with SessionLocal() as session:
        stadium_repo = StadiumRepository(session)
        place_repo = PlaceRepository(session)

        stadium_count = _seed_rows(stadium_repo, STADIUM_DATA, dry_run)
        place_count = _seed_rows(place_repo, PLACE_DATA, dry_run)

        # The session runs with autoflush disabled, so flush explicitly: the
        # food import and park-factor backfill below resolve stadiums with
        # session.get(), which cannot see unflushed pending rows.
        if not dry_run:
            session.flush()

        imported = _import_food_vendors(session, place_repo) if from_food and not dry_run else 0
        updated = (
            _backfill_park_factors(session, stadium_repo, park_factor_year)
            if park_factor_year is not None and not dry_run
            else 0
        )

        if not dry_run:
            session.commit()
        logger.info(
            "[SEED] Stadiums: %s, places: %s, food imported: %s, park factors updated: %s (dry_run=%s)",
            stadium_count,
            place_count,
            imported,
            updated,
            dry_run,
        )


def main(argv: list[str] | None = None) -> int:
    """Run the seed CLI.

    Args:
        argv: Argv.

    Returns:
        Process exit code.

    """
    parser = argparse.ArgumentParser(description="Seed stadiums/places master tables.")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--park-factor-year", type=int, default=None)
    parser.add_argument("--from-food", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO)
    run(dry_run=args.dry_run, park_factor_year=args.park_factor_year, from_food=args.from_food)
    return 0


if __name__ == "__main__":
    sys.exit(main())
