"""
Seed script to populate stadium_food_vendors and stadium_food_menu_items tables.
Initial seed data based on known stadium food information.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.engine import SessionLocal
from src.repositories.stadium_food_repository import StadiumFoodMenuItemRepository, StadiumFoodVendorRepository

FOOD_DATA: list[dict] = [
    # === 잠실 ===
    {
        "stadium_id": "JAMSIL",
        "vendor_name": "맥도날드 잠실야구장점",
        "location_text": "1루侧 1층 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "빅맥세트", "price": 6500, "category": "meal"},
            {"menu_name": "맥너겟(6조각)", "price": 3500, "category": "snack"},
            {"menu_name": "후렌치후라이(L)", "price": 2500, "category": "snack"},
            {"menu_name": "콜라(L)", "price": 2000, "category": "drink"},
        ],
    },
    {
        "stadium_id": "JAMSIL",
        "vendor_name": "더진순대",
        "location_text": "1루侧 1층 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "순대", "price": 5000, "category": "snack"},
            {"menu_name": "야채순대", "price": 5500, "category": "snack"},
            {"menu_name": "떡볶이", "price": 4000, "category": "snack"},
        ],
    },
    {
        "stadium_id": "JAMSIL",
        "vendor_name": "더진치킨",
        "location_text": "1루侧 2층 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨(한마리)", "price": 18000, "category": "chicken"},
            {"menu_name": "양념치킨(한마리)", "price": 19000, "category": "chicken"},
            {"menu_name": "치킨윙(10조각)", "price": 12000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "JAMSIL",
        "vendor_name": "GS25 잠실야구장점",
        "location_text": "1루侧 1층 / 3루侧 1층",
        "confidence": "high",
        "menus": [
            {"menu_name": "참치마요삼각김밥", "price": 1500, "category": "snack"},
            {"menu_name": "핫바", "price": 1500, "category": "snack"},
            {"menu_name": "맥주(캔)", "price": 4000, "category": "beer"},
            {"menu_name": "음료수", "price": 2000, "category": "drink"},
        ],
    },
    {
        "stadium_id": "JAMSIL",
        "vendor_name": "투썸플레이스",
        "location_text": "1루侧 1층",
        "confidence": "high",
        "menus": [
            {"menu_name": "아메리카노", "price": 4500, "category": "drink"},
            {"menu_name": "카페라떼", "price": 5000, "category": "drink"},
            {"menu_name": "스트로베리요거트", "price": 6000, "category": "drink"},
        ],
    },
    # === 문학 ===
    {
        "stadium_id": "MUNHAK",
        "vendor_name": "문학랜더스 치킨",
        "location_text": "1루侧 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 17000, "category": "chicken"},
            {"menu_name": "양념치킨", "price": 18000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "MUNHAK",
        "vendor_name": "메가커피 문학점",
        "location_text": "1루侧 1층",
        "confidence": "high",
        "menus": [
            {"menu_name": "메가아메리카노", "price": 3000, "category": "drink"},
            {"menu_name": "딸기주스", "price": 4500, "category": "drink"},
        ],
    },
    # === 사직 ===
    {
        "stadium_id": "SAJIK",
        "vendor_name": "맛찬들 사직점",
        "location_text": "1루侧 1층",
        "confidence": "high",
        "menus": [
            {"menu_name": "돼지국밥", "price": 9000, "category": "meal"},
            {"menu_name": "순대국밥", "price": 9000, "category": "meal"},
        ],
    },
    {
        "stadium_id": "SAJIK",
        "vendor_name": "자이언츠 치킨",
        "location_text": "1루侧 2층",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 18000, "category": "chicken"},
            {"menu_name": "양념치킨", "price": 19000, "category": "chicken"},
            {"menu_name": "치킨너겟", "price": 6000, "category": "chicken"},
        ],
    },
    # === 대구 ===
    {
        "stadium_id": "DAEGU",
        "vendor_name": "라이온즈 치킨",
        "location_text": "1루侧 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 17000, "category": "chicken"},
            {"menu_name": "간장치킨", "price": 18000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "DAEGU",
        "vendor_name": "대구명물 막창",
        "location_text": "1루侧 1층 푸드존",
        "confidence": "medium",
        "menus": [
            {"menu_name": "막창구이", "price": 12000, "category": "meal"},
            {"menu_name": "곱창구이", "price": 13000, "category": "meal"},
        ],
    },
    # === 대전 ===
    {
        "stadium_id": "HANBAT",
        "vendor_name": "이글스 치킨",
        "location_text": "1루侧 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 17000, "category": "chicken"},
            {"menu_name": "양념치킨", "price": 18000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "HANBAT",
        "vendor_name": "성심당",
        "location_text": "1루侧 1층",
        "confidence": "high",
        "menus": [
            {"menu_name": "튀김소보루", "price": 1500, "category": "snack"},
            {"menu_name": "메아리찹쌀떡", "price": 2000, "category": "dessert"},
            {"menu_name": "케이크(조각)", "price": 4500, "category": "dessert"},
        ],
    },
    # === 광주 ===
    {
        "stadium_id": "GWANGJU",
        "vendor_name": "타이거즈 치킨",
        "location_text": "1루侧 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 17000, "category": "chicken"},
            {"menu_name": "매운치킨", "price": 18000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "GWANGJU",
        "vendor_name": "광주명가 떡볶이",
        "location_text": "3루侧 매점",
        "confidence": "medium",
        "menus": [
            {"menu_name": "떡볶이", "price": 4000, "category": "snack"},
            {"menu_name": "튀김세트", "price": 5000, "category": "snack"},
        ],
    },
    # === 창원 ===
    {
        "stadium_id": "CHANGWON",
        "vendor_name": "다이노스 치킨",
        "location_text": "1루侧 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 17000, "category": "chicken"},
            {"menu_name": "갈비치킨", "price": 19000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "CHANGWON",
        "vendor_name": "창원할매 떡볶이",
        "location_text": "3루侧 매점",
        "confidence": "medium",
        "menus": [
            {"menu_name": "국물떡볶이", "price": 4000, "category": "snack"},
            {"menu_name": "찹쌀순대", "price": 5000, "category": "snack"},
        ],
    },
    # === 수원 ===
    {
        "stadium_id": "SUWON",
        "vendor_name": "위즈 치킨",
        "location_text": "1루侧 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 17000, "category": "chicken"},
            {"menu_name": "소금구이치킨", "price": 18000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "SUWON",
        "vendor_name": "수원갈비",
        "location_text": "1루侧 1층 푸드존",
        "confidence": "medium",
        "menus": [
            {"menu_name": "떡갈비버거", "price": 8000, "category": "meal"},
            {"menu_name": "소떡소떡", "price": 4000, "category": "snack"},
        ],
    },
    # === 고척 ===
    {
        "stadium_id": "GOCHEOK",
        "vendor_name": "고척돔 치킨",
        "location_text": "1루侧 매점",
        "confidence": "high",
        "menus": [
            {"menu_name": "후라이드치킨", "price": 17000, "category": "chicken"},
            {"menu_name": "간장치킨", "price": 18000, "category": "chicken"},
        ],
    },
    {
        "stadium_id": "GOCHEOK",
        "vendor_name": "구로명가 떡볶이",
        "location_text": "3루侧 매점",
        "confidence": "medium",
        "menus": [
            {"menu_name": "로제떡볶이", "price": 5000, "category": "snack"},
            {"menu_name": "김말이튀김", "price": 3000, "category": "snack"},
        ],
    },
]


def run(dry_run: bool = False) -> None:
    with SessionLocal() as session:
        vendor_repo = StadiumFoodVendorRepository(session)
        menu_repo = StadiumFoodMenuItemRepository(session)

        vendor_count = 0
        menu_count = 0

        for data in FOOD_DATA:
            menus = list(data.get("menus", []))
            vendor = vendor_repo.save(data)
            vendor_count += 1
            for menu_data in menus:
                menu_data["vendor_id"] = vendor.id
                menu_repo.save(menu_data)
                menu_count += 1

        if not dry_run:
            session.commit()
        print(f"[SEED] Food: {vendor_count} vendors, {menu_count} menu items (dry_run={dry_run})")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    run(dry_run=args.dry_run)
