"""DataSource 초기 시드 데이터를 데이터베이스에 등록하는 CLI 스크립트.

P0/P1/P2 대상 출처를 DataSource 테이블에 등록합니다.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository

logger = logging.getLogger(__name__)

SEED_DATA: list[dict] = [
    # === P0: 이벤트 ===
    {
        "source_key": "kbo_official_events",
        "source_type": "official_kbo",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.koreabaseball.com",
        "is_active": True,
    },
    {
        "source_key": "lg_twins_events",
        "source_type": "official_team",
        "team_id": "LG",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.lgtwins.com",
        "is_active": True,
    },
    {
        "source_key": "hanwha_eagles_events",
        "source_type": "official_team",
        "team_id": "HH",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.hanwhaeagles.co.kr",
        "is_active": True,
    },
    {
        "source_key": "doosan_bears_events",
        "source_type": "official_team",
        "team_id": "OB",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.doosanbears.com",
        "is_active": True,
    },
    {
        "source_key": "ssg_landers_events",
        "source_type": "official_team",
        "team_id": "SK",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.ssglanders.com",
        "is_active": True,
    },
    {
        "source_key": "nc_dinos_events",
        "source_type": "official_team",
        "team_id": "NC",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.ncdinos.com",
        "is_active": True,
    },
    {
        "source_key": "kia_tigers_events",
        "source_type": "official_team",
        "team_id": "HT",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.kiatigers.com",
        "is_active": True,
    },
    {
        "source_key": "lotte_giants_events",
        "source_type": "official_team",
        "team_id": "LT",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.giantsclub.com",
        "is_active": True,
    },
    {
        "source_key": "samsung_lions_events",
        "source_type": "official_team",
        "team_id": "SS",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.samsunglions.com",
        "is_active": True,
    },
    {
        "source_key": "kt_wiz_events",
        "source_type": "official_team",
        "team_id": "KT",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.ktwiz.co.kr",
        "is_active": True,
    },
    {
        "source_key": "kiwoom_heroes_events",
        "source_type": "official_team",
        "team_id": "WO",
        "target_domain": "event",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.heroesbaseball.co.kr",
        "is_active": True,
    },
    # === P0: 콜업/말소 ===
    {
        "source_key": "kbo_today_roster",
        "source_type": "official_kbo",
        "target_domain": "roster",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://m.koreabaseball.com/Kbo/PlayerAdd.aspx",
        "is_active": True,
    },
    {
        "source_key": "kbo_player_register",
        "source_type": "official_kbo",
        "target_domain": "roster",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.koreabaseball.com/Player/Register.aspx",
        "is_active": True,
    },
    {
        "source_key": "kbo_player_movement",
        "source_type": "official_kbo",
        "target_domain": "roster",
        "reliability": "high",
        "crawl_frequency": "daily",
        "base_url": "https://www.koreabaseball.com/Player/Trade.aspx",
        "is_active": True,
    },
    # === P0: 티켓 가격/예매처/예매오픈 ===
    {
        "source_key": "kbo_ticket_map",
        "source_type": "official_kbo",
        "target_domain": "ticket",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.koreabaseball.com/Kbo/League/Map.aspx",
        "is_active": True,
    },
    {
        "source_key": "lg_twins_ticket",
        "source_type": "official_team",
        "team_id": "LG",
        "target_domain": "ticket",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.lgtwins.com/ticket/general",
        "is_active": True,
    },
    # === P0: 티켓 (팀별 추가) ===
    {
        "source_key": "hanwha_eagles_ticket",
        "source_type": "official_team",
        "team_id": "HH",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.hanwhaeagles.co.kr",
        "is_active": True,
    },
    {
        "source_key": "samsung_lions_ticket",
        "source_type": "official_team",
        "team_id": "SS",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.samsunglions.com",
        "is_active": True,
    },
    {
        "source_key": "kt_wiz_ticket",
        "source_type": "official_team",
        "team_id": "KT",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.ktwiz.co.kr",
        "is_active": True,
    },
    {
        "source_key": "doosan_bears_ticket",
        "source_type": "official_team",
        "team_id": "OB",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.doosanbears.com",
        "is_active": True,
    },
    {
        "source_key": "lotte_giants_ticket",
        "source_type": "official_team",
        "team_id": "LT",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.giantsclub.com",
        "is_active": True,
    },
    {
        "source_key": "kia_tigers_ticket",
        "source_type": "official_team",
        "team_id": "HT",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.kiatigers.com",
        "is_active": True,
    },
    {
        "source_key": "nc_dinos_ticket",
        "source_type": "official_team",
        "team_id": "NC",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.ncdinos.com",
        "is_active": True,
    },
    {
        "source_key": "ssg_landers_ticket",
        "source_type": "official_team",
        "team_id": "SK",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.ssglanders.com",
        "is_active": True,
    },
    {
        "source_key": "kiwoom_heroes_ticket",
        "source_type": "official_team",
        "team_id": "WO",
        "target_domain": "ticket",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.heroesbaseball.co.kr",
        "is_active": True,
    },
    # === P1: 좌석 ===
    {
        "source_key": "lg_twins_seat",
        "source_type": "official_team",
        "team_id": "LG",
        "target_domain": "seat",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.lgtwins.com/ticket/general",
        "is_active": True,
    },
    # === P1: 주차 ===
    {
        "source_key": "jamsil_parking_official",
        "source_type": "official_kbo",
        "stadium_id": "JAMSIL",
        "target_domain": "parking",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://stadium.seoul.go.kr/about/park-info",
        "is_active": True,
    },
    {
        "source_key": "ssg_landers_parking",
        "source_type": "official_team",
        "team_id": "SK",
        "target_domain": "parking",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.ssglanders.com/stadium/parking",
        "is_active": True,
    },
    # === P2: 먹거리 ===
    {
        "source_key": "gujangfood_com",
        "source_type": "third_party",
        "target_domain": "food",
        "reliability": "medium",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.gujangfood.com",
        "is_active": True,
    },
    {
        "source_key": "lotte_giants_fnb",
        "source_type": "official_team",
        "team_id": "LT",
        "target_domain": "food",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.giantsclub.com/food",
        "is_active": True,
    },
    # === P1: 좌석 (보강) ===
    {
        "source_key": "seoul_stadium_seat",
        "source_type": "official_kbo",
        "stadium_id": "JAMSIL",
        "target_domain": "seat",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.lgtwins.com/ticket/general",
        "is_active": True,
    },
    # === P1: 주차 (보강) ===
    {
        "source_key": "daegu_parking",
        "source_type": "official_kbo",
        "stadium_id": "DAEGU",
        "target_domain": "parking",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.samsunglions.com/stadium/waytocome",
        "is_active": True,
    },
    {
        "source_key": "nc_dinos_food_seat",
        "source_type": "official_team",
        "team_id": "NC",
        "target_domain": "food",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.ncdinos.com/dinos/stadium.do",
        "is_active": True,
    },
    # === 기타: 팀 정보 ===
    {
        "source_key": "kbo_team_info",
        "source_type": "official_kbo",
        "target_domain": "team_info",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.koreabaseball.com/Kbo/League/TeamInfo.aspx",
        "is_active": True,
    },
    {
        "source_key": "kbo_team_history",
        "source_type": "official_kbo",
        "target_domain": "team_history",
        "reliability": "high",
        "crawl_frequency": "seasonal",
        "base_url": "https://www.koreabaseball.com/Kbo/League/TeamHistory.aspx",
        "is_active": True,
    },
    # === 기타: 뉴스/이슈 ===
    {
        "source_key": "naver_sports_news",
        "source_type": "third_party",
        "target_domain": "news",
        "reliability": "medium",
        "crawl_frequency": "hourly",
        "base_url": "https://sports.news.naver.com/kbaseball/news/index",
        "is_active": True,
    },
    {
        "source_key": "mlbpark_bullpen",
        "source_type": "third_party",
        "target_domain": "news",
        "reliability": "medium",
        "crawl_frequency": "hourly",
        "base_url": "https://mlbpark.donga.com/mp/b.php?b=bullpen",
        "is_active": True,
    },
    # === 기타: 나무위키 ===
    {
        "source_key": "namuwiki_kbo",
        "source_type": "third_party",
        "target_domain": "namuwiki",
        "reliability": "low",
        "crawl_frequency": "weekly",
        "base_url": "https://namu.wiki",
        "is_active": True,
    },
]


def run_seed(dry_run: bool = False) -> None:
    with SessionLocal() as session:
        repo = DataSourceRepository(session)
        created = 0
        updated = 0
        for data in SEED_DATA:
            existing = repo.get_by_key(data["source_key"])
            if existing:
                if not dry_run:
                    for key, value in data.items():
                        if key != "source_key" and value is not None:
                            setattr(existing, key, value)
                updated += 1
            else:
                if not dry_run:
                    repo.save(data)
                created += 1
        if not dry_run:
            session.commit()
        logger.info("[SEED] DataSource: %s created, %s updated (dry_run=%s)", created, updated, dry_run)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Seed initial DataSource entries")
    parser.add_argument("--dry-run", action="store_true", help="Preview changes without writing")
    return parser


def main(argv: Sequence[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    run_seed(dry_run=args.dry_run)


if __name__ == "__main__":  # pragma: no cover
    main()
