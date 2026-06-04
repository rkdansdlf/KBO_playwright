"""
Migration 015: Add DataSource entries for remaining ticket sources.
"""

from __future__ import annotations

from src.db.engine import SessionLocal
from src.repositories.source_registry_repository import DataSourceRepository

TICKET_SOURCES: list[dict] = [
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
]


def upgrade() -> None:
    with SessionLocal() as session:
        repo = DataSourceRepository(session)
        for data in TICKET_SOURCES:
            existing = repo.get_by_key(data["source_key"])
            if not existing:
                repo.save(data)
        session.commit()
        print(f"[MIGRATION] 015: Added {len(TICKET_SOURCES)} ticket DataSources.")


def downgrade() -> None:
    with SessionLocal() as session:
        repo = DataSourceRepository(session)
        for data in TICKET_SOURCES:
            existing = repo.get_by_key(data["source_key"])
            if existing:
                session.delete(existing)
        session.commit()
        print(f"[MIGRATION] 015: Removed {len(TICKET_SOURCES)} ticket DataSources.")
