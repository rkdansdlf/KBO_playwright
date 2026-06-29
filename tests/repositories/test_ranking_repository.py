from __future__ import annotations

from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.engine import Engine
from src.repositories.ranking_repository import RankingRepository


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    from src.models.base import Base

    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


@pytest.fixture(autouse=True)
def patch_session_local(session):
    with patch("src.repositories.team_stats_repository.SessionLocal", return_value=session):
        yield


@pytest.fixture(autouse=True)
def patch_dialect():
    with patch.object(Engine.dialect, "name", "sqlite"):
        yield


class TestRankingRepository:
    def test_save_rankings(self, session):
        repo = RankingRepository()
        rankings = [
            {
                "season": 2025,
                "metric": "avg",
                "entity_id": "1",
                "entity_label": "홍길동",
                "entity_type": "PLAYER",
                "value": 0.350,
                "rank": 1,
                "source": "CRAWLER",
            },
        ]
        count = repo.save_rankings(rankings)
        assert count == 1

    def test_save_rankings_empty(self, session):
        repo = RankingRepository()
        assert repo.save_rankings([]) == 0

    def test_save_rankings_upsert(self, session):
        repo = RankingRepository()
        r = {
            "season": 2025,
            "metric": "avg",
            "entity_id": "1",
            "entity_label": "홍길동",
            "entity_type": "PLAYER",
            "value": 0.300,
            "rank": 5,
            "source": "CRAWLER",
        }
        repo.save_rankings([r])
        r["value"] = 0.400
        r["rank"] = 1
        repo.save_rankings([r])
        from src.models.rankings import StatRanking

        record = session.query(StatRanking).filter_by(season=2025, metric="avg", entity_id="1").first()
        assert record.value == 0.400
        assert record.rank == 1
