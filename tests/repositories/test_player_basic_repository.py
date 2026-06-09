from __future__ import annotations

from collections import Counter
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.db.engine import Engine
from src.models.player import PlayerBasic
from src.repositories.player_basic_repository import PlayerBasicRepository, save_player_basic


@pytest.fixture
def engine():
    return create_engine("sqlite:///:memory:")


@pytest.fixture
def session(engine):
    PlayerBasic.__table__.create(engine)
    Session = sessionmaker(bind=engine)
    return Session()


@pytest.fixture(autouse=True)
def patch_deps(session):
    with (
        patch("src.repositories.player_basic_repository.SessionLocal", return_value=session),
        patch.object(Engine.dialect, "name", "sqlite"),
        patch(
            "src.repositories.player_basic_repository.filter_valid_player_payloads",
            side_effect=lambda players: (
                [p for p in players if p.get("player_id") and p.get("name")],
                Counter(),
            ),
        ),
    ):
        yield


class TestPlayerBasicRepository:
    def test_upsert_single(self, session):
        repo = PlayerBasicRepository()
        players = [{"player_id": 1001, "name": "Kim", "team": "LG", "position": "투수"}]
        count = repo.upsert_players(players)
        assert count == 1

        saved = session.query(PlayerBasic).filter_by(player_id=1001).first()
        assert saved is not None
        assert saved.name == "Kim"
        assert saved.team == "LG"

    def test_upsert_multiple(self, session):
        repo = PlayerBasicRepository()
        players = [
            {"player_id": 1, "name": "A", "team": "LG"},
            {"player_id": 2, "name": "B", "team": "KT"},
        ]
        count = repo.upsert_players(players)
        assert count == 2
        assert session.query(PlayerBasic).count() == 2

    def test_upsert_empty(self, session):
        repo = PlayerBasicRepository()
        assert repo.upsert_players([]) == 0

    def test_upsert_update_existing(self, session):
        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1001, "name": "Kim", "team": "LG"}])
        repo.upsert_players([{"player_id": 1001, "name": "Kim", "team": "KT", "position": "포수"}])

        saved = session.query(PlayerBasic).filter_by(player_id=1001).first()
        assert saved.team == "KT"
        assert saved.position == "포수"

    def test_upsert_dedup_by_player_id(self, session):
        repo = PlayerBasicRepository()
        players = [
            {"player_id": 1001, "name": "Kim", "team": "LG"},
            {"player_id": 1001, "name": "Kim", "team": "KT"},
        ]
        count = repo.upsert_players(players)
        assert count == 1

    def test_upsert_skip_missing_player_id(self, session):
        repo = PlayerBasicRepository()
        players = [
            {"name": "NoID"},
            {"player_id": 1001, "name": "HasID"},
        ]
        count = repo.upsert_players(players)
        assert count == 1

    @patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: ([], Counter({"invalid": 2})),
    )
    def test_filter_counts(self, mock_filter):
        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1, "name": "A"}, {"player_id": 2, "name": "B"}])
        assert repo.last_filter_counts.get("invalid") == 2

    @patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: ([p for p in players if p.get("player_id") and p.get("name")], Counter()),
    )
    def test_get_all(self, mock_filter, session):
        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1, "name": "A"}, {"player_id": 2, "name": "B"}])

        all_players = repo.get_all()
        assert len(all_players) == 2

    @patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: ([p for p in players if p.get("player_id") and p.get("name")], Counter()),
    )
    def test_get_all_with_limit(self, mock_filter, session):
        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1, "name": "A"}, {"player_id": 2, "name": "B"}])

        limited = repo.get_all(limit=1)
        assert len(limited) == 1

    def test_get_by_id(self, session):
        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1001, "name": "Kim"}])

        player = repo.get_by_id(1001)
        assert player is not None
        assert player.name == "Kim"
        assert repo.get_by_id(9999) is None

    def test_get_by_team(self, session):
        repo = PlayerBasicRepository()
        repo.upsert_players([
            {"player_id": 1, "name": "A", "team": "LG"},
            {"player_id": 2, "name": "B", "team": "LG"},
            {"player_id": 3, "name": "C", "team": "KT"},
        ])

        lg_players = repo.get_by_team("LG")
        assert len(lg_players) == 2

    def test_update_statuses(self, session):
        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1001, "name": "Kim"}])

        repo.update_statuses([
            {"player_id": 1001, "status": "active", "staff_role": None, "status_source": "profile"},
        ])

        updated = session.query(PlayerBasic).filter_by(player_id=1001).first()
        assert updated.status == "active"
        assert updated.status_source == "profile"

    def test_update_statuses_skip_no_player_id(self, session):
        repo = PlayerBasicRepository()
        result = repo.update_statuses([{"status": "active"}])
        assert result == 1

    def test_build_payload(self):
        repo = PlayerBasicRepository()
        data = {"player_id": 1, "name": "Test", "team": "LG", "bats": "R"}
        payload = repo._build_payload(data)
        assert payload["player_id"] == 1
        assert payload["name"] == "Test"
        assert payload["bats"] == "R"
        assert payload["throws"] is None


class TestSavePlayerBasic:
    @patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: ([p for p in players if p.get("player_id") and p.get("name")], Counter()),
    )
    def test_save_player_basic(self, mock_filter, session):
        result = save_player_basic({"player_id": 1001, "name": "Kim"})
        assert result == 1

        assert session.query(PlayerBasic).filter_by(player_id=1001).first() is not None
