from __future__ import annotations

from collections import Counter
from unittest.mock import MagicMock, patch

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
        repo.upsert_players(
            [
                {"player_id": 1, "name": "A", "team": "LG"},
                {"player_id": 2, "name": "B", "team": "LG"},
                {"player_id": 3, "name": "C", "team": "KT"},
            ],
        )

        lg_players = repo.get_by_team("LG")
        assert len(lg_players) == 2

    def test_update_statuses(self, session):
        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1001, "name": "Kim"}])

        repo.update_statuses(
            [
                {"player_id": 1001, "status": "active", "staff_role": None, "status_source": "profile"},
            ],
        )

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

    def test_upsert_players_sqlalchemy_error(self, session):
        from sqlalchemy.exc import SQLAlchemyError

        repo = PlayerBasicRepository()
        with patch("src.repositories.player_basic_repository.SessionLocal", return_value=session):
            with patch.object(session, "execute", side_effect=SQLAlchemyError("fail", "fail", Exception("fail"))):
                with patch.object(session, "rollback") as mock_rollback:
                    with pytest.raises(SQLAlchemyError):
                        repo.upsert_players([{"player_id": 1, "name": "A"}])
                    mock_rollback.assert_called_once()

    def test_upsert_one_valid(self, session):
        repo = PlayerBasicRepository()
        with patch(
            "src.repositories.player_basic_repository.validate_player_payload",
            return_value=(True, None),
        ):
            with patch.object(repo, "_build_upsert_stmt") as mock_stmt:
                mock_stmt.return_value = "stmt"
                with patch.object(session, "execute") as mock_exec:
                    repo._upsert_one(session, {"player_id": 1, "name": "A"})
                    mock_exec.assert_called_once_with("stmt")

    def test_upsert_one_invalid(self, session):
        repo = PlayerBasicRepository()
        repo.last_filter_counts = Counter()
        with patch(
            "src.repositories.player_basic_repository.validate_player_payload",
            return_value=(False, "missing_player_id"),
        ):
            with patch.object(session, "execute") as mock_exec:
                repo._upsert_one(session, {"name": "NoID"})
                mock_exec.assert_not_called()
                assert repo.last_filter_counts["missing_player_id"] == 1

    def test_upsert_one_invalid_no_reason(self, session):
        repo = PlayerBasicRepository()
        repo.last_filter_counts = Counter()
        with patch(
            "src.repositories.player_basic_repository.validate_player_payload",
            return_value=(False, None),
        ):
            repo._upsert_one(session, {"name": "NoID"})
            assert repo.last_filter_counts["invalid_player_payload"] == 1

    def test_unique_payload_rows_skip_none_id(self, session):
        repo = PlayerBasicRepository()
        players = [
            {"player_id": None, "name": "NoID"},
            {"player_id": 1001, "name": "HasID"},
        ]
        rows = repo._unique_payload_rows(players)
        assert len(rows) == 1
        assert rows[0]["player_id"] == 1001

    def test_build_upsert_stmt_mysql(self, session):
        repo = PlayerBasicRepository()
        with patch.object(Engine.dialect, "name", "mysql"):
            repo.dialect = "mysql"
            data = [{"player_id": 1, "name": "A", "team": "LG"}]
            stmt = repo._build_upsert_stmt(data)
            assert stmt is not None

    def test_build_upsert_stmt_postgres(self, session):
        repo = PlayerBasicRepository()
        with patch.object(Engine.dialect, "name", "postgresql"):
            repo.dialect = "postgresql"
            data = [{"player_id": 1, "name": "A", "team": "LG"}]
            stmt = repo._build_upsert_stmt(data)
            assert stmt is not None

    def test_update_statuses_empty(self, session):
        repo = PlayerBasicRepository()
        result = repo.update_statuses([])
        assert result == 0

    def test_update_statuses_sqlalchemy_error(self, session):
        from sqlalchemy.exc import SQLAlchemyError

        repo = PlayerBasicRepository()
        repo.upsert_players([{"player_id": 1001, "name": "Kim"}])

        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=False)
        mock_query = MagicMock()
        mock_session.query.return_value = mock_query
        mock_query.filter_by.return_value = mock_query
        mock_query.update.side_effect = SQLAlchemyError("fail", "fail", Exception("fail"))

        with patch("src.repositories.player_basic_repository.SessionLocal", return_value=mock_session):
            with pytest.raises(SQLAlchemyError):
                repo.update_statuses([{"player_id": 1001, "status": "active"}])
            mock_session.rollback.assert_called_once()

    def test_get_by_team_with_limit(self, session):
        repo = PlayerBasicRepository()
        repo.upsert_players(
            [
                {"player_id": 1, "name": "A", "team": "LG"},
                {"player_id": 2, "name": "B", "team": "LG"},
                {"player_id": 3, "name": "C", "team": "LG"},
            ],
        )

        limited = repo.get_by_team("LG", limit=2)
        assert len(limited) == 2


class TestSavePlayerBasic:
    @patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: ([p for p in players if p.get("player_id") and p.get("name")], Counter()),
    )
    def test_save_player_basic(self, mock_filter, session):
        result = save_player_basic({"player_id": 1001, "name": "Kim"})
        assert result == 1

        assert session.query(PlayerBasic).filter_by(player_id=1001).first() is not None
