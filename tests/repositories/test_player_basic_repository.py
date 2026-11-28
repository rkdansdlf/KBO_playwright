from __future__ import annotations

from collections import Counter
from unittest.mock import MagicMock, patch

import pytest
import contextlib
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
    with patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: (
            [p for p in players if p.get("player_id") and p.get("name")],
            Counter(),
        ),
    ):
        yield


class TestPlayerBasicRepository:
    def test_upsert_single(self, session):
        repo = PlayerBasicRepository(session)
        players = [{"player_id": 1001, "name": "Kim", "team": "LG", "position": "투수"}]
        count = repo.upsert_players(players)
        assert count == 1

        saved = session.query(PlayerBasic).filter_by(player_id=1001).first()
        assert saved is not None
        assert saved.name == "Kim"
        assert saved.team == "LG"

    def test_upsert_multiple(self, session):
        repo = PlayerBasicRepository(session)
        players = [
            {"player_id": 1, "name": "A", "team": "LG"},
            {"player_id": 2, "name": "B", "team": "KT"},
        ]
        count = repo.upsert_players(players)
        assert count == 2
        assert session.query(PlayerBasic).count() == 2

    def test_upsert_empty(self, session):
        repo = PlayerBasicRepository(session)
        assert repo.upsert_players([]) == 0

    def test_upsert_update_existing(self, session):
        repo = PlayerBasicRepository(session)
        repo.upsert_players([{"player_id": 1001, "name": "Kim", "team": "LG"}])
        repo.upsert_players([{"player_id": 1001, "name": "Kim", "team": "KT", "position": "포수"}])

        saved = session.query(PlayerBasic).filter_by(player_id=1001).first()
        assert saved.team == "KT"
        assert saved.position == "포수"

    def test_upsert_dedup_by_player_id(self, session):
        repo = PlayerBasicRepository(session)
        players = [
            {"player_id": 1001, "name": "Kim", "team": "LG"},
            {"player_id": 1001, "name": "Kim", "team": "KT"},
        ]
        count = repo.upsert_players(players)
        assert count == 1

    def test_upsert_skip_missing_player_id(self, session):
        repo = PlayerBasicRepository(session)
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
    def test_filter_counts(self, mock_filter, session):
        repo = PlayerBasicRepository(session)
        repo.upsert_players([{"player_id": 1, "name": "A"}, {"player_id": 2, "name": "B"}])
        assert repo.last_filter_counts.get("invalid") == 2

    @patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: ([p for p in players if p.get("player_id") and p.get("name")], Counter()),
    )
    def test_get_all(self, mock_filter, session):
        repo = PlayerBasicRepository(session)
        repo.upsert_players([{"player_id": 1, "name": "A"}, {"player_id": 2, "name": "B"}])

        all_players = repo.get_all()
        assert len(all_players) == 2

    @patch(
        "src.repositories.player_basic_repository.filter_valid_player_payloads",
        side_effect=lambda players: ([p for p in players if p.get("player_id") and p.get("name")], Counter()),
    )
    def test_get_all_with_limit(self, mock_filter, session):
        repo = PlayerBasicRepository(session)
        repo.upsert_players([{"player_id": 1, "name": "A"}, {"player_id": 2, "name": "B"}])

        limited = repo.get_all(limit=1)
        assert len(limited) == 1

    def test_get_by_id(self, session):
        repo = PlayerBasicRepository(session)
        repo.upsert_players([{"player_id": 1001, "name": "Kim"}])

        player = repo.get_by_id(1001)
        assert player is not None
        assert player.name == "Kim"
        assert repo.get_by_id(9999) is None

    def test_get_by_team(self, session):
        repo = PlayerBasicRepository(session)
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
        repo = PlayerBasicRepository(session)
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
        repo = PlayerBasicRepository(session)
        result = repo.update_statuses([{"status": "active"}])
        assert result == 1

    def test_build_payload(self, session):
        repo = PlayerBasicRepository(session)
        data = {"player_id": 1, "name": "Test", "team": "LG", "bats": "R"}
        payload = repo._build_payload(data)
        assert payload["player_id"] == 1
        assert payload["name"] == "Test"
        assert payload["bats"] == "R"
        assert payload["throws"] is None

    def test_upsert_players_sqlalchemy_error(self, session):
        from sqlalchemy.exc import SQLAlchemyError

        repo = PlayerBasicRepository(session)
        with patch.object(session, "execute", side_effect=SQLAlchemyError("fail", "fail", Exception("fail"))):
            with pytest.raises(SQLAlchemyError):
                repo.upsert_players([{"player_id": 1, "name": "A"}])

    def test_upsert_one_valid(self, session):
        repo = PlayerBasicRepository(session)
        with patch(
            "src.repositories.player_basic_repository.validate_player_payload",
            return_value=(True, None),
        ):
            with patch.object(repo, "_build_upsert_stmt") as mock_stmt:
                mock_stmt.return_value = "stmt"
                with patch.object(session, "execute") as mock_exec:
                    repo._upsert_one({"player_id": 1, "name": "A"})
                    mock_exec.assert_called_once_with("stmt")

    def test_upsert_one_invalid(self, session):
        repo = PlayerBasicRepository(session)
        repo.last_filter_counts = Counter()
        with patch(
            "src.repositories.player_basic_repository.validate_player_payload",
            return_value=(False, "missing_player_id"),
        ):
            with patch.object(session, "execute") as mock_exec:
                repo._upsert_one({"name": "NoID"})
                mock_exec.assert_not_called()
                assert repo.last_filter_counts["missing_player_id"] == 1

    def test_upsert_one_invalid_no_reason(self, session):
        repo = PlayerBasicRepository(session)
        repo.last_filter_counts = Counter()
        with patch(
            "src.repositories.player_basic_repository.validate_player_payload",
            return_value=(False, None),
        ):
            repo._upsert_one({"name": "NoID"})
            assert repo.last_filter_counts["invalid_player_payload"] == 1

    def test_unique_payload_rows_skip_none_id(self, session):
        repo = PlayerBasicRepository(session)
        players = [
            {"player_id": None, "name": "NoID"},
            {"player_id": 1001, "name": "HasID"},
        ]
        rows = repo._unique_payload_rows(players)
        assert len(rows) == 1
        assert rows[0]["player_id"] == 1001

    def test_build_upsert_stmt_mysql(self, session):
        repo = PlayerBasicRepository(session)
        with patch.object(Engine.dialect, "name", "mysql"):
            repo.dialect = "mysql"
            data = [{"player_id": 1, "name": "A", "team": "LG"}]
            stmt = repo._build_upsert_stmt(data)
            assert stmt is not None

    def test_build_upsert_stmt_postgres(self, session):
        repo = PlayerBasicRepository(session)
        with patch.object(Engine.dialect, "name", "postgresql"):
            repo.dialect = "postgresql"
            data = [{"player_id": 1, "name": "A", "team": "LG"}]
            stmt = repo._build_upsert_stmt(data)
            assert stmt is not None

    def test_update_statuses_empty(self, session):
        repo = PlayerBasicRepository(session)
        result = repo.update_statuses([])
        assert result == 0

    def test_update_statuses_sqlalchemy_error(self, session):
        from sqlalchemy.exc import SQLAlchemyError

        repo = PlayerBasicRepository(session)
        repo.upsert_players([{"player_id": 1001, "name": "Kim"}])

        mock_query = MagicMock()
        with patch.object(session, "query", return_value=mock_query):
            mock_query.filter_by.return_value = mock_query
            mock_query.update.side_effect = SQLAlchemyError("fail", "fail", Exception("fail"))

            with pytest.raises(SQLAlchemyError):
                repo.update_statuses([{"player_id": 1001, "status": "active"}])

    def test_get_by_team_with_limit(self, session):
        repo = PlayerBasicRepository(session)
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
    @patch("src.db.engine.get_db_session")
    def test_save_player_basic(self, mock_get_session, mock_filter, session):
        mock_get_session.return_value.__enter__.return_value = session
        result = save_player_basic({"player_id": 1001, "name": "Kim"})
        assert result == 1

        assert session.query(PlayerBasic).filter_by(player_id=1001).first() is not None


class TestTheBatchIsChunkedToTheBindLimit:
    """The repository splits a batch that could cross the dialect's ceiling.

    ``insert(PlayerBasic).values(rows)`` compiles to ONE statement costing
    ``rows x columns`` bind parameters; PostgreSQL refuses more than 65,535 and
    SQLite 32,766, and the refusal is an error for the whole batch. The proof here
    lowers the limit through the module constant — read at call time — and watches
    the statement builder get called once per chunk.
    """

    def test_every_row_lands_when_the_batch_is_split(self, session, monkeypatch):
        monkeypatch.setattr("src.db.bulk_insert.MAX_BIND_PARAMS_PER_STATEMENT", 12)
        players = [{"player_id": 5000 + i, "name": f"P{i}", "team": "LG"} for i in range(10)]
        repo = PlayerBasicRepository(session)

        count = repo.upsert_players(players)
        session.flush()

        assert count == 10
        assert session.query(PlayerBasic).count() == 10

    def test_the_statement_builder_is_called_once_per_chunk(self, session, monkeypatch):
        monkeypatch.setattr("src.db.bulk_insert.MAX_BIND_PARAMS_PER_STATEMENT", 5)
        original = PlayerBasicRepository._build_upsert_stmt
        calls: list[int] = []

        def _spy(self, values):
            calls.append(1 if isinstance(values, dict) else len(values))
            return original(self, values)

        monkeypatch.setattr(PlayerBasicRepository, "_build_upsert_stmt", _spy)
        players = [{"player_id": 6000 + i, "name": f"Q{i}", "team": "LG"} for i in range(10)]

        PlayerBasicRepository(session).upsert_players(players)

        assert sum(calls) == 10, "no row may be dropped by the split"
        assert len(calls) > 1, "the limit was lowered; one statement would mean no chunking"

    def test_a_single_chunk_still_uses_one_statement(self, session, monkeypatch):
        """Chunking must not turn the ordinary small batch into N statements."""
        original = PlayerBasicRepository._build_upsert_stmt
        calls: list[int] = []

        def _spy(self, values):
            calls.append(1 if isinstance(values, dict) else len(values))
            return original(self, values)

        monkeypatch.setattr(PlayerBasicRepository, "_build_upsert_stmt", _spy)

        PlayerBasicRepository(session).upsert_players([{"player_id": 7001, "name": "R", "team": "LG"}])

        assert calls == [1]
