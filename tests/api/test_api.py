import io
import os
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

pytestmark = pytest.mark.integration

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.api.app import app
from src.models.base import Base
from src.models.game import Game, GamePlayByPlay
from src.models.player import PlayerBasic


# Setup module-scoped test database factory
@pytest.fixture(name="db_session_factory", scope="module")
def _db_session_factory(tmp_path_factory: pytest.TempPathFactory) -> Generator[Any, None, None]:
    """Provide a file-based temporary SQLite database for sharing connections."""
    db_dir = tmp_path_factory.mktemp("api_test_db")
    db_file = db_dir / "test.db"

    test_engine = create_engine(f"sqlite:///{db_file}")
    Base.metadata.create_all(bind=test_engine)
    SessionFactory = sessionmaker(bind=test_engine, autoflush=False, autocommit=False, expire_on_commit=False)

    @contextmanager
    def mock_get_db_session() -> Generator[Session, None, None]:
        session = SessionFactory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    yield mock_get_db_session


@pytest.fixture(autouse=True)
def _setup_mock_db(db_session_factory: Any) -> Generator[None, None, None]:
    """Globally patch get_db_session for all tests in this file."""
    with patch("src.api.app.get_db_session", db_session_factory):
        yield


@pytest.fixture(autouse=True)
def _clean_tables(db_session_factory: Any) -> Generator[None, None, None]:
    """Clean all tables in the test database before each test run."""
    with db_session_factory() as session:
        session.query(GamePlayByPlay).delete()
        session.query(Game).delete()
        session.query(PlayerBasic).delete()
    yield


client = TestClient(app)


def test_health_check() -> None:
    """Test health check returns HTTP 200 ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_get_system_status(db_session_factory: Any) -> None:
    """Test status query reads database statistics and mocks lock checks."""
    # Seed mock data
    from datetime import date

    with db_session_factory() as session:
        game = Game(
            game_id="20260412SKLG0",
            game_date=date(2026, 4, 12),
            stadium="Incheon",
            home_team="SK",
            away_team="LG",
        )
        player = PlayerBasic(
            player_id=12345,
            name="홍길동",
            team="LG",
        )
        session.add(game)
        session.add(player)

    # Mock lock checks: live_refresh=True, others=False
    def mock_check_lock(lock_name: str) -> bool:
        return lock_name == "live_refresh"

    with patch("src.api.app._check_lock_status", side_effect=mock_check_lock):
        response = client.get("/status")
        assert response.status_code == 200
        data = response.json()

        assert data["database"]["games_count"] == 1
        assert data["database"]["players_count"] == 1
        assert data["database"]["latest_game_date"] == "2026-04-12"
        assert data["locks"]["live_refresh"] is True
        assert data["locks"]["daily_update"] is False


def test_trigger_daily_update_lock_held() -> None:
    """Test trigger daily update raises conflict if locks are already held."""
    with patch("src.api.app._check_lock_status", return_value=True):
        response = client.post("/crawl/daily-update")
        assert response.status_code == 409
        assert "already in progress" in response.json()["detail"]


def test_trigger_daily_update_success() -> None:
    """Test triggering daily update triggers background tasks."""
    mock_run = MagicMock()
    with (
        patch("src.api.app._check_lock_status", return_value=False),
        patch("src.api.app._async_run_daily_update", mock_run),
    ):
        response = client.post("/crawl/daily-update")
        assert response.status_code == 200
        assert "triggered in background" in response.json()["status"]


def test_upload_text_relay_validation() -> None:
    """Test text relay upload validates file extension and game ID."""
    # Test missing filename or invalid extension
    response = client.post("/upload/text-relay", files={"file": ("test.txt", b"dummy")})
    assert response.status_code == 400
    assert "Only CSV files" in response.json()["detail"]


def test_upload_text_relay_success(db_session_factory: Any) -> None:
    """Test uploading text relay CSV parses headers and saves entries to DB."""
    csv_data = (
        "inning,inning_half,pitcher_name,batter_name,play_description,event_type,result\n"
        "1,초,김광현,박해민,안타,HIT,SINGLE\n"
        "1,초,김광현,김현수,삼진,OUT,STRIKEOUT\n"
    )

    file_bytes = io.BytesIO(csv_data.encode("utf-8"))

    response = client.post(
        "/upload/text-relay",
        files={"file": ("20260412SKLG0_text_relay.csv", file_bytes, "text/csv")},
    )
    assert response.status_code == 200
    res_data = response.json()
    assert res_data["status"] == "success"
    assert res_data["game_id"] == "20260412SKLG0"
    assert res_data["rows_inserted"] == 2

    # Verify plays are inserted in DB
    with db_session_factory() as session:
        plays = session.query(GamePlayByPlay).filter(GamePlayByPlay.game_id == "20260412SKLG0").all()
        assert len(plays) == 2
        assert plays[0].pitcher_name == "김광현"
        assert plays[0].batter_name == "박해민"
        assert plays[0].result == "SINGLE"
        assert plays[1].pitcher_name == "김광현"
        assert plays[1].batter_name == "김현수"
        assert plays[1].result == "STRIKEOUT"


@pytest.fixture(autouse=True)
def _clear_env_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Clear REST_API_KEY from environment to keep default tests anonymous."""
    monkeypatch.delenv("REST_API_KEY", raising=False)


def test_api_key_unauthorized_when_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that protected endpoints return 403 when REST_API_KEY is set but header is missing or wrong."""
    monkeypatch.setenv("REST_API_KEY", "secure_secret_key")

    # /status should return 403
    response = client.get("/status")
    assert response.status_code == 403
    assert "Could not validate credentials" in response.json()["detail"]

    response = client.get("/status", headers={"X-API-Key": "wrong_key"})
    assert response.status_code == 403

    # /crawl/daily-update should return 403
    response = client.post("/crawl/daily-update")
    assert response.status_code == 403

    # /upload/text-relay should return 403
    response = client.post("/upload/text-relay", files={"file": ("test.csv", b"")})
    assert response.status_code == 403


def test_api_key_authorized_when_set(monkeypatch: pytest.MonkeyPatch, db_session_factory: Any) -> None:
    """Test that protected endpoints succeed when correct X-API-Key is provided."""
    monkeypatch.setenv("REST_API_KEY", "secure_secret_key")
    headers = {"X-API-Key": "secure_secret_key"}

    # /status should succeed
    response = client.get("/status", headers=headers)
    assert response.status_code == 200

    # /crawl/daily-update should succeed
    mock_run = MagicMock()
    with (
        patch("src.api.app._check_lock_status", return_value=False),
        patch("src.api.app._async_run_daily_update", mock_run),
    ):
        response = client.post("/crawl/daily-update", headers=headers)
        assert response.status_code == 200

    # /upload/text-relay should succeed
    csv_data = "inning,inning_half,pitcher_name,batter_name,play_description,event_type,result\n"
    file_bytes = io.BytesIO(csv_data.encode("utf-8"))
    response = client.post(
        "/upload/text-relay",
        headers=headers,
        files={"file": ("20260412SKLG0_text_relay.csv", file_bytes, "text/csv")},
    )
    assert response.status_code == 200


def test_health_always_public(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that health check is public and doesn't require API key even when configured."""
    monkeypatch.setenv("REST_API_KEY", "secure_secret_key")
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
