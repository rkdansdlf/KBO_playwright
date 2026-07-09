"""Pure/unit tests for REST API helpers without integration DB setup."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("fastapi")
from fastapi import HTTPException

from src.api import app as api_app
from src.cli import api_server


class _FakeUpload:
    def __init__(self, filename: str | None, payload: bytes) -> None:
        self.filename = filename
        self._payload = payload

    async def read(self) -> bytes:
        return self._payload


def test_get_api_key_without_expected_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("REST_API_KEY", raising=False)
    assert api_app.get_api_key("provided") == "provided"
    assert api_app.get_api_key(None) is None


def test_get_api_key_requires_matching_header(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REST_API_KEY", "secret")
    assert api_app.get_api_key("secret") == "secret"

    with pytest.raises(HTTPException) as exc_info:
        api_app.get_api_key("wrong")
    assert exc_info.value.status_code == 403


def test_check_lock_status_reports_free_and_held() -> None:
    free_lock = MagicMock()
    free_lock.acquire.return_value = True
    held_lock = MagicMock()
    held_lock.acquire.return_value = False

    with patch("src.api.app.ProcessLock", side_effect=[free_lock, held_lock]):
        assert api_app._check_lock_status("daily_update") is False
        assert api_app._check_lock_status("daily_update") is True

    free_lock.release.assert_called_once()


def test_health_check() -> None:
    assert api_app.health_check() == {"status": "ok"}


def test_get_system_status_with_mock_session() -> None:
    game_query = MagicMock()
    game_query.count.return_value = 7
    game_query.order_by.return_value.first.return_value = MagicMock(game_date=date(2026, 4, 2))
    player_query = MagicMock()
    player_query.count.return_value = 99
    movement_query = MagicMock()
    movement_query.order_by.return_value.first.return_value = MagicMock(created_at=datetime(2026, 4, 2, 12, 30))
    session = MagicMock()
    session.query.side_effect = [game_query, player_query, movement_query]

    @contextmanager
    def fake_db_session():
        yield session

    with (
        patch("src.api.app.get_db_session", fake_db_session),
        patch("src.api.app._check_lock_status", side_effect=lambda name: name == "live_refresh"),
    ):
        result = api_app.get_system_status()

    assert result["database"]["games_count"] == 7
    assert result["database"]["players_count"] == 99
    assert result["database"]["latest_game_date"] == "2026-04-02"
    assert result["database"]["latest_roster_movement_at"] == "2026-04-02T12:30:00"
    assert result["locks"]["live_refresh"] is True
    assert result["locks"]["daily_update"] is False


def test_get_system_status_raises_http_500_on_db_failure() -> None:
    @contextmanager
    def fake_db_session():
        msg = "db down"
        raise RuntimeError(msg)
        yield

    with patch("src.api.app.get_db_session", fake_db_session), pytest.raises(HTTPException) as exc_info:
        api_app.get_system_status()

    assert exc_info.value.status_code == 500
    assert "Database query failure" in exc_info.value.detail


def test_async_run_daily_update_logs_failures() -> None:
    with patch("src.cli.run_daily_update.main", side_effect=RuntimeError("boom")):
        api_app._async_run_daily_update()


def test_trigger_daily_update_conflict_and_success() -> None:
    background_tasks = MagicMock()
    with patch("src.api.app._check_lock_status", return_value=True), pytest.raises(HTTPException) as exc_info:
        api_app.trigger_daily_update(background_tasks)
    assert exc_info.value.status_code == 409

    with patch("src.api.app._check_lock_status", return_value=False):
        result = api_app.trigger_daily_update(background_tasks)
    assert "triggered" in result["status"]
    background_tasks.add_task.assert_called_once_with(api_app._async_run_daily_update)


@pytest.mark.asyncio
async def test_upload_text_relay_validation_errors() -> None:
    with pytest.raises(HTTPException) as missing_exc:
        await api_app.upload_text_relay(_FakeUpload(None, b"x"))  # type: ignore[arg-type]
    assert missing_exc.value.status_code == 400

    with pytest.raises(HTTPException) as extension_exc:
        await api_app.upload_text_relay(_FakeUpload("game.txt", b"x"))  # type: ignore[arg-type]
    assert extension_exc.value.status_code == 400

    with pytest.raises(HTTPException) as decode_exc:
        await api_app.upload_text_relay(_FakeUpload("20260402LGOB0.csv", b"\xff\xfe\xfd"))  # type: ignore[arg-type]
    assert decode_exc.value.status_code == 400


@pytest.mark.asyncio
async def test_upload_text_relay_success_with_mock_session() -> None:
    session = MagicMock()
    session.query.return_value.filter.return_value.delete.return_value = 2

    @contextmanager
    def fake_db_session():
        yield session

    csv_payload = (
        b"inning,inning_half,pitcher_name,batter_name,play_description,event_type,result\n"
        b"1,top,Pitcher,Batter,Single,HIT,SINGLE\n"
    )

    with patch("src.api.app.get_db_session", fake_db_session):
        result = await api_app.upload_text_relay(_FakeUpload("20260402LGOB0_text_relay.csv", csv_payload))  # type: ignore[arg-type]

    assert result == {"status": "success", "game_id": "20260402LGOB0", "rows_inserted": 1}
    session.add.assert_called_once()


def test_api_server_main_passes_args_to_uvicorn() -> None:
    with patch("src.cli.api_server.uvicorn.run") as mock_run:
        api_server.main(["--host", "127.0.0.1", "--port", "9000", "--reload"])

    mock_run.assert_called_once_with("src.api.app:app", host="127.0.0.1", port=9000, reload=True)
