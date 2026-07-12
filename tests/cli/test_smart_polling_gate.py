"""Tests for smart polling gate (Layer 1)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from src.cli import smart_polling_gate as gate_module
from src.cli.smart_polling_gate import (
    ACTIVE_LIFECYCLE_STATES,
    TERMINAL_LIFECYCLE_STATES,
    _build_query_params,
    _classify_games,
    _extract_game_status,
    _format_game_label,
    check_all_games_finished,
    get_kst_today_str,
    main,
    main_async,
)
from src.constants import KST


def _make_response(status_code: int, payload: dict[str, Any]) -> Any:
    return SimpleNamespace(status_code=status_code, json=lambda: payload)


def _make_game(
    *,
    away: str = "LG",
    home: str = "SSG",
    stadium: str = "문학",
    status: str | None = "RESULT",
    game_date: str | None = None,
) -> dict[str, Any]:
    g: dict[str, Any] = {
        "awayTeamName": away,
        "homeTeamName": home,
        "stadiumName": stadium,
    }
    if status is not None:
        g["status"] = status
    if game_date is not None:
        g["gameDate"] = game_date
    return g


class _FakeAsyncClient:
    def __init__(self, response: Any) -> None:
        self._response = response
        self.closed = False

    async def __aenter__(self) -> _FakeAsyncClient:
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.closed = True

    async def get(self, *args: Any, **kwargs: Any) -> Any:
        return self._response


def _patch_client(monkeypatch: pytest.MonkeyPatch, response: Any) -> None:
    fake = _FakeAsyncClient(response)
    monkeypatch.setattr(
        gate_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: fake,
    )
    return fake


class TestGetKstTodayStr:
    def test_returns_8_char_string(self, monkeypatch: pytest.MonkeyPatch) -> None:
        result = get_kst_today_str()
        assert len(result) == 8
        assert result.isdigit()


class TestBuildQueryParams:
    def test_correct_params(self) -> None:
        params = _build_query_params("20260624")
        assert params == {
            "sectionId": "kbaseball",
            "categoryId": "kbo",
            "seasonYear": "2026",
            "date": "2026-06-24",
        }

    def test_different_year(self) -> None:
        params = _build_query_params("20250101")
        assert params["seasonYear"] == "2025"
        assert params["date"] == "2025-01-01"


class TestExtractGameStatus:
    def test_status_field(self) -> None:
        game = {"status": "RESULT"}
        assert _extract_game_status(game) == "RESULT"

    def test_gameStatus_field(self) -> None:
        game = {"gameStatus": "RUNNING"}
        assert _extract_game_status(game) == "RUNNING"

    def test_gameState_field(self) -> None:
        game = {"gameState": "BEFORE"}
        assert _extract_game_status(game) == "BEFORE"

    def test_progressState_field(self) -> None:
        game = {"progressState": "DELAYED"}
        assert _extract_game_status(game) == "DELAYED"

    def test_priority_order(self) -> None:
        game = {
            "status": "RESULT",
            "gameStatus": "RUNNING",
            "gameState": "BEFORE",
        }
        assert _extract_game_status(game) == "RESULT"

    def test_none_when_no_field(self) -> None:
        game = {"awayTeamName": "LG"}
        assert _extract_game_status(game) is None

    def test_empty_string_returns_empty(self) -> None:
        game = {"status": "  "}
        result = _extract_game_status(game)
        assert result == ""
        assert result is not None

    def test_lowercase_input(self) -> None:
        game = {"status": "running"}
        assert _extract_game_status(game) == "RUNNING"


class TestClassifyGames:
    def test_all_terminal(self) -> None:
        games = [
            _make_game(status="CANCELLED"),
            _make_game(status="CANCEL"),
        ]
        terminal, active, unknown = _classify_games(games)
        assert len(terminal) == 2
        assert len(active) == 0
        assert len(unknown) == 0

    def test_all_active(self) -> None:
        games = [
            _make_game(status="RUNNING"),
            _make_game(status="BEFORE"),
            _make_game(status="DELAYED"),
            _make_game(status="SUSPENDED"),
        ]
        terminal, active, unknown = _classify_games(games)
        assert len(terminal) == 0
        assert len(active) == 4
        assert len(unknown) == 0

    def test_mixed(self) -> None:
        games = [
            _make_game(status="CANCELLED"),
            _make_game(status="RUNNING"),
            _make_game(status="CANCEL"),
        ]
        terminal, active, unknown = _classify_games(games)
        assert len(terminal) == 2
        assert len(active) == 1
        assert len(unknown) == 0

    def test_unknown_status(self) -> None:
        games = [_make_game(status="SOMETHING_ELSE")]
        terminal, active, unknown = _classify_games(games)
        assert len(unknown) == 1

    def test_empty_list(self) -> None:
        terminal, active, unknown = _classify_games([])
        assert len(terminal) == 0
        assert len(active) == 0
        assert len(unknown) == 0


class TestFormatGameLabel:
    def test_with_stadium(self) -> None:
        game = _make_game(away="LG", home="SSG", stadium="문학")
        assert _format_game_label(game) == "LG vs SSG (문학)"

    def test_without_stadium(self) -> None:
        game = {"awayTeamName": "두산", "homeTeamName": "KIA"}
        assert _format_game_label(game) == "두산 vs KIA"

    def test_missing_team_names(self) -> None:
        game = {"stadiumName": "잠실"}
        assert _format_game_label(game) == "? vs ? (잠실)"


class TestCheckAllGamesFinished:
    @pytest.mark.asyncio
    async def test_all_terminal_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        payload = {"result": {"games": [_make_game(status="CANCELLED"), _make_game(status="CANCEL")]}}
        _patch_client(monkeypatch, _make_response(200, payload))

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is True
        assert has_games is True
        assert details["reason"] == "all_games_finished"

    @pytest.mark.asyncio
    async def test_active_games_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        payload = {"result": {"games": [_make_game(status="RUNNING")]}}
        _patch_client(monkeypatch, _make_response(200, payload))

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is False
        assert has_games is True
        assert details["reason"] == "games_in_progress"

    @pytest.mark.asyncio
    async def test_no_games_today_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        empty_payload = {"result": {"games": []}}
        _patch_client(monkeypatch, _make_response(200, empty_payload))

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is False
        assert has_games is False

    @pytest.mark.asyncio
    async def test_http_error_graceful(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import httpx

        class _FailClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                raise httpx.HTTPError("connection failed")

        monkeypatch.setattr(gate_module.httpx, "AsyncClient", lambda *a, **k: _FailClient())

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is False
        assert has_games is False

    @pytest.mark.asyncio
    async def test_non_200_response(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _patch_client(monkeypatch, _make_response(500, {}))

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is False
        assert has_games is False

    @pytest.mark.asyncio
    async def test_malformed_json(self, monkeypatch: pytest.MonkeyPatch) -> None:
        class _BadJsonClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                import json

                return SimpleNamespace(status_code=200, json=lambda: json.loads("invalid"))

        monkeypatch.setattr(gate_module.httpx, "AsyncClient", lambda *a, **k: _BadJsonClient())

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is False
        assert has_games is False

    @pytest.mark.asyncio
    async def test_yesterday_active_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_count = 0

        class _TwoCallClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return _make_response(200, {"result": {"games": []}})
                return _make_response(200, {"result": {"games": [_make_game(status="RUNNING")]}})

        monkeypatch.setattr(gate_module.httpx, "AsyncClient", lambda *a, **k: _TwoCallClient())

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is True
        assert has_games is True
        assert details["reason"] == "yesterday_games_still_active"

    @pytest.mark.asyncio
    async def test_yesterday_all_terminal_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        call_count = 0

        class _TwoCallClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def get(self, *args, **kwargs):
                nonlocal call_count
                call_count += 1
                return _make_response(200, {"result": {"games": []}})

        monkeypatch.setattr(gate_module.httpx, "AsyncClient", lambda *a, **k: _TwoCallClient())

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is False
        assert has_games is False

    @pytest.mark.asyncio
    async def test_unknown_status_today_skips(self, monkeypatch: pytest.MonkeyPatch) -> None:
        today_str = datetime.now(KST).strftime("%Y-%m-%d")
        games = [{"status": "UNKNOWN", "gameDate": today_str, "awayTeamName": "LG", "homeTeamName": "SSG"}]
        payload = {"result": {"games": games}}
        _patch_client(monkeypatch, _make_response(200, payload))

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is False
        assert has_games is True

    @pytest.mark.asyncio
    async def test_unknown_status_past_date_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        games = [{"status": "UNKNOWN", "gameDate": "2020-01-01", "awayTeamName": "LG", "homeTeamName": "SSG"}]
        payload = {"result": {"games": games}}
        _patch_client(monkeypatch, _make_response(200, payload))

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is True
        assert has_games is True

    @pytest.mark.asyncio
    async def test_unknown_status_no_date_field_proceeds(self, monkeypatch: pytest.MonkeyPatch) -> None:
        games = [{"status": "UNKNOWN", "awayTeamName": "LG", "homeTeamName": "SSG"}]
        payload = {"result": {"games": games}}
        _patch_client(monkeypatch, _make_response(200, payload))

        should_proceed, has_games, details = await check_all_games_finished()
        assert should_proceed is True
        assert has_games is True

    @pytest.mark.asyncio
    async def test_proceed_returns_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fake_check():
            return True, True, {"reason": "all_games_finished"}

        monkeypatch.setattr(gate_module, "check_all_games_finished", fake_check)

        exit_code = await main_async(["--json"])
        assert exit_code == 0

    @pytest.mark.asyncio
    async def test_skip_returns_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fake_check():
            return False, True, {"reason": "games_in_progress"}

        monkeypatch.setattr(gate_module, "check_all_games_finished", fake_check)

        exit_code = await main_async([])
        assert exit_code == 1

    @pytest.mark.asyncio
    async def test_json_output(self, monkeypatch: pytest.MonkeyPatch) -> None:
        async def fake_check():
            return True, True, {"reason": "all_games_finished"}

        monkeypatch.setattr(gate_module, "check_all_games_finished", fake_check)

        import io
        from contextlib import redirect_stdout

        buf = io.StringIO()
        with redirect_stdout(buf):
            await main_async(["--json"])

        output = json.loads(buf.getvalue())
        assert output["should_proceed"] is True
        assert output["has_games_today"] is True
        assert "timestamp_kst" in output


class TestMain:
    def test_main_exits_zero_on_proceed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_asyncio = MagicMock()
        fake_asyncio.run.side_effect = lambda coro: _close_and_return(coro, 0)

        monkeypatch.setattr(gate_module, "asyncio", fake_asyncio)
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code == 0

    def test_main_exits_one_on_skip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        fake_asyncio = MagicMock()
        fake_asyncio.run.side_effect = lambda coro: _close_and_return(coro, 1)

        monkeypatch.setattr(gate_module, "asyncio", fake_asyncio)
        with pytest.raises(SystemExit) as exc_info:
            main([])
        assert exc_info.value.code == 1


def _close_and_return(coro, value: int) -> int:
    coro.close()
    return value
