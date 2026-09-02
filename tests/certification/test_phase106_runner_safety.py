"""Safety contracts for Phase 106 certification runners."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from scripts.certification.phase106 import run_live_smoke_gate, run_offline_preflight


def test_live_smoke_help_exits_before_starting_async_runner(capsys) -> None:
    with patch.object(run_live_smoke_gate.asyncio, "run") as async_run:
        with pytest.raises(SystemExit) as exc_info:
            run_live_smoke_gate.main(["--help"])

    assert exc_info.value.code == 0
    async_run.assert_not_called()
    assert "Phase 106D" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_live_smoke_aborts_when_api_xhr_budget_is_exceeded() -> None:
    policy = run_live_smoke_gate._NetworkBudget(max_api_xhr_calls=1)
    ledger: list[dict[str, object]] = []
    route = AsyncMock()

    for _ in range(2):
        request = SimpleNamespace(
            url="https://www.koreabaseball.com/api/test",
            resource_type="xhr",
            method="GET",
        )
        await run_live_smoke_gate._route_interceptor(route, request, ledger, policy)

    assert policy.violation is not None
    assert "API/XHR budget" in policy.violation
    route.abort.assert_awaited_once()
    route.continue_.assert_awaited_once()


@pytest.mark.asyncio
async def test_live_smoke_route_interceptor_blocks_unauthorized_host() -> None:
    policy = run_live_smoke_gate._NetworkBudget()
    ledger: list[dict[str, object]] = []
    route = AsyncMock()

    request = SimpleNamespace(
        url="https://evilwikipedia.org/fake",
        resource_type="document",
        method="GET",
    )
    await run_live_smoke_gate._route_interceptor(route, request, ledger, policy)

    assert policy.violation is not None
    assert "Host not in allowlist" in policy.violation
    route.abort.assert_awaited_once()


@pytest.mark.parametrize("status", [403, 429])
def test_live_smoke_response_policy_rejects_rate_or_access_denial(status: int) -> None:
    policy = run_live_smoke_gate._NetworkBudget()
    response = SimpleNamespace(
        url="https://www.koreabaseball.com/target",
        status=status,
    )

    run_live_smoke_gate._inspect_response(response, policy)

    assert policy.violation == f"HTTP {status} encountered: {response.url}"


def test_live_smoke_response_policy_detects_bot_challenge() -> None:
    policy = run_live_smoke_gate._NetworkBudget()
    response = SimpleNamespace(
        url="https://www.koreabaseball.com/verify-you-are-human",
        status=200,
    )

    run_live_smoke_gate._inspect_response(response, policy)

    assert policy.violation is not None
    assert "challenge" in policy.violation.lower()


def test_offline_preflight_fails_closed_when_protected_db_is_missing(tmp_path, monkeypatch, capsys) -> None:
    monkeypatch.setattr(run_offline_preflight, "PROTECTED_DB_PATH", tmp_path / "missing.db")

    assert run_offline_preflight.main() == 2
    assert "refusing to certify" in capsys.readouterr().out


@pytest.mark.asyncio
async def test_live_smoke_fails_closed_before_network_when_protected_db_is_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(run_live_smoke_gate, "PROTECTED_DB_PATH", tmp_path / "missing.db")

    with (
        patch.object(run_live_smoke_gate, "run_target_1_player_search", new_callable=AsyncMock) as target_one,
        patch.object(run_live_smoke_gate, "run_target_2_basic2_headers", new_callable=AsyncMock) as target_two,
        patch.object(run_live_smoke_gate, "run_target_3_live_awards", new_callable=AsyncMock) as target_three,
    ):
        assert await run_live_smoke_gate.main_async() == 2

    target_one.assert_not_awaited()
    target_two.assert_not_awaited()
    target_three.assert_not_awaited()
