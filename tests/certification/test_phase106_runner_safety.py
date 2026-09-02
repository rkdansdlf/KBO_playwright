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


# --- _NetworkBudget policy: navigation budget, challenge detection, stickiness ---


def test_network_budget_detects_top_level_navigation_budget_exceeded() -> None:
    policy = run_live_smoke_gate._NetworkBudget(max_top_level_navigations=1)
    budget = run_live_smoke_gate._NetworkBudget(max_top_level_navigations=1)
    budget.inspect_request(
        "https://www.koreabaseball.com/page1", "document"
    )
    assert budget.violation is None
    budget.inspect_request(
        "https://www.koreabaseball.com/page2", "document"
    )
    assert budget.violation is not None
    assert "Top-level navigation budget" in budget.violation


def test_network_budget_detects_challenge_url_in_request() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    request_url = "https://www.koreabaseball.com/some/verify-you-are-human/path"
    budget.inspect_request(request_url, "document")
    assert budget.violation is not None
    assert "Challenge" in budget.violation


@pytest.mark.parametrize(
    "path",
    [
        "/verify-you-are-human",
        "/verify_you_are_human",
        "/recaptcha",
        "/bot-detection",
        "/bot_detection",
        "/some/captchacheck",
        "/some/recaptcha/v2",
        "/some/bot-detection-here",
    ],
)
def test_contains_challenge_detects_all_markers(path: str) -> None:
    assert run_live_smoke_gate._contains_challenge(
        f"https://www.koreabaseball.com{path}"
    ) is True


def test_contains_challenge_cloudflare_path_is_safe() -> None:
    url = "https://www.koreabaseball.com" + run_live_smoke_gate.CLOUDFLARE_STATIC_PATH + "something"
    assert run_live_smoke_gate._contains_challenge(url) is False


def test_contains_challenge_non_cloudflare_cloudflare_is_blocked() -> None:
    url = "https://www.koreabaseball.com/some/cloudflare/challenge"
    assert run_live_smoke_gate._contains_challenge(url) is True


def test_network_budget_violation_is_sticky() -> None:
    budget = run_live_smoke_gate._NetworkBudget(max_api_xhr_calls=1)
    request = SimpleNamespace(
        url="https://www.koreabaseball.com/api/a",
        resource_type="xhr",
        method="GET",
    )
    route = AsyncMock()
    import asyncio

    asyncio.run(run_live_smoke_gate._route_interceptor(route, request, [], budget))
    budget.inspect_request("https://www.koreabaseball.com/api/b", "xhr")
    assert budget.violation is not None
    assert "API/XHR budget" in budget.violation


def test_raise_if_violated_raises_on_violation() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    budget._set_violation("test violation")
    with pytest.raises(RuntimeError, match="test violation"):
        budget.raise_if_violated()


def test_raise_if_violated_passes_when_clean() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    budget.raise_if_violated()


def test_default_budget_matches_constants() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    assert budget.max_top_level_navigations == run_live_smoke_gate.MAX_TOP_LEVEL_NAVIGATIONS
    assert budget.max_api_xhr_calls == run_live_smoke_gate.MAX_API_XHR_CALLS


@pytest.mark.asyncio
async def test_route_interceptor_blocks_image_resources() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    ledger: list[dict[str, object]] = []
    route = AsyncMock()

    request = SimpleNamespace(
        url="https://www.koreabaseball.com/logo.png",
        resource_type="image",
        method="GET",
    )
    await run_live_smoke_gate._route_interceptor(route, request, ledger, budget)

    assert budget.violation is None
    route.abort.assert_awaited_once()
    assert ledger[0]["action"] == "BLOCKED_BY_POLICY"


@pytest.mark.asyncio
async def test_route_interceptor_allows_kbo_host() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    ledger: list[dict[str, object]] = []
    route = AsyncMock()

    request = SimpleNamespace(
        url="https://www.koreabaseball.com/Player/Search.aspx",
        resource_type="document",
        method="GET",
    )
    await run_live_smoke_gate._route_interceptor(route, request, ledger, budget)

    assert budget.violation is None
    route.continue_.assert_awaited_once()
    assert ledger[0]["action"] == "ALLOWED_REQUEST"


@pytest.mark.asyncio
async def test_route_interceptor_blocked_pattern_aborts_without_violation() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    ledger: list[dict[str, object]] = []
    route = AsyncMock()

    request = SimpleNamespace(
        url="https://www.koreabaseball.com/some/google-analytics.com/script.js",
        resource_type="script",
        method="GET",
    )
    await run_live_smoke_gate._route_interceptor(route, request, ledger, budget)

    assert budget.violation is None
    route.abort.assert_awaited_once()
    assert ledger[0]["action"] == "BLOCKED_BY_POLICY"


def test_inspect_response_detects_challenge_in_url() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    response = SimpleNamespace(
        url="https://www.koreabaseball.com/verify-you-are-human",
        status=200,
    )
    run_live_smoke_gate._inspect_response(response, budget)
    assert budget.violation is not None
    assert "challenge" in budget.violation.lower()


def test_inspect_text_detects_challenge_content() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    budget.inspect_text("Please solve the recaptcha", "https://www.koreabaseball.com/page")
    assert budget.violation is not None
    assert "Challenge content detected" in budget.violation


def test_inspect_text_clean_when_no_challenge() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    budget.inspect_text("This is normal page content", "https://www.koreabaseball.com/page")
    assert budget.violation is None


def test_inspect_response_ignores_safe_status() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    response = SimpleNamespace(
        url="https://www.koreabaseball.com/target",
        status=200,
    )
    run_live_smoke_gate._inspect_response(response, budget)
    assert budget.violation is None


def test_inspect_response_503_does_not_trigger_violation() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    response = SimpleNamespace(
        url="https://www.koreabaseball.com/target",
        status=503,
    )
    run_live_smoke_gate._inspect_response(response, budget)
    assert budget.violation is None


def test_route_interceptor_wildcard_host_allowed() -> None:
    budget = run_live_smoke_gate._NetworkBudget()
    ledger: list[dict[str, object]] = []
    route = AsyncMock()

    request = SimpleNamespace(
        url="https://edge.naverncp.com/some/resource",
        resource_type="fetch",
        method="GET",
    )
    import asyncio
    asyncio.run(run_live_smoke_gate._route_interceptor(route, request, ledger, budget))

    assert budget.violation is None
    route.continue_.assert_awaited_once()
