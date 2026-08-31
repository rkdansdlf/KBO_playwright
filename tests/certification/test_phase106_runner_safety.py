"""Safety contracts for Phase 106 certification runners."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from scripts.certification.phase106 import run_live_smoke_gate, run_offline_preflight


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
