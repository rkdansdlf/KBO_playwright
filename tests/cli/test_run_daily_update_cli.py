from __future__ import annotations

import pytest

import src.cli.run_daily_update as cli
import src.utils.lock as lock_module


class _FakeLock:
    acquired = True
    events: list[tuple[str, str | bool]] = []

    def __init__(self, name: str, *, blocking: bool = False):
        self.name = name
        self.blocking = blocking
        self.events.append(("init", name))
        self.events.append(("blocking", blocking))

    def acquire(self) -> bool:
        self.events.append(("acquire", self.name))
        return self.acquired

    def release(self) -> None:
        self.events.append(("release", self.name))


def test_main_forwards_cli_options_and_releases_lock(monkeypatch):
    calls = {}
    _FakeLock.acquired = True
    _FakeLock.events = []

    async def fake_run_update(target_date: str, options: cli.DailyUpdateOptions | None = None):
        calls["target_date"] = target_date
        calls["options"] = options
        return 0

    monkeypatch.setattr(lock_module, "ProcessLock", _FakeLock)
    monkeypatch.setattr(cli, "run_update", fake_run_update)
    monkeypatch.setenv("DAILY_AUTO_REMEDIATION", "0")

    result = cli.main(
        [
            "--date",
            "20260618",
            "--sync",
            "--no-headless",
            "--limit",
            "2",
            "--summary-dir",
            "logs/test-summary",
            "--seed-tomorrow-preview",
            "--skip-auto-healer",
            "--skip-postgame-reconciliation",
            "--postgame-reconcile-lookback-days",
            "5",
            "--fix",
            "--skip-season-stats",
            "--skip-oci-supporting-sync",
            "--skip-p0-non-game",
        ],
    )

    assert result == 0
    assert calls["target_date"] == "20260618"
    assert calls["options"] == cli.DailyUpdateOptions(
        sync=True,
        headless=False,
        limit=2,
        summary_dir="logs/test-summary",
        seed_tomorrow_preview=True,
        run_auto_healer=False,
        run_postgame_reconciliation=False,
        postgame_reconcile_lookback_days=5,
        fix=True,
        skip_season_stats=True,
        skip_oci_supporting_sync=True,
        run_p0_non_game=False,
    )
    assert _FakeLock.events == [
        ("init", "daily_update"),
        ("blocking", False),
        ("acquire", "daily_update"),
        ("release", "daily_update"),
    ]


def test_main_returns_one_when_daily_lock_is_held(monkeypatch):
    _FakeLock.acquired = False
    _FakeLock.events = []

    async def fail_run_update(*_args, **_kwargs):
        pytest.fail("run_update should not run when lock acquisition fails")

    monkeypatch.setattr(lock_module, "ProcessLock", _FakeLock)
    monkeypatch.setattr(cli, "run_update", fail_run_update)

    assert cli.main(["--date", "20260618"]) == 1
    assert _FakeLock.events == [
        ("init", "daily_update"),
        ("blocking", False),
        ("acquire", "daily_update"),
    ]


def test_main_rejects_invalid_date_before_lock(monkeypatch):
    _FakeLock.events = []
    monkeypatch.setattr(lock_module, "ProcessLock", _FakeLock)

    with pytest.raises(SystemExit) as exc_info:
        cli.main(["--date", "2026-06-18"])

    assert exc_info.value.code == 1
    assert _FakeLock.events == []
