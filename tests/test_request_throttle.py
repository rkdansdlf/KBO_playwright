from __future__ import annotations

import asyncio

from src.utils import request_policy
from src.utils.throttle import AsyncThrottle


def test_async_throttle_uses_environment_defaults(monkeypatch):
    monkeypatch.setenv("KBO_REQUEST_DELAY", "2.5")
    monkeypatch.setenv("KBO_REQUEST_JITTER", "0.75")

    throttle = AsyncThrottle()

    assert throttle.default_delay == 2.5
    assert throttle.jitter == 0.75


def test_wait_sync_tracks_hosts_independently(monkeypatch):
    throttle = AsyncThrottle(delay=1.0, jitter=0.0)
    now = {"value": 10.0}
    sleeps: list[float] = []

    monkeypatch.setattr("src.utils.throttle.time.monotonic", lambda: now["value"])

    def _sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now["value"] += seconds

    monkeypatch.setattr("src.utils.throttle.time.sleep", _sleep)

    throttle.wait_sync("a.example")
    throttle.wait_sync("b.example")
    throttle.wait_sync("a.example")

    assert sleeps == [1.0]
    assert set(throttle._last_request_times) == {"a.example", "b.example"}


def test_wait_async_tracks_hosts_independently(monkeypatch):
    throttle = AsyncThrottle(delay=1.0, jitter=0.0)
    now = {"value": 20.0}
    sleeps: list[float] = []

    monkeypatch.setattr("src.utils.throttle.time.monotonic", lambda: now["value"])

    async def _sleep(seconds: float) -> None:
        sleeps.append(seconds)
        now["value"] += seconds

    monkeypatch.setattr("src.utils.throttle.asyncio.sleep", _sleep)

    async def _run():
        await throttle.wait("a.example")
        await throttle.wait("b.example")
        await throttle.wait("a.example")

    asyncio.run(_run())

    assert sleeps == [1.0]
    assert set(throttle._last_request_times) == {"a.example", "b.example"}


def test_request_policy_delegates_sync_delay_to_shared_throttle(monkeypatch):
    monkeypatch.delenv("KBO_REQUEST_DELAY_MIN", raising=False)
    monkeypatch.delenv("KBO_REQUEST_DELAY_MAX", raising=False)
    calls = []

    class FakeThrottle:
        default_delay = 0.0

        def wait_sync(self, host):
            calls.append((self.default_delay, host))

    fake = FakeThrottle()
    monkeypatch.setattr(request_policy, "throttle", fake)

    policy = request_policy.RequestPolicy(min_delay=1.25, max_delay=1.5)
    policy.delay("koreabaseball.com")

    assert calls == [(1.25, "koreabaseball.com")]


def test_request_policy_delegates_async_delay_to_shared_throttle(monkeypatch):
    monkeypatch.delenv("KBO_REQUEST_DELAY_MIN", raising=False)
    monkeypatch.delenv("KBO_REQUEST_DELAY_MAX", raising=False)
    calls = []

    class FakeThrottle:
        default_delay = 0.0

        async def wait(self, host):
            calls.append((self.default_delay, host))

    fake = FakeThrottle()
    monkeypatch.setattr(request_policy, "throttle", fake)

    policy = request_policy.RequestPolicy(min_delay=1.25, max_delay=1.5)
    asyncio.run(policy.delay_async("koreabaseball.com"))

    assert calls == [(1.25, "koreabaseball.com")]
