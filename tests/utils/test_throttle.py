import asyncio

from src.utils.throttle import AsyncThrottle


class TestAsyncThrottle:
    def test_uses_environment_defaults(self, monkeypatch):
        monkeypatch.setenv("KBO_REQUEST_DELAY", "2.5")
        monkeypatch.setenv("KBO_REQUEST_JITTER", "0.75")
        throttle = AsyncThrottle()
        assert throttle.default_delay == 2.5
        assert throttle.jitter == 0.75

    def test_default_constructor(self):
        throttle = AsyncThrottle(delay=1.0, jitter=0.3)
        assert throttle.default_delay == 1.0
        assert throttle.jitter == 0.3

    def test_wait_sync_tracks_hosts_independently(self, monkeypatch):
        throttle = AsyncThrottle(delay=1.0, jitter=0.0)
        now = {"value": 10.0}
        sleeps = []

        monkeypatch.setattr("src.utils.throttle.time.monotonic", lambda: now["value"])

        def _sleep(seconds):
            sleeps.append(seconds)
            now["value"] += seconds

        monkeypatch.setattr("src.utils.throttle.time.sleep", _sleep)

        throttle.wait_sync("host_a")
        throttle.wait_sync("host_b")
        throttle.wait_sync("host_a")
        assert sleeps == [1.0]
        assert set(throttle._last_request_times) == {"host_a", "host_b"}

    def test_wait_sync_no_sleep_if_elapsed(self, monkeypatch):
        throttle = AsyncThrottle(delay=1.0, jitter=0.0)
        now = {"value": 10.0}
        sleeps = []

        monkeypatch.setattr("src.utils.throttle.time.monotonic", lambda: now["value"])

        def _sleep(seconds):
            sleeps.append(seconds)
            now["value"] += seconds

        monkeypatch.setattr("src.utils.throttle.time.sleep", _sleep)

        throttle.wait_sync("host")
        throttle.wait_sync("host")
        assert len(sleeps) == 1

    @pytest.mark.asyncio
    async def test_wait_async_tracks_hosts(self, monkeypatch):
        throttle = AsyncThrottle(delay=0.1, jitter=0.0)
        sleeps = []

        async def _asleep(seconds):
            sleeps.append(seconds)

        monkeypatch.setattr("src.utils.throttle.asyncio.sleep", _asleep)

        await throttle.wait("host_a")
        await throttle.wait("host_b")
        assert len(sleeps) == 2

    def test_setter_updates_delay(self):
        throttle = AsyncThrottle(delay=1.0)
        throttle.default_delay = 3.0
        assert throttle.default_delay == 3.0

    def test_negative_jitter_not_allowed(self, monkeypatch):
        throttle = AsyncThrottle(delay=0.5, jitter=0.0)
        now = {"value": 0.0}
        monkeypatch.setattr("src.utils.throttle.time.monotonic", lambda: now["value"])
        sleep_vals = []

        def _sleep(seconds):
            sleep_vals.append(seconds)
            now["value"] += seconds

        monkeypatch.setattr("src.utils.throttle.time.sleep", _sleep)
        throttle.wait_sync("test")
        assert sleep_vals[0] >= 0
