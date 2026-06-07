import asyncio
from datetime import datetime
from types import SimpleNamespace

import src.cli.crawl_futures as module


class _FakePool:
    created = []

    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        _FakePool.created.append(self)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _FakeRepository:
    def __init__(self, player=None):
        self.player = player

    def upsert_player_profile(self, *_args, **_kwargs):
        return self.player


def test_has_player_basic_uses_sqlalchemy_2_execute(monkeypatch):
    class _FakeResult:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    class _FakeSession:
        def __init__(self, value):
            self.value = value
            self.execute_calls = 0

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _stmt):
            self.execute_calls += 1
            return _FakeResult(self.value)

        def query(self, *_args, **_kwargs):
            raise AssertionError("legacy query() should not be used")

    sessions = []
    results = iter([1234, None])

    def fake_session_local():
        session = _FakeSession(next(results))
        sessions.append(session)
        return session

    monkeypatch.setattr(module, "SessionLocal", fake_session_local)

    assert module._has_player_basic("1234") is True
    assert module._has_player_basic("5678") is False
    assert [session.execute_calls for session in sessions] == [1, 1]


def _args(**overrides):
    defaults = {
        "season": 2025,
        "concurrency": 2,
        "delay": 0,
        "limit": None,
        "json_summary": False,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_crawl_futures_reports_empty_player_list(monkeypatch):
    async def empty_ids(_season, _delay):
        return {}

    monkeypatch.setattr(module, "gather_active_player_ids", empty_ids)

    summary = asyncio.run(module.crawl_futures(_args()))

    assert summary["ok"] is False
    assert summary["processed"] == 0
    assert summary["failure_counts"] == {"player_list_empty": 1}


def test_process_player_result_marks_empty_futures_as_skip(monkeypatch):
    async def empty_rows(*_args, **_kwargs):
        return []

    monkeypatch.setattr(module, "fetch_and_parse_futures_batting", empty_rows)
    monkeypatch.setattr(module, "_has_player_basic", lambda _pid: True)

    result = asyncio.run(
        module.process_player_result("1001", "hitter", "PlayerA", _FakeRepository(), delay=0, pool=None)
    )

    assert result == {
        "player_id": "1001",
        "status": "skipped",
        "saved": 0,
        "failure_reason": "futures_empty",
    }


def test_process_player_result_does_not_save_when_profile_upsert_fails(monkeypatch):
    async def rows(*_args, **_kwargs):
        return [{"season": 2025, "AVG": 0.25}]

    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("save_futures_batting should not be called")

    monkeypatch.setattr(module, "fetch_and_parse_futures_batting", rows)
    monkeypatch.setattr(module, "save_futures_batting", fail_if_called)
    monkeypatch.setattr(module, "_has_player_basic", lambda _pid: True)

    result = asyncio.run(
        module.process_player_result("1001", "hitter", "PlayerA", _FakeRepository(player=None), delay=0, pool=None)
    )

    assert result["status"] == "failed"
    assert result["failure_reason"] == "profile_upsert_failed"


def test_process_player_result_skips_when_player_basic_missing(monkeypatch):
    async def rows(*_args, **_kwargs):
        return [{"season": 2026, "G": 10, "AB": 40, "AVG": 0.25}]

    monkeypatch.setattr(module, "fetch_and_parse_futures_batting", rows)
    monkeypatch.setattr(
        module,
        "save_futures_batting",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("save_futures_batting should be skipped")),
    )
    monkeypatch.setattr(
        module,
        "save_pitching_stats_to_db",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("save_pitching_stats_to_db should be skipped")),
    )
    monkeypatch.setattr(module, "_has_player_basic", lambda _pid: False)

    result = asyncio.run(
        module.process_player_result("5669", "hitter", "PlayerA", _FakeRepository(), delay=0, pool=None)
    )

    assert result == {
        "player_id": "5669",
        "status": "skipped",
        "saved": 0,
        "failure_reason": "missing_player_basic",
    }


def test_crawl_futures_continues_when_player_processing_raises(monkeypatch):
    async def ids(_season, _delay):
        return {"1001": {"position": "hitter", "name": "PlayerA"}}

    async def fail_on_player(*_args, **_kwargs):
        raise RuntimeError("processor exploded")

    monkeypatch.setattr(module, "gather_active_player_ids", ids)
    monkeypatch.setattr(module, "process_player_result", fail_on_player)
    monkeypatch.setattr(module, "AsyncPlaywrightPool", _FakePool)

    summary = asyncio.run(module.crawl_futures(_args()))

    assert summary["ok"] is False
    assert summary["processed"] == 1
    assert summary["failure_counts"] == {"exception": 1}


def test_crawl_futures_changed_since_skips_recent_futures_rows(monkeypatch):
    cutoff = datetime(2026, 6, 3)

    class _FakeQuery:
        def __init__(self, rows):
            self.rows = rows

        def filter(self, *_args):
            return self

        def all(self):
            return self.rows

    class _FakeSession:
        def __init__(self):
            self.query_results = [
                [
                    SimpleNamespace(player_id=1001, updated_at=datetime(2026, 6, 7)),
                    SimpleNamespace(player_id=1002, updated_at=datetime(2026, 6, 1)),
                ],
                [SimpleNamespace(player_id=1001, updated_at=datetime(2026, 6, 7))],
            ]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def query(self, *_args):
            return _FakeQuery(self.query_results.pop(0))

    async def ids(_season, _delay):
        return {
            "1001": {"position": "hitter", "name": "PlayerA"},
            "1002": {"position": "pitcher", "name": "PlayerB"},
        }

    processed = []

    async def fake_process(pid, pos, name, *_args, **_kwargs):
        processed.append((pid, pos, name))
        return {"player_id": pid, "status": "success", "saved": 1, "failure_reason": None}

    monkeypatch.setattr(module, "gather_active_player_ids", ids)
    monkeypatch.setattr(module, "SessionLocal", lambda: _FakeSession())
    monkeypatch.setattr(module, "process_player_result", fake_process)
    monkeypatch.setattr(module, "PlayerRepository", lambda: object())
    monkeypatch.setattr(module, "AsyncPlaywrightPool", _FakePool)

    summary = asyncio.run(module.crawl_futures(_args(changed_since=cutoff.isoformat())))

    assert processed == [("1002", "pitcher", "PlayerB")]
    assert summary["ok"] is True
    assert summary["processed"] == 1
    assert summary["success_count"] == 1


def test_crawl_futures_summary_groups_failure_reasons(monkeypatch):
    async def ids(_season, _delay):
        return {"1001": {"position": "hitter", "name": "PlayerA"}, "1002": {"position": "pitcher", "name": "PlayerB"}}

    async def fake_process(pid, pos, name, *_args, **_kwargs):
        if pid == "1001":
            return {"player_id": pid, "status": "success", "saved": 2, "failure_reason": None}
        return {"player_id": pid, "status": "skipped", "saved": 0, "failure_reason": "futures_empty"}

    monkeypatch.setattr(module, "gather_active_player_ids", ids)
    monkeypatch.setattr(module, "process_player_result", fake_process)
    monkeypatch.setattr(module, "PlayerRepository", lambda: object())
    monkeypatch.setattr(module, "AsyncPlaywrightPool", _FakePool)

    summary = asyncio.run(module.crawl_futures(_args()))

    assert summary["ok"] is True
    assert summary["processed"] == 2
    assert summary["success_count"] == 1
    assert summary["total_saved"] == 2
    assert summary["failure_counts"] == {"futures_empty": 1}
