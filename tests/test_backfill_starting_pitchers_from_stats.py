from argparse import Namespace

import src.cli.backfill_starting_pitchers_from_stats as backfill


class _FakeScalarResult:
    def __init__(self, rows):
        self.rows = rows

    def __iter__(self):
        return iter(self.rows)


class _FakeMappingResult:
    def __init__(self, rows):
        self.rows = rows

    def mappings(self):
        return self

    def all(self):
        return self.rows


class _FakeTargetConnection:
    def __init__(self, rows, calls):
        self.rows = rows
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params):
        self.calls.append((str(query), dict(params)))
        return _FakeScalarResult(self.rows)


class _FakeTargetEngine:
    def __init__(self, rows, calls):
        self.rows = rows
        self.calls = calls

    def connect(self):
        return _FakeTargetConnection(self.rows, self.calls)


class _FakeSession:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, query, params):
        self.calls.append((str(query), dict(params)))
        return _FakeMappingResult(self.rows)


def test_find_target_missing_ready_games_uses_local_game_pitchers_without_stats_join(monkeypatch):
    target_calls = []
    session = _FakeSession(
        [
            {
                "game_id": "20250101KTHH0",
                "away_pitcher": "Away Starter",
                "home_pitcher": "Home Starter",
            },
            {
                "game_id": "20250101LGSS0",
                "away_pitcher": "Other Away",
                "home_pitcher": "Other Home",
            },
        ]
    )

    monkeypatch.setenv("OCI_DB_URL", "sqlite:///target.db")
    monkeypatch.setattr(
        backfill,
        "create_engine",
        lambda url: _FakeTargetEngine(
            [("20250101KTHH0",), ("20250101HHKT0",)],
            target_calls,
        ),
    )

    rows = backfill.find_target_missing_ready_games(
        session,
        Namespace(start_date="20250101", end_date="20250101"),
    )

    assert rows == [
        {
            "game_id": "20250101KTHH0",
            "away_pitcher": "Away Starter",
            "home_pitcher": "Home Starter",
        }
    ]
    assert target_calls[0][1] == {
        "start_date": "2025-01-01",
        "end_date": "2025-01-01",
    }
    assert "game_pitching_stats" not in session.calls[0][0]
