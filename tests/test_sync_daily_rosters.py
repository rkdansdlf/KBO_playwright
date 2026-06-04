from __future__ import annotations

import pytest

from src.models.team import TeamDailyRoster
from src.sync.oci_sync import OCISync
from src.sync.sync_misc import _normalize_daily_roster_date


class _FakeRosterQuery:
    def __init__(self):
        self.filters = []

    def filter(self, *expressions):
        self.filters.extend(expressions)
        return self

    def count(self):
        return 0

    def all(self):
        return []


class _FakeSession:
    def __init__(self):
        self.query_model = None
        self.query_obj = _FakeRosterQuery()

    def query(self, *args):
        if args:
            # Extract the model class from the column expression if available
            self.query_model = getattr(args[0], "class_", None) or TeamDailyRoster
        return self.query_obj


def test_normalize_daily_roster_date_accepts_supported_formats():
    assert _normalize_daily_roster_date("20260531").isoformat() == "2026-05-31"
    assert _normalize_daily_roster_date("2026-05-31").isoformat() == "2026-05-31"


def test_sync_daily_rosters_applies_date_scope_to_local_query():
    syncer = object.__new__(OCISync)
    fake_session = _FakeSession()
    syncer.sqlite_session = fake_session

    result = syncer.sync_daily_rosters(start_date="20260531", end_date="2026-06-01")

    assert result == 0
    assert fake_session.query_model is TeamDailyRoster
    assert len(fake_session.query_obj.filters) == 2


def test_sync_daily_rosters_rejects_inverted_date_range():
    syncer = object.__new__(OCISync)

    with pytest.raises(ValueError, match="start_date"):
        syncer.sync_daily_rosters(start_date="20260602", end_date="20260601")
