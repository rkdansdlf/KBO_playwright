from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from src.models.award import Award
from src.models.franchise import Franchise
from src.models.team import Team, TeamCodeMap
from src.models.team_history import TeamHistory
from src.sync.oci_sync import OCISync


def _build_memory_session():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Franchise.__table__,
        Team.__table__,
        TeamHistory.__table__,
        TeamCodeMap.__table__,
        Award.__table__,
    ):
        table.create(bind=engine)
    return Session(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


class TestSyncFranchises:
    def test_calls_sync_simple_table(self):
        syncer = object.__new__(OCISync)
        syncer.sqlite_session = _build_memory_session()

        calls = []
        def fake_sync_simple_table(model, conflict_keys, **kw):
            calls.append((model, conflict_keys, kw))
            return 5
        syncer._sync_simple_table = fake_sync_simple_table

        result = syncer.sync_franchises()
        assert result == 5
        assert len(calls) == 1
        model, conflict_keys, kw = calls[0]
        assert model is Franchise
        assert conflict_keys == ["original_code"]

    def test_returns_zero_when_no_data(self):
        syncer = object.__new__(OCISync)
        syncer.sqlite_session = _build_memory_session()
        assert syncer.sync_franchises() == 0


class TestSyncTeams:
    def test_calls_bulk_copy_upsert_with_pg_array_aliases(self):
        syncer = object.__new__(OCISync)
        session = _build_memory_session()
        syncer.sqlite_session = session

        # Seed one team with list aliases
        session.add(Franchise(id=1, name="Test", original_code="TT", current_code="TT"))
        session.add(Team(
            team_id="TT", team_name="Test Team", team_short_name="TT",
            city="Seoul", franchise_id=1, is_active=True,
            aliases=["Test", "TT"],
        ))
        # Seed a second team with string aliases
        session.add(Team(
            team_id="SS", team_name="Second", team_short_name="SS",
            city="Busan", franchise_id=1, is_active=True,
            aliases='["Second","SS"]',
        ))
        # Seed a third team with None aliases
        session.add(Team(
            team_id="NN", team_name="No Alias", team_short_name="NN",
            city="Daegu", franchise_id=1, is_active=True,
            aliases=None,
        ))
        session.flush()

        calls = []
        def fake_bulk_copy_upsert(table_name, records, unique_cols, **kw):
            calls.append((table_name, records, unique_cols, kw))
        syncer._bulk_copy_upsert = fake_bulk_copy_upsert

        # Mock franchise mapping
        syncer._get_franchise_id_mapping = lambda: {1: 100}

        result = syncer.sync_teams()
        assert result == 3
        assert len(calls) == 1
        table_name, records, unique_cols, _ = calls[0]
        assert table_name == "teams"
        assert unique_cols == ["team_id"]
        assert len(records) == 3

        r0 = next(r for r in records if r["team_id"] == "TT")
        assert r0["aliases"] == "{Test,TT}"
        assert r0["franchise_id"] == 100

        r1 = next(r for r in records if r["team_id"] == "SS")
        assert r1["aliases"] == "{Second,SS}"

        r2 = next(r for r in records if r["team_id"] == "NN")
        assert r2["aliases"] == "{}"


class TestSyncAwards:
    def test_calls_sync_simple_table(self):
        syncer = object.__new__(OCISync)
        syncer.sqlite_session = _build_memory_session()

        calls = []
        def fake(model, conflict_keys, **kw):
            calls.append((model, conflict_keys, kw))
            return 3
        syncer._sync_simple_table = fake

        result = syncer.sync_awards()
        assert result == 3
        assert len(calls) == 1
        model, conflict_keys, kw = calls[0]
        assert model is Award
        assert conflict_keys == ["year", "award_type", "category", "player_name", "team_name"]


class TestSyncTeamHistory:
    def test_calls_bulk_copy_upsert_with_franchise_mapping(self):
        syncer = object.__new__(OCISync)
        session = _build_memory_session()
        syncer.sqlite_session = session

        session.add(Franchise(id=1, name="Test", original_code="TT", current_code="TT"))
        session.add(TeamHistory(id=10, franchise_id=1, season=2025, team_name="Test", team_code="TT"))
        session.flush()

        syncer._get_franchise_id_mapping = lambda: {1: 100}
        syncer._target_table_exists = lambda model: True

        calls = []
        def fake(table_name, records, unique_cols, **kw):
            calls.append((table_name, records, unique_cols, kw))
        syncer._bulk_copy_upsert = fake

        result = syncer.sync_team_history()
        assert result == 1
        assert len(calls) == 1
        _, records, unique_cols, _ = calls[0]
        assert unique_cols == ["id"]
        assert records[0]["franchise_id"] == 100
        assert records[0]["id"] == 10


class TestSyncTeamCodeMap:
    def test_calls_sync_simple_table_with_transform(self):
        syncer = object.__new__(OCISync)
        session = _build_memory_session()
        syncer.sqlite_session = session

        session.add(TeamCodeMap(franchise_id=1, season=2025, curr_code="TT", canonical_code="TT"))
        session.flush()

        syncer._get_franchise_id_mapping = lambda: {1: 100}

        calls = []
        def fake(model, conflict_keys, **kw):
            calls.append((model, conflict_keys, kw))
            if kw.get("transform_fn"):
                # Apply transform to verify mapping
                data = {"franchise_id": 1}
                kw["transform_fn"](data)
                assert data["franchise_id"] == 100
            return 1
        syncer._sync_simple_table = fake

        result = syncer.sync_team_code_map()
        assert result == 1
        assert len(calls) == 1
        model, conflict_keys, kw = calls[0]
        assert model is TeamCodeMap
        assert conflict_keys == ["season", "curr_code"]
        assert "transform_fn" in kw
