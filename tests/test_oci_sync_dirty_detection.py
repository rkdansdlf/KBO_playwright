from __future__ import annotations

from datetime import date, datetime, time

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

import src.sync.sync_base as sync_base_module
import src.sync.sync_games as sync_games_module
from src.models.game import (
    Game,
    GameBattingStat,
    GameEvent,
    GameHighlight,
    GameIdAlias,
    GameInningScore,
    GameLineup,
    GameMetadata,
    GamePitchingStat,
    GamePlayByPlay,
    GameSummary,
    GameValidationMetrics,
    PlayerGameBatting,
    PlayerGamePitching,
)
from src.models.player import PlayerBasic
from src.sync.oci_sync import OCISync
from src.sync.sync_base import (
    _dedupe_records_for_conflict_keys,
    build_game_sync_eligibility,
    detect_dirty_game_ids,
    filter_game_ids_by_year,
    filter_publishable_game_ids,
)


def _build_session_factory():
    engine = create_engine("sqlite:///:memory:")
    for table in (
        Game.__table__,
        PlayerBasic.__table__,
        GameMetadata.__table__,
        GameInningScore.__table__,
        GameLineup.__table__,
        GameBattingStat.__table__,
        GamePitchingStat.__table__,
        GameEvent.__table__,
        GameSummary.__table__,
        GamePlayByPlay.__table__,
        GameIdAlias.__table__,
        GameValidationMetrics.__table__,
        GameHighlight.__table__,
        PlayerGameBatting.__table__,
        PlayerGamePitching.__table__,
    ):
        table.create(bind=engine)
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


def test_reset_target_sequence_for_table_uses_postgres_serial_sequence():
    class _Result:
        def __init__(self, value):
            self.value = value

        def scalar(self):
            return self.value

    class _Dialect:
        name = "postgresql"

    class _Bind:
        dialect = _Dialect()

    class _Session:
        def __init__(self):
            self.executed = []
            self.commits = 0

        def get_bind(self):
            return _Bind()

        def execute(self, stmt, params=None):
            sql = str(stmt)
            self.executed.append((sql, params or {}))
            if "pg_get_serial_sequence" in sql:
                return _Result("public.game_play_by_play_id_seq")
            return _Result(None)

        def commit(self):
            self.commits += 1

    syncer = OCISync.__new__(OCISync)
    syncer.target_session = _Session()

    assert syncer._reset_target_sequence_for_table("game_play_by_play") is True
    assert syncer.target_session.commits == 1
    assert syncer.target_session.executed[0][1] == {
        "table_name": "game_play_by_play",
        "column_name": "id",
    }
    reset_sql = syncer.target_session.executed[1][0]
    assert "setval" in reset_sql
    assert "to_regclass" in reset_sql
    assert '"game_play_by_play"' in reset_sql
    assert '"id"' in reset_sql


def test_reset_target_sequence_for_table_skips_non_postgres_target():
    class _Dialect:
        name = "sqlite"

    class _Bind:
        dialect = _Dialect()

    class _Session:
        def get_bind(self):
            return _Bind()

        def execute(self, *_args, **_kwargs):
            raise AssertionError("non-Postgres targets should not execute sequence SQL")

    syncer = OCISync.__new__(OCISync)
    syncer.target_session = _Session()

    assert syncer._reset_target_sequence_for_table("game_play_by_play") is False


def test_reset_target_sequence_for_table_skips_when_table_has_no_sequence():
    class _Result:
        def scalar(self):
            return None

    class _Dialect:
        name = "postgresql"

    class _Bind:
        dialect = _Dialect()

    class _Session:
        def __init__(self):
            self.executed = 0
            self.commits = 0

        def get_bind(self):
            return _Bind()

        def execute(self, *_args, **_kwargs):
            self.executed += 1
            return _Result()

        def commit(self):
            self.commits += 1

    syncer = OCISync.__new__(OCISync)
    syncer.target_session = _Session()

    assert syncer._reset_target_sequence_for_table("game_play_by_play") is False


class _PlayerBasicTargetSession:
    def __init__(self, error: Exception | None = None):
        self.error = error
        self.executes = 0
        self.commits = 0
        self.rollbacks = 0

    def execute(self, *_args, **_kwargs):
        self.executes += 1
        if self.error:
            raise self.error

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _timeout_operational_error() -> OperationalError:
    return OperationalError(
        "INSERT INTO player_basic ...",
        {},
        Exception("could not receive data from server: Operation timed out"),
    )


def test_sync_player_basic_by_ids_retries_transient_target_execute(monkeypatch):
    local_factory = _build_session_factory()

    with local_factory() as session:
        session.add(PlayerBasic(player_id=50204, name="박지훈", status="retired"))
        session.commit()

        syncer = object.__new__(OCISync)
        syncer.sqlite_session = session
        syncer.oci_engine = session.bind
        reconnects = []

        def _reconnect():
            reconnects.append("reconnect")

        syncer._reconnect_oci = _reconnect
        monkeypatch.setattr(sync_base_module.time, "sleep", lambda _seconds: None)

        attempts = 0

        def _mock_do_bulk_copy_upsert(_self, table_name, records, unique_cols, update_timestamp, **kwargs):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise _timeout_operational_error()
            return len(records)

        monkeypatch.setattr(OCISync, "_do_bulk_copy_upsert", _mock_do_bulk_copy_upsert)

        assert syncer.sync_player_basic_by_ids([50204]) == 1
        assert attempts == 2
        assert reconnects == ["reconnect"]


def test_sync_player_basic_by_ids_rolls_back_and_raises_after_retry_exhaustion(monkeypatch):
    local_factory = _build_session_factory()

    with local_factory() as session:
        session.add(PlayerBasic(player_id=50204, name="박지훈", status="retired"))
        session.commit()

        syncer = object.__new__(OCISync)
        syncer.sqlite_session = session
        syncer.oci_engine = session.bind
        reconnects = []

        def _reconnect():
            reconnects.append("reconnect")

        syncer._reconnect_oci = _reconnect
        monkeypatch.setattr(sync_base_module.time, "sleep", lambda _seconds: None)

        attempts = 0

        def _mock_do_bulk_copy_upsert(_self, table_name, records, unique_cols, update_timestamp, **kwargs):
            nonlocal attempts
            attempts += 1
            raise _timeout_operational_error()

        monkeypatch.setattr(OCISync, "_do_bulk_copy_upsert", _mock_do_bulk_copy_upsert)

        with pytest.raises(OperationalError):
            syncer.sync_player_basic_by_ids([50204])

        assert attempts == 3
        assert reconnects == ["reconnect", "reconnect"]


def test_reset_target_sequence_for_table_rejects_unsafe_identifiers():
    class _Dialect:
        name = "postgresql"

    class _Bind:
        dialect = _Dialect()

    class _Session:
        def get_bind(self):
            return _Bind()

        def execute(self, *_args, **_kwargs):
            raise AssertionError("unsafe identifiers should be rejected before SQL execution")

    syncer = OCISync.__new__(OCISync)
    syncer.target_session = _Session()

    with pytest.raises(ValueError, match="unsafe SQL identifier"):
        syncer._reset_target_sequence_for_table("game_play_by_play;drop")


def test_sync_game_play_by_play_resets_target_sequence_before_replace(monkeypatch):
    local_factory = _build_session_factory()
    stamp = datetime(2026, 5, 14, 18, 0, 0)
    calls = []

    class _DeleteQuery:
        def filter(self, *_args):
            calls.append("delete_filter")
            return self

        def delete(self, **_kwargs):
            calls.append("delete_rows")
            return 1

    class _TargetSession:
        def query(self, model):
            assert model is GamePlayByPlay
            calls.append("target_query")
            return _DeleteQuery()

        def execute(self, _stmt, mappings):
            calls.append(("insert", len(mappings)))

        def commit(self):
            calls.append("commit")

    def _reset_sequence(_self, table_name, column_name="id"):
        calls.append(("reset_sequence", table_name, column_name))
        return True

    with local_factory() as session:
        session.add(
            GamePlayByPlay(
                game_id="20260514NCLT0",
                inning=1,
                inning_half="top",
                pitcher_name="투수",
                batter_name="타자",
                play_description="타자 : 안타",
                event_type="single",
                result="1B",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

        syncer = object.__new__(OCISync)
        syncer.sqlite_session = session
        syncer.target_session = _TargetSession()
        syncer.oci_engine = session.bind
        monkeypatch.setattr(OCISync, "_reset_target_sequence_for_table", _reset_sequence)

        def _fake_bulk_copy_upsert(_self, table_name, records, unique_cols, **kwargs):
            calls.append(("insert", len(records)))

        monkeypatch.setattr(OCISync, "_bulk_copy_upsert", _fake_bulk_copy_upsert)

        assert syncer._sync_game_play_by_play() == 1

    assert calls == [
        ("reset_sequence", "game_play_by_play", "id"),
        "target_query",
        "delete_filter",
        "delete_rows",
        "commit",
        ("insert", 1),
    ]


def test_sync_game_play_by_play_skips_sequence_reset_when_no_rows(monkeypatch):
    local_factory = _build_session_factory()
    syncer = object.__new__(OCISync)

    with local_factory() as session:
        syncer.sqlite_session = session
        monkeypatch.setattr(
            OCISync,
            "_reset_target_sequence_for_table",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("sequence reset should be skipped")),
        )

        assert syncer._sync_game_play_by_play() == 0


def test_sync_referenced_player_basic_for_games_skips_missing_local_player_basic(caplog):
    local_factory = _build_session_factory()
    stamp = datetime(2026, 5, 14, 18, 0, 0)

    with local_factory() as session:
        session.add(
            Game(
                game_id="20260514NCLT0",
                game_date=date(2026, 5, 14),
                away_team="NC",
                home_team="LT",
                game_status="SCHEDULED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GameLineup(
                game_id="20260514NCLT0",
                team_side="away",
                team_code="NC",
                player_id=901654,
                player_name="박시원",
                batting_order=9,
                appearance_seq=9,
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

        syncer = object.__new__(OCISync)
        syncer.sqlite_session = session

        with caplog.at_level("WARNING", logger="src.sync.sync_players"):
            assert syncer._sync_referenced_player_basic_for_games(["20260514NCLT0"]) == 0

        assert "Skipping 1 missing player_ids" in caplog.text
        assert "901654" in caplog.text


def test_sync_referenced_player_basic_for_games_syncs_local_stubs(monkeypatch):
    local_factory = _build_session_factory()
    stamp = datetime(2026, 5, 14, 18, 0, 0)

    with local_factory() as session:
        session.add(PlayerBasic(player_id=901654, name="박시원", team="NC", status="Unknown/Local"))
        session.add(
            Game(
                game_id="20260514NCLT0",
                game_date=date(2026, 5, 14),
                away_team="NC",
                home_team="LT",
                game_status="SCHEDULED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GameLineup(
                game_id="20260514NCLT0",
                team_side="away",
                team_code="NC",
                player_id=901654,
                player_name="박시원",
                batting_order=9,
                appearance_seq=9,
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

        syncer = object.__new__(OCISync)
        syncer.sqlite_session = session
        synced_ids = []

        def _sync_player_basic_by_ids(_self, player_ids):
            synced_ids.extend(player_ids)
            return len(player_ids)

        monkeypatch.setattr(OCISync, "sync_player_basic_by_ids", _sync_player_basic_by_ids)

        assert syncer._sync_referenced_player_basic_for_games(["20260514NCLT0"]) == 1
        assert synced_ids == [901654]


def test_sync_pregame_game_syncs_player_basic_before_lineups(monkeypatch):
    calls = []
    syncer = object.__new__(OCISync)

    class _DeleteQuery:
        def filter(self, *_args):
            return self

        def delete(self, **_kwargs):
            calls.append("delete_lineups")
            return 1

    class _TargetSession:
        def query(self, model):
            assert model is GameLineup
            return _DeleteQuery()

        def commit(self):
            calls.append("commit")

    def sync_simple_table(_self, model, _conflict_keys, **_kwargs):
        calls.append(model.__tablename__)
        return 1

    def _sync_refs(_self, game_ids):
        calls.append("player_basic_refs")
        assert game_ids == ["20260514NCLT0"]
        return 1

    def _sync_summary(_self, **_kwargs):
        calls.append("game_summary")
        return 1

    syncer.target_session = _TargetSession()
    monkeypatch.setattr(OCISync, "test_connection", lambda _self: True)
    monkeypatch.setattr(OCISync, "sync_simple_table", sync_simple_table)
    monkeypatch.setattr(OCISync, "_sync_referenced_player_basic_for_games", _sync_refs)
    monkeypatch.setattr(OCISync, "_sync_game_summary_rows", _sync_summary)

    result = syncer.sync_pregame_game("20260514NCLT0")

    assert result == {
        "game": 1,
        "game_id_aliases": 1,
        "player_basic": 1,
        "metadata": 1,
        "lineups": 1,
        "summary": 1,
    }
    assert calls == [
        "game",
        "game_id_aliases",
        "player_basic_refs",
        "delete_lineups",
        "commit",
        "game_metadata",
        "game_lineups",
        "game_summary",
    ]


def test_sync_specific_game_syncs_player_basic_before_child_replacement(monkeypatch):
    calls = []
    syncer = object.__new__(OCISync)

    class _Eligibility:
        detail_game_ids = ["20260514NCLT0"]
        relay_game_ids = ["20260514NCLT0"]

        def counts(self):
            return {}

    class _DeleteQuery:
        def __init__(self, model):
            self.model = model

        def filter(self, *_args):
            return self

        def delete(self, **_kwargs):
            calls.append(f"delete:{self.model.__tablename__}")
            return 1

    class _TargetSession:
        def query(self, model):
            return _DeleteQuery(model)

        def commit(self):
            calls.append("commit")

    def sync_simple_table(_self, model, _conflict_keys, **_kwargs):
        calls.append(model.__tablename__)
        return 1

    def _sync_refs(_self, game_ids):
        calls.append("player_basic_refs")
        assert game_ids == ["20260514NCLT0"]
        return 1

    def _sync_pbp(_self, **_kwargs):
        calls.append("game_play_by_play")
        return 1

    def _sync_summary(_self, **_kwargs):
        calls.append("game_summary")
        return 1

    monkeypatch.setattr(sync_games_module, "build_game_sync_eligibility", lambda *_args, **_kwargs: _Eligibility())
    monkeypatch.setattr(sync_games_module, "_log_sync_eligibility", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(OCISync, "test_connection", lambda _self: True)
    monkeypatch.setattr(OCISync, "sync_simple_table", sync_simple_table)
    monkeypatch.setattr(OCISync, "_sync_referenced_player_basic_for_games", _sync_refs)
    monkeypatch.setattr(OCISync, "_sync_game_play_by_play", _sync_pbp)
    monkeypatch.setattr(OCISync, "_sync_game_summary_rows", _sync_summary)

    syncer.sqlite_session = object()
    syncer.target_session = _TargetSession()

    result = syncer.sync_specific_game("20260514NCLT0")

    assert result["player_basic"] == 1
    assert calls[:3] == ["game", "game_id_aliases", "player_basic_refs"]
    assert calls.index("player_basic_refs") < calls.index("delete:game_lineups")
    assert calls.index("commit") < calls.index("game_metadata")
    assert calls[-4:] == ["game_events", "game_validation_metrics", "game_summary", "game_highlights"]


def test_sync_specific_game_skips_missing_optional_validation_metrics_table(monkeypatch):
    calls = []
    syncer = object.__new__(OCISync)

    class _Eligibility:
        detail_game_ids = ["20260514NCLT0"]
        relay_game_ids = ["20260514NCLT0"]

        def counts(self):
            return {}

    class _DeleteQuery:
        def __init__(self, model):
            self.model = model

        def filter(self, *_args):
            return self

        def delete(self, **_kwargs):
            calls.append(f"delete:{self.model.__tablename__}")
            return 1

    class _TargetSession:
        def query(self, model):
            return _DeleteQuery(model)

        def commit(self):
            calls.append("commit")

    def sync_simple_table(_self, model, _conflict_keys, **_kwargs):
        calls.append(model.__tablename__)
        return 1

    monkeypatch.setattr(sync_games_module, "build_game_sync_eligibility", lambda *_args, **_kwargs: _Eligibility())
    monkeypatch.setattr(sync_games_module, "_log_sync_eligibility", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(OCISync, "test_connection", lambda _self: True)
    monkeypatch.setattr(
        OCISync,
        "_target_table_exists",
        lambda _self, model: model is not GameValidationMetrics,
    )
    monkeypatch.setattr(OCISync, "sync_simple_table", sync_simple_table)
    monkeypatch.setattr(OCISync, "_sync_referenced_player_basic_for_games", lambda *_args: 1)
    monkeypatch.setattr(OCISync, "_sync_game_play_by_play", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(OCISync, "_sync_game_summary_rows", lambda *_args, **_kwargs: 1)

    syncer.sqlite_session = object()
    syncer.target_session = _TargetSession()

    result = syncer.sync_specific_game("20260514NCLT0")

    assert result["validation_metrics"] == 0
    assert "delete:game_validation_metrics" not in calls
    assert "game_validation_metrics" not in calls


def _seed_game(
    SessionLocal,
    game_id: str,
    *,
    game_updated_at: datetime,
    start_time: time = time(18, 30),
    metadata_updated_at: datetime | None = None,
    lineup_count: int = 0,
    lineup_updated_at: datetime | None = None,
):
    with SessionLocal() as session:
        session.add(
            Game(
                game_id=game_id,
                game_date=date(2025, 4, 1),
                away_team="LG",
                home_team="SS",
                game_status="LIVE",
                away_score=1,
                home_score=0,
                away_pitcher="임찬규",
                home_pitcher="원태인",
                created_at=game_updated_at,
                updated_at=game_updated_at,
            )
        )
        session.add(
            GameMetadata(
                game_id=game_id,
                start_time=start_time,
                created_at=metadata_updated_at or game_updated_at,
                updated_at=metadata_updated_at or game_updated_at,
            )
        )
        for idx in range(1, lineup_count + 1):
            session.add(
                GameLineup(
                    game_id=game_id,
                    team_side="away",
                    team_code="LG",
                    player_id=1000 + idx,
                    player_name=f"타자{idx}",
                    batting_order=idx,
                    appearance_seq=idx,
                    standard_position="CF",
                    created_at=lineup_updated_at or game_updated_at,
                    updated_at=lineup_updated_at or game_updated_at,
                )
            )
        session.commit()


def test_detect_dirty_game_ids_when_child_row_count_differs():
    local_factory = _build_session_factory()
    remote_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    _seed_game(local_factory, "20250401LGSS0", game_updated_at=stamp, lineup_count=1, lineup_updated_at=stamp)
    _seed_game(remote_factory, "20250401LGSS0", game_updated_at=stamp, lineup_count=0)

    with local_factory() as local_session, remote_factory() as remote_session:
        dirty = detect_dirty_game_ids(local_session, remote_session)

    assert dirty == ["20250401LGSS0"]


def test_detect_dirty_game_ids_uses_local_newer_updated_at_but_not_remote_newer():
    local_factory = _build_session_factory()
    remote_factory = _build_session_factory()
    older = datetime(2025, 4, 1, 18, 0, 0)
    newer = datetime(2025, 4, 1, 18, 5, 0)

    _seed_game(
        local_factory,
        "20250402LGSS0",
        game_updated_at=older,
        lineup_count=1,
        lineup_updated_at=newer,
    )
    _seed_game(
        remote_factory,
        "20250402LGSS0",
        game_updated_at=older,
        lineup_count=1,
        lineup_updated_at=older,
    )
    _seed_game(
        local_factory,
        "20250403LGSS0",
        game_updated_at=older,
        lineup_count=1,
        lineup_updated_at=older,
    )
    _seed_game(
        remote_factory,
        "20250403LGSS0",
        game_updated_at=newer,
        lineup_count=1,
        lineup_updated_at=newer,
    )

    with local_factory() as local_session, remote_factory() as remote_session:
        dirty = detect_dirty_game_ids(local_session, remote_session)

    assert dirty == ["20250402LGSS0"]


def test_filter_publishable_game_ids_excludes_schedule_only_parent_rows():
    local_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    with local_factory() as session:
        session.add(
            Game(
                game_id="20250404LGSS0",
                game_date=date(2025, 4, 4),
                away_team="LG",
                home_team="SS",
                game_status="SCHEDULED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            Game(
                game_id="20250405LGSS0",
                game_date=date(2025, 4, 5),
                away_team="LG",
                home_team="SS",
                game_status="CANCELLED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            Game(
                game_id="20250406LGSS0",
                game_date=date(2025, 4, 6),
                away_team="LG",
                home_team="SS",
                game_status="SCHEDULED",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GameLineup(
                game_id="20250406LGSS0",
                team_side="away",
                team_code="LG",
                player_name="타자1",
                batting_order=1,
                appearance_seq=1,
                standard_position="CF",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

    with local_factory() as session:
        publishable = filter_publishable_game_ids(
            session,
            ["20250404LGSS0", "20250405LGSS0", "20250406LGSS0"],
        )

    assert publishable == ["20250405LGSS0", "20250406LGSS0"]


def test_build_game_sync_eligibility_splits_detail_and_relay_targets():
    local_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    with local_factory() as session:
        session.add_all(
            [
                Game(
                    game_id="20250407LGSS0",
                    game_date=date(2025, 4, 7),
                    away_team="LG",
                    home_team="SS",
                    game_status="SCHEDULED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250408LGSS0",
                    game_date=date(2025, 4, 8),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250409LGSS0",
                    game_date=date(2025, 4, 9),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250410LGSS0",
                    game_date=date(2025, 4, 10),
                    away_team="LG",
                    home_team="SS",
                    game_status="CANCELLED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
            ]
        )
        for side, player_id in (("away", 1001), ("home", 1002)):
            session.add(
                GameBattingStat(
                    game_id="20250408LGSS0",
                    team_side=side,
                    player_id=player_id,
                    player_name=f"{side} batter",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
            session.add(
                GamePitchingStat(
                    game_id="20250408LGSS0",
                    team_side=side,
                    player_id=player_id + 100,
                    player_name=f"{side} pitcher",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
        session.add(
            GameBattingStat(
                game_id="20250409LGSS0",
                team_side="away",
                player_name="away only",
                appearance_seq=1,
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GamePlayByPlay(
                game_id="20250409LGSS0",
                inning=1,
                inning_half="top",
                play_description="타자A : 안타",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

    with local_factory() as session:
        eligibility = build_game_sync_eligibility(
            session,
            [
                "20250407LGSS0",
                "20250408LGSS0",
                "20250409LGSS0",
                "20250410LGSS0",
            ],
        )

    assert eligibility.parent_game_ids == ["20250408LGSS0", "20250409LGSS0", "20250410LGSS0"]
    assert eligibility.detail_game_ids == ["20250408LGSS0"]
    assert eligibility.relay_game_ids == ["20250409LGSS0"]
    assert eligibility.skipped_schedule_only == ["20250407LGSS0"]
    assert eligibility.skipped_incomplete_detail == ["20250409LGSS0"]
    assert eligibility.skipped_empty_relay == ["20250408LGSS0"]
    assert eligibility.skipped_cancelled == ["20250410LGSS0"]


def test_sync_game_details_filters_child_datasets_by_eligibility(monkeypatch):
    local_factory = _build_session_factory()
    stamp = datetime(2025, 4, 1, 18, 0, 0)

    with local_factory() as session:
        session.add_all(
            [
                Game(
                    game_id="20250411LGSS0",
                    game_date=date(2025, 4, 11),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
                Game(
                    game_id="20250412LGSS0",
                    game_date=date(2025, 4, 12),
                    away_team="LG",
                    home_team="SS",
                    away_score=1,
                    home_score=2,
                    game_status="COMPLETED",
                    created_at=stamp,
                    updated_at=stamp,
                ),
            ]
        )
        for side, player_id in (("away", 1101), ("home", 1102)):
            session.add(
                GameBattingStat(
                    game_id="20250411LGSS0",
                    team_side=side,
                    player_id=player_id,
                    player_name=f"{side} batter",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
            session.add(
                GamePitchingStat(
                    game_id="20250411LGSS0",
                    team_side=side,
                    player_id=player_id + 100,
                    player_name=f"{side} pitcher",
                    appearance_seq=1,
                    created_at=stamp,
                    updated_at=stamp,
                )
            )
        session.add(
            GameBattingStat(
                game_id="20250412LGSS0",
                team_side="away",
                player_name="away only",
                appearance_seq=1,
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.add(
            GamePlayByPlay(
                game_id="20250412LGSS0",
                inning=1,
                inning_half="top",
                play_description="타자A : 안타",
                created_at=stamp,
                updated_at=stamp,
            )
        )
        session.commit()

        syncer = object.__new__(OCISync)
        syncer.sqlite_session = session

        monkeypatch.setattr(
            OCISync,
            "get_unsynced_or_modified_game_ids",
            lambda _self: ["20250411LGSS0", "20250412LGSS0"],
        )
        monkeypatch.setattr(
            OCISync,
            "sync_games",
            lambda _self, filters=None, **_kwargs: session.query(Game).filter(*(filters or [])).count(),
        )
        monkeypatch.setattr(OCISync, "test_connection", lambda _self: True)

        def _count_table(_self, model, _conflict_keys, filters=None, **_kwargs):
            return session.query(model).filter(*(filters or [])).count()

        monkeypatch.setattr(OCISync, "sync_simple_table", _count_table)
        monkeypatch.setattr(
            OCISync,
            "_sync_game_play_by_play",
            lambda _self, filters=None: session.query(GamePlayByPlay).filter(*(filters or [])).count(),
        )
        monkeypatch.setattr(
            OCISync,
            "_sync_game_summary_rows",
            lambda _self, filters=None, **_kwargs: session.query(GameSummary).filter(*(filters or [])).count(),
        )

        results = syncer.sync_game_details(unsynced_only=True)

    assert results["games"] == 2
    assert results["batting_stats"] == 2
    assert results["pitching_stats"] == 2
    assert results["play_by_play"] == 1
    assert results["skipped_incomplete_detail"] == 1
    assert results["skipped_empty_relay"] == 1


def test_filter_game_ids_by_year_preserves_only_requested_year():
    game_ids = ["20240401LGSS0", "20250401LGSS0", "20250402LGSS0", "20260401LGSS0"]

    assert filter_game_ids_by_year(game_ids, 2025) == ["20250401LGSS0", "20250402LGSS0"]
    assert filter_game_ids_by_year(game_ids, None) == game_ids


def test_dedupe_records_for_conflict_keys_preserves_null_key_rows():
    records = [
        {"game_id": "20260426KTSK0", "player_id": None, "appearance_seq": 1, "team_side": "away"},
        {"game_id": "20260426KTSK0", "player_id": None, "appearance_seq": 1, "team_side": "home"},
        {"game_id": "20260426KTSK0", "player_id": 50859, "appearance_seq": 1, "team_side": "away"},
        {"game_id": "20260426KTSK0", "player_id": 50859, "appearance_seq": 1, "team_side": "away"},
    ]

    deduped = _dedupe_records_for_conflict_keys(
        records,
        ["game_id", "player_id", "appearance_seq"],
    )

    assert deduped == records[:3]
