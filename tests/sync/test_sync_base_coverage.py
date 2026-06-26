from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from src.sync.sync_base import (
    GAME_SIGNATURE_CHILD_TABLES,
    GameSyncEligibility,
    OCISyncBase,
    SyncBatchConfig,
    _build_composite_signature_query,
    _dedupe_records_for_conflict_keys,
    _execute_signature_query,
    _has_both_team_sides,
    _is_game_dirty,
    _is_game_dirty_by_child_tables,
    _is_game_dirty_by_game_section,
    _is_game_dirty_by_metadata_section,
    _load_game_ids_with_rows,
    _load_team_sides,
    _row_to_record,
    _serialize_scalar,
    build_game_sync_eligibility,
    detect_dirty_game_ids,
    filter_game_ids_by_year,
    filter_publishable_game_ids,
    load_game_sync_signatures,
)

pytestmark = pytest.mark.usefixtures("_db_engine")


@pytest.fixture
def _db_engine():
    engine = create_engine("sqlite:///:memory:", echo=False)
    Base = pytest.importorskip("src.models.base").Base
    Base.metadata.create_all(bind=engine)
    yield engine
    engine.dispose()


class TestGameSyncEligibilityCounts:
    def test_counts_returns_dict(self):
        e = GameSyncEligibility(
            skipped_schedule_only=["g1", "g2"],
            skipped_incomplete_detail=["g3"],
            skipped_empty_relay=["g4", "g5"],
            skipped_cancelled=["g6"],
        )
        assert e.counts() == {
            "skipped_schedule_only": 2,
            "skipped_incomplete_detail": 1,
            "skipped_empty_relay": 2,
            "skipped_cancelled": 1,
        }


class TestSerializeScalarEdgeCases:
    def test_float(self):
        assert _serialize_scalar(3.14) == 3.14

    def test_bool(self):
        assert _serialize_scalar(True) is True


class TestDedupeRecordsEdgeCases:
    def test_single_record(self):
        records = [{"id": 1, "val": "a"}]
        result = _dedupe_records_for_conflict_keys(records, ["id"])
        assert len(result) == 1

    def test_all_duplicates(self):
        records = [{"id": 1, "val": "a"}, {"id": 1, "val": "b"}]
        result = _dedupe_records_for_conflict_keys(records, ["id"])
        assert len(result) == 1


class TestRowToRecordTransform:
    def test_transform_fn_applied(self):
        mock = MagicMock()
        mock._mapping = {"a": 1, "b": 2}

        def transform(data):
            data["c"] = 3
            return data

        result = _row_to_record(mock, ["a", "b"], transform_fn=transform)
        assert result["c"] == 3


class TestLoadTeamSides:
    def test_empty_game_ids(self):
        session = MagicMock(spec=Session)
        result = _load_team_sides(session, MagicMock(), [])
        assert result == {}

    def test_filters_nulls(self, _db_engine):
        from src.models.base import Base
        from src.models.game import GameBattingStat

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        stat = GameBattingStat(
            game_id="20250601_01",
            team_side="home",
            player_id="p1",
            player_name="Test Player",
            appearance_seq=1,
        )
        session.add(stat)
        session.commit()

        result = _load_team_sides(session, GameBattingStat, ["20250601_01"])
        assert "20250601_01" in result
        assert "home" in result["20250601_01"]
        session.close()


class TestLoadGameIdsWithRows:
    def test_empty_game_ids(self):
        session = MagicMock(spec=Session)
        result = _load_game_ids_with_rows(session, MagicMock(), [])
        assert result == set()


class TestHasBothTeamSides:
    def test_both_sides(self):
        side_map = {"g1": {"home", "away"}}
        assert _has_both_team_sides(side_map, "g1") is True

    def test_one_side(self):
        side_map = {"g1": {"home"}}
        assert _has_both_team_sides(side_map, "g1") is False

    def test_missing_game(self):
        assert _has_both_team_sides({}, "g1") is False


class TestIsGameDirtyByGameSection:
    def test_different_home_score(self):
        local = {"game": {"home_score": 5}}
        remote = {"game": {"home_score": 3}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_away_score(self):
        local = {"game": {"away_score": 2}}
        remote = {"game": {"away_score": 4}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_home_pitcher(self):
        local = {"game": {"home_pitcher": "p1"}}
        remote = {"game": {"home_pitcher": "p2"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_away_pitcher(self):
        local = {"game": {"away_pitcher": "p1"}}
        remote = {"game": {"away_pitcher": "p2"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_home_team(self):
        local = {"game": {"home_team": "SSG"}}
        remote = {"game": {"home_team": "LG"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_different_away_team(self):
        local = {"game": {"away_team": "SSG"}}
        remote = {"game": {"away_team": "LG"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True

    def test_local_updated_at_none_not_dirty(self):
        local = {"game": {"updated_at": None}}
        remote = {"game": {"updated_at": "2025-01-01"}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is False

    def test_remote_updated_at_none_is_dirty(self):
        local = {"game": {"updated_at": "2025-01-01"}}
        remote = {"game": {"updated_at": None}}
        assert _is_game_dirty_by_game_section("g1", local, remote) is True


class TestIsGameDirtyByMetadataSection:
    def test_different_row_count(self):
        local = {"game_metadata": {"row_count": 1}}
        remote = {"game_metadata": {"row_count": 2}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is True

    def test_different_start_time(self):
        local = {"game_metadata": {"start_time": "2025-01-01T12:00:00"}}
        remote = {"game_metadata": {"start_time": "2025-01-01T13:00:00"}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is True

    def test_local_max_updated_none_not_dirty(self):
        local = {"game_metadata": {"max_updated_at": None}}
        remote = {"game_metadata": {"max_updated_at": "2025-01-01"}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is False

    def test_remote_max_updated_none_is_dirty(self):
        local = {"game_metadata": {"max_updated_at": "2025-01-01"}}
        remote = {"game_metadata": {"max_updated_at": None}}
        assert _is_game_dirty_by_metadata_section("g1", local, remote) is True


class TestIsGameDirtyByChildTables:
    def test_different_row_count(self):
        local = {"game_inning_scores": {"row_count": 1}}
        remote = {"game_inning_scores": {"row_count": 2}}
        assert _is_game_dirty_by_child_tables("g1", local, remote) is True

    def test_newer_max_updated(self):
        local = {"game_inning_scores": {"max_updated_at": "2025-06-02"}}
        remote = {"game_inning_scores": {"max_updated_at": "2025-06-01"}}
        assert _is_game_dirty_by_child_tables("g1", local, remote) is True

    def test_identical_not_dirty(self):
        local = {"game_inning_scores": {"row_count": 1, "max_updated_at": "2025-06-01"}}
        remote = {"game_inning_scores": {"row_count": 1, "max_updated_at": "2025-06-01"}}
        assert _is_game_dirty_by_child_tables("g1", local, remote) is False


class TestFilterGameIdsByYearEdgeCases:
    def test_mixed_prefixes(self):
        result = filter_game_ids_by_year(["20240101", "20250101", "20241231", "20231201"], 2024)
        assert result == ["20240101", "20241231"]


class TestBuildCompositeSignatureQueryEdgeCases:
    def test_all_tables_included(self):
        sql = _build_composite_signature_query(None)
        for table in GAME_SIGNATURE_CHILD_TABLES:
            assert table in sql


class TestBuildGameSyncEligibilityCompleted:
    def test_completed_with_detail(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game, GameBattingStat, GamePitchingStat

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="COMPLETED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="home", player_id="p1", player_name="P1", appearance_seq=1)
        )
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="away", player_id="p2", player_name="P2", appearance_seq=2)
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="home", player_id="p3", player_name="P3", appearance_seq=1
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="away", player_id="p4", player_name="P4", appearance_seq=2
            )
        )
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.parent_game_ids
        assert "20250601_01" in e.detail_game_ids
        session.close()

    def test_completed_without_relay_skipped(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game, GameBattingStat, GamePitchingStat

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="COMPLETED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="home", player_id="p1", player_name="P1", appearance_seq=1)
        )
        session.add(
            GameBattingStat(game_id="20250601_01", team_side="away", player_id="p2", player_name="P2", appearance_seq=2)
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="home", player_id="p3", player_name="P3", appearance_seq=1
            )
        )
        session.add(
            GamePitchingStat(
                game_id="20250601_01", team_side="away", player_id="p4", player_name="P4", appearance_seq=2
            )
        )
        session.commit()

        e = build_game_sync_eligibility(session, ["20250601_01"])
        assert "20250601_01" in e.skipped_empty_relay
        session.close()


class TestFilterPublishableGameIds:
    def test_returns_parent_ids(self, _db_engine):
        from src.models.base import Base
        from src.models.game import Game

        Base.metadata.create_all(bind=_db_engine)
        session = sessionmaker(bind=_db_engine)()

        g = Game(
            game_id="20250601_01",
            game_date=datetime(2025, 6, 1),
            home_team="SSG",
            away_team="LG",
            game_status="COMPLETED",
            home_score=5,
            away_score=3,
        )
        session.add(g)
        session.commit()

        result = filter_publishable_game_ids(session, ["20250601_01"])
        assert result == ["20250601_01"]
        session.close()


class TestOCISyncBaseUnit:
    def test_init(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            assert sync.sqlite_session == sqlite_session
            assert sync._season_map_cache is None
            assert sync._franchise_id_mapping_cache is None

    def test_chunked(self):
        result = OCISyncBase._chunked(["a", "b", "c", "d", "e"], 2)
        assert result == [["a", "b"], ["c", "d"], ["e"]]

    def test_chunked_empty(self):
        result = OCISyncBase._chunked([], 2)
        assert result == []

    def test_quote_identifier_valid(self):
        assert OCISyncBase._quote_identifier("valid_table") == '"valid_table"'

    def test_quote_identifier_empty(self):
        with pytest.raises(ValueError, match="must not be empty"):
            OCISyncBase._quote_identifier("")

    def test_quote_identifier_invalid_start(self):
        with pytest.raises(ValueError, match="unsafe SQL identifier"):
            OCISyncBase._quote_identifier("123table")

    def test_quote_identifier_invalid_chars(self):
        with pytest.raises(ValueError, match="unsafe SQL identifier"):
            OCISyncBase._quote_identifier("table name")

    def test_target_table_exists_no_engine(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            sync.oci_engine = None
            assert sync._target_table_exists(MagicMock()) is True

    def test_resolve_sync_columns(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            sync.oci_engine = MagicMock()

            class FakeModel:
                __tablename__ = "test_table"

                class __table__:
                    columns = []

            result = sync._resolve_sync_columns(FakeModel, ["id"])
            assert isinstance(result, list)

    def test_sync_in_batches_empty(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            sync.oci_engine = MagicMock()
            sync._raw_oci_connection_with_retries = MagicMock()
            sync._bulk_copy_upsert = MagicMock()

            config = SyncBatchConfig(
                model=MagicMock(),
                query=MagicMock(),
                total_count=0,
                columns=["a"],
                conflict_keys=["a"],
                transform_fn=None,
                batch_size=100,
                update_timestamp=True,
            )
            config.query.offset.return_value.limit.return_value.all.return_value = []
            result = sync._sync_in_batches(config)
            assert result == 0

    def test_reset_target_sequence_non_postgres(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("sqlite:///:memory:", sqlite_session)
            result = sync._reset_target_sequence_for_table("test_table")
            assert result is False

    def test_direct_insert_upsert_no_conflict_keys(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor

            record = {"a": 1, "b": 2}
            sync._direct_insert_upsert(
                "test_table",
                record,
                [],
                update_timestamp=True,
                connection=mock_conn,
            )
            mock_cursor.execute.assert_called_once()
            mock_conn.commit.assert_called_once()

    def test_direct_insert_upsert_no_update_cols(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor

            record = {"id": 1}
            sync._direct_insert_upsert(
                "test_table",
                record,
                ["id"],
                update_timestamp=True,
                connection=mock_conn,
            )
            mock_cursor.execute.assert_called_once()
            mock_conn.commit.assert_called_once()

    def test_direct_insert_upsert_with_update_cols(self):
        sqlite_session = MagicMock(spec=Session)
        with patch("src.sync.sync_base.create_engine"):
            sync = OCISyncBase("postgresql://user:pass@host/db", sqlite_session)
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor

            record = {"id": 1, "name": "test"}
            sync._direct_insert_upsert(
                "test_table",
                record,
                ["id"],
                update_timestamp=True,
                connection=mock_conn,
            )
            mock_cursor.execute.assert_called_once()
            mock_conn.commit.assert_called_once()


class TestExecuteSignatureQuery:
    def test_without_game_ids(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_session.execute.return_value = mock_result

        sql = "SELECT * FROM game"
        result = _execute_signature_query(mock_session, sql)
        assert result == mock_result

    def test_with_game_ids(self):
        mock_session = MagicMock()
        mock_result = MagicMock()
        mock_session.execute.return_value = mock_result

        sql = "SELECT * FROM game WHERE game_id IN :game_ids"
        result = _execute_signature_query(mock_session, sql, game_ids=["g1", "g2"])
        assert result == mock_result


class TestDetectDirtyGameIdsEdgeCases:
    def test_dirty_by_child_tables(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch(
            "src.sync.sync_base.load_game_sync_signatures",
            side_effect=[
                {"g1": {"game_inning_scores": {"row_count": 2}}},
                {"g1": {"game_inning_scores": {"row_count": 1}}},
            ],
        ):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == ["g1"]

    def test_not_dirty(self):
        local = MagicMock(spec=Session)
        remote = MagicMock(spec=Session)
        with patch(
            "src.sync.sync_base.load_game_sync_signatures",
            side_effect=[
                {"g1": {"game": {"game_status": "COMPLETED", "updated_at": "2025-01-01"}}},
                {"g1": {"game": {"game_status": "COMPLETED", "updated_at": "2025-01-01"}}},
            ],
        ):
            dirty = detect_dirty_game_ids(local, remote)
            assert dirty == []
