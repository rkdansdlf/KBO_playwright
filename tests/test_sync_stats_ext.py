"""Extended tests for sync_stats.py coverage."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.player import PlayerBasic, PlayerSeasonBatting
from src.models.team import Team
from src.sync.sync_stats import StatsSyncMixin


def _make_syncer_tables():
    engine = create_engine("sqlite:///:memory:")
    for table in (Team.__table__, PlayerBasic.__table__, PlayerSeasonBatting.__table__):
        table.create(bind=engine)
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)
    return engine, SessionLocal


def _make_syncer(session):
    """Create a minimal syncer with both sessions."""

    class _Syncer(StatsSyncMixin):
        def __init__(self, sess, target=None):
            self.sqlite_session = sess
            self.synced_player_ids = []
            self.target_session = target or MagicMock()

        def sync_simple_table(self, model, *, filters=None, **_kwargs):
            self.synced_player_ids = [
                row.player_id for row in self.sqlite_session.query(model).filter(*(filters or [])).all()
            ]
            return len(self.synced_player_ids)

    return _Syncer(session)


def test_add_existing_player_basic_filter_logs_warning(caplog):
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        session.add(PlayerSeasonBatting(player_id=9999, season=2026, league="REGULAR", level="KBO1", source="X"))
        session.commit()

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess):
                self.sqlite_session = sess

        syncer = _Syncer(session)
        with caplog.at_level(logging.WARNING):
            syncer._add_existing_player_basic_filter(PlayerSeasonBatting, [])
        assert any("Skipping" in rec.message for rec in caplog.records)


def test_sync_pitcher_data_success_path():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

            def sync_simple_table(self, model, **_kwargs):
                return 123

        syncer = _Syncer(session, MagicMock())
        result = syncer.sync_pitcher_data()
        assert result == 123


def test_sync_batting_data_success_path():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

            def sync_simple_table(self, model, **_kwargs):
                return 500

        syncer = _Syncer(session, MagicMock())
        result = syncer.sync_batting_data()
        assert result == 500


def test_meets_expected():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, MagicMock())
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (150,)
        with patch.object(syncer.target_session, "execute", return_value=mock_result):
            syncer.verify_pitcher_sync(expected_count=100)


def test_verify_batting_sync_meets_expected():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, MagicMock())
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (200,)
        with patch.object(syncer.target_session, "execute", return_value=mock_result):
            syncer.verify_batting_sync(expected_count=100)


def test_verify_sync_below_expected(caplog):
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, MagicMock())
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (50,)
        with patch.object(syncer.target_session, "execute", return_value=mock_result):
            with caplog.at_level(logging.WARNING):
                syncer.verify_pitcher_sync(expected_count=100)
        assert any("적습니다" in rec.message for rec in caplog.records)


def test_show_oci_data_sample_empty():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, MagicMock())
        mock_result = MagicMock()
        mock_result.fetchall.return_value = []
        with patch.object(syncer.target_session, "execute", return_value=mock_result):
            syncer.show_oci_data_sample()


def test_show_oci_data_sample_with_data(caplog):
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, MagicMock())

        pitcher_row = ("P001", 2026, 30, 15, 3, 1.80, 200.0)
        batter_row = ("B001", 2026, 50, 0.300, 100, 25)

        def mock_execute(query):
            mock_r = MagicMock()
            text_str = str(query) if hasattr(query, "__str__") else ""
            if "pitching" in text_str.lower():
                mock_r.fetchall.return_value = [pitcher_row]
            else:
                mock_r.fetchall.return_value = [batter_row]
            return mock_r

        with patch.object(syncer.target_session, "execute", side_effect=mock_execute):
            with caplog.at_level(logging.INFO):
                syncer.show_oci_data_sample()
        any("샘플" in rec.message for rec in caplog.records)


def test_get_table_signature_match():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, MagicMock())
        sig = syncer._get_table_signature(PlayerBasic)
        assert "local" in sig
        assert "remote" in sig
        assert "match" in sig


def test_sync_player_season_batting_skipped_when_matched():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        session.add(PlayerSeasonBatting(player_id=1, season=2026, league="REG", level="KBO1", source="X"))
        session.add(PlayerBasic(player_id=1, name="A"))

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

            def _get_table_signature(self, model, year=None):
                return {
                    "local": {"count": 1, "max_updated_at": "2026-01-01"},
                    "remote": {"count": 1, "max_updated_at": "2026-01-01"},
                    "match": True,
                }

        syncer = _Syncer(session, MagicMock())
        result = syncer.sync_player_season_batting(year=2026)
        assert result == 0


def test_sync_player_season_pitching_skipped_when_matched():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

            def _get_table_signature(self, model, year=None):
                return {"match": True}

        syncer = _Syncer(session, MagicMock())
        result = syncer.sync_player_season_pitching(year=2026)
        assert result == 0


def test_purge_season_stats_all_types():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        mock_target = MagicMock()

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, mock_target)
        syncer.purge_season_stats(2024, type="all")
        assert mock_target.execute.call_count == 8
        mock_target.commit.assert_called_once()


def test_purge_season_stats_batting():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        mock_target = MagicMock()

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, mock_target)
        syncer.purge_season_stats(2024, type="batting")
        assert mock_target.execute.call_count == 2
        mock_target.commit.assert_called_once()


def test_purge_season_stats_pitching():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        mock_target = MagicMock()

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, mock_target)
        syncer.purge_season_stats(2024, type="pitching")
        assert mock_target.execute.call_count == 2


def test_sync_all_player_data_returns_dict():
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

            def sync_players(self):
                return 10

            def sync_player_identities(self):
                return 5

            def sync_player_season_batting(self, **kwargs):
                return 50

            def sync_player_season_pitching(self, **kwargs):
                return 40

            def sync_team_season_batting(self, **kwargs):
                return 20

            def sync_team_season_pitching(self, **kwargs):
                return 15

        syncer = _Syncer(session, MagicMock())
        result = syncer.sync_all_player_data()
        assert result == {
            "players": 10,
            "player_identities": 5,
            "player_season_batting": 50,
            "player_season_pitching": 40,
            "team_season_batting": 20,
            "team_season_pitching": 15,
        }


def test_sync_stats_success_log(caplog):
    engine, SessionLocal = _make_syncer_tables()
    with SessionLocal() as session:
        session.add(PlayerBasic(player_id=1, name="A"))
        session.commit()

        mock_target = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (500,)
        mock_target.execute.return_value = mock_result

        class _Syncer(StatsSyncMixin):
            def __init__(self, sess, target):
                self.sqlite_session = sess
                self.target_session = target

        syncer = _Syncer(session, mock_target)
        with caplog.at_level(logging.INFO):
            syncer.verify_batting_sync(expected_count=500)
        assert any("성공" in rec.message for rec in caplog.records)
