from __future__ import annotations

from datetime import date, datetime
from unittest.mock import MagicMock

import pytest

from src.services.postgame_reconciliation_service import (
    GameScoreStatusSnapshot,
    PostgameReconciliationChange,
    _display,
    _format_game_date,
    _normalize_range,
    _parse_yyyymmdd,
    _score,
    format_reconciliation_report,
    write_reconciliation_csv,
)


class TestParseYyyymmdd:
    def test_parses_correctly(self):
        assert _parse_yyyymmdd("20240315") == date(2024, 3, 15)

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            _parse_yyyymmdd("notadate")


class TestNormalizeRange:
    def test_already_ordered(self):
        s, e = _normalize_range("20240301", "20240315")
        assert s == "20240301"
        assert e == "20240315"

    def test_reversed_order(self):
        s, e = _normalize_range("20240315", "20240301")
        assert s == "20240301"
        assert e == "20240315"

    def test_same_date(self):
        s, e = _normalize_range("20240301", "20240301")
        assert s == e


class TestDisplay:
    def test_none_returns_dash(self):
        assert _display(None) == "-"

    def test_value_returns_str(self):
        assert _display(3) == "3"
        assert _display("hello") == "hello"


class TestScore:
    def test_both_none(self):
        assert _score(None, None) == "---"

    def test_with_values(self):
        assert _score(3, 2) == "3-2"

    def test_one_none(self):
        assert _score(3, None) == "3--"


class TestFormatGameDate:
    def test_datetime_object(self):
        d = datetime(2024, 3, 15, 12, 0)
        assert _format_game_date(d, fallback_game_id="x") == "20240315"

    def test_date_object(self):
        d = date(2024, 3, 15)
        assert _format_game_date(d, fallback_game_id="x") == "20240315"

    def test_yyyymmdd_string(self):
        assert _format_game_date("20240315", fallback_game_id="x") == "20240315"

    def test_none_falls_back(self):
        assert _format_game_date(None, fallback_game_id="20240315LG0") == "20240315"


class TestGameScoreStatusSnapshot:
    def test_score_tuple_property(self):
        snap = GameScoreStatusSnapshot(
            game_id="G1",
            game_date="20240315",
            game_status="completed",
            away_score=3,
            home_score=5,
        )
        assert snap.score_tuple == (3, 5)

    def test_score_tuple_none(self):
        snap = GameScoreStatusSnapshot(
            game_id="G1",
            game_date="20240315",
            game_status="scheduled",
            away_score=None,
            home_score=None,
        )
        assert snap.score_tuple == (None, None)


class TestPostgameReconciliationChange:
    def test_status_changed_true(self):
        change = PostgameReconciliationChange(
            game_id="G1",
            game_date="20240315",
            before_status="started",
            after_status="completed",
            before_away_score=None,
            before_home_score=None,
            after_away_score=3,
            after_home_score=5,
            detail_status="saved",
        )
        assert change.status_changed is True

    def test_status_changed_false(self):
        change = PostgameReconciliationChange(
            game_id="G1",
            game_date="20240315",
            before_status="completed",
            after_status="completed",
            before_away_score=3,
            before_home_score=5,
            after_away_score=3,
            after_home_score=5,
            detail_status="saved",
        )
        assert change.status_changed is False

    def test_score_changed_true(self):
        change = PostgameReconciliationChange(
            game_id="G1",
            game_date="20240315",
            before_status="started",
            after_status="completed",
            before_away_score=None,
            before_home_score=None,
            after_away_score=3,
            after_home_score=5,
            detail_status="saved",
        )
        assert change.score_changed is True


class TestFormatReconciliationReport:
    def test_no_changes(self):
        result = format_reconciliation_report([])
        assert "No status or score changes" in result

    def test_with_changes(self):
        changes = [
            PostgameReconciliationChange(
                game_id="G1",
                game_date="20240315",
                before_status="started",
                after_status="completed",
                before_away_score=None,
                before_home_score=None,
                after_away_score=3,
                after_home_score=5,
                detail_status="saved",
            ),
        ]
        result = format_reconciliation_report(changes)
        assert "G1" in result
        assert "started -> completed" in result
        assert "-- -> 3-5" in result


class TestWriteReconciliationCsv:
    def test_writes_file(self, tmp_path):
        changes = [
            PostgameReconciliationChange(
                game_id="G1",
                game_date="20240315",
                before_status="started",
                after_status="completed",
                before_away_score=None,
                before_home_score=None,
                after_away_score=3,
                after_home_score=5,
                detail_status="saved",
            ),
        ]
        out_path = tmp_path / "report.csv"
        result = write_reconciliation_csv(changes, out_path)
        assert result.exists()
        content = result.read_text(encoding="utf-8")
        assert "G1" in content
        assert "game_id" in content


class TestFindPostgameReconciliationTargets:
    """find_postgame_reconciliation_targets — SessionLocal mock."""

    def test_empty_db_returns_empty_list(self, monkeypatch):
        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = []
        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.SessionLocal",
            lambda: _Ctx(mock_session),
        )
        from src.services.postgame_reconciliation_service import find_postgame_reconciliation_targets

        result = find_postgame_reconciliation_targets("20240301", "20240315")
        assert result == []

    def test_extra_game_ids_added(self, monkeypatch):
        from src.services.postgame_reconciliation_service import find_postgame_reconciliation_targets

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = []
        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.SessionLocal",
            lambda: _Ctx(mock_session),
        )
        result = find_postgame_reconciliation_targets("20240301", "20240315", extra_game_ids=["20240315LGHH0"])
        assert result == []


class TestLoadScoreStatusSnapshots:
    """_load_score_status_snapshots — SessionLocal mock."""

    def test_empty_ids_returns_empty(self):
        from src.services.postgame_reconciliation_service import _load_score_status_snapshots

        result = _load_score_status_snapshots([])
        assert result == {}

    def test_loads_from_db(self, monkeypatch):
        from src.services.postgame_reconciliation_service import _load_score_status_snapshots

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.all.return_value = [
            ("20240315LGHH0", "2024-03-15", "completed", 5, 3),
        ]
        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.SessionLocal",
            lambda: _Ctx(mock_session),
        )
        result = _load_score_status_snapshots(["20240315LGHH0"])
        assert "20240315LGHH0" in result
        snap = result["20240315LGHH0"]
        assert snap.game_status == "completed"
        assert snap.score_tuple == (5, 3)


class TestHasFinalDetailRows:
    """_has_final_detail_rows — SessionLocal mock."""

    def test_empty_game_id_returns_false(self):
        from src.services.postgame_reconciliation_service import _has_final_detail_rows

        assert _has_final_detail_rows("") is False

    def test_no_rows_returns_false(self, monkeypatch):
        from src.services.postgame_reconciliation_service import _has_final_detail_rows

        mock_session = MagicMock()
        mock_session.query.return_value.filter.return_value.first.return_value = None
        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.SessionLocal",
            lambda: _Ctx(mock_session),
        )
        assert _has_final_detail_rows("20240315LGHH0") is False


class _Ctx:
    """Context manager mock for SessionLocal."""

    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self.session

    def __exit__(self, *args):
        pass


class TestReconcilePostgameRange:
    """reconcile_postgame_range — 전체 플로우 테스트."""

    @pytest.mark.asyncio
    async def test_no_targets_returns_early(self, monkeypatch):
        from src.services.postgame_reconciliation_service import (
            ReconciliationRequest,
            PostgameReconciliationResult,
            reconcile_postgame_range,
        )

        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.find_postgame_reconciliation_targets",
            lambda *a, **kw: [],
        )
        req = ReconciliationRequest(
            start_date="20240301",
            end_date="20240315",
            detail_crawler=MagicMock(),
        )
        result = await reconcile_postgame_range(req)
        assert result.candidates == 0
        assert result.changes == []

    @pytest.mark.asyncio
    async def test_with_targets_detects_changes(self, monkeypatch):
        from src.services.postgame_reconciliation_service import (
            ReconciliationRequest,
            PostgameReconciliationResult,
            reconcile_postgame_range,
        )
        from src.services.game_collection_service import GameCollectionTarget

        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.find_postgame_reconciliation_targets",
            lambda *a, **kw: [GameCollectionTarget(game_id="20240315LGHH0", game_date="20240315")],
        )

        async def _mock_crawl(targets, **kw):
            from src.services.game_collection_service import GameCollectionResult, GameCollectionItemResult

            return GameCollectionResult(
                items={
                    t.game_id: GameCollectionItemResult(game_id=t.game_id, game_date=t.game_date, detail_saved=True)
                    for t in targets
                },
            )

        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.crawl_and_save_game_details",
            _mock_crawl,
        )

        # Mock snapshot loading
        from src.services.postgame_reconciliation_service import GameScoreStatusSnapshot

        before_snap = GameScoreStatusSnapshot(
            game_id="20240315LGHH0",
            game_date="20240315",
            game_status="started",
            away_score=None,
            home_score=None,
        )
        after_snap = GameScoreStatusSnapshot(
            game_id="20240315LGHH0",
            game_date="20240315",
            game_status="completed",
            away_score=5,
            home_score=3,
        )
        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service._load_score_status_snapshots",
            lambda ids: {ids[0]: before_snap} if ids else {},
        )
        # Second call returns 'after'
        call_count = [0]

        def _snapshots(ids):
            call_count[0] += 1
            if call_count[0] == 1:
                return {ids[0]: before_snap}
            return {ids[0]: after_snap}

        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service._load_score_status_snapshots",
            _snapshots,
        )
        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service._has_final_detail_rows",
            lambda gid: True,
        )
        monkeypatch.setattr(
            "src.services.postgame_reconciliation_service.repair_game_parent_from_existing_children",
            lambda gid: None,
        )

        req = ReconciliationRequest(
            start_date="20240301",
            end_date="20240315",
            detail_crawler=MagicMock(),
        )
        result = await reconcile_postgame_range(req)
        assert result.candidates == 1
        assert len(result.changes) == 1
        assert result.changes[0].before_status == "started"
        assert result.changes[0].after_status == "completed"
