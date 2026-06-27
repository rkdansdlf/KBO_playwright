from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.cli.rebuild_relay_events import (
    RebuildReportRow,
    _batter_from_description,
    _chunked,
    _dedupe_game_ids,
    _event_to_payload,
    _format_base_string,
    _load_candidate_game_ids,
    _load_game_ids_from_file,
    _result_from_description,
    _should_keep_event,
    _validate_rebuilt_events,
    _write_report,
    rebuild_relay_events,
)


class TestDedupeGameIds:
    def test_empty(self):
        assert _dedupe_game_ids([]) == []

    def test_duplicates_removed(self):
        assert _dedupe_game_ids(["a", "b", "a", "c", "b"]) == ["a", "b", "c"]

    def test_whitespace_stripped(self):
        assert _dedupe_game_ids([" a ", "b", " a "]) == ["a", "b"]

    def test_none_values_skipped(self):
        assert _dedupe_game_ids(["", "a", "", "b"]) == ["a", "b"]


class TestBatterFromDescription:
    def test_with_colon(self):
        assert _batter_from_description("김타자: 안타") == "김타자"

    def test_without_colon(self):
        assert _batter_from_description("안타 처리") is None

    def test_empty_string(self):
        assert _batter_from_description("") is None

    def test_none(self):
        assert _batter_from_description(None) is None

    def test_colon_at_end(self):
        assert _batter_from_description("데이터:") == "데이터"

    def test_colon_at_start(self):
        assert _batter_from_description(":안타") is None


class TestResultFromDescription:
    def test_with_colon(self):
        assert _result_from_description("김타자: 안타") == "안타"

    def test_without_colon(self):
        assert _result_from_description("안타 처리") is None

    def test_empty(self):
        assert _result_from_description("") is None

    def test_none(self):
        assert _result_from_description(None) is None


class TestChunked:
    def test_even_split(self):
        result = list(_chunked(["a", "b", "c", "d"], 2))
        assert result == [["a", "b"], ["c", "d"]]

    def test_uneven_split(self):
        result = list(_chunked(["a", "b", "c"], 2))
        assert result == [["a", "b"], ["c"]]

    def test_single_element(self):
        result = list(_chunked(["a"], 5))
        assert result == [["a"]]

    def test_empty(self):
        result = list(_chunked([], 3))
        assert result == []

    def test_chunk_size_one(self):
        result = list(_chunked(["a", "b", "c"], 1))
        assert result == [["a"], ["b"], ["c"]]


class TestWriteReport:
    def test_writes_csv(self, tmp_path: Path):
        rows = [
            RebuildReportRow(
                game_id="20230625LGSS0",
                status="READY",
                old_rows=10,
                new_rows=8,
                notes="",
                backup_path="",
                oci_status="disabled",
            ),
        ]
        report_path = tmp_path / "report.csv"
        _write_report(report_path, rows)

        assert report_path.exists()
        with report_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data = list(reader)
        assert len(data) == 1
        assert data[0]["game_id"] == "20230625LGSS0"
        assert data[0]["status"] == "READY"

    def test_empty_rows(self, tmp_path: Path):
        report_path = tmp_path / "report.csv"
        _write_report(report_path, [])
        assert report_path.exists()
        with report_path.open(encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data = list(reader)
        assert data == []


class TestShouldKeepEvent:
    def test_noise_text_excluded(self, tmp_path: Path):
        event = SimpleNamespace(description="경기 시작", event_type="unknown", event_seq=1)
        assert _should_keep_event(event) is False

    def test_known_event_type_kept(self):
        event = SimpleNamespace(description="타석", event_type="batting", event_seq=1)
        assert _should_keep_event(event) is True

    def test_result_event_kept(self):
        event = SimpleNamespace(description="�길동: 안타", event_type="unknown", event_seq=1)
        assert _should_keep_event(event) is True

    def test_unknown_type_no_colon_excluded(self):
        event = SimpleNamespace(description="중계 문자", event_type="unknown", event_seq=1)
        assert _should_keep_event(event) is False

    def test_substitution_type_filtered(self):
        event = SimpleNamespace(description="투수 교체", event_type="substitution", event_seq=1)
        assert _should_keep_event(event) is False


class TestValidateRebuiltEvents:
    def test_missing_game(self):
        result = _validate_rebuilt_events(None, [], min_events=5)
        assert result == ("SKIPPED_MISSING_GAME", "No parent game row")

    def test_too_few_events(self):
        game = SimpleNamespace(home_score=1, away_score=2)
        result = _validate_rebuilt_events(game, [{"home_score": 1, "away_score": 2}], min_events=5)
        assert result[0] == "SKIPPED_TOO_FEW_EVENTS"

    def test_no_score_state(self):
        game = SimpleNamespace(home_score=3, away_score=2)
        events = [{"home_score": None, "away_score": None} for _ in range(10)]
        result = _validate_rebuilt_events(game, events, min_events=5)
        assert result[0] == "SKIPPED_MISSING_SCORE_STATE"

    def test_score_mismatch(self):
        game = SimpleNamespace(home_score=5, away_score=2)
        events = [{"home_score": 3, "away_score": 4} for _ in range(10)]
        result = _validate_rebuilt_events(game, events, min_events=5)
        assert result[0] == "SKIPPED_SCORE_MISMATCH"

    def test_ready(self):
        game = SimpleNamespace(home_score=5, away_score=2)
        events = [{"home_score": 5, "away_score": 2} for _ in range(10)]
        status, notes = _validate_rebuilt_events(game, events, min_events=5)
        assert status == "READY"
        assert notes == ""

    def test_none_scores_skip_validation(self):
        game = SimpleNamespace(home_score=None, away_score=None)
        events = [{"home_score": 5, "away_score": 2} for _ in range(10)]
        status, notes = _validate_rebuilt_events(game, events, min_events=5)
        assert status == "READY"


class TestLoadGameIdsFromFile:
    def test_csv_file(self, tmp_path: Path):
        p = tmp_path / "ids.csv"
        p.write_text("game_id\n20230625LGSS0\n20230625LGSS0\n20230626KTNC0\n", encoding="utf-8")
        result = _load_game_ids_from_file(str(p))
        assert result == ["20230625LGSS0", "20230626KTNC0"]

    def test_none_path(self):
        assert _load_game_ids_from_file(None) == []

    def test_empty_path(self):
        assert _load_game_ids_from_file("") == []


class TestEventToPayload:
    def test_all_fields(self):
        event = SimpleNamespace(
            event_seq=3,
            inning=5,
            inning_half="top",
            outs=2,
            batter_id="12345",
            batter_name="홍길동",
            pitcher_id="67890",
            pitcher_name="박투수",
            description="홍길동: 안타",
            event_type="batting",
            result_code="안타",
            rbi=1,
            bases_before="1--",
            bases_after="12-",
            extra_json='{"key":"val"}',
            wpa=0.05,
            win_expectancy_before=0.55,
            win_expectancy_after=0.60,
            score_diff=1,
            base_state=3,
            home_score=4,
            away_score=2,
        )
        payload = _event_to_payload(event)
        assert payload["event_seq"] == 3
        assert payload["batter_name"] == "홍길동"
        assert payload["pitcher_name"] == "박투수"
        assert payload["wpa"] == 0.05

    def test_none_batter_name_falls_back(self):
        event = SimpleNamespace(
            event_seq=1,
            inning=1,
            inning_half="top",
            outs=0,
            batter_id=None,
            batter_name=None,
            pitcher_id=None,
            pitcher_name=None,
            description="김타자: 안타",
            event_type="unknown",
            result_code=None,
            rbi=0,
            bases_before=None,
            bases_after=None,
            extra_json=None,
            wpa=None,
            win_expectancy_before=None,
            win_expectancy_after=None,
            score_diff=None,
            base_state=None,
            home_score=0,
            away_score=0,
        )
        payload = _event_to_payload(event)
        assert payload["batter_name"] == "김타자"


class TestRebuildRelayEventsIntegration:
    def test_dry_run_no_apply(self, tmp_path: Path):
        with (
            patch("src.cli.rebuild_relay_events._load_candidate_game_ids", return_value=[]),
            patch("src.cli.rebuild_relay_events._write_report"),
        ):
            rows = rebuild_relay_events(
                seasons=[2023],
                game_ids=[],
                apply=False,
                sync_oci=False,
                min_events=20,
                report_out=str(tmp_path / "report.csv"),
                backup_out=str(tmp_path / "backup.csv"),
            )
        assert rows == []

    def test_single_game_ready(self, tmp_path: Path):
        mock_game = MagicMock()
        mock_game.home_score = 5
        mock_game.away_score = 2
        mock_game.game_id = "20230625LGSS0"

        mock_event = MagicMock()
        mock_event.description = "홍길동: 안타"
        mock_event.event_type = "batting"
        mock_event.result_code = "안타"
        mock_event.inning = 1
        mock_event.inning_half = "top"
        mock_event.outs = 1
        mock_event.batter_id = "123"
        mock_event.batter_name = "홍길동"
        mock_event.pitcher_id = "456"
        mock_event.pitcher_name = "박투수"
        mock_event.event_seq = 1
        mock_event.rbi = 0
        mock_event.bases_before = None
        mock_event.bases_after = None
        mock_event.extra_json = None
        mock_event.wpa = None
        mock_event.win_expectancy_before = None
        mock_event.win_expectancy_after = None
        mock_event.score_diff = None
        mock_event.base_state = None
        mock_event.home_score = 5
        mock_event.away_score = 2
        mock_event.id = 1

        with (
            patch("src.cli.rebuild_relay_events._load_candidate_game_ids", return_value=["20230625LGSS0"]),
            patch(
                "src.cli.rebuild_relay_events._rebuild_events_for_game",
                return_value=[{"home_score": 5, "away_score": 2}] * 25,
            ),
            patch("src.cli.rebuild_relay_events._validate_rebuilt_events", return_value=("READY", "")),
            patch("src.cli.rebuild_relay_events.SessionLocal") as mock_session_factory,
            patch("src.cli.rebuild_relay_events._write_report"),
        ):
            mock_session = MagicMock()
            mock_game_obj = MagicMock()
            mock_game_obj.home_score = 5
            mock_game_obj.away_score = 2
            mock_session.query.return_value.filter.return_value.one_or_none.return_value = mock_game_obj
            mock_session_factory.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_session_factory.return_value.__exit__ = MagicMock(return_value=False)

            rows = rebuild_relay_events(
                seasons=[2023],
                game_ids=["20230625LGSS0"],
                apply=False,
                sync_oci=False,
                min_events=20,
                report_out=str(tmp_path / "report.csv"),
                backup_out=str(tmp_path / "backup.csv"),
            )
        assert len(rows) == 1
        assert rows[0].game_id == "20230625LGSS0"


class TestFormatBaseString:
    def test_calls_shared(self):
        assert _format_base_string(1) == "1--"
        assert _format_base_string(7) == "123"
