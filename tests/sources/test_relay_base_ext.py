from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pytest

from src.sources.relay.base import (
    ALLOWED_MANIFEST_FORMATS,
    ALLOWED_SOURCE_TYPES,
    INTERNATIONAL_DATE_RANGES,
    POSTSEASON_DATE_RANGES,
    REGULAR_BUCKET_SOURCE_ORDER,
    SPECIAL_BUCKET_SOURCE_ORDER,
    CapabilityRecord,
    ManifestEntry,
    NormalizedRelayResult,
    RelaySourceAdapter,
    _coerce_manifest_paths,
    _derive_bucket_by_date,
    default_source_order_for_bucket,
    derive_bucket_id,
    event_has_minimum_state,
    event_to_pbp_row,
    events_have_minimum_state,
    load_capability_records,
    normalize_inning_half,
    normalize_pbp_row,
    read_manifest_entries,
    trailing_result_from_description,
    upsert_capability_record,
)


class TestNormalizeInningHalf:
    def test_top_variants(self):
        assert normalize_inning_half("TOP") == "top"
        assert normalize_inning_half("away") == "top"
        assert normalize_inning_half("초") == "top"

    def test_bottom_variants(self):
        assert normalize_inning_half("BOTTOM") == "bottom"
        assert normalize_inning_half("HOME") == "bottom"
        assert normalize_inning_half("말") == "bottom"

    def test_none_and_empty(self):
        assert normalize_inning_half(None) is None
        assert normalize_inning_half("") is None
        assert normalize_inning_half("   ") is None

    def test_unknown(self):
        assert normalize_inning_half("middle") is None


class TestTrailingResultFromDescription:
    def test_colon_split(self):
        assert trailing_result_from_description("play: 안타") == "안타"
        assert trailing_result_from_description("a:b:c") == "b:c"

    def test_trailing_token(self):
        assert trailing_result_from_description("삼진 아웃") == "아웃"
        assert trailing_result_from_description("홈런") == "홈런"

    def test_empty(self):
        assert trailing_result_from_description("") is None
        assert trailing_result_from_description(None) is None

    def test_colon_no_value(self):
        assert trailing_result_from_description("play: ") is None


class TestEventToPbpRow:
    def test_basic_mapping(self):
        event = {
            "inning": 1,
            "inning_half": "top",
            "pitcher_name": "김원중",
            "batter_name": "이대호",
            "description": "삼진",
            "event_type": "SO",
            "result_code": "삼진",
        }
        row = event_to_pbp_row(event)
        assert row["inning"] == 1
        assert row["inning_half"] == "top"
        assert row["pitcher_name"] == "김원중"
        assert row["batter_name"] == "이대호"
        assert row["result"] == "삼진"

    def test_fallback_keys(self):
        event = {
            "pitcher": "박병호",
            "batter": "양의지",
            "description": "볼넷",
        }
        row = event_to_pbp_row(event)
        assert row["pitcher_name"] == "박병호"
        assert row["batter_name"] == "양의지"

    def test_result_from_description(self):
        event = {"description": "병살타 처리"}
        row = event_to_pbp_row(event)
        assert row["result"] == "처리"


class TestNormalizePbpRow:
    def test_normalizes_inning_half(self):
        row = {"inning_half": "초", "play_description": "안타"}
        result = normalize_pbp_row(row)
        assert result["inning_half"] == "top"

    def test_description_fallback(self):
        row = {"description": "홈런", "play_description": None}
        result = normalize_pbp_row(row)
        assert result["play_description"] == "홈런"

    def test_preserves_trace_keys(self):
        row = {
            "provider_log_id": "log-1",
            "source_row_index": 5,
            "source_name": "naver",
        }
        result = normalize_pbp_row(row)
        assert result["provider_log_id"] == "log-1"
        assert result["source_row_index"] == 5
        assert result["source_name"] == "naver"


class TestNormalizedRelayResult:
    def test_is_empty_default(self):
        result = NormalizedRelayResult(game_id="g1", source_name="naver")
        assert result.is_empty

    def test_not_empty_with_events(self):
        result = NormalizedRelayResult(game_id="g1", source_name="naver", events=[{"event_type": "SO"}])
        assert not result.is_empty

    def test_not_empty_with_raw_pbp(self):
        result = NormalizedRelayResult(game_id="g1", source_name="naver", raw_pbp_rows=[{"inning": 1}])
        assert not result.is_empty


class TestRelaySourceAdapter:
    def test_init_defaults(self):
        class DummyAdapter(RelaySourceAdapter):
            async def fetch_game(self, game_id: str) -> NormalizedRelayResult:
                return NormalizedRelayResult(game_id=game_id, source_name=self.source_name)

        adapter = DummyAdapter("test_source")
        assert adapter.source_name == "test_source"
        assert adapter.supports_bucket_probe is True
        assert adapter.cache_negative_probe is True


class TestDeriveBucketId:
    def test_legacy_year(self):
        assert derive_bucket_id("20230315DBLT") == "2023_legacy"

    def test_all_star_team_code(self):
        assert derive_bucket_id("20241010EAWE").endswith("_all_star")

    def test_all_star_league_name(self):
        assert derive_bucket_id("20241010DBLT", "올스타 게임").endswith("_all_star")

    def test_postseason_league_name(self):
        assert derive_bucket_id("20241015DBLT", "한국시리즈").endswith("_postseason")
        assert derive_bucket_id("20241015DBLT", "플레이오프").endswith("_postseason")
        assert derive_bucket_id("20241015DBLT", "와일드카드").endswith("_postseason")
        assert derive_bucket_id("20241015DBLT", "준플레이오프").endswith("_postseason")

    def test_international_league_name(self):
        assert derive_bucket_id("20241115DBLT", "국가대표").endswith("_international")
        assert derive_bucket_id("20241115DBLT", "WBC").endswith("_international")

    def test_postseason_date_fallback(self):
        game_id = "20241010DBLT"
        assert derive_bucket_id(game_id) == "2024_postseason"

    def test_international_date_fallback(self):
        game_id = "20241115DBLT"
        assert derive_bucket_id(game_id) == "2024_international"

    def test_regular_kbo_2024(self):
        game_id = "20240501DBLT"
        assert derive_bucket_id(game_id) == "2024_regular_kbo"

    def test_regular_kbo_2025(self):
        game_id = "20250501DBLT"
        assert derive_bucket_id(game_id) == "2025_regular_kbo"

    def test_regular_kbo_2026(self):
        game_id = "20260501DBLT"
        assert derive_bucket_id(game_id) == "2026_regular_kbo"

    def test_2025_all_star_override(self):
        game_id = "20250801EAWE"
        assert derive_bucket_id(game_id) == "2025_all_star"


class TestDeriveBucketByDate:
    def test_outside_ranges_legacy(self):
        result = _derive_bucket_by_date("20240501DBLT", 2024, "DBLT")
        assert result == "2024_regular_kbo"

    def test_unknown_year_legacy(self):
        result = _derive_bucket_by_date("20270501DBLT", 2027, "DBLT")
        assert result == "2027_legacy"


class TestDefaultSourceOrderForBucket:
    def test_regular_bucket(self):
        order = default_source_order_for_bucket("2025_regular_kbo")
        assert order == list(REGULAR_BUCKET_SOURCE_ORDER)

    def test_special_bucket(self):
        order = default_source_order_for_bucket("2025_postseason")
        assert order == list(SPECIAL_BUCKET_SOURCE_ORDER)

    def test_all_star_bucket(self):
        order = default_source_order_for_bucket("2025_all_star")
        assert order == list(SPECIAL_BUCKET_SOURCE_ORDER)


class TestCoerceManifestPaths:
    def test_single_path_object(self):
        p = Path("/tmp/manifest.csv")
        result = _coerce_manifest_paths(p)
        assert result == [p]

    def test_single_string(self):
        result = _coerce_manifest_paths("/tmp/a.csv")
        assert result == [Path("/tmp/a.csv")]

    def test_comma_separated_string(self):
        result = _coerce_manifest_paths("/tmp/a.csv,/tmp/b.csv")
        assert result == [Path("/tmp/a.csv"), Path("/tmp/b.csv")]

    def test_list_of_paths(self):
        result = _coerce_manifest_paths([Path("/a.csv"), Path("/b.csv")])
        assert result == [Path("/a.csv"), Path("/b.csv")]

    def test_list_with_none(self):
        result = _coerce_manifest_paths([Path("/a.csv"), None, Path("/b.csv")])
        assert result == [Path("/a.csv"), Path("/b.csv")]

    def test_nested_list(self):
        result = _coerce_manifest_paths([[Path("/a.csv"), Path("/b.csv")], Path("/c.csv")])
        assert result == [Path("/a.csv"), Path("/b.csv"), Path("/c.csv")]


class TestReadManifestEntries:
    def _write_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "game_id",
                    "season",
                    "source_type",
                    "locator",
                    "format",
                    "priority",
                    "sha256",
                    "captured_at",
                    "notes",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def test_reads_valid_entries(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "20260501DBLT",
                    "source_type": "naver",
                    "locator": "https://naver.com/1",
                    "format": "naver_json",
                    "priority": "10",
                    "notes": "",
                },
            ],
        )
        entries = read_manifest_entries(str(path))
        assert len(entries) == 1
        assert entries[0].game_id == "20260501DBLT"
        assert entries[0].priority == 10

    def test_reads_archive_metadata(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "20010405LTHU0",
                    "season": "2001",
                    "source_type": "json_archive",
                    "locator": "20010405LTHU0.json",
                    "format": "normalized_events_json",
                    "priority": "1",
                    "sha256": "a" * 64,
                    "captured_at": "2026-07-19T02:00:00Z",
                    "notes": "archive probe",
                },
            ],
        )

        entries = read_manifest_entries(path)

        assert entries[0].season == 2001
        assert entries[0].sha256 == "a" * 64
        assert entries[0].captured_at == "2026-07-19T02:00:00Z"

    def test_rejects_invalid_archive_checksum(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "20010405LTHU0",
                    "source_type": "json_archive",
                    "locator": "archive.json",
                    "format": "normalized_events_json",
                    "sha256": "not-a-sha256",
                },
            ],
        )

        with pytest.raises(ValueError, match="sha256"):
            read_manifest_entries(path)

    def test_skips_missing_file(self, tmp_path: Path):
        entries = read_manifest_entries(str(tmp_path / "missing.csv"))
        assert entries == []

    def test_skips_incomplete_rows(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "",
                    "source_type": "naver",
                    "locator": "https://naver.com/1",
                    "format": "naver_json",
                    "priority": "10",
                    "notes": "",
                },
            ],
        )
        entries = read_manifest_entries(str(path))
        assert entries == []

    def test_raises_on_invalid_source_type(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "20260501DBLT",
                    "source_type": "invalid_source",
                    "locator": "https://x.com",
                    "format": "naver_json",
                    "priority": "10",
                    "notes": "",
                },
            ],
        )
        with pytest.raises(ValueError, match="Unsupported manifest source_type"):
            read_manifest_entries(str(path))

    def test_raises_on_invalid_format(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "20260501DBLT",
                    "source_type": "naver",
                    "locator": "https://x.com",
                    "format": "invalid_format",
                    "priority": "10",
                    "notes": "",
                },
            ],
        )
        with pytest.raises(ValueError, match="Unsupported manifest format"):
            read_manifest_entries(str(path))

    def test_deduplicates_entries(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "20260501DBLT",
                    "source_type": "naver",
                    "locator": "https://naver.com/1",
                    "format": "naver_json",
                    "priority": "10",
                    "notes": "",
                },
                {
                    "game_id": "20260501DBLT",
                    "source_type": "naver",
                    "locator": "https://naver.com/1",
                    "format": "naver_json",
                    "priority": "10",
                    "notes": "",
                },
            ],
        )
        entries = read_manifest_entries(str(path))
        assert len(entries) == 1

    def test_sorts_by_game_priority_locator(self, tmp_path: Path):
        path = tmp_path / "manifest.csv"
        self._write_csv(
            path,
            [
                {
                    "game_id": "20260502DBLT",
                    "source_type": "naver",
                    "locator": "https://naver.com/2",
                    "format": "naver_json",
                    "priority": "10",
                    "notes": "",
                },
                {
                    "game_id": "20260501DBLT",
                    "source_type": "naver",
                    "locator": "https://naver.com/1",
                    "format": "naver_json",
                    "priority": "20",
                    "notes": "",
                },
            ],
        )
        entries = read_manifest_entries(str(path))
        assert entries[0].game_id == "20260501DBLT"
        assert entries[1].game_id == "20260502DBLT"


class TestCapabilityRecords:
    def _write_csv(self, path: Path, rows: list[dict[str, str]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "bucket_id",
                    "source_name",
                    "sample_size",
                    "supported",
                    "last_checked_at",
                    "notes",
                ],
            )
            writer.writeheader()
            for row in rows:
                writer.writerow(row)

    def test_load_missing_file(self, tmp_path: Path):
        result = load_capability_records(str(tmp_path / "missing.csv"))
        assert result == {}

    def test_load_valid_records(self, tmp_path: Path):
        path = tmp_path / "capabilities.csv"
        self._write_csv(
            path,
            [
                {
                    "bucket_id": "2025_regular_kbo",
                    "source_name": "naver",
                    "sample_size": "10",
                    "supported": "true",
                    "last_checked_at": "2026-01-01T00:00:00Z",
                    "notes": "ok",
                },
            ],
        )
        result = load_capability_records(str(path))
        assert ("2025_regular_kbo", "naver") in result
        record = result[("2025_regular_kbo", "naver")]
        assert record.supported is True
        assert record.sample_size == 10

    def test_upsert_creates_file(self, tmp_path: Path):
        path = tmp_path / "caps.csv"
        record = CapabilityRecord(
            bucket_id="2025_regular_kbo",
            source_name="kbo",
            sample_size=5,
            supported=True,
            last_checked_at="2026-01-01T00:00:00Z",
        )
        upsert_capability_record(str(path), record)
        assert path.exists()
        loaded = load_capability_records(str(path))
        assert ("2025_regular_kbo", "kbo") in loaded

    def test_upsert_updates_existing(self, tmp_path: Path):
        path = tmp_path / "caps.csv"
        record1 = CapabilityRecord(
            bucket_id="2025_regular_kbo",
            source_name="kbo",
            sample_size=5,
            supported=True,
            last_checked_at="2026-01-01T00:00:00Z",
        )
        upsert_capability_record(str(path), record1)
        record2 = CapabilityRecord(
            bucket_id="2025_regular_kbo",
            source_name="kbo",
            sample_size=10,
            supported=False,
            last_checked_at="2026-02-01T00:00:00Z",
        )
        upsert_capability_record(str(path), record2)
        loaded = load_capability_records(str(path))
        assert len(loaded) == 1
        assert loaded[("2025_regular_kbo", "kbo")].sample_size == 10
        assert loaded[("2025_regular_kbo", "kbo")].supported is False


class TestEventsHaveMinimumState:
    def test_empty_list(self):
        assert events_have_minimum_state([]) is False

    def test_all_have_state(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            "src.sources.relay.base.event_has_wpa_state",
            lambda e: True,
        )
        assert events_have_minimum_state([{"a": 1}, {"b": 2}]) is True

    def test_some_missing_state(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            "src.sources.relay.base.event_has_wpa_state",
            lambda e: e.get("valid", False),
        )
        assert events_have_minimum_state([{"valid": True}, {"valid": False}]) is False


class TestEventHasMinimumState:
    def test_delegates_to_wpa(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(
            "src.sources.relay.base.event_has_wpa_state",
            lambda e: True,
        )
        assert event_has_minimum_state({}) is True
