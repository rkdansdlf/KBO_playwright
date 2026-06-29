from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from src.sources.relay.base import ManifestEntry
from src.sources.relay.importer import ImportRelayAdapter


@pytest.fixture
def adapter():
    return ImportRelayAdapter(manifest_entries=[])


@pytest.fixture
def manifest_entry():
    return ManifestEntry(
        game_id="20251001_LG_KT_0",
        source_type="raw_relay",
        format="normalized_events_json",
        locator='{"events": [{"id": 1}], "raw_pbp_rows": [{"text": "play1"}]}',
        priority=10,
    )


class TestImportRelayAdapter:
    def test_init_defaults(self):
        adapter = ImportRelayAdapter()
        assert adapter.supports_bucket_probe is False
        assert adapter.cache_negative_probe is False
        assert adapter.manifest_entries == []

    def test_init_with_entries(self, manifest_entry):
        adapter = ImportRelayAdapter(manifest_entries=[manifest_entry])
        assert len(adapter.manifest_entries) == 1

    @pytest.mark.asyncio
    async def test_fetch_game_no_match(self, adapter):
        result = await adapter.fetch_game("nonexistent")
        assert result.is_empty
        assert "No manifest entry matched" in (result.notes or "")

    @pytest.mark.asyncio
    async def test_fetch_game_no_match_with_allowed_types(self):
        adapter = ImportRelayAdapter(allowed_source_types={"processed_relay"})
        result = await adapter.fetch_game("nonexistent")
        assert result.is_empty

    @pytest.mark.asyncio
    async def test_fetch_game_with_match(self, manifest_entry):
        adapter = ImportRelayAdapter(manifest_entries=[manifest_entry])
        result = await adapter.fetch_game("20251001_LG_KT_0")
        assert not result.is_empty
        assert len(result.events) == 1

    @pytest.mark.asyncio
    async def test_fetch_game_with_priority_order(self):
        entries = [
            ManifestEntry(
                game_id="GAME1",
                source_type="raw_relay",
                format="normalized_events_json",
                locator='{"events": [], "raw_pbp_rows": []}',
                priority=5,
            ),
            ManifestEntry(
                game_id="GAME1",
                source_type="raw_relay",
                format="normalized_events_json",
                locator='{"events": [{"id": 2}], "raw_pbp_rows": []}',
                priority=1,
            ),
        ]
        adapter = ImportRelayAdapter(manifest_entries=entries)
        result = await adapter.fetch_game("GAME1")
        assert len(result.events) == 1
        assert result.events[0]["id"] == 2

    def test_parse_normalized_events_json_dict(self, adapter):
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="normalized_events_json",
            locator='{"events": [{"id": 1}], "raw_pbp_rows": [], "notes": "test_note"}',
            priority=1,
        )
        result = adapter._parse_normalized_events_json(entry)
        assert len(result.events) == 1
        assert result.has_event_state is False

    def test_parse_normalized_events_json_list(self, adapter):
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="normalized_events_json",
            locator='[{"id": 1}, {"id": 2}]',
            priority=1,
        )
        result = adapter._parse_normalized_events_json(entry)
        assert len(result.events) == 2
        assert result.has_raw_pbp is False

    def test_parse_normalized_events_json_invalid(self, adapter):
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="normalized_events_json",
            locator="42",
            priority=1,
        )
        result = adapter._parse_normalized_events_json(entry)
        assert result.is_empty
        assert "Unexpected" in (result.notes or "")

    def test_parse_naver_json_dict(self, adapter):
        payload = {"result": {"textRelayData": {"textRelays": [{"id": 1}]}}}
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="naver_json",
            locator=json.dumps(payload),
            priority=1,
        )
        with patch.object(
            adapter._relay_parser,
            "_parse_naver_payload",
            return_value={"events": [{"id": 1}], "raw_pbp_rows": []},
        ):
            result = adapter._parse_naver_json(entry)
            assert not result.is_empty
            assert len(result.events) == 1

    def test_parse_naver_json_list(self, adapter):
        payload = [{"id": 1}]
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="naver_json",
            locator=json.dumps(payload),
            priority=1,
        )
        with patch.object(
            adapter._relay_parser,
            "_parse_naver_payload",
            return_value={"events": [{"id": 1}], "raw_pbp_rows": []},
        ):
            result = adapter._parse_naver_json(entry)
            assert not result.is_empty
            assert len(result.events) == 1

    def test_parse_naver_json_no_relays(self, adapter):
        payload = {"result": {"textRelayData": {}}}
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="naver_json",
            locator=json.dumps(payload),
            priority=1,
        )
        result = adapter._parse_naver_json(entry)
        assert result.is_empty

    def test_parse_html_archive(self, adapter):
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="kbo_html",
            locator="1회초 <b>안타</b> 2회말 <i>삼진</i>",
            priority=1,
        )
        result = adapter._parse_html_archive(entry)
        assert result.has_raw_pbp
        assert len(result.raw_pbp_rows) > 0
        assert result.has_event_state is False

    def test_parse_plain_text(self, adapter):
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="pbp_text",
            locator="1회초 안타\n2회말 삼진",
            priority=1,
        )
        result = adapter._parse_plain_text(entry)
        assert result.has_raw_pbp
        assert len(result.raw_pbp_rows) > 0

    def test_parse_unsupported_format(self, adapter):
        entry = ManifestEntry(
            game_id="G",
            source_type="t",
            format="unknown_format",
            locator="data",
            priority=1,
        )
        result = adapter._parse_entry(entry)
        assert result.is_empty
        assert "Unsupported manifest format" in (result.notes or "")

    def test_resolve_locator_absolute(self, adapter):
        result = adapter._resolve_locator("/absolute/path/file.json")
        assert result == Path("/absolute/path/file.json")

    def test_resolve_locator_relative(self, adapter):
        adapter.manifest_base_dir = Path("/base")
        result = adapter._resolve_locator("relative/file.json")
        assert result == Path("/base/relative/file.json")

    def test_read_text_from_locator_string(self, adapter):
        result = adapter._read_text("inline text content")
        assert result == "inline text content"

    def test_read_json_from_locator_string(self, adapter):
        result = adapter._read_json('{"key": "value"}')
        assert result == {"key": "value"}

    def test_lines_to_pbp_rows(self, adapter):
        lines = [
            "1회초 안타",
            "2회말 삼진",
            "non inning text",
        ]
        rows = adapter._lines_to_pbp_rows(lines)
        assert len(rows) == 3
        assert rows[0]["inning"] == 1
        assert rows[0]["inning_half"] == "top"
        assert rows[1]["inning"] == 2
        assert rows[1]["inning_half"] == "bottom"
        assert rows[2]["inning"] == 2
        assert rows[2]["inning_half"] == "bottom"

    def test_lines_to_pbp_rows_empty_lines(self, adapter):
        rows = adapter._lines_to_pbp_rows(["", "   ", "1회초 안타"])
        assert len(rows) == 1

    @pytest.mark.asyncio
    async def test_fetch_game_entry_notes_appended(self):
        entry = ManifestEntry(
            game_id="GAME1",
            source_type="raw_relay",
            format="normalized_events_json",
            locator='{"events": [{"id": 1}], "raw_pbp_rows": []}',
            priority=1,
            notes="entry_note",
        )
        adapter = ImportRelayAdapter(manifest_entries=[entry])
        result = await adapter.fetch_game("GAME1")
        assert not result.is_empty
        assert "entry_note" in (result.notes or "")

    @pytest.mark.asyncio
    async def test_fetch_game_all_miss(self):
        entries = [
            ManifestEntry(
                game_id="G",
                source_type="t",
                format="normalized_events_json",
                locator='{"events": [], "raw_pbp_rows": []}',
                priority=1,
            ),
        ]
        adapter = ImportRelayAdapter(manifest_entries=entries)
        result = await adapter.fetch_game("G")
        assert result.is_empty
        assert "Manifest entries found, but none yielded usable relay data" in (result.notes or "")
