from __future__ import annotations

import json
import re
from html import unescape
from pathlib import Path
from typing import Any, Iterable

from src.crawlers.relay_crawler import RelayCrawler

from .base import (
    ManifestEntry,
    NormalizedRelayResult,
    RelaySourceAdapter,
    event_to_pbp_row,
    events_have_minimum_state,
    normalize_inning_half,
    normalize_pbp_row,
)


class ImportRelayAdapter(RelaySourceAdapter):
    def __init__(
        self,
        manifest_entries: Iterable[ManifestEntry] | None = None,
        *,
        source_name: str = "import",
        allowed_source_types: set[str] | None = None,
        manifest_base_dir: str | Path | None = None,
    ):
        super().__init__(source_name)
        self.supports_bucket_probe = False
        self.cache_negative_probe = False
        self.manifest_entries = list(manifest_entries or [])
        self.allowed_source_types = allowed_source_types
        self.manifest_base_dir = Path(manifest_base_dir or ".")
        self._relay_parser = RelayCrawler()

    async def fetch_game(self, game_id: str) -> NormalizedRelayResult:
        candidates = [
            entry
            for entry in self.manifest_entries
            if entry.game_id == game_id
            and (self.allowed_source_types is None or entry.source_type in self.allowed_source_types)
        ]
        if not candidates:
            return NormalizedRelayResult(
                game_id=game_id,
                source_name=self.source_name,
                notes="No manifest entry matched this game",
            )

        for entry in sorted(candidates, key=lambda item: item.priority):
            parsed = self._parse_entry(entry)
            if parsed.is_empty:
                continue
            if entry.notes and not parsed.notes:
                parsed.notes = entry.notes
            elif entry.notes and parsed.notes:
                parsed.notes = f"{entry.notes} | {parsed.notes}"
            parsed.source_name = self.source_name
            return parsed

        return NormalizedRelayResult(
            game_id=game_id,
            source_name=self.source_name,
            notes="Manifest entries found, but none yielded usable relay data",
        )

    def _parse_entry(self, entry: ManifestEntry) -> NormalizedRelayResult:
        if entry.format == "normalized_events_json":
            return self._parse_normalized_events_json(entry)
        if entry.format == "naver_json":
            return self._parse_naver_json(entry)
        if entry.format in {"kbo_html", "relay_html"}:
            return self._parse_html_archive(entry)
        if entry.format == "pbp_text":
            return self._parse_plain_text(entry)
        return NormalizedRelayResult(
            game_id=entry.game_id,
            source_name=self.source_name,
            notes=f"Unsupported manifest format: {entry.format}",
        )

    def _resolve_locator(self, locator: str) -> Path:
        path = Path(locator)
        if path.is_absolute():
            return path
        return self.manifest_base_dir / path

    def _read_text(self, locator: str) -> str:
        path = self._resolve_locator(locator)
        if path.exists():
            return path.read_text(encoding="utf-8")
        return locator

    def _read_json(self, locator: str) -> Any:
        path = self._resolve_locator(locator)
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
        return json.loads(locator)

    def _parse_normalized_events_json(self, entry: ManifestEntry) -> NormalizedRelayResult:
        payload = self._read_json(entry.locator)
        if isinstance(payload, dict):
            events = list(payload.get("events") or [])
            raw_pbp_rows = list(payload.get("raw_pbp_rows") or [])
            notes = payload.get("notes") or entry.notes
        elif isinstance(payload, list):
            events = list(payload)
            raw_pbp_rows = []
            notes = entry.notes
        else:
            events = []
            raw_pbp_rows = []
            notes = f"Unexpected normalized_events_json payload type: {type(payload).__name__}"
        return NormalizedRelayResult(
            game_id=entry.game_id,
            source_name=self.source_name,
            events=events,
            raw_pbp_rows=[normalize_pbp_row(row) for row in raw_pbp_rows],
            has_event_state=events_have_minimum_state(events),
            has_raw_pbp=bool(raw_pbp_rows),
            notes=notes,
        )

    def _parse_naver_json(self, entry: ManifestEntry) -> NormalizedRelayResult:
        payload = self._read_json(entry.locator)
        if isinstance(payload, dict):
            relays = (
                payload.get("result", {})
                .get("textRelayData", {})
                .get("textRelays", [])
            )
            if not relays and isinstance(payload.get("textRelays"), list):
                relays = payload.get("textRelays") or []
        elif isinstance(payload, list):
            relays = payload
        else:
            relays = []
        events = self._relay_parser._parse_naver_data(relays) if relays else []
        return NormalizedRelayResult(
            game_id=entry.game_id,
            source_name=self.source_name,
            events=events,
            raw_pbp_rows=[],
            has_event_state=events_have_minimum_state(events),
            has_raw_pbp=False,
            notes=entry.notes,
        )

    def _parse_html_archive(self, entry: ManifestEntry) -> NormalizedRelayResult:
        html = self._read_text(entry.locator)
        text = unescape(re.sub(r"<[^>]+>", "\n", html))
        rows = self._lines_to_pbp_rows(text.splitlines())
        return NormalizedRelayResult(
            game_id=entry.game_id,
            source_name=self.source_name,
            events=[],
            raw_pbp_rows=rows,
            has_event_state=False,
            has_raw_pbp=bool(rows),
            notes=entry.notes,
        )

    def _parse_plain_text(self, entry: ManifestEntry) -> NormalizedRelayResult:
        rows = self._lines_to_pbp_rows(self._read_text(entry.locator).splitlines())
        return NormalizedRelayResult(
            game_id=entry.game_id,
            source_name=self.source_name,
            events=[],
            raw_pbp_rows=rows,
            has_event_state=False,
            has_raw_pbp=bool(rows),
            notes=entry.notes,
        )

    def _lines_to_pbp_rows(self, lines: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        current_inning = None
        current_half = None
        for raw_line in lines:
            line = " ".join(str(raw_line or "").strip().split())
            if not line:
                continue
            match = re.match(r"(\d+)회\s*(초|말)\s*(.*)", line)
            if match:
                current_inning = int(match.group(1))
                current_half = "top" if match.group(2) == "초" else "bottom"
                line = match.group(3).strip() or line
            rows.append(
                normalize_pbp_row(
                    {
                        "inning": current_inning,
                        "inning_half": normalize_inning_half(current_half),
                        "play_description": line,
                        "event_type": "unknown",
                    }
                )
            )
        return rows
