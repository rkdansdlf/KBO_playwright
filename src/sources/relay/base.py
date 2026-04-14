from __future__ import annotations

import csv
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional

ALLOWED_SOURCE_TYPES = {
    "naver",
    "kbo",
    "html_archive",
    "json_archive",
    "manual_text",
}
ALLOWED_MANIFEST_FORMATS = {
    "naver_json",
    "kbo_html",
    "relay_html",
    "pbp_text",
    "normalized_events_json",
}

SPECIAL_BUCKET_SOURCE_ORDER = ("kbo", "import", "manual")
REGULAR_BUCKET_SOURCE_ORDER = ("naver", "kbo", "import")


@dataclass(slots=True)
class ManifestEntry:
    game_id: str
    source_type: str
    locator: str
    format: str
    priority: int = 100
    notes: str | None = None


@dataclass(slots=True)
class CapabilityRecord:
    bucket_id: str
    source_name: str
    sample_size: int
    supported: bool
    last_checked_at: str
    notes: str | None = None


@dataclass(slots=True)
class NormalizedRelayResult:
    game_id: str
    source_name: str
    events: list[dict[str, Any]] = field(default_factory=list)
    raw_pbp_rows: list[dict[str, Any]] = field(default_factory=list)
    has_event_state: bool = False
    has_raw_pbp: bool = False
    notes: str | None = None

    @property
    def is_empty(self) -> bool:
        return not self.events and not self.raw_pbp_rows


class RelaySourceAdapter(ABC):
    def __init__(self, source_name: str):
        self.source_name = source_name

    @abstractmethod
    async def fetch_game(self, game_id: str) -> NormalizedRelayResult:
        raise NotImplementedError


def normalize_inning_half(value: Any) -> Any:
    normalized = str(value or "").strip().lower()
    if normalized in {"top", "away", "초"}:
        return "top"
    if normalized in {"bottom", "home", "말"}:
        return "bottom"
    return value


def trailing_result_from_description(description: Any) -> Any:
    text = str(description or "").strip()
    if not text:
        return None
    if ":" in text:
        return text.rsplit(":", 1)[-1].strip() or None
    tokens = [token for token in text.split() if token]
    return tokens[-1] if tokens else None


def event_to_pbp_row(event: dict[str, Any]) -> dict[str, Any]:
    return normalize_pbp_row(
        {
            "inning": event.get("inning"),
            "inning_half": event.get("inning_half"),
            "pitcher_name": event.get("pitcher_name") or event.get("pitcher"),
            "batter_name": event.get("batter_name") or event.get("batter"),
            "play_description": event.get("description"),
            "event_type": event.get("event_type"),
            "result": event.get("result_code")
            or event.get("result")
            or trailing_result_from_description(event.get("description")),
        }
    )


def normalize_pbp_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "inning": row.get("inning"),
        "inning_half": normalize_inning_half(row.get("inning_half")),
        "pitcher_name": row.get("pitcher_name"),
        "batter_name": row.get("batter_name"),
        "play_description": row.get("play_description") or row.get("description"),
        "event_type": row.get("event_type"),
        "result": row.get("result")
        or trailing_result_from_description(row.get("play_description") or row.get("description")),
    }


def event_has_minimum_state(event: dict[str, Any]) -> bool:
    required = (
        event.get("inning") is not None,
        event.get("inning_half") is not None,
        event.get("outs") is not None,
        event.get("description") not in (None, ""),
        event.get("wpa") is not None,
        event.get("win_expectancy_before") is not None,
        event.get("win_expectancy_after") is not None,
        event.get("home_score") is not None,
        event.get("away_score") is not None,
    )
    has_base_state = event.get("base_state") is not None or (
        event.get("bases_before") is not None and event.get("bases_after") is not None
    )
    return all(required) and has_base_state


def events_have_minimum_state(events: Iterable[dict[str, Any]]) -> bool:
    events = list(events)
    return bool(events) and all(event_has_minimum_state(event) for event in events)


def derive_bucket_id(game_id: str, league_type_name: str | None = None) -> str:
    year = int(str(game_id)[:4])
    team_code = str(game_id)[8:12]
    league_name = str(league_type_name or "").strip().lower()

    if year <= 2023:
        return f"{year}_legacy"
    if team_code in {"EAWE", "WEEA"} or "올스타" in league_name:
        return f"{year}_all_star"
    if any(token in league_name for token in ("한국시리즈", "포스트", "플레이오프", "와일드카드", "준플레이오프")):
        return f"{year}_postseason"
    if any(token in league_name for token in ("international", "wbc", "premier", "대표팀", "국가대표")):
        return f"{year}_international"

    game_date = str(game_id)[:8]
    if year == 2024 and "20241002" <= game_date <= "20241028":
        return "2024_postseason"
    if year == 2024 and "20241110" <= game_date <= "20241124":
        return "2024_international"
    if year == 2025 and team_code == "EAWE":
        return "2025_all_star"
    if year in {2024, 2025, 2026}:
        return f"{year}_regular_kbo"
    return f"{year}_legacy"


def default_source_order_for_bucket(bucket_id: str) -> list[str]:
    if bucket_id.endswith("regular_kbo"):
        return list(REGULAR_BUCKET_SOURCE_ORDER)
    return list(SPECIAL_BUCKET_SOURCE_ORDER)


def read_manifest_entries(manifest_path: str | Path) -> list[ManifestEntry]:
    path = Path(manifest_path)
    if not path.exists():
        return []

    entries: list[ManifestEntry] = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row:
                continue
            game_id = str(row.get("game_id") or "").strip()
            source_type = str(row.get("source_type") or "").strip()
            locator = str(row.get("locator") or "").strip()
            manifest_format = str(row.get("format") or "").strip()
            if not game_id or not source_type or not locator or not manifest_format:
                continue
            if source_type not in ALLOWED_SOURCE_TYPES:
                raise ValueError(f"Unsupported manifest source_type: {source_type}")
            if manifest_format not in ALLOWED_MANIFEST_FORMATS:
                raise ValueError(f"Unsupported manifest format: {manifest_format}")
            entries.append(
                ManifestEntry(
                    game_id=game_id,
                    source_type=source_type,
                    locator=locator,
                    format=manifest_format,
                    priority=int(row.get("priority") or 100),
                    notes=(row.get("notes") or "").strip() or None,
                )
            )
    return sorted(entries, key=lambda entry: (entry.game_id, entry.priority, entry.locator))


def load_capability_records(capability_path: str | Path) -> dict[tuple[str, str], CapabilityRecord]:
    path = Path(capability_path)
    if not path.exists():
        return {}

    records: dict[tuple[str, str], CapabilityRecord] = {}
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if not row:
                continue
            bucket_id = str(row.get("bucket_id") or "").strip()
            source_name = str(row.get("source_name") or "").strip()
            if not bucket_id or not source_name:
                continue
            records[(bucket_id, source_name)] = CapabilityRecord(
                bucket_id=bucket_id,
                source_name=source_name,
                sample_size=int(row.get("sample_size") or 0),
                supported=str(row.get("supported") or "").strip().lower() == "true",
                last_checked_at=str(row.get("last_checked_at") or "").strip(),
                notes=(row.get("notes") or "").strip() or None,
            )
    return records


def upsert_capability_record(capability_path: str | Path, record: CapabilityRecord) -> None:
    path = Path(capability_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = load_capability_records(path)
    existing[(record.bucket_id, record.source_name)] = record

    rows = sorted(existing.values(), key=lambda item: (item.bucket_id, item.source_name))
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
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
            writer.writerow(
                {
                    "bucket_id": row.bucket_id,
                    "source_name": row.source_name,
                    "sample_size": row.sample_size,
                    "supported": str(bool(row.supported)).lower(),
                    "last_checked_at": row.last_checked_at
                    or datetime.now(timezone.utc).isoformat(),
                    "notes": row.notes or "",
                }
            )

