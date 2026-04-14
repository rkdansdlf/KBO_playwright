from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .base import (
    CapabilityRecord,
    NormalizedRelayResult,
    RelaySourceAdapter,
    default_source_order_for_bucket,
    load_capability_records,
    upsert_capability_record,
)


class RelayRecoveryOrchestrator:
    def __init__(
        self,
        adapters: dict[str, RelaySourceAdapter],
        *,
        capability_path: str | Path,
        sample_size: int = 3,
    ):
        self.adapters = adapters
        self.capability_path = Path(capability_path)
        self.sample_size = sample_size

    def source_order_for_bucket(self, bucket_id: str, override: Iterable[str] | None = None) -> list[str]:
        if override:
            return [token.strip() for token in override if token and token.strip()]
        return default_source_order_for_bucket(bucket_id)

    def get_capability(self, bucket_id: str, source_name: str) -> CapabilityRecord | None:
        return load_capability_records(self.capability_path).get((bucket_id, source_name))

    async def probe_bucket(
        self,
        bucket_id: str,
        game_ids: Iterable[str],
        source_order: Iterable[str],
    ) -> dict[str, CapabilityRecord]:
        sample_ids = [game_id for game_id in game_ids if game_id][: self.sample_size]
        records: dict[str, CapabilityRecord] = {}
        if not sample_ids:
            return records

        for source_name in source_order:
            cached = self.get_capability(bucket_id, source_name)
            if cached is not None:
                records[source_name] = cached
                continue

            adapter = self.adapters.get(source_name)
            if adapter is None:
                continue

            misses = 0
            successes = 0
            last_notes: str | None = None
            for game_id in sample_ids:
                result = await adapter.fetch_game(game_id)
                if result.is_empty:
                    misses += 1
                    last_notes = result.notes
                else:
                    successes += 1
                    last_notes = result.notes
                    break

            record = CapabilityRecord(
                bucket_id=bucket_id,
                source_name=source_name,
                sample_size=min(len(sample_ids), self.sample_size),
                supported=successes > 0,
                last_checked_at=datetime.now(timezone.utc).isoformat(),
                notes=last_notes or ("probe_success" if successes else "three_sample_miss"),
            )
            upsert_capability_record(self.capability_path, record)
            records[source_name] = record
        return records

    async def fetch_game(
        self,
        game_id: str,
        bucket_id: str,
        source_order: Iterable[str],
    ) -> tuple[NormalizedRelayResult, list[dict[str, Any]]]:
        attempts: list[dict[str, Any]] = []
        for source_name in source_order:
            capability = self.get_capability(bucket_id, source_name)
            if capability is not None and not capability.supported:
                attempts.append(
                    {
                        "game_id": game_id,
                        "bucket_id": bucket_id,
                        "source_name": source_name,
                        "status": "cached_unsupported",
                        "notes": capability.notes,
                    }
                )
                continue

            adapter = self.adapters.get(source_name)
            if adapter is None:
                attempts.append(
                    {
                        "game_id": game_id,
                        "bucket_id": bucket_id,
                        "source_name": source_name,
                        "status": "missing_adapter",
                        "notes": None,
                    }
                )
                continue

            result = await adapter.fetch_game(game_id)
            attempts.append(
                {
                    "game_id": game_id,
                    "bucket_id": bucket_id,
                    "source_name": source_name,
                    "status": "success" if not result.is_empty else "miss",
                    "has_event_state": result.has_event_state,
                    "has_raw_pbp": result.has_raw_pbp,
                    "notes": result.notes,
                }
            )
            if not result.is_empty:
                return result, attempts

        return NormalizedRelayResult(
            game_id=game_id,
            source_name="none",
            notes="All configured relay sources missed",
        ), attempts
