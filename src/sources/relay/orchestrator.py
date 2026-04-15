from __future__ import annotations

import asyncio
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
        timeout_seconds: float = 30.0,
    ):
        self.adapters = adapters
        self.capability_path = Path(capability_path)
        self.sample_size = sample_size
        self.timeout_seconds = timeout_seconds

    @staticmethod
    def _uses_bucket_probe(adapter: RelaySourceAdapter | None) -> bool:
        return bool(adapter is not None and getattr(adapter, "supports_bucket_probe", True))

    @staticmethod
    def _can_skip_from_capability(
        adapter: RelaySourceAdapter | None,
        capability: CapabilityRecord | None,
    ) -> bool:
        if capability is None or capability.supported or adapter is None:
            return False
        return bool(
            getattr(adapter, "supports_bucket_probe", True)
            and getattr(adapter, "cache_negative_probe", True)
        )

    async def _fetch_with_timeout(
        self,
        adapter: RelaySourceAdapter,
        game_id: str,
    ) -> tuple[NormalizedRelayResult, str]:
        try:
            result = await asyncio.wait_for(adapter.fetch_game(game_id), timeout=self.timeout_seconds)
            status = "success" if not result.is_empty else "miss"
            return result, status
        except asyncio.TimeoutError:
            return (
                NormalizedRelayResult(
                    game_id=game_id,
                    source_name=adapter.source_name,
                    notes=f"timeout after {self.timeout_seconds:.1f}s",
                ),
                "timeout",
            )

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
            adapter = self.adapters.get(source_name)
            cached = self.get_capability(bucket_id, source_name)
            if cached is not None and (
                cached.supported or self._can_skip_from_capability(adapter, cached)
            ):
                records[source_name] = cached
                continue

            if adapter is None:
                continue
            if not self._uses_bucket_probe(adapter):
                continue

            misses = 0
            successes = 0
            last_notes: str | None = None
            for game_id in sample_ids:
                result, status = await self._fetch_with_timeout(adapter, game_id)
                if result.is_empty:
                    misses += 1
                    last_notes = result.notes or status
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
            if successes > 0 or getattr(adapter, "cache_negative_probe", True):
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
            adapter = self.adapters.get(source_name)
            capability = self.get_capability(bucket_id, source_name)
            if self._can_skip_from_capability(adapter, capability):
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

            result, status = await self._fetch_with_timeout(adapter, game_id)
            attempts.append(
                {
                    "game_id": game_id,
                    "bucket_id": bucket_id,
                    "source_name": source_name,
                    "status": status,
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
