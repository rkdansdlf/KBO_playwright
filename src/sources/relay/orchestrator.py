"""데이터 소스: orchestrator."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

try:
    from datetime import UTC
except ImportError:
    UTC = UTC
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .base import (
    CapabilityRecord,
    NormalizedRelayResult,
    RelaySourceAdapter,
    default_source_order_for_bucket,
    load_capability_records,
    upsert_capability_record,
)
from .circuit_breaker import SourceCircuitBreaker

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

logger = logging.getLogger(__name__)


class RelayRecoveryOrchestrator:
    """RelayRecoveryOrchestrator class."""

    def __init__(
        self,
        adapters: dict[str, RelaySourceAdapter],
        *,
        capability_path: str | Path,
        sample_size: int = 3,
        timeout_seconds: float = 30.0,
        circuit_breaker: SourceCircuitBreaker | None = None,
    ) -> None:
        """Initializes a new instance."""
        self.adapters = adapters
        self.capability_path = Path(capability_path)
        self.sample_size = sample_size
        self.timeout_seconds = timeout_seconds
        self.circuit_breaker = circuit_breaker or SourceCircuitBreaker()
        self._capability_cache: dict[tuple[str, str], CapabilityRecord] | None = None

    def _invalidate_capability_cache(self) -> None:
        self._capability_cache = None

    def _load_capability(self) -> dict[tuple[str, str], CapabilityRecord]:
        if self._capability_cache is None:
            self._capability_cache = load_capability_records(self.capability_path)
        return self._capability_cache

    def get_capability(self, bucket_id: str, source_name: str) -> CapabilityRecord | None:
        """
        Gets capability.

        Args:
            bucket_id: Bucket ID.
            source_name: Source Name.

        Returns:
            The result of the operation.

        """
        return self._load_capability().get((bucket_id, source_name))

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
        return bool(getattr(adapter, "supports_bucket_probe", True) and getattr(adapter, "cache_negative_probe", True))

    async def _fetch_with_timeout(
        self,
        adapter: RelaySourceAdapter,
        game_id: str,
    ) -> tuple[NormalizedRelayResult, str]:
        try:
            result = await asyncio.wait_for(adapter.fetch_game(game_id), timeout=self.timeout_seconds)
        except TimeoutError:
            return (
                NormalizedRelayResult(
                    game_id=game_id,
                    source_name=adapter.source_name,
                    notes=f"timeout after {self.timeout_seconds:.1f}s",
                ),
                "timeout",
            )
        except (RuntimeError, ValueError, OSError) as exc:
            logger.warning("Fetch failed for %s from %s: %s", game_id, adapter.source_name, exc)
            return (
                NormalizedRelayResult(
                    game_id=game_id,
                    source_name=adapter.source_name,
                    notes=f"exception: {exc}",
                ),
                "exception",
            )
        else:
            status = "success" if not result.is_empty else "miss"
            return result, status

    def source_order_for_bucket(self, bucket_id: str, override: Iterable[str] | None = None) -> list[str]:
        """
        Handles the source order for bucket operation.

        Args:
            bucket_id: Bucket ID.
            override: Override.

        Returns:
            List of results.

        """
        if override:
            return [token.strip() for token in override if token and token.strip()]
        return default_source_order_for_bucket(bucket_id)

    async def probe_bucket(
        self,
        bucket_id: str,
        game_ids: Iterable[str],
        source_order: Iterable[str],
    ) -> dict[str, CapabilityRecord]:
        """
        Handles the probe bucket operation.

        Args:
            bucket_id: Bucket ID.
            game_ids: Game Ids.
            source_order: Source Order.

        Returns:
            Dictionary result.

        """
        self._invalidate_capability_cache()
        sample_ids = [game_id for game_id in game_ids if game_id][: self.sample_size]
        records: dict[str, CapabilityRecord] = {}
        if not sample_ids:
            return records

        for source_name in source_order:
            adapter = self.adapters.get(source_name)
            cached = self.get_capability(bucket_id, source_name)
            if cached is not None and (cached.supported or self._can_skip_from_capability(adapter, cached)):
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
                last_checked_at=datetime.now(UTC).isoformat(),
                notes=last_notes or ("probe_success" if successes else "three_sample_miss"),
            )
            if successes > 0 or getattr(adapter, "cache_negative_probe", True):
                upsert_capability_record(self.capability_path, record)
            if self._capability_cache is not None:
                self._capability_cache[(bucket_id, source_name)] = record
            records[source_name] = record
        return records

    async def fetch_game(
        self,
        game_id: str,
        bucket_id: str,
        source_order: Iterable[str],
        *,
        validator: Callable[[NormalizedRelayResult], str | None] | None = None,
    ) -> tuple[NormalizedRelayResult, list[dict[str, Any]]]:
        """
        Fetches game.

        Args:
            game_id: Game ID.
            bucket_id: Bucket ID.
            source_order: Source Order.

        Returns:
            Tuple result.

        """
        attempts: list[dict[str, Any]] = []
        for source_name in source_order:
            cb = self.circuit_breaker
            if cb is not None and not cb.is_available(source_name, bucket_id):
                logger.warning(
                    "Skipping source %s / %s: circuit breaker open",
                    source_name,
                    bucket_id,
                )
                attempts.append(
                    {
                        "game_id": game_id,
                        "bucket_id": bucket_id,
                        "source_name": source_name,
                        "status": "cb_open",
                        "notes": f"circuit breaker open, consecutive_failures={cb.consecutive_failures(source_name, bucket_id)}",
                    },
                )
                continue

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
                    },
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
                    },
                )
                continue

            result, status = await self._fetch_with_timeout(adapter, game_id)

            validation_error = None
            if not result.is_empty and validator:
                validation_error = validator(result)
                if validation_error:
                    status = "skipped_validation"

            attempts.append(
                {
                    "game_id": game_id,
                    "bucket_id": bucket_id,
                    "source_name": source_name,
                    "status": status,
                    "has_event_state": result.has_event_state,
                    "has_raw_pbp": result.has_raw_pbp,
                    "notes": validation_error or result.notes,
                },
            )

            if result.is_empty or validation_error:
                if cb is not None:
                    cb.record_failure(source_name, bucket_id)
            else:
                if cb is not None:
                    cb.record_success(source_name, bucket_id)
                return result, attempts

        attempt_summary = (
            "; ".join(f"{a.get('source_name', '?')}={a.get('status', '?')}" for a in attempts)
            if attempts
            else "no_attempts"
        )
        logger.error(
            "All sources exhausted for %s (bucket=%s, order=%s, attempts: %s)",
            game_id,
            bucket_id,
            list(source_order),
            attempt_summary,
        )
        return NormalizedRelayResult(
            game_id=game_id,
            source_name="none",
            notes="All configured relay sources missed or failed validation",
        ), attempts
