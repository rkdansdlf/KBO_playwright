"""데이터 소스: circuit breaker."""

from __future__ import annotations

import csv
import logging
import time
from pathlib import Path
from typing import ClassVar

logger = logging.getLogger(__name__)


class SourceCircuitBreaker:
    """Tracks consecutive failures per (source_name, bucket_id) and enforces cooldown.

    When a source exceeds the consecutive-failure threshold for a given bucket,
    it enters cooldown. During cooldown, `is_available()` returns False and the
    orchestrator can skip that source. The breaker auto-resets once the cooldown
    window expires (no manual reset needed).

    If *persist_path* is provided, the breaker state (failure counts and active
    cooldowns) is persisted to a CSV file and reloaded on construction. This
    allows cooldown state to survive process restarts.

    """

    PERSIST_HEADER: ClassVar[list[str]] = ["source_name", "bucket_id", "failures", "cooldown_until_epoch"]

    def __init__(
        self,
        threshold: int | None = None,
        cooldown_seconds: float | None = None,
        persist_path: str | Path | None = None,
    ) -> None:
        """Initialize a new instance.

        Args:
            threshold: Failure count threshold before opening breaker.
            cooldown_seconds: Cooldown duration in seconds.
            persist_path: Persist file path.

        """
        import os

        if threshold is None:
            threshold = int(os.getenv("RELAY_BREAKER_THRESHOLD", "3"))
        if cooldown_seconds is None:
            cooldown_seconds = float(os.getenv("RELAY_BREAKER_COOLDOWN", "60.0"))

        self._threshold = threshold
        self._cooldown = cooldown_seconds
        self._failures: dict[tuple[str, str], int] = {}
        self._cooldowns: dict[tuple[str, str], float] = {}
        self._persist_path = Path(persist_path) if persist_path else None
        if self._persist_path:
            self._load_state()

    # ------------------------------------------------------------------
    # CSV persistence
    # ------------------------------------------------------------------

    def _load_state(self) -> None:
        if not self._persist_path or not self._persist_path.exists():
            return
        now = time.time()
        now_mono = time.monotonic()
        try:
            with self._persist_path.open(newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    key = (row["source_name"], row["bucket_id"])
                    failures = int(row["failures"])
                    cooldown_epoch_str = row.get("cooldown_until_epoch", "").strip()

                    if failures > 0:
                        self._failures[key] = failures
                    if cooldown_epoch_str:
                        cooldown_epoch = float(cooldown_epoch_str)
                        remaining = cooldown_epoch - now
                        if remaining > 0:
                            self._cooldowns[key] = now_mono + remaining
                        else:
                            self._failures.pop(key, None)
            if self._failures or self._cooldowns:
                logger.info(
                    "Loaded circuit breaker state from %s (%d sources in cooldown, %d sources with failures)",
                    self._persist_path,
                    len(self._cooldowns),
                    len(self._failures),
                )
        except (csv.Error, KeyError, OSError, TypeError, ValueError):
            logger.exception(
                "Failed to load circuit breaker state from %s, starting fresh",
                self._persist_path,
            )
            self._failures.clear()
            self._cooldowns.clear()

    def _save_state(self) -> None:
        if not self._persist_path:
            return
        now = time.time()
        now_mono = time.monotonic()
        self._persist_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with self._persist_path.open("w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(self.PERSIST_HEADER)
                for (source_name, bucket_id), failures in self._failures.items():
                    cooldown_expiry = self._cooldowns.get((source_name, bucket_id))
                    cooldown_epoch = ""
                    if cooldown_expiry is not None:
                        remaining = cooldown_expiry - now_mono
                        if remaining > 0:
                            cooldown_epoch = f"{now + remaining:.2f}"
                    writer.writerow([source_name, bucket_id, failures, cooldown_epoch])
        except (csv.Error, OSError, TypeError, ValueError):
            logger.exception(
                "Failed to persist circuit breaker state to %s",
                self._persist_path,
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record_success(self, source_name: str, bucket_id: str) -> None:
        """Handle the record success operation.

        Args:
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.

        """
        key = (source_name, bucket_id)

        cleared = key in self._failures or key in self._cooldowns
        self._failures.pop(key, None)
        self._cooldowns.pop(key, None)
        if cleared:
            logger.info(
                "Circuit breaker reset for %s / %s (success)",
                source_name,
                bucket_id,
            )
        self._save_state()

    def record_failure(self, source_name: str, bucket_id: str) -> None:
        """Handle the record failure operation.

        Args:
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.

        """
        key = (source_name, bucket_id)

        count = self._failures.get(key, 0) + 1
        self._failures[key] = count
        if count >= self._threshold and key not in self._cooldowns:
            self._cooldowns[key] = time.monotonic() + self._cooldown
            logger.warning(
                "Circuit breaker opened for %s / %s after %d failures, cooldown %.0fs",
                source_name,
                bucket_id,
                count,
                self._cooldown,
            )
        self._save_state()

    def is_available(self, source_name: str, bucket_id: str) -> bool:
        """Return whether the available.

        Args:
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.

        Returns:
            True if successful, False otherwise.

        """
        key = (source_name, bucket_id)

        expiry = self._cooldowns.get(key)
        if expiry is None:
            return True
        if time.monotonic() >= expiry:
            self._cooldowns.pop(key, None)
            self._failures.pop(key, None)
            self._save_state()
            logger.info(
                "Circuit breaker auto-reset for %s / %s (cooldown expired)",
                source_name,
                bucket_id,
            )
            return True
        logger.debug(
            "Circuit breaker still open for %s / %s (%.0fs remaining)",
            source_name,
            bucket_id,
            expiry - time.monotonic(),
        )
        return False

    def consecutive_failures(self, source_name: str, bucket_id: str) -> int:
        """Handle the consecutive failures operation.

        Args:
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.

        Returns:
            Integer result.

        """
        return self._failures.get((source_name, bucket_id), 0)

    def is_open(self, source_name: str, bucket_id: str) -> bool:
        """Return whether the open.

        Args:
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.
            source_name: Source Name.
            bucket_id: Bucket ID.

        Returns:
            True if successful, False otherwise.

        """
        return not self.is_available(source_name, bucket_id)

    def reset_all(self) -> None:
        """Reset all."""
        self._failures.clear()
        self._cooldowns.clear()
        self._save_state()
