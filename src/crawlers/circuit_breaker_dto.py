"""DTOs and data structures for crawler circuit breaker."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class CircuitState(StrEnum):
    """Lifecycle states of a circuit breaker."""

    CLOSED = "CLOSED"  # Normal operation: requests pass through
    OPEN = "OPEN"  # Tripped / Fault isolation: requests blocked / redirected to fallback
    HALF_OPEN = "HALF_OPEN"  # Probing: trial request allowed to verify service recovery


class CircuitOpenError(Exception):
    """Exception raised when an operation is blocked by an open circuit breaker."""

    def __init__(self, name: str, recovery_remaining_seconds: float) -> None:
        """Initialize with circuit name and remaining cooldown seconds."""
        self.name = name
        self.recovery_remaining_seconds = recovery_remaining_seconds
        super().__init__(
            f"Circuit breaker '{name}' is OPEN. Requests blocked (Cooldown: {recovery_remaining_seconds:.1f}s)."
        )


@dataclass
class CircuitBreakerStats:
    """Statistical snapshot of a circuit breaker instance."""

    name: str
    state: CircuitState
    failure_count: int = 0
    consecutive_failures: int = 0
    success_count: int = 0
    failure_threshold: int = 5
    recovery_timeout_seconds: float = 60.0
    last_failure_time: float | None = None
    last_state_change: float = field(default_factory=time.time)

    def to_dict(self) -> dict[str, Any]:
        """Convert stats to JSON-serializable dictionary."""
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self.failure_count,
            "consecutive_failures": self.consecutive_failures,
            "success_count": self.success_count,
            "failure_threshold": self.failure_threshold,
            "recovery_timeout_seconds": self.recovery_timeout_seconds,
            "last_failure_time": self.last_failure_time,
            "last_state_change": self.last_state_change,
        }


__all__ = ["CircuitBreakerStats", "CircuitOpenError", "CircuitState"]
