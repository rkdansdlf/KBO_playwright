"""Data Transfer Objects and Enums for WebSocket Live Stream and Circuit Breaker."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class CircuitState(StrEnum):
    """Lifecycle states of the Circuit Breaker."""

    CLOSED = "CLOSED"  # Normal operational state, all traffic passed
    OPEN = "OPEN"  # Tripped state, calls immediately rejected or routed to fallback
    HALF_OPEN = "HALF_OPEN"  # Trial probe state, testing upstream service recovery


class StreamEventType(StrEnum):
    """Standardized event types emitted across WebSocket live streams."""

    CONNECTION_ESTABLISHED = "CONNECTION_ESTABLISHED"
    PLAY_EVENT = "PLAY_EVENT"
    WPA_UPDATE = "WPA_UPDATE"
    GAME_FINISHED = "GAME_FINISHED"
    CIRCUIT_STATE_CHANGED = "CIRCUIT_STATE_CHANGED"
    STREAM_DEGRADED = "STREAM_DEGRADED"
    HEARTBEAT_PING = "HEARTBEAT_PING"
    HEARTBEAT_PONG = "HEARTBEAT_PONG"
    CHANNEL_SUBSCRIBED = "CHANNEL_SUBSCRIBED"
    CHANNEL_UNSUBSCRIBED = "CHANNEL_UNSUBSCRIBED"
    ERROR = "ERROR"


class CircuitStatusDTO(BaseModel):
    """Telemetry and status information for a circuit breaker."""

    name: str = Field(..., description="Unique circuit breaker identifier")
    state: CircuitState = Field(..., description="Current circuit state")
    failure_count: int = Field(..., description="Total lifetime failure count")
    consecutive_failures: int = Field(..., description="Current streak of consecutive failures")
    success_count: int = Field(..., description="Total lifetime success count")
    rejection_count: int = Field(..., description="Total requests rejected while OPEN")
    failure_threshold: int = Field(..., description="Failures required to trip circuit")
    recovery_timeout_seconds: float = Field(..., description="Cooldown duration before probe")
    time_in_current_state_seconds: float = Field(..., description="Seconds since last state transition")
    last_failure_reason: str | None = Field(default=None, description="Exception message of last failure")


class CircuitActionResponse(BaseModel):
    """Response payload for administrative circuit breaker actions (trip/reset)."""

    status: str = Field(..., description="Action outcome (e.g. RESET_SUCCESS, TRIPPED_SUCCESS)")
    circuit_name: str = Field(..., description="Target circuit breaker name")
    previous_state: CircuitState = Field(..., description="State prior to action")
    new_state: CircuitState = Field(..., description="State following action")
    message: str = Field(..., description="Descriptive status message")


class LiveChannelStatsDTO(BaseModel):
    """Active subscriber metrics for live stream channels."""

    channel_id: str = Field(..., description="Channel name or game ID")
    active_viewers: int = Field(..., description="Count of connected client WebSockets")
    is_active_game: bool = Field(default=True, description="Whether the game stream is currently live")


class LiveStreamStatsResponse(BaseModel):
    """Aggregated WebSocket streaming server metrics."""

    active_global_clients: int = Field(..., description="Total unique active WebSocket connections")
    total_active_channels: int = Field(..., description="Total active distinct game channels")
    channels: dict[str, int] = Field(default_factory=dict, description="Active viewer counts per channel")
    circuit_breakers: list[CircuitStatusDTO] = Field(default_factory=list, description="Circuit breaker statuses")


class MultiGameSubscriptionAction(BaseModel):
    """Client payload for dynamically subscribing/unsubscribing to multiple games."""

    action: str = Field(..., description="Action to perform: 'subscribe', 'unsubscribe', or 'ping'")
    games: list[str] = Field(default_factory=list, description="List of game IDs to subscribe/unsubscribe")
    payload: dict[str, Any] = Field(default_factory=dict, description="Optional metadata or client context")
