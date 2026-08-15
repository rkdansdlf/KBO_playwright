"""Play-by-Play Realtime Event Streaming and Pub/Sub Queue for KBO Pipeline."""

from __future__ import annotations

import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class LivePbpEvent:
    """Realtime immutable play-by-play event payload."""

    game_id: str
    event_seq: int
    inning: int
    half: str  # 'TOP', 'BOTTOM'
    batter_name: str
    pitcher_name: str
    description: str
    score_home: int
    score_away: int
    outs: int
    base_state: str  # e.g., '100', '110', '111', '000'
    wpa: float | None = None
    win_expectancy: float | None = None
    leverage_index: float | None = None
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    extra: dict[str, Any] | None = None


class LivePbpEventStream:
    """In-memory Pub/Sub event streaming engine with replay buffer."""

    _instance: ClassVar[LivePbpEventStream | None] = None

    def __init__(self, max_buffer_per_game: int = 500) -> None:
        """Initialize empty streaming broker."""
        self._max_buffer = max_buffer_per_game
        self._buffers: dict[str, list[LivePbpEvent]] = defaultdict(list)
        self._subscribers: list[tuple[str | None, Callable[[LivePbpEvent], Any]]] = []

    @classmethod
    def get_instance(cls) -> LivePbpEventStream:
        """Return shared singleton instance."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def subscribe(
        self,
        callback: Callable[[LivePbpEvent], Any],
        *,
        game_id: str | None = None,
    ) -> None:
        """Register a subscriber callback. If game_id is None, receives events for all games."""
        self._subscribers.append((game_id, callback))

    def publish(self, event: LivePbpEvent) -> int:
        """Publish event to replay buffer and notify all active subscribers. Returns notified count."""
        buf = self._buffers[event.game_id]
        buf.append(event)
        if len(buf) > self._max_buffer:
            buf.pop(0)

        notified = 0
        for target_game_id, callback in self._subscribers:
            if target_game_id is None or target_game_id == event.game_id:
                try:
                    callback(event)
                    notified += 1
                except Exception as exc:  # noqa: BLE001
                    logger.warning("[EventStream] Subscriber error for game %s: %s", event.game_id, exc)

        return notified

    def get_history(self, game_id: str) -> list[LivePbpEvent]:
        """Retrieve historical event stream for a specific game."""
        return list(self._buffers.get(game_id, []))

    def clear_game(self, game_id: str) -> None:
        """Purge in-memory buffer after game finalization."""
        self._buffers.pop(game_id, None)


__all__ = ["LivePbpEvent", "LivePbpEventStream"]
