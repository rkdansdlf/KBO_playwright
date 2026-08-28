"""WebSocket connection manager for real-time live game broadcasting."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages active WebSocket connections grouped by game_id."""

    def __init__(self) -> None:
        """Initialize connection manager."""
        self._game_connections: dict[str, set[WebSocket]] = defaultdict(set)
        self._global_connections: set[WebSocket] = set()

    async def connect(self, websocket: WebSocket, game_id: str | None = None) -> None:
        """Accept incoming WebSocket connection and register to game channel."""
        await websocket.accept()
        self._global_connections.add(websocket)
        if game_id:
            self._game_connections[game_id].add(websocket)
            logger.info("Client connected to game stream %s (Total: %d)", game_id, len(self._game_connections[game_id]))
        else:
            logger.info("Client connected to global stream (Total: %d)", len(self._global_connections))

    def disconnect(self, websocket: WebSocket, game_id: str | None = None) -> None:
        """Remove WebSocket connection upon disconnect."""
        self._global_connections.discard(websocket)
        if game_id and game_id in self._game_connections:
            self._game_connections[game_id].discard(websocket)
            if not self._game_connections[game_id]:
                del self._game_connections[game_id]
            logger.info("Client disconnected from game %s", game_id)
        else:
            logger.info("Client disconnected from global stream")

    async def broadcast_to_game(self, game_id: str, message: dict[str, Any] | str) -> int:
        """Broadcast payload to all clients subscribed to a specific game."""
        targets = list(self._game_connections.get(game_id, []))
        if not targets:
            return 0

        text_payload = json.dumps(message, ensure_ascii=False) if isinstance(message, dict) else message
        success_count = 0
        dead_connections: list[WebSocket] = []

        for ws in targets:
            try:
                await ws.send_text(text_payload)
                success_count += 1
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to send WebSocket payload to client for game %s: %s", game_id, e)
                dead_connections.append(ws)

        for ws in dead_connections:
            self.disconnect(ws, game_id)

        return success_count

    async def broadcast_global(self, message: dict[str, Any] | str) -> int:
        """Broadcast payload to all connected clients."""
        targets = list(self._global_connections)
        if not targets:
            return 0

        text_payload = json.dumps(message, ensure_ascii=False) if isinstance(message, dict) else message
        success_count = 0
        dead_connections: list[WebSocket] = []

        for ws in targets:
            try:
                await ws.send_text(text_payload)
                success_count += 1
            except Exception as e:  # noqa: BLE001
                logger.warning("Failed to send global WebSocket payload: %s", e)
                dead_connections.append(ws)

        for ws in dead_connections:
            self.disconnect(ws)

        return success_count

    def get_active_count(self, game_id: str | None = None) -> int:
        """Return count of active clients for a specific game or globally."""
        if game_id:
            return len(self._game_connections.get(game_id, []))
        return len(self._global_connections)

    def get_all_active_counts(self) -> dict[str, int]:
        """Return active connection counts for all games."""
        counts = {gid: len(conns) for gid, conns in self._game_connections.items()}
        counts["_global"] = len(self._global_connections)
        return counts


ws_manager = ConnectionManager()

__all__ = ["ConnectionManager", "ws_manager"]
