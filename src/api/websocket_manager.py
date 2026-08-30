"""WebSocket connection manager for real-time live game broadcasting and multi-channel routing."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from src.api.live_stream_dto import LiveChannelStatsDTO

if TYPE_CHECKING:
    from fastapi import WebSocket

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manages active WebSocket connections grouped by game channels and global feeds."""

    def __init__(self) -> None:
        """Initialize connection manager."""
        self._game_connections: dict[str, set[WebSocket]] = defaultdict(set)
        self._global_connections: set[WebSocket] = set()
        self._client_subscriptions: dict[WebSocket, set[str]] = defaultdict(set)

    async def connect(self, websocket: WebSocket, game_id: str | None = None) -> None:
        """Accept incoming WebSocket connection and register to optional initial channel."""
        await websocket.accept()
        self._global_connections.add(websocket)
        if game_id:
            self.subscribe(websocket, game_id)
            logger.info(
                "Client connected to game stream %s (Channel Viewers: %d, Global: %d)",
                game_id,
                len(self._game_connections[game_id]),
                len(self._global_connections),
            )
        else:
            logger.info("Client connected to global stream (Total: %d)", len(self._global_connections))

    def subscribe(self, websocket: WebSocket, game_id: str) -> None:
        """Subscribe a connected client to a specific game stream channel."""
        self._game_connections[game_id].add(websocket)
        self._client_subscriptions[websocket].add(game_id)

    def unsubscribe(self, websocket: WebSocket, game_id: str) -> None:
        """Unsubscribe a client from a specific game stream channel."""
        if game_id in self._game_connections:
            self._game_connections[game_id].discard(websocket)
            if not self._game_connections[game_id]:
                del self._game_connections[game_id]
        if websocket in self._client_subscriptions:
            self._client_subscriptions[websocket].discard(game_id)

    def disconnect(self, websocket: WebSocket, game_id: str | None = None) -> None:
        """Remove WebSocket connection and prune all channel subscriptions."""
        self._global_connections.discard(websocket)

        if game_id:
            self.unsubscribe(websocket, game_id)
        else:
            # Clean up all subscribed channels for this client
            subscribed = set(self._client_subscriptions.get(websocket, set()))
            for gid in subscribed:
                self.unsubscribe(websocket, gid)
            if websocket in self._client_subscriptions:
                del self._client_subscriptions[websocket]

        logger.debug("Client disconnected (Remaining Global: %d)", len(self._global_connections))

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
            self.disconnect(ws)

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

    def get_channel_stats(self) -> list[LiveChannelStatsDTO]:
        """Return structured channel statistics."""
        stats: list[LiveChannelStatsDTO] = []
        for gid, conns in self._game_connections.items():
            stats.append(LiveChannelStatsDTO(channel_id=gid, active_viewers=len(conns)))
        return stats

    def prune_dead_connections(self) -> int:
        """Scan and remove closed or defunct client connections."""
        pruned = 0
        for ws in list(self._global_connections):
            if getattr(ws, "client_state", None) is not None and getattr(ws.client_state, "name", "") == "DISCONNECTED":
                self.disconnect(ws)
                pruned += 1
        return pruned


# Shared singleton connection manager
ws_manager = ConnectionManager()
