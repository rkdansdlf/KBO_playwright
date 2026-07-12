"""Deduplication utility for play-by-play text relay events."""

from __future__ import annotations

import hashlib
from typing import Any


class RelayDeduplicator:
    """Sliding-window deduplication queue for incremental streaming.

    Tracks previously processed event identifiers/hashes to filter out duplicates
    across consecutive crawls of the same game.
    """

    def __init__(self, window_size: int = 200) -> None:
        """Initialize the deduplicator with a target window size.

        Args:
            window_size: Maximum number of historical event IDs to keep in memory.

        """
        self.window_size = window_size
        self._seen_ids: list[str] = []

    def filter_new_events(
        self,
        events: list[dict[str, Any]],
        key_field: str = "provider_log_id",
    ) -> list[dict[str, Any]]:
        """Filter a list of events to return only those that haven't been seen yet.

        Args:
            events: List of event dictionaries to filter.
            key_field: Dictionary key to use as the unique identifier.

        Returns:
            Filtered list containing only new events.

        """
        new_events = []
        for event in events:
            event_id = event.get(key_field)
            if not event_id:
                # Compute a fallback hash if key_field is missing or empty
                desc = str(event.get("play_description") or event.get("text") or "")
                inn = str(event.get("inning") or "")
                seq = str(event.get("source_row_index") or "")
                raw = f"{inn}:{seq}:{desc}"
                event_id = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]

            if event_id not in self._seen_ids:
                new_events.append(event)
                self._seen_ids.append(event_id)
                # Keep window size bounded
                if len(self._seen_ids) > self.window_size:
                    self._seen_ids.pop(0)

        return new_events

    def reset(self) -> None:
        """Clear the deduplication cache."""
        self._seen_ids.clear()
