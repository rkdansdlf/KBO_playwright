"""Recovery Manager to handle checkpoints for large-scale data repair jobs."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from src.utils.team_codes import normalize_kbo_game_id

logger = logging.getLogger(__name__)


class RecoveryManager:
    """RecoveryManager class."""

    def __init__(self, checkpoint_path: str = "data/recovery/healer_checkpoint.json") -> None:
        """
        Initialize a new instance.

        Args:
            checkpoint_path: Checkpoint file path.
            checkpoint_path: Checkpoint file path.

        """
        self.path = Path(checkpoint_path)

        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state: dict[str, Any] = {
            "run_id": None,
            "total_count": 0,
            "completed": [],
            "failed": {},
            "pending": [],
            "detail_recovery_queue": {},
        }
        self.load()

    @staticmethod
    def _utc_now() -> datetime:
        return datetime.now(UTC)

    @staticmethod
    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not isinstance(value, str):
            return None
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed

    def _get_detail_recovery_queue(self) -> dict[str, dict[str, Any]]:
        queue = self.state.get("detail_recovery_queue")
        if not isinstance(queue, dict):
            queue = {}
            self.state["detail_recovery_queue"] = queue
        return queue

    @staticmethod
    def _detail_queue_key(target_date: str, game_id: str) -> str:
        return f"{target_date}:{normalize_kbo_game_id(game_id)}"

    @staticmethod
    def _split_detail_queue_key(key: str) -> tuple[str, str]:
        if ":" not in key:
            return "", key
        date_part, game_id = key.split(":", 1)
        return date_part, game_id

    def get_due_detail_recovery_targets(
        self,
        target_date: str,
        *,
        cooldown_minutes: int | None = None,
        now: datetime | None = None,
    ) -> list[str]:
        """
        Get due detail recovery targets.

        Args:
            target_date: Target date for the operation.
            cooldown_minutes: Cooldown Minutes.
            now: Now.
            target_date: Target date for the operation.
            cooldown_minutes: Cooldown Minutes.
            now: Now.
            target_date: Target Date.

        Returns:
            List of results.

        """
        queue = self._get_detail_recovery_queue()

        if not queue:
            return []

        now_dt = now or self._utc_now()
        cooldown = (
            timedelta(minutes=int(cooldown_minutes))
            if cooldown_minutes is not None and int(cooldown_minutes) > 0
            else None
        )
        game_ids: list[str] = []
        for key, data in queue.items():
            if not isinstance(key, str):
                continue
            queue_date, game_id = self._split_detail_queue_key(key)
            if queue_date != target_date or not game_id:
                continue
            if not isinstance(data, dict):
                continue
            if cooldown is not None:
                last_failed_at = self._parse_iso_datetime(data.get("last_failed_at"))
                if last_failed_at and (now_dt - last_failed_at) < cooldown:
                    continue
            game_ids.append(game_id)
        return sorted(set(game_ids))

    def mark_detail_recovery_success(self, target_date: str, game_id: str) -> None:
        """
        Handle the mark detail recovery success operation.

        Args:
            target_date: Target date for the operation.
            game_id: Game ID.
            target_date: Target date for the operation.
            game_id: Game ID.
            target_date: Target Date.
            game_id: Game ID.

        """
        queue = self._get_detail_recovery_queue()

        key = self._detail_queue_key(target_date, game_id)
        if key in queue:
            queue.pop(key, None)
            self.save()

    def mark_detail_recovery_failure(
        self,
        target_date: str,
        game_id: str,
        *,
        failure_reason: str | None = None,
    ) -> None:
        """
        Handle the mark detail recovery failure operation.

        Args:
            target_date: Target date for the operation.
            game_id: Game ID.
            failure_reason: Failure Reason.
            target_date: Target date for the operation.
            game_id: Game ID.
            failure_reason: Failure Reason.
            target_date: Target Date.
            game_id: Game ID.

        """
        queue = self._get_detail_recovery_queue()

        key = self._detail_queue_key(target_date, game_id)
        existing = queue.get(key)
        if not isinstance(existing, dict):
            existing = {}
        attempts = existing.get("attempts", 0) or 0
        entry = dict(existing)
        entry["target_date"] = target_date
        entry["game_id"] = normalize_kbo_game_id(game_id)
        entry["attempts"] = int(attempts) + 1
        if failure_reason:
            entry["reason"] = str(failure_reason)
        entry["last_failed_at"] = self._utc_now().isoformat()
        queue[key] = entry
        self.save()

    def purge_detail_recovery_queue(self, *, max_age_days: int = 7) -> None:
        """
        Purge detail recovery queue.

        Args:
            max_age_days: Max Age Days.
            max_age_days: Max Age Days.

        """
        queue = self._get_detail_recovery_queue()

        if not queue:
            return

        cutoff = self._utc_now() - timedelta(days=max(1, int(max_age_days)))
        changed = False
        for key in list(queue.keys()):
            data = queue.get(key)
            if not isinstance(data, dict):
                queue.pop(key, None)
                changed = True
                continue
            last_failed_at = self._parse_iso_datetime(data.get("last_failed_at"))
            if last_failed_at and last_failed_at < cutoff:
                queue.pop(key, None)
                changed = True
                continue
            if not isinstance(last_failed_at, datetime):
                queue.pop(key, None)
                changed = True
        if changed:
            self.save()

    def load(self) -> None:
        """Load load."""
        if self.path.exists():
            try:
                with self.path.open(encoding="utf-8") as f:
                    self.state.update(json.load(f))
            except (OSError, json.JSONDecodeError):
                logger.debug("No existing recovery state at %s", self.path)

    def save(self) -> None:
        """Save save."""
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def initialize_run(self, run_id: str, targets: list[str]) -> None:
        # If it's a new run ID, reset everything
        """
        Initialize initialize run.

        Args:
            run_id: Run ID.
            targets: Targets.
            run_id: Run ID.
            targets: Targets.
            run_id: Run ID.
            targets: Targets.

        """
        if self.state.get("run_id") != run_id:
            queue = self.state.get("detail_recovery_queue", {})
            self.state = {
                "run_id": run_id,
                "total_count": len(targets),
                "completed": [],
                "failed": {},
                "pending": targets,
                "detail_recovery_queue": queue if isinstance(queue, dict) else {},
            }
            self.save()

    def mark_completed(self, game_id: str) -> None:
        """
        Handle the mark completed operation.

        Args:
            game_id: Game ID.
            game_id: Game ID.
            game_id: Game ID.

        """
        if game_id not in self.state["completed"]:
            self.state["completed"].append(game_id)
        if game_id in self.state["pending"]:
            self.state["pending"].remove(game_id)
        self.save()

    def mark_failed(self, game_id: str, reason: str) -> None:
        """
        Handle the mark failed operation.

        Args:
            game_id: Game ID.
            reason: Reason.
            game_id: Game ID.
            reason: Reason.
            game_id: Game ID.
            reason: Reason.

        """
        self.state["failed"][game_id] = reason

        if game_id in self.state["pending"]:
            self.state["pending"].remove(game_id)
        self.save()

    def get_pending_targets(self) -> list[str]:
        """
        Get pending targets.

        Returns:
            List of results.

        """
        return self.state.get("pending", [])

    def clear(self) -> None:
        """Clear clear."""
        if self.path.exists():
            self.path.unlink()
        self.state = {
            "run_id": None,
            "total_count": 0,
            "completed": [],
            "failed": {},
            "pending": [],
            "detail_recovery_queue": {},
        }
