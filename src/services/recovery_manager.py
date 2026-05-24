"""
Recovery Manager to handle checkpoints for large-scale data repair jobs.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Set


class RecoveryManager:
    def __init__(self, checkpoint_path: str = "data/recovery/healer_checkpoint.json"):
        self.path = Path(checkpoint_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.state: Dict[str, Any] = {
            "run_id": None,
            "total_count": 0,
            "completed": [],
            "failed": {},
            "pending": []
        }
        self.load()

    def load(self) -> None:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self.state.update(json.load(f))
            except Exception:
                pass

    def save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self.state, f, indent=2, ensure_ascii=False)

    def initialize_run(self, run_id: str, targets: List[str]) -> None:
        # If it's a new run ID, reset everything
        if self.state.get("run_id") != run_id:
            self.state = {
                "run_id": run_id,
                "total_count": len(targets),
                "completed": [],
                "failed": {},
                "pending": targets
            }
            self.save()

    def mark_completed(self, game_id: str) -> None:
        if game_id not in self.state["completed"]:
            self.state["completed"].append(game_id)
        if game_id in self.state["pending"]:
            self.state["pending"].remove(game_id)
        self.save()

    def mark_failed(self, game_id: str, reason: str) -> None:
        self.state["failed"][game_id] = reason
        if game_id in self.state["pending"]:
            self.state["pending"].remove(game_id)
        self.save()

    def get_pending_targets(self) -> List[str]:
        return self.state.get("pending", [])

    def clear(self) -> None:
        if self.path.exists():
            self.path.unlink()
        self.state = {"run_id": None, "total_count": 0, "completed": [], "failed": {}, "pending": []}
