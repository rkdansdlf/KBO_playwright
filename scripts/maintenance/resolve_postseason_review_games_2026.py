"""Resolve 2026 postseason and all-star boundary games held in review.

These 10 games were left unchanged by repair_season_classification because they
lie inside postseason windows or have boundary/placeholder conditions.

This script assigns their verified, ground-truth canonical season_ids:
- 20260711WEEA0: 313 (All-Star Game)
- 20261014OBLG0, 20261015OBLG0: 202602 (Wildcard Game 1 & cancelled Game 2)
- 20261017LGHT0, 20261018LGHT0, 20261020HTLG0, 20261023LGHT0: 202603 (Semi-Playoff games)
- 20261025HTSS0, 20261026HTSS0, 20261031HTSS0: 202604 (Playoff scheduled games)

Usage:
    python -m scripts.maintenance.resolve_postseason_review_games_2026
    python -m scripts.maintenance.resolve_postseason_review_games_2026 --apply
    python -m scripts.maintenance.resolve_postseason_review_games_2026 --rollback <backup.json>
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import logging
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import select

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game

logger = logging.getLogger(__name__)

# Ground-truth verified resolutions for the 10 review games
VERIFIED_RESOLUTIONS: dict[str, tuple[int, str]] = {
    "20260711WEEA0": (313, "2026 All-Star Game (올스타전)"),
    "20261014OBLG0": (202602, "2026 Wildcard Game 1 (와일드카드 1차전)"),
    "20261015OBLG0": (202602, "2026 Wildcard Game 2 placeholder (와일드카드 2차전 취소)"),
    "20261017LGHT0": (202603, "2026 준플레이오프 1차전 (LG vs KIA)"),
    "20261018LGHT0": (202603, "2026 준플레이오프 2차전 (LG vs KIA)"),
    "20261020HTLG0": (202603, "2026 준플레이오프 3차전 placeholder (취소)"),
    "20261023LGHT0": (202603, "2026 준플레이오프 5차전 placeholder (예정)"),
    "20261025HTSS0": (202604, "2026 플레이오프 1차전 (KIA vs 삼성 예정)"),
    "20261026HTSS0": (202604, "2026 플레이오프 2차전 (KIA vs 삼성 예정)"),
    "20261031HTSS0": (202604, "2026 플레이오프 5차전 (KIA vs 삼성 예정)"),
}

DEFAULT_BACKUP_DIR = Path("data/repair_backups")


def plan_resolutions(session: Any) -> list[dict[str, Any]]:
    """Inspect the database and plan changes for the 10 target games."""
    changes: list[dict[str, Any]] = []
    for game_id, (target_season_id, reason) in sorted(VERIFIED_RESOLUTIONS.items()):
        game = session.execute(select(Game).where(Game.game_id == game_id)).scalar_one_or_none()
        if game is None:
            logger.warning("Game %s not found in database", game_id)
            continue
        old_season_id = game.season_id
        if old_season_id != target_season_id:
            changes.append(
                {
                    "game_id": game_id,
                    "game_date": game.game_date.isoformat(),
                    "game_status": game.game_status,
                    "old_season_id": old_season_id,
                    "new_season_id": target_season_id,
                    "reason": reason,
                }
            )
    return changes


def apply_resolutions(session: Any, changes: list[dict[str, Any]]) -> None:
    """Apply planned changes to the games."""
    for change in changes:
        game = session.execute(select(Game).where(Game.game_id == change["game_id"])).scalar_one()
        game.season_id = change["new_season_id"]


def rollback_resolutions(session: Any, backup: dict[str, Any]) -> int:
    """Restore old season_ids from a backup payload."""
    restored = 0
    for change in backup.get("changes", []):
        game = session.execute(select(Game).where(Game.game_id == change["game_id"])).scalar_one_or_none()
        if game and game.season_id == change["new_season_id"]:
            game.season_id = change["old_season_id"]
            restored += 1
    return restored


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Resolve 2026 postseason review games")
    parser.add_argument("--apply", action="store_true", help="Apply resolutions to database")
    parser.add_argument("--rollback", type=Path, help="Roll back from backup JSON file")
    parser.add_argument("--backup-dir", type=Path, default=DEFAULT_BACKUP_DIR, help="Backup output directory")
    args = parser.parse_args(argv)

    with SessionLocal() as session:
        if args.rollback:
            if not args.rollback.is_file():
                print(f"Error: backup file not found: {args.rollback}", file=sys.stderr)
                return 1
            backup = json.loads(args.rollback.read_text(encoding="utf-8"))
            if not args.apply:
                print(f"Dry run: would roll back {len(backup.get('changes', []))} games from {args.rollback}")
                print("Pass --apply to execute rollback.")
                return 0
            restored = rollback_resolutions(session, backup)
            session.commit()
            print(f"Rolled back {restored} games successfully.")
            return 0

        changes = plan_resolutions(session)
        print(f"== 2026 Postseason Review Games Resolution Plan: {len(changes)} games ==")
        for c in changes:
            print(
                f"  {c['game_id']} ({c['game_date']}, {c['game_status']}): "
                f"season_id {c['old_season_id']} -> {c['new_season_id']} ({c['reason']})"
            )

        if not changes:
            print("All 10 target games already hold their verified season_id. Nothing to do.")
            return 0

        if not args.apply:
            print("\nDry run: pass --apply to execute updates.")
            return 0

        args.backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(KST).strftime("%Y%m%dT%H%M%S")
        backup_path = args.backup_dir / f"resolve_review_games_2026_{stamp}.json"
        payload = {
            "kind": "resolve_review_games_2026",
            "created_at_kst": datetime.now(KST).isoformat(),
            "changes": changes,
        }
        backup_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Backup written to: {backup_path}")

        apply_resolutions(session, changes)
        session.commit()
        print(f"Applied resolutions to {len(changes)} games.")
        print("Next step: re-run repair_season_classification --year 2026 --apply")
        return 0


if __name__ == "__main__":
    sys.exit(main())
