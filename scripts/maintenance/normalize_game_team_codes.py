"""Rewrite legacy team codes on ``game`` rows to their season code (issue #39).

Some detail-crawl fallbacks stored the KBO GameCenter technical code of a team
(``HT``/``OB``/``SK``/``WO``) on ``game.home_team``/``away_team``/``winning_team``
instead of its season brand (``KIA``/``DB``/``SSG``/``KH``). From late June 2026
onward a growing share of KIA, 두산, SSG and 키움 games were stored that way. Every
consumer that groups by the stored string -- the standings calculator and the
public ``/api/kbo/rankings/snapshot`` query -- then sees one franchise as two
teams, and the snapshot's top-10 cut drops the legacy-coded half: KIA showed 106
games instead of 143, and league wins no longer equalled losses.

The write path now normalizes codes; this script repairs rows written before it.

Rules:

* The new code is :func:`src.utils.team_codes.season_team_code` for the game's
  calendar year, the same resolution child rows already use. Historical seasons
  keep their own brand (``HT`` in 1995 stays ``HT``).
* A column is only rewritten when old and new code name the same franchise.
  Codes no franchise claims (all-star, international) are never touched.
* Writing is opt-in. Without ``--apply`` the run only reports.
* ``--apply`` first writes a JSON backup of every changed row's old values;
  ``--rollback <backup.json>`` restores exactly those values.

Usage (PowerShell on the Windows server, from the repository root)::

    python -m scripts.maintenance.normalize_game_team_codes --year 2026
    python -m scripts.maintenance.normalize_game_team_codes --year 2026 --apply
    python -m scripts.maintenance.normalize_game_team_codes --rollback logs/game_team_code_backup_2026_<ts>.json

After ``--apply`` recompute the derived standings:
``python -m src.cli.calc.calculate_standings --year 2026``.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import Counter
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from sqlalchemy import extract

from src.constants import KST
from src.db.engine import SessionLocal
from src.models.game import Game
from src.utils.team_codes import season_team_code
from src.utils.team_history import franchise_id_for_team_code

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

TEAM_CODE_COLUMNS = ("home_team", "away_team", "winning_team")
DEFAULT_BACKUP_DIR = Path("logs")


@dataclass(frozen=True)
class TeamCodeChange:
    """One column of one game row that should change."""

    game_id: str
    column: str
    old: str
    new: str


@dataclass
class NormalizeReport:
    """Summary of a normalization run."""

    year: int
    scanned: int = 0
    changes: list[TeamCodeChange] = field(default_factory=list)
    backup_path: Path | None = None

    @property
    def games_changed(self) -> int:
        """Return how many distinct games have at least one changed column."""
        return len({change.game_id for change in self.changes})

    def render(self) -> str:
        """Return a human-readable summary."""
        by_pair = Counter((change.old, change.new) for change in self.changes)
        lines = [
            (
                f"year={self.year} scanned_games={self.scanned} "
                f"games_to_change={self.games_changed} columns_to_change={len(self.changes)}"
            ),
        ]
        lines.extend(f"  {old} -> {new}: {count}" for (old, new), count in sorted(by_pair.items()))
        if self.backup_path:
            lines.append(f"backup={self.backup_path}")
        return "\n".join(lines)


def _target_code(code: str | None, season_year: int) -> str | None:
    """Return the season code for ``code`` when it is a safe same-franchise rewrite."""
    if not code:
        return None
    new_code = season_team_code(code, season_year)
    if not new_code or new_code == code:
        return None
    # Season-free franchise of the stored token (HT -> KIA -> franchise 5) must
    # match the franchise of the season code, otherwise leave the row alone.
    old_franchise = franchise_id_for_team_code(season_team_code(code, None) or str(code))
    new_franchise = franchise_id_for_team_code(new_code, season_year)
    if new_franchise is None or old_franchise != new_franchise:
        return None
    return new_code


def plan_changes(session: Session, year: int) -> tuple[int, list[TeamCodeChange]]:
    """Return (scanned game count, column changes) for one calendar year."""
    games = (
        session.query(Game).filter(extract("year", Game.game_date) == year).order_by(Game.game_date, Game.game_id).all()
    )
    changes: list[TeamCodeChange] = []
    for game in games:
        for column in TEAM_CODE_COLUMNS:
            old = getattr(game, column)
            new = _target_code(old, year)
            if new:
                changes.append(TeamCodeChange(str(game.game_id), column, str(old), new))
    return len(games), changes


def _write_backup(changes: Sequence[TeamCodeChange], year: int, backup_dir: Path) -> Path:
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
    path = backup_dir / f"game_team_code_backup_{year}_{stamp}.json"
    payload = {"year": year, "created_at": datetime.now(KST).isoformat(), "changes": [asdict(c) for c in changes]}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _set_columns(session: Session, changes: Sequence[TeamCodeChange], *, use_old: bool) -> int:
    by_game: dict[str, list[TeamCodeChange]] = {}
    for change in changes:
        by_game.setdefault(change.game_id, []).append(change)
    updated = 0
    for game_id, game_changes in by_game.items():
        game = session.query(Game).filter(Game.game_id == game_id).one_or_none()
        if game is None:
            logger.warning("game %s no longer exists; skipped", game_id)
            continue
        for change in game_changes:
            current = getattr(game, change.column)
            if use_old:
                if current != change.new:
                    logger.warning(
                        "Conflict during rollback for %s.%s: current '%s' != expected '%s'; skipped",
                        game_id,
                        change.column,
                        current,
                        change.new,
                    )
                    continue
                setattr(game, change.column, change.old)
                updated += 1
            else:
                setattr(game, change.column, change.new)
                updated += 1
    return updated


def normalize_game_team_codes(
    year: int,
    *,
    apply: bool = False,
    backup_dir: Path = DEFAULT_BACKUP_DIR,
    session: Session | None = None,
) -> NormalizeReport:
    """Plan and optionally apply season-code normalization for one year."""
    report = NormalizeReport(year=year)
    own_session = session is None
    db = session or SessionLocal()
    try:
        report.scanned, report.changes = plan_changes(db, year)
        if apply and report.changes:
            report.backup_path = _write_backup(report.changes, year, backup_dir)
            _set_columns(db, report.changes, use_old=False)
            db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()
    return report


def rollback_from_backup(backup_path: Path, *, session: Session | None = None) -> int:
    """Restore the old values recorded in a backup file. Returns columns restored."""
    payload = json.loads(Path(backup_path).read_text(encoding="utf-8"))
    changes = [TeamCodeChange(**row) for row in payload.get("changes", [])]
    own_session = session is None
    db = session or SessionLocal()
    try:
        restored = _set_columns(db, changes, use_old=True)
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        if own_session:
            db.close()
    return restored


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI entrypoint."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Normalize legacy team codes on game rows (issue #39)")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--year", type=int, default=datetime.now(KST).year, help="Season year (default: current KST year)"
    )
    group.add_argument("--rollback", type=Path, help="Restore old values from a backup JSON written by --apply")
    parser.add_argument("--apply", action="store_true", help="Write the changes (default: dry-run)")
    parser.add_argument("--backup-dir", type=Path, default=DEFAULT_BACKUP_DIR, help="Where --apply writes its backup")
    args = parser.parse_args(argv)

    if args.rollback:
        restored = rollback_from_backup(args.rollback)
        print(f"[ROLLBACK] restored {restored} column value(s) from {args.rollback}")
        return 0

    report = normalize_game_team_codes(args.year, apply=args.apply, backup_dir=args.backup_dir)
    mode = "APPLIED" if args.apply else "DRY RUN (nothing written)"
    print(f"[{mode}]")
    print(report.render())
    if args.apply and report.changes:
        print(f"Next: python -m src.cli.calc.calculate_standings --year {args.year}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
