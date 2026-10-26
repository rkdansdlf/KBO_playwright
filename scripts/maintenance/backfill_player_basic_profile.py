"""Backfill missing ``player_basic.team`` / ``player_basic.position`` from local game data.

``player_basic`` is the player registry skeleton used by the RAG index. 85% of its
rows carry neither a team nor a position, so every generated profile chunk degrades
to ``선수: <이름>`` plus a status marker. The data needed to fix most of them is
already in the warehouse -- no network access is required:

* ``team``     <- newest ``player_season_batting.team_code``, then
                  ``player_season_pitching.team_code``, then newest
                  ``game_lineups.team_code``.
* ``position`` <- the player's most frequent fielding position across
                  ``game_lineups`` (values there are era notation such as
                  ``중좌`` or ``주二``, so a priority parse is applied), falling
                  back to ``투수`` when the player only appears as a pitcher.

Every candidate value is validated against ``teams.team_id`` before it is written
and only NULL columns are filled, so an existing value is never overwritten. The
era-specific codes that already exist (``HT``, ``OB``, ``SK`` ...) can additionally
be rewritten to the modern canonical code with ``--normalize-existing``; that is a
separate decision because ``player_id_resolver`` and ``kbo_entity_resolver`` read
this column with different expectations.

Dry-run is the default; ``--apply`` performs the write.
"""

from __future__ import annotations

import argparse
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime

from sqlalchemy import text
from sqlalchemy.orm import Session

from src.db.engine import SessionLocal
from src.utils.team_history import canonical_code_for_team_code

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

#: ``game_lineups.position`` uses era notation: a single character per position,
#: concatenated when a player moved during the game (``중좌`` = CF then LF).
POSITION_MAP: dict[str, str] = {
    "一": "1루수",
    "二": "2루수",
    "三": "3루수",
    "유": "유격수",
    "포": "포수",
    "중": "중견수",
    "좌": "좌익수",
    "우": "우익수",
    "지": "지명타자",
    "투": "투수",
}

#: Real fielding positions, ordered by how strongly they identify a player.
FIELDING_CHARS: tuple[str, ...] = ("一", "二", "三", "유", "포", "중", "좌", "우")
DESIGNATED_HITTER_CHAR = "지"
PITCHER_CHAR = "투"

#: Marks that do not describe a fielding position: ``타`` pinch hitter, ``주``
#: pinch runner. A player who only ever appears this way gets no position.
NON_POSITION_CHARS = frozenset({"타", "주"})

#: A player with this many plate appearances in any season is a hitter, not a
#: pitcher. Without this guard, two-way or pre-lineup era hitters that merely
#: recorded a pitching appearance (``김성한``: 5,331 PA, 1986 pitching season)
#: would be labelled ``투수`` -- and they have no lineup rows at all, so nothing
#: else in the warehouse would correct it.
MIN_BATTING_PA_FOR_HITTER = 10

#: ``teams`` also carries non-club entries: the two all-star squads (``EA`` 나눔,
#: ``WE`` 드림) and ``OT``, a catch-all for 경찰/상무/대학/독립 구단. They are valid
#: ``team_id`` values but they are not a player's club, so they are rejected as
#: candidates and the cascade continues to an older season or another source.
PSEUDO_TEAM_CODES = frozenset({"EA", "WE", "OT"})

#: Evidence is read newest-first and the first hit per player wins. ``DISTINCT ON``
#: is PostgreSQL-only, so the deduplication happens in Python and these statements
#: stay portable enough to exercise against SQLite in tests.
TEAM_EVIDENCE_SQL: tuple[tuple[str, str], ...] = (
    (
        "player_season_batting",
        """
        SELECT l.player_id, l.team_code
        FROM player_season_batting l
        JOIN player_basic p ON p.player_id = l.player_id
        WHERE p.team IS NULL AND l.player_id IS NOT NULL AND l.team_code IS NOT NULL
        ORDER BY l.player_id, l.season DESC, l.id DESC
        """,
    ),
    (
        "player_season_pitching",
        """
        SELECT l.player_id, l.team_code
        FROM player_season_pitching l
        JOIN player_basic p ON p.player_id = l.player_id
        WHERE p.team IS NULL AND l.player_id IS NOT NULL AND l.team_code IS NOT NULL
        ORDER BY l.player_id, l.season DESC, l.id DESC
        """,
    ),
    (
        "game_lineups",
        """
        SELECT l.player_id, l.team_code
        FROM game_lineups l
        JOIN player_basic p ON p.player_id = l.player_id
        WHERE p.team IS NULL AND l.player_id IS NOT NULL AND l.team_code IS NOT NULL
        ORDER BY l.player_id, l.game_id DESC, l.id DESC
        """,
    ),
)


def resolve_lineup_position(raw: str) -> str | None:
    """Map one era-notation lineup position to a Korean position name.

    Args:
        raw: Raw value from ``game_lineups.position`` (for example ``중좌``, ``주二``, ``3루수``).

    Returns:
        The Korean position name, or ``None`` when the value carries no fielding
        position (pinch hitter / pinch runner only).

    """
    value = (raw or "").strip()
    if not value:
        return None
    if value in POSITION_MAP.values():
        # A handful of rows already use the modern full name.
        return value

    for char in value:
        if char in FIELDING_CHARS:
            return POSITION_MAP[char]
    if DESIGNATED_HITTER_CHAR in value:
        return POSITION_MAP[DESIGNATED_HITTER_CHAR]
    if PITCHER_CHAR in value:
        return POSITION_MAP[PITCHER_CHAR]
    if all(char in NON_POSITION_CHARS for char in value):
        return None
    return None


@dataclass(slots=True)
class BackfillReport:
    """Summarise what the backfill selected and wrote."""

    players: int = 0
    team_missing: int = 0
    position_missing: int = 0
    team_candidates: int = 0
    position_candidates: int = 0
    team_written: int = 0
    position_written: int = 0
    existing_team_normalized: int = 0
    team_source: Counter[str] = field(default_factory=Counter)
    position_source: Counter[str] = field(default_factory=Counter)
    samples: list[str] = field(default_factory=list)


def _load_team_ids(session: Session) -> set[str]:
    ids = {row[0] for row in session.execute(text("SELECT team_id FROM teams")).fetchall()}
    return ids - PSEUDO_TEAM_CODES


def _load_totals(session: Session) -> tuple[int, int, int]:
    row = session.execute(
        text(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE team IS NULL),
                   count(*) FILTER (WHERE position IS NULL)
            FROM player_basic
            """
        )
    ).fetchone()
    return int(row[0]), int(row[1]), int(row[2])


def _load_team_candidates(session: Session, valid_team_ids: set[str]) -> dict[int, tuple[str, str]]:
    """Resolve a team code per player from the newest local evidence.

    Returns:
        ``player_id -> (team_code, evidence_source)``.

    """
    candidates: dict[int, tuple[str, str]] = {}
    for source, sql in TEAM_EVIDENCE_SQL:
        for player_id, team_code in session.execute(text(sql)).fetchall():
            if player_id in candidates:
                continue
            code = (team_code or "").strip()
            if code not in valid_team_ids:
                continue
            candidates[int(player_id)] = (code, source)
    return candidates


def _load_position_candidates(session: Session) -> dict[int, tuple[str, str]]:
    """Resolve a position per player from lineup appearances and pitching stats.

    Returns:
        ``player_id -> (position, evidence_source)``.

    """
    counts: dict[int, Counter[str]] = defaultdict(Counter)
    rows = session.execute(
        text(
            """
            SELECT l.player_id, l.position, count(*) AS n
            FROM game_lineups l
            JOIN player_basic p ON p.player_id = l.player_id
            WHERE p.position IS NULL
              AND l.position IS NOT NULL
              AND l.position <> 'UNKNOWN'
            GROUP BY l.player_id, l.position
            """
        )
    ).fetchall()
    for player_id, raw_position, n in rows:
        resolved = resolve_lineup_position(raw_position)
        if resolved is None:
            continue
        counts[int(player_id)][resolved] += int(n)

    positions: dict[int, tuple[str, str]] = {}
    for player_id, counter in counts.items():
        top, _ = counter.most_common(1)[0]
        positions[player_id] = (top, "game_lineups")

    pitcher_rows = session.execute(
        text(
            """
            SELECT DISTINCT p.player_id
            FROM player_basic p
            JOIN player_season_pitching s ON s.player_id = p.player_id
            WHERE p.position IS NULL
              AND NOT EXISTS (
                  SELECT 1 FROM player_season_batting b
                  WHERE b.player_id = p.player_id
                    AND COALESCE(b.plate_appearances, 0) >= :min_pa
              )
            """
        ),
        {"min_pa": MIN_BATTING_PA_FOR_HITTER},
    ).fetchall()
    for (player_id,) in pitcher_rows:
        positions.setdefault(int(player_id), ("투수", "player_season_pitching"))
    return positions


def _record_candidates(
    report: BackfillReport,
    team_targets: dict[int, tuple[str, str]],
    position_targets: dict[int, tuple[str, str]],
) -> None:
    report.team_candidates = len(team_targets)
    report.position_candidates = len(position_targets)
    for _, source in team_targets.values():
        report.team_source[source] += 1
    for _, source in position_targets.values():
        report.position_source[source] += 1
    report.samples = [f"team  {pid} -> {code} ({source})" for pid, (code, source) in list(team_targets.items())[:3]] + [
        f"pos   {pid} -> {pos} ({source})" for pid, (pos, source) in list(position_targets.items())[:3]
    ]


def _log_dry_run(report: BackfillReport) -> None:
    logger.info(
        "dry-run | team 후보=%s (미결 %s) · position 후보=%s (미결 %s)",
        report.team_candidates,
        report.team_missing,
        report.position_candidates,
        report.position_missing,
    )
    for source, count in report.team_source.items():
        logger.info("  team 근거 %s: %s", source, count)
    for source, count in report.position_source.items():
        logger.info("  position 근거 %s: %s", source, count)
    for line in report.samples:
        logger.info("  %s", line)


def _apply_team(session: Session, targets: dict[int, tuple[str, str]], updated_at: datetime) -> int:
    rows = [{"pid": pid, "team": code, "ts": updated_at} for pid, (code, _) in targets.items()]
    if not rows:
        return 0
    result = session.execute(
        text("UPDATE player_basic SET team = :team, updated_at = :ts WHERE player_id = :pid AND team IS NULL"),
        rows,
    )
    return result.rowcount or 0


def _apply_position(session: Session, targets: dict[int, tuple[str, str]], updated_at: datetime) -> int:
    rows = [{"pid": pid, "position": pos, "ts": updated_at} for pid, (pos, _) in targets.items()]
    if not rows:
        return 0
    result = session.execute(
        text(
            "UPDATE player_basic SET position = :position, updated_at = :ts WHERE player_id = :pid AND position IS NULL"
        ),
        rows,
    )
    return result.rowcount or 0


def _normalize_existing(session: Session, valid_team_ids: set[str], updated_at: datetime) -> int:
    existing = session.execute(text("SELECT player_id, team FROM player_basic WHERE team IS NOT NULL")).fetchall()
    changes = []
    for player_id, current in existing:
        canonical = canonical_code_for_team_code(str(current))
        if canonical and canonical != current and canonical in valid_team_ids:
            changes.append({"pid": int(player_id), "team": canonical, "ts": updated_at})
    if not changes:
        return 0
    session.execute(
        text("UPDATE player_basic SET team = :team, updated_at = :ts WHERE player_id = :pid"),
        changes,
    )
    return len(changes)


def run_backfill(*, apply: bool = False, limit: int | None = None, normalize_existing: bool = False) -> BackfillReport:
    """Execute the ``player_basic`` profile backfill.

    Args:
        apply: Write the resolved values. When ``False`` only the report is produced.
        limit: Maximum number of players to write per column (staged application).
        normalize_existing: Rewrite era-specific ``team`` codes to modern canonical codes.

    Returns:
        The report describing what was selected and written.

    """
    report = BackfillReport()
    # ``TimestampMixin.updated_at`` is a naive ``DateTime``; raw SQL bypasses the
    # ORM ``onupdate`` hook, so the value is supplied explicitly and portably.
    updated_at = datetime.now(UTC).replace(tzinfo=None)

    with SessionLocal() as session:
        valid_team_ids = _load_team_ids(session)
        report.players, report.team_missing, report.position_missing = _load_totals(session)

        team_targets = _load_team_candidates(session, valid_team_ids)
        position_targets = _load_position_candidates(session)
        _record_candidates(report, team_targets, position_targets)

        if limit is not None:
            team_targets = dict(list(team_targets.items())[:limit])
            position_targets = dict(list(position_targets.items())[:limit])

        if not apply:
            _log_dry_run(report)
            return report

        report.team_written = _apply_team(session, team_targets, updated_at)
        report.position_written = _apply_position(session, position_targets, updated_at)
        if normalize_existing:
            report.existing_team_normalized = _normalize_existing(session, valid_team_ids, updated_at)

        session.commit()
        logger.info(
            "applied | team %s/%s · position %s/%s · 기존 team 정규화 %s",
            report.team_written,
            report.team_candidates,
            report.position_written,
            report.position_candidates,
            report.existing_team_normalized,
        )
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="실제로 기록합니다 (기본은 dry-run)")
    parser.add_argument("--limit", type=int, default=None, help="컬럼별 최대 기록 행 수 (단계 적용)")
    parser.add_argument(
        "--normalize-existing",
        action="store_true",
        help="기존 team 코드(HT/OB/SK 등)를 현대 정규 코드로 재작성합니다",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    report = run_backfill(apply=args.apply, limit=args.limit, normalize_existing=args.normalize_existing)
    logger.info(
        "요약 | players=%s team미결=%s position미결=%s team후보=%s position후보=%s",
        report.players,
        report.team_missing,
        report.position_missing,
        report.team_candidates,
        report.position_candidates,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
