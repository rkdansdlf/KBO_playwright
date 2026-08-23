"""Classify season team-code gaps without mutating source data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import text

if TYPE_CHECKING:
    from sqlalchemy.engine import Connection
    from sqlalchemy.orm import Session

ALL_STAR_TEAM_CODES = frozenset({"EA", "WE"})
OFFICIAL_ARCHIVE_SOURCE = "OFFICIAL_ARCHIVE"
OFFICIAL_ARCHIVE_SEASON = 1982
EXPECTED_AUDIT_ROWS = 2

_SEASON_TEAM_CODE_AUDIT_QUERY = """
SELECT
    COUNT(*) AS missing_count,
    COALESCE(SUM(CASE
        WHEN ps.source = :archive_source AND ps.season = :archive_season THEN 1
        ELSE 0
    END), 0) AS archive_count,
    COALESCE(SUM(CASE
        WHEN NOT (ps.source = :archive_source AND ps.season = :archive_season)
         AND EXISTS (
             SELECT 1
             FROM player_game_batting AS pg
             WHERE pg.player_id = ps.player_id
               AND SUBSTR(pg.game_id, 1, 4) = CAST(ps.season AS TEXT)
               AND TRIM(pg.team_code) IN ('EA', 'WE')
         )
         AND NOT EXISTS (
             SELECT 1
             FROM player_game_batting AS pg
             WHERE pg.player_id = ps.player_id
               AND SUBSTR(pg.game_id, 1, 4) = CAST(ps.season AS TEXT)
               AND COALESCE(TRIM(pg.team_code), '') <> ''
               AND TRIM(pg.team_code) NOT IN ('EA', 'WE')
         ) THEN 1
        ELSE 0
    END), 0) AS all_star_count
FROM player_season_batting AS ps
WHERE COALESCE(TRIM(ps.team_code), '') = ''
  AND (:audit_season IS NULL OR ps.season = :audit_season)
UNION ALL
SELECT
    COUNT(*) AS missing_count,
    COALESCE(SUM(CASE
        WHEN ps.source = :archive_source AND ps.season = :archive_season THEN 1
        ELSE 0
    END), 0) AS archive_count,
    COALESCE(SUM(CASE
        WHEN NOT (ps.source = :archive_source AND ps.season = :archive_season)
         AND EXISTS (
             SELECT 1
             FROM player_game_pitching AS pg
             WHERE pg.player_id = ps.player_id
               AND SUBSTR(pg.game_id, 1, 4) = CAST(ps.season AS TEXT)
               AND TRIM(pg.team_code) IN ('EA', 'WE')
         )
         AND NOT EXISTS (
             SELECT 1
             FROM player_game_pitching AS pg
             WHERE pg.player_id = ps.player_id
               AND SUBSTR(pg.game_id, 1, 4) = CAST(ps.season AS TEXT)
               AND COALESCE(TRIM(pg.team_code), '') <> ''
               AND TRIM(pg.team_code) NOT IN ('EA', 'WE')
         ) THEN 1
        ELSE 0
    END), 0) AS all_star_count
FROM player_season_pitching AS ps
WHERE COALESCE(TRIM(ps.team_code), '') = ''
  AND (:audit_season IS NULL OR ps.season = :audit_season)
"""


@dataclass(frozen=True, slots=True)
class SeasonTeamCodeAudit:
    """Summarize missing and accepted source-limited season team codes."""

    batting_missing: int
    batting_archive: int
    batting_all_star: int
    pitching_missing: int
    pitching_archive: int
    pitching_all_star: int

    @property
    def batting_source_limited(self) -> int:
        """Return batting rows accepted as source-limited."""
        return self.batting_archive + self.batting_all_star

    @property
    def pitching_source_limited(self) -> int:
        """Return pitching rows accepted as source-limited."""
        return self.pitching_archive + self.pitching_all_star

    @property
    def total_missing(self) -> int:
        """Return all rows without a usable team code."""
        return self.batting_missing + self.pitching_missing

    @property
    def total_source_limited(self) -> int:
        """Return all accepted source-limited rows."""
        return self.batting_source_limited + self.pitching_source_limited

    @property
    def total_unresolved(self) -> int:
        """Return rows that remain unexplained after source classification."""
        return self.total_missing - self.total_source_limited

    @property
    def batting_unresolved(self) -> int:
        """Return unresolved batting rows."""
        return self.batting_missing - self.batting_source_limited

    @property
    def pitching_unresolved(self) -> int:
        """Return unresolved pitching rows."""
        return self.pitching_missing - self.pitching_source_limited


def _row_counts(row: object) -> tuple[int, int, int]:
    """Convert one aggregate SQL row to integer counts."""
    return tuple(int(row[index] or 0) for index in range(3))  # type: ignore[index]


def audit_season_team_codes(
    session: Session | Connection,
    *,
    season: int | None = None,
) -> SeasonTeamCodeAudit:
    """Count unresolved season team codes and known source-limited rows."""
    rows = session.execute(
        text(_SEASON_TEAM_CODE_AUDIT_QUERY),
        {
            "archive_source": OFFICIAL_ARCHIVE_SOURCE,
            "archive_season": OFFICIAL_ARCHIVE_SEASON,
            "audit_season": season,
        },
    ).fetchall()
    if len(rows) != EXPECTED_AUDIT_ROWS:
        msg = f"Expected batting and pitching audit rows, got {len(rows)}"
        raise ValueError(msg)

    batting_missing, batting_archive, batting_all_star = _row_counts(rows[0])
    pitching_missing, pitching_archive, pitching_all_star = _row_counts(rows[1])
    return SeasonTeamCodeAudit(
        batting_missing=batting_missing,
        batting_archive=batting_archive,
        batting_all_star=batting_all_star,
        pitching_missing=pitching_missing,
        pitching_archive=pitching_archive,
        pitching_all_star=pitching_all_star,
    )
