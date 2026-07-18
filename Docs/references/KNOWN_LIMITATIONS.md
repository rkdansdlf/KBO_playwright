# Known Data Limitations

Last updated: 2026-07-19

This document tracks known data quality issues and their current status.

---

## Summary

| Category | Status | Coverage |
|----------|--------|----------|
| Game season_id | ✅ Resolved | 100% (0 orphan) |
| Game team codes | ✅ Resolved | 100% (0 legacy) |
| game_metadata stadium_code | ✅ Resolved | 100% (0 NULL) |
| player_season team_code | ⚠️ Known gap | 99.98% (2 NULL batting, 0 pitching; dry-run leaves 김택연 665/2025 without evidence and 이병헌 52204/2026 with conflicting roster evidence) |

---

## player_season NULL team_code (2 batting rows remaining)

**Status**: Known gap, acceptable for analysis

**Affected**: 2 `player_season_batting` rows (both REGULAR/KBO1); `player_season_pitching` fully resolved. The current dry-run resolves neither row: **김택연 (665, 2025)** has no team evidence, while **이병헌 (52204, 2026)** has conflicting same-season roster codes and is conservatively skipped.

**History**:
- Originally 1,248 NULL rows (6%, 620 players) across 2010-2026.
- A conservative, evidence-based backfill (`scripts/maintenance/backfill_season_team_codes.py`)
  resolved all rows with a single, unambiguous team code from
  `player_game_batting` / `player_game_pitching` (same season) → `team_daily_roster`
  (same year) → `player_basic.career` → `player_basic.team` (current team, last resort).
  It resolved **4 pitching rows** and left the remaining **2 batting rows** unresolved
  because they had no unique team evidence. The current dry-run makes no changes:
  **이병헌 (52204, 2026)** has conflicting roster evidence and **김택연 (665, 2025)**
  has neither a team nor career record (team=None, career=None).

**Root cause (residual gap)**:
- **김택연 (665, 2025)**: has a season-level batting record but no corresponding
  `player_game_batting` data for 2025, no `team_daily_roster` entry, an empty
  `player_basic.career`, and a NULL `player_basic.team`. There is no usable evidence,
  so the backfill (correctly) leaves it NULL.
- **이병헌 (52204, 2026)**: has no same-season game evidence, career text contains
  only school history, and roster evidence contains six team codes (`DB`, `KH`, `KIA`,
  `LT`, `NC`, `SSG`). The resolver correctly classifies this as ambiguous rather than
  applying the current `player_basic.team` value.
- The backfill intentionally **skips** ambiguous or evidence-less rows rather than
  inventing a team code.

**Impact**: Minimal - 2 edge-case rows out of ~19,600 batting rows. No rows were changed by the dry-run.

**Mitigation**: When aggregating player stats by team, filter out NULL team_code rows:
```sql
SELECT team_code, COUNT(*), SUM(games)
FROM player_season_batting
WHERE team_code IS NOT NULL
GROUP BY team_code;
```

**Monitoring**: `gap_report` SEASON_TEAM_CODE check reports `ok = (batting_null == 0 and pitching_null == 0)`. The current dry-run leaves both batting rows NULL, so the check remains `False` by design. Do not use `--apply` until the conflicting roster evidence for 이병헌 (52204, 2026) is reviewed. The threshold-based alert (NULL rate > 10%) remains aspirational/monitored out-of-band.

---

## Historical Data Coverage (2001-2009)

**Status**: Incomplete but usable

**Issue**: Years 2001-2009 contain 126-246 games/year (normal: 600-900)

**Root cause**: Initial crawling scope was limited to regular season games

**Resolution**: These years provide partial data suitable for historical trend analysis
but should not be used for complete statistical calculations.

---

## Team Code Normalization

**Status**: Completed 2026-06-30

**Mapping**: All legacy codes now canonical:
- OB, DO → DB (두산)
- SK → SSG
- HT → KIA
- WO, NX, KI → KH (키움)
- BE, HE → HH (한화)
- MBC → LG
- SM, CB, TP → HU (현대, historical)

**Reference**: `team_code_map` table (459 entries, complete since 2026-06-28)

---

## game_metadata stadium_code

**Status**: Completed 2026-06-30

**Coverage**: 12,133/12,133 (100%)

**Method**:
- 9,508 rows from OCI hydration
- 2,453 rows inferred from team modal stadium mapping
- 8 remaining 2020 HH games manually backfilled
