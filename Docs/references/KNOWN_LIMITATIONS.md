# Known Data Limitations

Last updated: 2026-06-30

This document tracks known data quality issues and their current status.

---

## Summary

| Category | Status | Coverage |
|----------|--------|----------|
| Game season_id | ✅ Resolved | 100% (0 orphan) |
| Game team codes | ✅ Resolved | 100% (0 legacy) |
| game_metadata stadium_code | ✅ Resolved | 100% (0 NULL) |
| player_season team_code | ⚠️ Known gap | 94% (1,248 NULL) |

---

## player_season NULL team_code (1,248 rows, 6%)

**Status**: Known gap, acceptable for analysis

**Affected**: 620 distinct players across 2010-2026

**Root cause**:
- These players have season-level batting records but no corresponding
  `player_game_batting` data for the same season
- Possible reasons:
  - Player was registered but did not actually play
  - Defensive/baserunning stats only (no plate appearances)
  - Data collected from season summary pages but not game-level detail pages

**Impact**: Minimal - these represent edge cases (players with very few games)

**Mitigation**: When aggregating player stats by team, filter out NULL team_code rows:
```sql
SELECT team_code, COUNT(*), SUM(games)
FROM player_season_batting
WHERE team_code IS NOT NULL
GROUP BY team_code;
```

**Monitoring**: Alert if NULL rate exceeds 10% via gap_report SEASON_TEAM_CODE check.

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
