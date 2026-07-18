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
| player_season team_code | ⚠️ Known gap | 99.98% (2 NULL batting, 0 pitching) |

---

## player_season NULL team_code (2 batting rows remaining)

**Status**: Known gap, acceptable for analysis

**Affected**: 2 `player_season_batting` rows (both REGULAR/KBO1); `player_season_pitching` fully resolved.

**History**:
- Originally 1,248 NULL rows (6%, 620 players) across 2010-2026.
- A conservative, evidence-based backfill (`scripts/maintenance/backfill_season_team_codes.py`)
  resolved all rows with a single, unambiguous team code from
  `player_game_batting` / `player_game_pitching` (same season) → `team_daily_roster`
  (same year) → `player_basic.career`. It resolved **4 pitching rows** and left the
  remaining **2 batting rows** unresolved because they have no unique team evidence
  (one has no career text, the other appears on 6 distinct roster teams in that season).

**Root cause (residual 2 rows)**:
- These players have season-level batting records but no corresponding
  `player_game_batting` data for the same season, no `team_daily_roster` entry with a
  single team, and no parseable `player_basic.career`.
- The backfill intentionally **skips** ambiguous or evidence-less rows rather than
  inventing a team code.

**Impact**: Minimal - 2 edge-case rows out of ~19,600 batting rows.

**Mitigation**: When aggregating player stats by team, filter out NULL team_code rows:
```sql
SELECT team_code, COUNT(*), SUM(games)
FROM player_season_batting
WHERE team_code IS NOT NULL
GROUP BY team_code;
```

**Monitoring**: `gap_report` SEASON_TEAM_CODE check reports `ok = (batting_null == 0 and pitching_null == 0)`; the 2 residual batting rows keep this `False` by design. The threshold-based alert (NULL rate > 10%) remains aspirational/monitored out-of-band.

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
