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
| player_season team_code | ⚠️ 1 accepted residual | 99.99% (1 NULL batting = 김택연 665/2025, accepted; 이병헌 52204/2026 resolves to DB via player_basic.team evidence) |

---

## player_season NULL team_code (1 accepted residual)

**Status**: 1 intentional residual (김택연 665/2025); 이병헌 52204/2026 resolvable

**Affected**: After the backfill enhancement (`feat: player_season 팀코드 백필에 player_basic.team 최후 증거 추가`, commit `1b0ee693`), `player_season_pitching` is fully resolved and `player_season_batting` has a single remaining NULL: **김택연 (665, 2025)**, which has no team evidence of any kind and is accepted as an intentional residual.

**History**:
- Originally 1,248 NULL rows (6%, 620 players) across 2010-2026.
- A conservative, evidence-based backfill (`scripts/maintenance/backfill_season_team_codes.py`)
  resolved all rows with a single, unambiguous team code from
  `player_game_batting` / `player_game_pitching` (same season) → `team_daily_roster`
  (same year) → `player_basic.career` → `player_basic.team` (current team, last resort).
- The backfill previously left 2 batting rows unresolved: **김택연 (665, 2025)** had no
  evidence at all, and **이병헌 (52204, 2026)** had conflicting same-season roster codes.
  The `_resolve_from_player_team` helper (commit `1b0ee693`) now treats a populated
  `player_basic.team` (normalized via `FULL_TEAM_MAP`) as a last-resort, non-ambiguous
  evidence: 이병헌's `player_basic.team='두산'` yields `DB`, so running `--apply` resolves
  it. 김택연 has `team=None` and `career=None`, so no evidence exists and it stays NULL.

**Root cause (residual gap)**:
- **김택연 (665, 2025)**: has a season-level batting record but no corresponding
  `player_game_batting` data for 2025, no `team_daily_roster` entry, an empty
  `player_basic.career`, and a NULL `player_basic.team`. There is genuinely no usable
  evidence, so the row is **accepted as an intentional residual** rather than force-assigned.
- The backfill intentionally **skips** ambiguous or evidence-less rows rather than
  inventing a team code. 김택연 is the sole remaining such row.

**Impact**: Minimal - 1 edge-case row out of ~19,600 batting rows.

**Mitigation**: When aggregating player stats by team, filter out NULL team_code rows:
```sql
SELECT team_code, COUNT(*), SUM(games)
FROM player_season_batting
WHERE team_code IS NOT NULL
GROUP BY team_code;
```

**Monitoring**: `gap_report` SEASON_TEAM_CODE check reports `ok = (batting_null == 0 and pitching_null == 0)`. After `--apply` for 이병헌, only 김택연 (665/2025) remains NULL, so the check stays `False` by design and is accepted. The threshold-based alert (NULL rate > 10%) remains aspirational/monitored out-of-band.

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
