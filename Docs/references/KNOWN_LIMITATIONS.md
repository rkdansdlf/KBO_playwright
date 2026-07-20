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

**Monitoring**: `gap_report` SEASON_TEAM_CODE check reports `ok = (batting_null == 0 and pitching_null == 0)`. After `--apply` for 이병헌, only 김택연 (665/2025) remains NULL, so the check stays `False` by design and is accepted. Alerts are suppressed below `SEASON_TEAM_CODE_GAP_ALERT_RATE` (default 10%), while the residual remains visible in the report.

**OCI propagation**: The backfill is now dialect-agnostic (roster lookup uses SQLAlchemy `extract` instead of SQLite `strftime`), so it runs against both local SQLite and OCI. To resolve 이병헌 in OCI, dispatch the manual `backfill_season_team_codes` GitHub Actions workflow with `table=batting` (dry-run first, then `apply=true`); CI reaches OCI via `secrets.OCI_DB_URL`. The local dev SQLite already has the fix applied via `--apply`.

**OCI verification (2026-07-20)**: The renewed mTLS wallet at
`/Users/mac/keypair/Wallet_EFH9M9C9H109963K 2` restored OCI connectivity. OCI contained
three NULL `player_season_batting.team_code` rows for 2021: 이대은 (2365), 김지용
(60181), and 김건태 (60339). The OCI-aware backfill used `game_batting_stats` when
`player_game_batting` was absent and applied two unambiguous current-team resolutions:
이대은 → `KT`, 김지용 → `DB`. 김건태 remains the sole OCI residual because no team or
career evidence exists. `player_season_pitching` had zero NULL team-code rows.

**OCI quality verification (2026-07-20)**:
- The regression pack now runs against OCI after adapting the shared engine, Oracle
  `FETCH FIRST`, `TO_CHAR`, and the `source`/`data_source` schema alias. Nine of ten
  checks passed for 2021; the remaining `era_range` check found two rows (박관진 73 and
  강경학 1352) with extreme ERA values and no innings basis. No automatic stat repair was
  applied.
- The statistical quality gate runs without schema errors, but team-season totals and
  player-season sums disagree substantially in OCI for 2021. This is a data-scope/source
  reconciliation issue, not a team-code backfill failure, and remains a separate audit
  item.

---

## Historical Data Coverage (2001-2009)

**Status**: Schedule coverage backfilled; detail/stat completeness remains unverified

**Result**: The 2026-07-19 schedule backfill increased parent game rows from 1,430 to
4,688 across 2001-2009. Annual counts are now 504-544, with no duplicate game IDs and
no NULL game dates. `ScheduleCrawler` with no series filter traverses exhibition,
regular-season, and postseason series.

**Remaining limitation**: This pass backfilled schedule parent rows only. Historical
boxscore detail, player game stats, and PBP coverage are not complete. A read-only audit
found the following distinct-game coverage in the local SQLite database:

| Season | Parent games | Boxscore detail | `game_events` | Player-game batting | Player-game pitching |
|--------|--------------|-----------------|---------------|---------------------|----------------------|
| 2001 | 544 | 166 | 0 | 164 | 163 |
| 2002-2004 | 532 each | 133 each | 0 | 133 each | 133 each |
| 2005-2007 | 504 each | 126 each | 0 | 126 each | 126 each |
| 2008 | 504 | 231 | 0 | 231 | 231 |
| 2009 | 532 | 246 | 0 | 246 | 246 |

The detail/stat rows therefore cover only a subset of the new parent schedule rows, and
`game_events` has no matching rows for these seasons. Do not run a full historical stat
recalculation until a separate detail/stat/PBP backfill plan is reviewed.

**Source probe (2026-07-19)**: A read-only `GameDetailCrawler` pilot against missing 2001
games was inconclusive. `20010405LTHU0` timed out waiting for boxscore selectors;
`20010412OBHD1` returned a partial payload (2 hitters, 2 pitchers, 4 summaries); and
`20010412OBHD2` returned a payload with empty hitter/pitcher arrays. None of these probes
saved game rows. The Naver relay API returned HTTP 404 and `relay_not_found` for
`20010412OBHD1`. Do not start a batch detail or PBP backfill from these results without
an explicit completeness predicate and an alternate historical source.

**Public-source probe (2026-07-19)**:
- One missing terminal game from each season (2001-2009) was checked with
  `scripts/fetch_kbo_pbp.py --dry-run`.
- KBO and Naver legacy paths were classified as unsupported/timeout for every sample;
  import and manual manifests had no matching entries.
- A direct GameCenter detail attempt for `20010405LTHU0` returned the common shell but
  no boxscore selectors and saved no detail rows.
- No bulk historical crawl should run until an archive payload or import manifest is
  available. The sample probe report is kept outside the repository runtime data tree.

**Coverage measurement and collector status**:
- `src.cli.historical_coverage_report` is the read-only coverage tool for this gap. It
  reports per-year and per-series terminal-game coverage, missing game IDs, and coverage
  percentages for lineups, boxscores, player-game stats, events, and PBP.
- `scripts/crawl_2009_game_details.py` is not an operational collector: its referenced
  `LegacyGameDetailCrawler` and `save_game_detail` implementations are absent. Do not
  use that legacy debug script for a batch backfill until a maintained collector and an
  archive-backed source are available.

**Future backfill acceptance gate**:
- Boxscore/statistical backfill must contain both away/home hitter rows and both
  away/home pitcher rows. Metadata-only or scoreboard-only recovery is not sufficient
  for statistical aggregation.
- PBP backfill must pass final-score validation and inning-continuity validation. A
  minimum event-count threshold should be calibrated from a known-good payload before
  it is applied to historical games.
- Archive imports must use a manifest with matching season, capture timestamp, and
  SHA-256 checksum. Failed source probes remain dry-run reports and must not write
  partial rows.

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
