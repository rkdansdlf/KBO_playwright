# Known Data Limitations

Last updated: 2026-08-23

This document tracks known data quality issues and their current status.

---

## 1982 Season: Synthetic → Measured (namu.wiki boxscores) Replacement (2026-08-17)

**Status**: ✅ Resolved — 1982 정규시즌 경기결과를 합성 데이터에서 실측으로 전량 교체.

**배경**: KBO 공식 채널은 2000년 이전 시즌 경기별 기록을 제공하지 않고, 스탯티즈
(1982 일정 404/로그인 필요)와 위키백과(경기별 점수 미기재)는 부적합이었다.
나무위키 팀별 월별 문서의 박스스코어가 유일한 경기별 실측 소스로 확정되었다.

**단일 소스**: `https://namu.moe/w/{팀 문서명}/1982년/{3~4월|5월|6월|7월|8월|9~10월}`
(OB 베어스는 "9~10월" 404 → "9월" 단일 문서). 수집 482 박스 → 병합/정정 후
**241경기** (정규 240 + 무승부 1). 캘린더 표는 날짜 시프트/스코어 방향 혼재로
박스스코어의 날짜/스코어를 진실로 취급.

**교체 내용** (local SQLite + Oracle 운영 DB 동일 적용):
- 합성 240행 DELETE → 실측 241행 INSERT (`season_id=198200`, `game_status=COMPLETED`).
- 사실관계: 개막전은 1982-03-27 동대문 삼성 7:11 MBC (기존 합성은 3/26 OB:MB 7:11로
  날짜·매치업·스코어 전부 오류). 무승부 1건: 8/5 MB:HT 7:7 (무등).
- DH: 8그룹 (6/20, 6/26, 6/27, 8/18, 9/22, 9/28, 10/3, 10/6). 재경기 1건:
  8/18 MB:HT 8:7 (동대문, 8/5 무승부의 재경기).
- 검증: 팀별 승패가 위키백과 1982 최종순위와 전부 일치 (OB 56-24, SS 54-26,
  MB 46-34+D, HT 38-42+D, LT 31-49, SM 15-65), 매치업 균형 16~17, boxscore 유니크
  매칭 unmatched 0.
- 홈/원정: 경기장 소유 규칙으로 220경기 확정, 순회(중립) 경기장 21경기는
  박스 순서 유지 (순회 홈 제도: 전주/춘천/마산/한밭/청주 등에서 홈 경기 개최).

**재현 파이프라인**: `scripts/historical/1982_namu_boxscores.py` (`--crawl` 네트워크
전체 수집, 기본 모드 로컬 archive 재현, `--verify-only` 검증). 산출물:
`data/archives/1982_namu_raw.json` (482 박스), `data/archives/1982_answer_set_final.json`
(241경기, `game_id` 포함). 유령 경기(해태 4/8 = 4/14 복사 오기) 제거 규칙과
재경기 날짜 교정 규칙이 스크립트에 명시.

**제한**: 1982~1989, 1991, 1993, 1994, 2000 시즌(총 12개 시즌)은 실측 박스스코어로 전량 교체 완료.
나머지 7개 시즌(1990, 1992, 1995, 1996, 1997, 1998, 1999)은 외부 추가 증거 대조 후 순차 교체 예정.
선수 레벨 세부 스탯은 해당 과거 시즌들에 존재하지 않음. 나무위키 원본 오기(박스 복사)를
캘린더/이닝 대조로 감지해 DROP_BOXES/SCORE_FIXES 규칙으로 교정함.

## Supabase Migration Cleanup (2026-07-25)

**Status**: Retired integration path removed from the repository.

The runtime and CI pipeline use local SQLite and the OCI PostgreSQL-compatible target;
there are no active references to `SUPABASE_DB_URL`, Supabase workflows, or the removed
`scripts/supabase` helpers. The obsolete Supabase migration chain, inspection/fix
scripts, and their tests were removed together so the repository does not present an
unsupported second schema authority.

This cleanup does not delete or modify any external Supabase project. If a historical
Supabase database must be recovered, use the migration files from the pre-cleanup git
history and treat that recovery as a separate migration project.

---

## Summary

| Category | Status | Coverage |
|----------|--------|----------|
| Game season_id | ✅ Resolved | 100% (0 orphan) |
| Game team codes | ✅ Resolved | 100% (0 legacy) |
| game_metadata stadium_code | ✅ Resolved | 100% (0 NULL) |
| player_season team_code | ⚠️ Source-limited residuals classified | Integrity checks distinguish raw missing rows from unresolved rows; no source-limited row is written back as a guessed team |

---

## player_season team_code Source-Limited Residuals (2026-08-23)

**Status**: Known source-limited rows are accepted by the integrity checker; unresolved regular-season rows remain blocking.

The local SQLite audit on 2026-08-23 found 19 rows with no usable `team_code`:

- 7 rows from the 1982 `OFFICIAL_ARCHIVE` player-level source (3 batting, 4 pitching).
- 12 rows whose only same-season player-game evidence is the All-Star `EA`/`WE` raw team code (6 batting, 6 pitching).

The configured database can contain a different count as source coverage changes. The policy is evidence-based and does not whitelist player IDs.

**Classification policy**:
- A 1982 row with `source = OFFICIAL_ARCHIVE` is source-limited because the archive does not provide a reliable player-to-team mapping for these rows.
- A missing season row is source-limited when its same-season player-game rows contain only `EA`/`WE` and no regular team code. These are All-Star participation records, not canonical regular-season team assignments.
- Rows with no evidence, conflicting evidence, or regular-season evidence that cannot be resolved remain unresolved and fail the integrity check.

**Backfill safety**:
- `scripts/maintenance/backfill_season_team_codes.py` remains conservative and writes only when `--apply` is explicitly provided.
- All-Star-only `EA`/`WE` evidence is reported as `source_limited_all_star` and is never written as a canonical season team code.
- Ambiguous or evidence-less rows remain unchanged.

**Integrity/report behavior**:
- `data_integrity_checker` reports `batting_null` and `pitching_null` as raw missing counts, while `source_limited` and `unresolved` make the decision explicit.
- The `season_stat_team_code` check passes when `unresolved = 0`, not when raw missing count is zero.
- `gap_report` keeps raw missing rows visible, but alerts only when the unresolved rate exceeds `SEASON_TEAM_CODE_GAP_ALERT_RATE` (default 10%).

**Mitigation**: When aggregating player stats by team, filter out NULL team_code rows:
```sql
SELECT team_code, COUNT(*), SUM(games)
FROM player_season_batting
WHERE team_code IS NOT NULL
GROUP BY team_code;
```

**Read-only verification**:
```bash
DATABASE_URL=sqlite:///./data/kbo_dev.db \
  venv/bin/python -m src.cli.data_integrity_checker --date YYYYMMDD --json
DATABASE_URL=sqlite:///./data/kbo_dev.db \
  venv/bin/python -m src.cli.gap_report --dry-run --no-alert
DATABASE_URL=sqlite:///./data/kbo_dev.db \
  venv/bin/python -m scripts.maintenance.backfill_season_team_codes --table all
```

Do not add `--apply` during source classification. A source-specific remediation decision is required before any data mutation.

**Database propagation**: The backfill is dialect-agnostic (roster lookup uses SQLAlchemy `extract` instead of SQLite `strftime`) and runs against the configured `DATABASE_URL`. The local development database already has the approved fix applied.

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
- The read-only local game-level audit confirms that `player_game_pitching` is not a
  complete 2021 source: it covers 50 games versus 1,440 games in `game_pitching_stats`,
  with no duplicate `(game_id, player_id)` rows. It therefore cannot explain or repair
  the official staging difference of 18 outs and 18 earned runs.
- A read-only `recalc_team_stats --season 2021 --dry-run` confirmed that the OCI
  `team_season_*` rows are partial (roughly 30-65 games per team), while the
  `player_season_*` rollups contain near-full-season totals. OCI also lacks
  `team_standings_daily`, so the dry-run cannot safely derive team W-L-T values. Do not
  apply the team-stat recalculation until the source scope and standings source are fixed.
- A live, no-write probe of the KBO legacy team pages returned complete 2021 batting and
  pitching tables: 10 teams with 144 games each. The crawler previously accepted a
  stale current-season table because the asynchronous season change was not awaited.
  It now requires a successful season selection, complete team coverage, and completed
  season game counts before accepting the page; accepted rows are marked
  `extra_stats.source = kbo_team_page`.
- The official team source is usable. A read-only 2021 staging run now collects 394
  batting rows and 308 pitching rows. Batting global totals reconcile exactly with the
  official team table; pitching innings now reconcile exactly after preserving the raw
  KBO innings notation. Earned runs differ by 18 (`6243` team versus `6261` player)
   because the official team and player sources use different attribution semantics.
   Earned runs, stolen bases, and caught stealing differences are retained as
   `semantics_exempt_diff` and do not block the quality gate or synchronization.
   Individual impossible-stat checks, including `ER > R`, still block synchronization.
- Team-level player splits previously differed because the public player table did not
  have a safe multi-team season key in the existing schema. The player batting and
  pitching crawlers now preserve `(player_id, team_code)` rows when `by_team=True`, and
  the season-stat unique key includes `team_code`. OCI migrations 048, 049, and 050 are
  applied to the PostgreSQL sync target; the legacy unique constraint/index that could
  collapse team splits has been removed there.
- The pitcher collector now waits for the delayed team-filter postback and returns to
  page 1 before selecting the next team. The live 2021 probe verified complete team
  page traversal; the subsequent approved local/OCI application is recorded below.
- A 2021 team-filter batting probe returned 362 split rows versus 394 global rows. The
  crawler now preserves the complete `(player_id, team_code)` rows it receives rather
  than overwriting a player with the last team encountered; source coverage remains
  visible in the staging report. SQLite migration 047 and OCI migrations 048-050 add
  `team_code` to the logical season-stat unique key and remove conflicting legacy keys.
- A read-only 2026 staging probe on 2026-07-25 collected 297 batting rows and 271
  pitching rows with complete 10-team coverage. Official pitching global totals
  reconcile exactly for innings, hits, runs, home runs, walks, and strikeouts; the
  earned-run difference is 17 (`4157` team versus `4174` player) and remains a
  non-blocking source-semantics difference. Team-level pitching still has a 72-out
  HH/KIA split discrepancy, so the staging result is not ready for synchronization.
- Official batting staging is not ready: team totals exceed the collected player rows
  substantially (for example, 31,610 team AB versus 28,434 player AB), and
  `plate_appearances`, stolen bases, and caught stealing are unavailable from the
  current-season team source. The probe wrote only `/tmp/kbo_official_2026_20260725.json`
  and did not modify the database.

### 2026 Team/Player Rollup Source Policy (updated 2026-07-25)

**Status**: Local 2026 quality gates pass after aggregate-key remediation. The current
`team_season_*` rows are derived operational rollups and are marked
`extra_stats.source = player_rollup`; they are not treated as an independent official
KBO source.

The original mismatch came from older `recalc_player_stats` payloads that populated only
`canonical_team_code`. Because the logical UPSERT key includes nullable `team_code`,
repeated runs inserted duplicate `AGGREGATED` rows. The repair now writes both team keys
and removes only stale `AGGREGATED` rows from the target season when `team_code` is NULL
and `canonical_team_code` is populated.

The 2026 remediation removed 650 stale batting rows and 556 stale pitching rows, then
upserted 325 batting and 278 pitching aggregates. No duplicate `AGGREGATED` logical key
remains in the local 2026 regular-season tables. The quality gate and regression pack
both pass after the repair.

**Source policy**:

- `player_rollup` is the current derived operational source for local team-season rows.
- Official KBO team-page results remain staging evidence from
  `stage_official_season_stats`; they must not silently overwrite the derived rows.
- A future official-source promotion requires complete team coverage, explicit handling
  of unavailable fields, and a recorded comparison for earned-run semantics.
- Do not use `--truncate` against the canonical database to remove pre-existing source rows
  during routine season-stat synchronization; routine updates must remain idempotent.

### 2026-07-25 Manual Collection Policy (confirmed 2026-07-25)

**Status**: Manual confirmation and manual execution are required for same-day game
collection. Automatic polling must not start the collection pipeline.

Before collecting, confirm all target games are terminal and have scores in the local
schedule. Then run the target-game pipeline explicitly:

```bash
venv/bin/python -m src.cli.data_integrity_checker --date 20260725 --json
venv/bin/python -m src.cli.collect_games --year 2026 --game-ids "<completed-game-ids>"
venv/bin/python -m src.cli.recalc_player_game_stats --date 20260725 --save
venv/bin/python -m src.cli.recalc_player_stats --season 2026
venv/bin/python -m src.cli.recalc_team_stats --season 2026 --save
venv/bin/python -m src.cli.quality_gate_check --year 2026
venv/bin/python -m src.cli.data_quality_regression_pack --year 2026 --json
venv/bin/python -m src.cli.freshness_gate --days 7 --json
venv/bin/python -m src.cli.quality_gate_check --year 2026
```

The `player_rollup` team-stat policy remains in force during this pipeline. Do not
promote official KBO staging rows or use OCI `--truncate` as part of the daily manual
collection.

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
- `LegacyGameDetailCrawler` (`src/crawlers/legacy_game_detail_crawler.py`) and
  `historical_boxscore_import` (`src/cli/historical_boxscore_import.py`) are now
  fully implemented. They provide maintained HTML boxscore parsing and manifest-driven
  backfill capabilities (`--dry-run` and `--save` with strict quality gates).
- `scripts/crawl_2009_game_details.py` has been updated to use `LegacyGameDetailCrawler`.

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
- 9,508 rows from the historical database snapshot
- 2,453 rows inferred from team modal stadium mapping
- 8 remaining 2020 HH games manually backfilled
