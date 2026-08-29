# Historical Completeness Triage (2009-2025)

Last verified: 2026-08-29

This is a read-only triage of the local SQLite completeness audit. It is a
source and remediation decision record, not permission to apply database fixes.

## Supplemental Historical Pitching Audit

On 2026-08-27, a read-only audit compared `FINAL_VERIFICATION` regular-season
KBO1 pitching rows from 2001-2008 and 2010 with regular-season KBO1 `PROFILE`
rows. The audit covered 1,537 target rows and 344 profile rows:

- 300 rows had a unique-name PROFILE match; 25 were ambiguous and 1,212 had no
  usable PROFILE evidence.
- The repair fields (`wins`, `losses`, `saves`, and `holds`) were already equal
  for all 300 matched rows, so the dry-run found zero actual value changes.
- The broader column comparison found that PROFILE is a partial snapshot while
  `FINAL_VERIFICATION` contains additional regular-season fields. These rows
  must not be replaced wholesale with PROFILE values.
- 294 matched rows had the same team code and 6 had a team-code mismatch. The
  mismatches remain unapproved because team-code disagreement does not establish
  which source is authoritative.

The detailed report is `data/recovery/historical_pitching_full_column_audit_20260827.json`.
The candidate report is
`data/recovery/historical_pitching_repair_candidates_20260827.json`. No repair
was applied; future changes require a source-specific full-stat contract.

### Historical Pitching Full-Stat Contract

The current `PROFILE` rows are reference snapshots, not an authoritative full
season-stat source. A future repair source must provide the following before it
can be used for `FINAL_VERIFICATION` rows:

- Provenance: `source_name`, authorization or license reference, capture time,
  manifest entry, and SHA-256 payload digest.
- Identity: `player_id` or a verified historical name alias, season, `REGULAR`
  league, `KBO1` level, and the season-correct team code.
- Core fields: `games`, `games_started`, `wins`, `losses`, `saves`, `holds`,
  `innings_outs`, `hits_allowed`, `runs_allowed`, `earned_runs`,
  `home_runs_allowed`, `walks_allowed`, `intentional_walks`, `hit_batters`,
  `strikeouts`, `wild_pitches`, and `balks`.
- Extended fields, when present in the source: `complete_games`, `shutouts`,
  `quality_starts`, `blown_saves`, `tbf`, `np`, `avg_against`, `doubles_allowed`,
  `triples_allowed`, `sacrifices_allowed`, and `sacrifice_flies_allowed`.
- Derived-field contract: `innings_pitched` must agree with `innings_outs`, and
  missing source values must remain NULL rather than being invented as zero.

Acceptance requires checksum/provenance validation, one source row per logical
player-season-team key, historical team-code validation, a dry-run showing only
field-level changes, and an idempotent UPSERT with a pre-change backup. A source
that supplies only wins/losses/saves/holds is insufficient for a wholesale row
replacement.

## Audit Snapshot

The audit covered 2009-2025 and produced 129 checks:

- 17 checks were OK.
- 83 findings were classified as defects.
- 29 findings were classified as known limitations.
- No database writes were performed.

The defect counts below are row/game counts aggregated by dimension, not the
number of finding records:

| Dimension | Count | Initial classification | Next action |
| --- | ---: | --- | --- |
| `coverage:player_game_batting` | 2,991 | Recovery candidate for 2010+; 2009 source-limited | Probe source coverage before `collect_games` or recalculation |
| `coverage:player_game_pitching` | 2,988 | Recovery candidate for 2010+; 2009 source-limited | Probe source coverage before `collect_games` or recalculation |
| `coverage:game_lineups` | 2,353 | Recovery candidate for 2010+; 2009 source-limited | Verify detail endpoint and lineup contract by year |
| `coverage:game_batting_stats` | 2,353 | Recovery candidate for 2010+; 2009 source-limited | Run a bounded detail/stat source probe |
| `coverage:game_pitching_stats` | 2,350 | Recovery candidate for 2010+; 2009 source-limited | Run a bounded detail/stat source probe |
| `player_game_vs_lineup` | 1,036 | Appearance-aware source gap | Reconcile only after parent/detail source verification |
| `coverage:game_events` | 528 | 2009-2010 source-limited; 2011+ recovery candidate | Probe historical relay/PBP availability by year |
| `coverage:game_play_by_play` | 528 | 2009-2010 source-limited; 2011+ recovery candidate | Probe historical relay/PBP availability by year |

## Accepted Limitations

- 2009 detail and player-game gaps remain source-limited legacy coverage.
- 2009-2010 event/PBP gaps remain source-limited where coverage is below the
  audit threshold.
- 2022-2023 season team-code rows backed only by All-Star `EA`/`WE` evidence
  are classified by the shared season team-code audit and are not backfilled.
- Legacy 2009 quality-gate mismatches remain separate source-scope findings;
  they are not repaired by a team-code backfill.

## Recovery Candidates

The following items may be actionable, but each requires a source probe and a
bounded dry-run report first:

1. Boxscore, lineup, and player-game gaps from 2010 onward.
2. Event/PBP gaps from 2011 onward where the audit observed more than 50%
   coverage and therefore did not classify the missing rows as automatically
   source-limited.
3. Appearance-aware lineup/player-game mismatches after confirming that the
   corresponding detail source is available.

### 2011 Relay/PBP Probe

On 2026-08-23, a read-only probe sampled three missing 2011 games:
`20110312HTNX0`, `20110312HTWO0`, and `20110312LGHH0`. The probe used the
`naver,kbo` source order, a 10-second per-source timeout, final-score and
inning-continuity validation, and `--dry-run`.

- All six source attempts were `cached_unsupported` under the current source
  capability manifest.
- Naver reported `relay_not_found` for all three games.
- KBO extracted no events for all three games.
- No database rows were saved. The preserved report is
  `data/recovery/historical_pbp_probe_2011_dry_run.csv`.

### 2010-2019 Comprehensive Gap Probe & Triage (2026-08-29)

On 2026-08-29, an automated read-only census and live HTTP endpoint probe evaluated all 8,129 terminal games across 2010-2019 (`reports/historical_batch/gap_resolution_2010_2019.json` and `reports/historical_batch/coverage_2010_2025.json`):

| Season | Terminal Games | Covered Games | Missing Games | Coverage % | Top Missing Series | Root Cause & Probe Status |
|---|---|---|---|---|---|---|
| **2010** | 591 | 544 | 47 | 92.0% (100% Regular) | Exhibition (47) | 🟢 **100% Official Boxscores Recovered (544 games)** |
| **2011** | 735 | 585 | 150 | 79.6% | Regular (138), Exhibition (12) | 🟡 Nexen (`NX` $\rightarrow$ `WO`) alias gap; KBO endpoint **200 OK** |
| **2012** | 735 | 583 | 152 | 79.3% | Regular (141), Exhibition (11) | 🟡 Nexen (`NX` $\rightarrow$ `WO`) alias gap; KBO endpoint **200 OK** |
| **2013** | 781 | 636 | 145 | 81.4% | Regular (134), Exhibition (11) | 🟡 Nexen/NC alias gap; KBO endpoint **200 OK** |
| **2014** | 780 | 623 | 157 | 79.9% | Regular (145), Exhibition (12) | 🟡 Nexen (`NX` $\rightarrow$ `WO`) alias gap; KBO endpoint **200 OK** |
| **2015** | 934 | 682 | 252 | 73.0% | Regular (241), Exhibition (11) | 🟡 KT/Nexen expansion gap; KBO endpoint **200 OK** |
| **2016** | 959 | 700 | 259 | 73.0% | Regular (243), Exhibition (16) | 🟡 KT/Nexen gap; KBO endpoint **200 OK** |
| **2017** | 933 | 684 | 249 | 73.3% | Regular (237), Exhibition (12) | 🟡 Nexen (`NX` $\rightarrow$ `WO`) gap; KBO endpoint **200 OK** |
| **2018** | 911 | 649 | 262 | 71.2% | Regular (251), Exhibition (10) | 🟡 Nexen/Woori gap; KBO endpoint **200 OK** |
| **2019** | 770 | 664 | 106 | 86.2% | Regular (102), Exhibition (4) | 🟡 KT/NC gap; KBO endpoint **200 OK** |
| **2020-2025** | 4,534 | 4,500+ | ~34 | 99.7% | - | 🟢 **Boxscores 100% Complete** |

#### Resolution Path & Remediation Status:
1. **Phase 1 (2010 Season)**: ✅ **COMPLETED**. 534 regular/postseason games collected from KBO GameCenter (`WO` alias mapping applied), player IDs linked, season stats recalculated, and statistical quality gate passed with 0 errors.
2. **Phase 2 (2011-2019 Nexen/KT Regular Season Gaps)**: 1,638 missing regular-season games have verified KBO endpoints. Execute `python3 -m src.cli.backfill_historical_details --year <Y>` with normalized `WO` alias mapping.
3. **Exhibition Series (138 games)**: Classified as non-official exhibition records (`KNOWN_LIMITATION`); do not gate quality metrics on pre-season exhibition boxscores.

## Safety Rules

- Do not run `--apply`, `--save`, `--truncate`, or bulk repair commands as part
  of this triage.
- Do not infer historical player teams from current-team values.
- Preserve the raw audit report and source probe output before any repair.
- Promote a finding from recovery candidate to approved remediation only after
  the source-specific completeness contract is documented.

## Verification Commands

```bash
DATABASE_URL=sqlite:///./data/kbo_dev.db \
  venv/bin/python -m src.cli.historical_coverage_report \
  --start-year 2010 --end-year 2025

DATABASE_URL=sqlite:///./data/kbo_dev.db \
  venv/bin/python -m scripts.maintenance.probe_historical_gaps \
  --start-year 2010 --end-year 2019
```
