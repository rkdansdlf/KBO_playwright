# Historical Completeness Triage (2009-2025)

Last verified: 2026-08-23

This is a read-only triage of the local SQLite completeness audit. It is a
source and remediation decision record, not permission to apply database fixes.

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

This bounded sample is not sufficient to prove whole-season unavailability,
but it does not support promoting 2011 PBP to an approved batch recovery. Keep
the 2011 PBP gap unapproved until an alternate archive or import manifest is
available.

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
  --start-year 2001 --end-year 2009

DATABASE_URL=sqlite:///./data/kbo_dev.db \
  venv/bin/python -m scripts.maintenance.audit_completeness \
  --start-year 2009 --end-year 2025 --dry-run --json
```
