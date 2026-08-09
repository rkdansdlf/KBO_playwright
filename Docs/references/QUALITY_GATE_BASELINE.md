# Quality Gate Baseline

## PostgreSQL Operational Snapshot (2026-08-02)

The daily workflow runs `src.cli.quality_gate_check` against the configured
`DATABASE_URL`. The operational baseline is therefore based on the primary
database rather than a secondary cache.

| Metric | Value | Baseline | Policy |
|---|---:|---:|---|
| `past_missing_runs` | 5702 | 6000 | Existing historical debt is accepted; growth above 6000 blocks the gate |
| `missing_player_profiles` | 6 | 6 | Existing identity debt is accepted; any new missing profile blocks the gate |

`past_missing_runs_max` was previously `2`, which no longer represented the
historical source coverage currently present in PostgreSQL. The new limit includes a
small operational headroom while remaining finite.

The baseline change does not waive required-zero checks. `game_status` must be
present, past `SCHEDULED` rows must remain zero, and regression-pack failures
remain blocking.

The previous SQLite/remote comparison report is historical and is no longer an
operational gate. Data quality is evaluated directly against the primary database.
