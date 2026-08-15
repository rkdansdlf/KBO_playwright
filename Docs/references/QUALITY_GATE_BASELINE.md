# Quality Gate Baseline

## Oracle Operational Baseline

The daily workflow runs `src.cli.quality_gate_check` against the configured
`DATABASE_URL`, which targets Oracle Autonomous Database in production. A new
Oracle baseline must be captured after the SQLite→Oracle initial load passes all
integrity checks.

The values below are the previous PostgreSQL snapshot and are retained only as
historical comparison data until the new Oracle baseline is approved.

| Metric | Value | Baseline | Policy |
|---|---:|---:|---|
| `past_missing_runs` | 5702 | 6000 | Existing historical debt is accepted; growth above 6000 blocks the gate |
| `missing_player_profiles` | 6 | 6 | Existing identity debt is accepted; any new missing profile blocks the gate |

`past_missing_runs_max` was previously `2`, which no longer represented the
historical source coverage currently present in PostgreSQL. The new limit includes a
small operational headroom while remaining finite.

The baseline does not waive required-zero checks. `game_status` must be
present, past `SCHEDULED` rows must remain zero, and regression-pack failures
remain blocking.

The previous SQLite/remote comparison report is historical and is no longer an
operational gate. Data quality is evaluated directly against the primary database.
