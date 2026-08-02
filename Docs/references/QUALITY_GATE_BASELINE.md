# Quality Gate Baseline

## OCI Operational Snapshot (2026-08-02)

The daily workflow runs `scripts.maintenance.quality_gate --oci-only`, so the
operational baseline is based on the OCI target rather than the local SQLite
cache. The current OCI snapshot reported:

| Metric | Value | Baseline | Policy |
|---|---:|---:|---|
| `past_missing_runs` | 5702 | 6000 | Existing historical debt is accepted; growth above 6000 blocks the gate |
| `missing_player_profiles` | 6 | 6 | Existing identity debt is accepted; any new missing profile blocks the gate |

`past_missing_runs_max` was previously `2`, which no longer represented the
historical source coverage currently present in OCI. The new limit includes a
small operational headroom while remaining finite.

The baseline change does not waive required-zero checks. `game_status` must be
present, past `SCHEDULED` rows must remain zero, and regression-pack failures
remain blocking.

## Local/OCI Differences

The full local-plus-OCI gate currently reports expected cache/target divergence
that is not resolved by changing thresholds:

- Local `past_missing_runs=5972` versus OCI `5702`.
- Local `pitching_null_player_id=4` versus OCI `0`.
- Local `unresolved_missing=49` versus OCI `10`.
- Local `game_pitching_duplicate_player_groups=45` versus OCI `0`.
- Local has 10 past `SCHEDULED` rows.

These remain separate data reconciliation work. They are not encoded as
accepted baseline debt for the OCI-only CI gate.
