# Oracle Migration Manual Port Plan

## Scope

The files under `migrations/oci/` are PostgreSQL-oriented despite the OCI
name. They remain unchanged for the PostgreSQL CI migration job. Oracle
versions will be maintained separately after the target schema and deployment
owner are confirmed.

## Rules

- Do not apply the PostgreSQL files directly to Oracle.
- Keep one Oracle file per source migration number for auditability.
- Use Oracle dictionary views (`USER_TABLES`, `USER_TAB_COLUMNS`,
  `USER_CONSTRAINTS`, and `USER_INDEXES`) for idempotency checks.
- Use `IDENTITY` or an explicit sequence/trigger instead of `SERIAL`.
- Use `VARCHAR2`, `CLOB`/Oracle JSON, `NUMBER`, `TIMESTAMP`, and `NUMBER(1)`
  according to the existing Oracle schema.
- Run each converted file in an isolated staging schema before production.
- High-risk data rewrites and constraint validation require a separate backup
  and an explicit approval.

## Runner Status

The runner now supports `--dialect oracle` and implements the following:

- Oracle-safe `KBO_SCHEMA_MIGRATIONS` metadata bootstrap instead of
  `_schema_migrations` with
   PostgreSQL `TEXT` and `IF NOT EXISTS` syntax.
- Slash-delimited Oracle PL/SQL block splitting.
- Separate `migrations/oracle/` and `migrations/oci/` bundles.
- Existing PostgreSQL migration behavior preserved as the default.

Still required before production:

1. Validate the converted bundle against an Oracle staging schema.
2. Review high-risk data rewrites and obtain explicit approval before applying.

The read-only 024 preflight can be run with:

```bash
python3 -m scripts.maintenance.audit_oracle_migration_preflight --json \
  --output data/audit/oracle_024_preflight.json
```

It records required source objects, planned columns, orphan counts, identity
backfill duplicates, unresolved team mappings, and trigger states. It does not
create the migration metadata table or modify the target schema.

## File-by-File Matrix

### Oracle-Only Schema Bootstrap

| File | Oracle work | Risk | Status |
|---|---|---:|---|
| 000_oracle_team_daily_roster_bootstrap.sql | Create the ORM-owned `TEAM_DAILY_ROSTER` table and its model indexes, which have no source migration | High | Applied to current OCI target |

| Source file | Oracle work | Risk | Status |
|---|---|---:|---|
| 019_create_awards.sql | Rewrite PL/pgSQL and catalog checks; `SERIAL`/`NOW()` types | Medium | Pending |
| 020_create_advanced_stats.sql | Convert four table definitions and JSON/numeric types | Medium | Pending |
| 020_relax_game_summary_detail_text_index.sql | Guard constraint/index operations | Medium | Pending |
| 021_game_summary_hash_index.sql | Replace `DELETE ... USING`, `md5`, and CLOB index strategy | High | Drafted, not applied |
| 022_add_source_to_advanced_stats.sql | Guard column additions with `USER_TAB_COLUMNS` | Low | Applied to current OCI target |
| 023_reference_integrity_foreign_keys.sql | Rewrite catalog checks, partial indexes, and validation | Critical | Drafted, not applied |
| 024_deletion_anomaly_integrity.sql | Full PL/SQL rewrite of cleanup and constraint rebuild | Critical | Design only; not drafted |
| 024_game_stat_partial_unique_indexes.sql | Replace partial unique indexes with function-based indexes | High | Drafted, not applied |
| 024_increase_pitching_string_lengths.sql | Convert `ALTER COLUMN TYPE` to `MODIFY` | Low | Applied; adds missing quoted `"level"` and `source` columns |
| 025_increase_lineup_notes_length.sql | Convert `ALTER COLUMN TYPE` to `MODIFY` | Low | Applied to current OCI target |
| 025_player_movement_position_backfill.sql | Replace regex/cast/update-from logic | Medium | Pending |
| 026_player_movement_profile_mirror_backfill.sql | Replace window boolean aggregation and update-from | High | Pending |
| 027_player_movement_roster_backfill.sql | Replace casts and update-from with `MERGE` | Medium | Pending |
| 028_player_movement_franchise_history_backfill.sql | Replace regex/cast/update-from logic | Medium | Pending |
| 029_add_team_profiles_indexes.sql | Guard table/index creation | Low | Applied to current OCI target |
| 030_create_hnsw_vector_index.sql | Remove PostgreSQL session settings; design Oracle vector index | High | Drafted as safe no-op, not applied |
| 031_phase1_new_models.sql | Convert identity, JSON, boolean, timestamp, and guards | High | Applied to current OCI target |
| 032_fix_team_season_fielding_float_columns.sql | Rewrite PL/SQL type inspection and conversion | Medium | Pending |
| 033_phase0_source_registry.sql | Convert identity/text/timestamps and guards | Medium | Applied to current OCI target |
| 034_phase1_p0_models.sql | Convert identity and standalone `TIME` representation | High | Applied to current OCI target |
| 035_phase1_p1_models.sql | Convert identity/real/boolean/text/JSON types | High | Applied to current OCI target |
| 036_phase1_team_event_unique.sql | Replace `pg_constraint` lookup with Oracle dictionary lookup | Medium | Applied to current OCI target |
| 037_relay_enhancement_phase1.sql | Convert columns, JSON, timestamps, and FK/index guards | High | Pending |
| 038_player_basic_birth_date_index.sql | Remove PostgreSQL transaction/schema syntax | Low | Pending |
| 039_stadium_realtime_tables.sql | Convert identity/real/boolean/JSON types | High | Applied to current OCI target |
| 040_add_player_parsed_profile_fields.sql | Convert transaction/schema/JSON/guard syntax | Medium | Pending |
| 041_create_player_game_tables.sql | Convert identity/JSON/double precision/types | High | Applied to current OCI target |
| 042_crawl_runs_unique.sql | Guard unique index creation | Low | Deferred; `CRAWL_RUNS` is absent in OCI |
| 043_create_schema_migrations_table.sql | Replace with Oracle metadata bootstrap | High | Pending |
| 044_performance_indexes.sql | Replace partial indexes with function-based indexes | High | Drafted, not applied |
| 045_additional_performance_indexes.sql | Guard ordinary index creation | Low | Pending |
| 046_add_team_stats_extended_fields.sql | Guard columns and map floating types | Low | Pending |
| 047_remove_redundant_phase1_indexes.sql | Guard index drops through `USER_INDEXES` | Low | Applied to current OCI target |
| 048_add_team_code_to_player_season_unique.sql | Guard constraint replacement and duplicate check | Medium | Drafted, not applied |
| 049_remove_legacy_player_season_unique_constraints.sql | Guard constraint removal | Low | Applied to current OCI target |
| 050_drop_legacy_player_season_pitching_index.sql | Guard index removal | Low | Applied to current OCI target |

## Execution Phases

1. Build and test the Oracle runner and metadata bootstrap.
2. Apply the Oracle-only `000_oracle_team_daily_roster_bootstrap.sql` after
   confirming `TEAMS` and `PLAYER_BASIC` are present.
3. Port low-risk DDL and index files: 022, 024 length changes, 025, 029,
   036, 038, 045, 046, 047, 049, and 050. Defer 042 until `CRAWL_RUNS`
   exists.
4. Port model creation files: 019, 020, 031, 033-035, 037, 039-041.
5. Port data backfills and integrity constraints only after a staging census:
   021, 023-028, 032, and 044.
6. Port or explicitly defer vector indexing in 030 based on Oracle Vector
   support in the target database version.
7. Apply to a staging schema, re-apply for idempotency, then run the model and
   reference-integrity checks before production.

Current connection verification: Oracle wallet and TNS alias connect
successfully with `SELECT 1 FROM dual`. The current OCI target now has 15
selected Oracle migrations applied; no high-risk data rewrite migrations have
been applied.

Current read-only 024 preflight (`2026-08-02`):

- `preflight_clear=true` after applying the Oracle-only roster bootstrap and
  model-table migrations.
- Existing `PLAYERS.PLAYER_BASIC_ID` has no duplicates, and the planned
  numeric backfill predicts no duplicate IDs.
- Checked game and roster foreign-key orphan counts are zero. Movement foreign
  keys remain skipped because their 024-added columns are not present.
- One unresolved movement row remains: `PLAYER_MOVEMENTS.id=4780`,
  `team_code=N/A`; it is intentionally not auto-mapped.
- Report: `data/reports/oracle_024_preflight_20260802.json`.

The Oracle-only bootstrap above is now applied to the current OCI target. The
design-only 024 integrity work still requires a separate approval after a
staging census and a decision on the one `N/A` movement row.
