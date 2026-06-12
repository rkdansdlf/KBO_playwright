# Reference Integrity Repair Notes - 2026-05-12

## Summary
- Restored local SQLite reference integrity for game metadata, team codes, season stat player references, and `Unknown <id>` player stubs.
- Strengthened local quality gates so `PRAGMA foreign_key_check` is not the only integrity signal.
- Added dry-run-first repair/backfill tools for repeatable local and OCI recovery.
- Added physical FK migrations for SQLite and OCI so declared FK coverage can be enforced after data is clean.

## Applied Local Repair
- Backup created before mutation:
  - `data/backups/kbo_dev_before_reference_integrity_20260511_235454.db`
- Team code repair:
  - Normalized legacy `HD` team references to canonical `HU`.
  - Applied across `game` team columns and fact `team_code`/`team_id` columns where present.
- Game metadata repair:
  - Repaired 116 `game_metadata` rows without a parent `game`.
  - Reused existing parent games where a safe mapping existed.
  - Created `CANCELLED` parent games for metadata-only schedule rows that could be parsed.
  - Recorded aliases in `game_id_aliases` for remapped metadata rows.
- Player profile repair:
  - Backfilled missing pitching profile references and `Unknown <id>` stubs.
  - Preferred verified local canonical `players`/`player_identities` data.
  - Used official KBO profile pages only when local canonical data was unavailable.
  - Used `players.kbo_person_id -> player_basic` fallback for legacy ID aliases such as `5683 -> 91153`.
- SQLite FK coverage repair:
  - Applied `migrations/sqlite/004_reference_integrity_foreign_keys.sql`.
  - Rebuilt legacy tables that SQLite cannot alter in place for FK additions.
  - `SQLite declared FK coverage` now reports `PASS`.

## Verification Commands
Run these after local repair and before any OCI sync:

```bash
./venv/bin/python scripts/verification/check_orphan_data.py --strict --json --sample-limit 20
./venv/bin/python -m scripts.maintenance.quality_gate --skip-oci
./venv/bin/python -m pytest
```

Expected local status after this repair:
- `check_orphan_data.py --strict`: pass.
- `scripts.maintenance.quality_gate --skip-oci`: pass.
- `pytest`: pass with only opt-in/live tests skipped.

## New And Updated Tools
- `scripts/verification/check_orphan_data.py`
  - CLI options: `--db-url`, `--db-path`, `--strict`, `--json`, `--sample-limit`.
  - Checks logical relationships that SQLite may not enforce physically.
  - Reports both row counts and distinct reference counts.
- Legacy `repair_reference_integrity.py` was removed after the repair window. Use `scripts/verification/check_orphan_data.py` for current reference-integrity validation.
- `scripts/crawling/backfill_missing_players.py`
  - Default mode is dry-run.
  - Use `--include-pitching --include-unknown-stubs` for the full player repair set.
  - Use `--ids 1,2,3` for targeted retries.

## OCI Sync Plan
After local verification passes, sync the smallest needed OCI scope:

```bash
./venv/bin/python3 -m src.cli.sync_oci --teams
./venv/bin/python3 -m src.cli.sync_oci --player-basic
./venv/bin/python3 -m src.cli.sync_oci --season-stats
```

If game metadata repairs must be reflected in OCI, also sync the affected game rows and metadata through the existing game sync path. Then verify OCI directly:

```bash
./venv/bin/python scripts/verification/check_orphan_data.py --db-url "$OCI_DB_URL" --strict --json --sample-limit 20
./venv/bin/python -m scripts.maintenance.quality_gate
```

## OCI FK Enforcement
Applied the OCI FK migration after the OCI target passed logical verification:

```bash
./venv/bin/python scripts/verification/check_orphan_data.py --db-url env:OCI_DB_URL --strict --json --sample-limit 20
```

The migration adds `NOT VALID` constraints first, then validates them. After application:
- `Declared FK coverage`: `PASS`
- `game_metadata -> game`: `PASS`
- season stat player references: `PASS`
- team references: `PASS`
- `Unknown <id>` stubs: `PASS`

OCI sync smoke commands also passed:

```bash
./venv/bin/python -m src.cli.sync_oci --games-only --year 2026
./venv/bin/python -m src.cli.sync_oci --game-details --game-ids 20260510SSNC0
```
