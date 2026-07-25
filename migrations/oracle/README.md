# Oracle Migration Bundle

This directory contains the manually ported Oracle versions of selected files
from `migrations/oci/`. The source `migrations/oci/` files remain the
PostgreSQL-compatible bundle used by CI and are not modified by this port.

## Current Status

- Drafted files: 021, 022, 023, 024 partial unique indexes, 024 string-length
  change, 025, 029, 030 safe no-op, 031, 033, 034, 035, 036, 039, 041, 042,
  044, 047, 048, 049, and 050.
- `024_deletion_anomaly_integrity.sql` remains design-only because it performs
  large data updates, trigger disabling, and foreign-key rebuilds.
- No Oracle migration has been applied from this directory.
- The Oracle runner now supports `--dialect oracle`, slash-delimited PL/SQL
  blocks, and the `KBO_SCHEMA_MIGRATIONS` metadata table.
- Applied migration rows include the selected dialect and SHA-256 file
  checksum.
- No staging or production apply has been performed.
- Constraint migrations require a duplicate-data census before application.

## Manual Port Assumptions

- PostgreSQL JSON-family and text payloads are represented by `CLOB`.
  Oracle JSON validation and any CLOB indexing strategy remain outside these
  drafts.
- PostgreSQL flag values are represented by `NUMBER(1)` with `0`/`1`
  defaults.
- PostgreSQL floating-point values are represented by unconstrained `NUMBER`;
  application precision requirements should be confirmed before applying
  these drafts.
- Oracle has no standalone clock value type. `TICKET_OPEN_RULES.OPEN_TIME` is
  drafted as `VARCHAR2(8)` and assumes `HH24:MI:SS` values. This is the one
  type mapping requiring application/query review before application.
- The draft order assumes the existing `GAME` and `PLAYER_BASIC` tables are
  present. File 031 creates `STADIUM_INFO`; files 035 and 039 depend on it.
  File 033 creates the source registry required by files 034 and 035, while
  file 034 also depends on `GAME`, `PLAYER_BASIC`, and the file 033 tables.
  File 041 depends on `GAME` and `PLAYER_BASIC`.
- PostgreSQL no-action foreign keys are represented by Oracle's default
  foreign-key action; `CASCADE` and `SET NULL` actions are retained where
  present in the source.

## Safety Contract

- Apply only against a staging Oracle schema first.
- Keep the source filename and checksum in migration metadata.
- Do not run the high-risk data rewrite files until their Oracle versions are
  reviewed independently.
