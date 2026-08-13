# PostgreSQL Migrations

PostgreSQL migrations use an incremental contract.

## Contract

1. SQLAlchemy models are the baseline schema definition.
2. Run `python3 -m src.cli.apply_postgres_migrations` against the target database. Normal mode bootstraps the ORM schema when needed, then applies pending files under `migrations/postgresql/`.
3. Run the command again to verify idempotency, or use `--check` to fail when migrations are pending. `--check` is read-only.

The runner creates the ORM baseline before applying incremental migrations in normal mode. `--check` expects the schema to already exist.

## Existing Database Adoption

Existing PostgreSQL databases may predate the ``schema_migrations`` tracking table. The
local operational ``kbo`` database was inspected read-only on 2026-08-09 and has no
recorded migration history; its current schema already contains the award player-link
columns and indexes introduced by migration 048.

This confirms the current ORM-baseline schema shape, but it does not prove that an
older database can be upgraded from the historical OCI migration chain. Do not run
normal migration mode against an existing database until a schema/data backup and a
read-only metadata comparison have completed. Do not recreate missing historical
migrations as no-op files. If upgradeability is required, recover the original chain
from version control or deployment artifacts, or create a reviewed baseline-adoption
migration from an actual schema diff.

Git history contains a legacy OCI/Oracle chain under `migrations/oci/` and
`migrations/oracle/` in the parent of cleanup commit `052ea630`. That chain covers
legacy versions 019-050, uses `_schema_migrations` or Oracle-specific DDL, and is not
a PostgreSQL migration chain. It must not be copied into `migrations/postgresql/`.

For the current ORM-shaped database, the explicit adoption command is:

```bash
DATABASE_URL="postgresql://..." python3 -m src.cli.apply_postgres_migrations --adopt-existing
```

`--adopt-existing` is intentionally separate from normal migration mode. It validates
the ORM baseline plus `awards.player_id`, `awards.team_code`, and
`idx_award_player_id`, then writes only the `schema_migrations` tracking table and
the current 047/048 records. It does not call `init_db()`, execute migration SQL, or
change application data. Run it only after backup and read-only schema/data review.

## CI Verification

The `migration-apply` job in `.github/workflows/test_suite.yml` creates a PostgreSQL 16 service, initializes the ORM schema, applies the incremental migrations twice, and runs `--check`.

## Operational Commands

```bash
DATABASE_URL="postgresql://..." python3 -m src.cli.apply_postgres_migrations
DATABASE_URL="postgresql://..." python3 -m src.cli.apply_postgres_migrations --check
DATABASE_URL="postgresql://..." python3 -m src.cli.apply_postgres_migrations --adopt-existing
```

The runner supports PostgreSQL URLs only. SQLite migrations remain under `migrations/sqlite/`.
