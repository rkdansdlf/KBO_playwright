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

## CI Verification

The `migration-apply` job in `.github/workflows/test_suite.yml` creates a PostgreSQL 16 service, initializes the ORM schema, applies the incremental migrations twice, and runs `--check`.

## Operational Commands

```bash
DATABASE_URL="postgresql://..." python3 -m src.cli.apply_postgres_migrations
DATABASE_URL="postgresql://..." python3 -m src.cli.apply_postgres_migrations --check
```

The runner supports PostgreSQL URLs only. SQLite migrations remain under `migrations/sqlite/`.
