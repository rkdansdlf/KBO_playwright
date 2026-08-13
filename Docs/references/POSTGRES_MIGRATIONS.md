# PostgreSQL Database

PostgreSQL is the primary application database. SQLite remains available for fast local tests and development fixtures.

## Schema Contract

1. SQLAlchemy models define the baseline schema.
2. `init_db()` creates the ORM baseline and PostgreSQL views.
3. `python3 -m src.cli.apply_postgres_migrations` applies pending PostgreSQL migrations.
4. `python3 -m src.cli.apply_postgres_migrations --check` checks pending versions without writing.
5. `python3 -m src.cli.apply_postgres_migrations --adopt-existing` validates the current ORM-shaped schema and records only the current migration baseline metadata.

The migration runner uses `DATABASE_URL` only. It does not connect to a second database or perform a synchronization step.

The explicit adoption mode does not call `init_db()` or execute migration SQL. It requires
the current `awards` player-link columns and index, then writes only `schema_migrations`
metadata for migrations 047 and 048. Use it only after backup and read-only review of an
existing database. The historical OCI/Oracle chain from Git cleanup commit `052ea630`
is not a PostgreSQL chain and must not be copied into `migrations/postgresql/`.

## Data Migration

Use `scripts/migrate_sqlite_to_postgres.py` to plan or apply a one-time migration from the local SQLite database:

```bash
python3 -m scripts.migrate_sqlite_to_postgres \
  --source-url "sqlite:///./data/kbo_dev.db" \
  --target-url "$DATABASE_URL"

python3 -m scripts.migrate_sqlite_to_postgres \
  --source-url "sqlite:///./data/kbo_dev.db" \
  --target-url "$DATABASE_URL" \
  --apply
```

The default mode is dry-run. Apply mode refuses to merge into a non-empty target database. Keep the SQLite source unchanged until row counts, foreign keys, sequences, and data-quality gates have passed on PostgreSQL.
