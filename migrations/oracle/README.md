# Oracle Autonomous Database Migrations

Oracle uses the SQLAlchemy ORM metadata as the initial schema baseline.

Run:

```bash
python3 -m src.cli.apply_oracle_migrations
python3 -m src.cli.apply_oracle_migrations --check
```

The application commands use `DATABASE_URL` as the primary Oracle URL. For
disposable migration verification, pass `OCI_DB_URL` explicitly and include the
safety-gated files:

```bash
python3 -m src.cli.apply_oracle_migrations \
  --url "$OCI_DB_URL" --include-safety-gated
python3 -m src.cli.apply_oracle_migrations \
  --url "$OCI_DB_URL" --include-safety-gated --check
```

Both targets use `TNS_ADMIN` for the Autonomous Database wallet. Never use
`DATABASE_URL` with the destructive verification-schema reset tool. Add
numbered `.sql` files here for changes that cannot be represented safely by the
ORM baseline.

Migration `067_add_rag_vector_search.sql` adds the native Oracle AI Vector
Search column and HNSW index to `rag_chunks`. It intentionally leaves the
legacy JSON-compatible `EMBEDDING` column in place while the application uses
`EMBEDDING_VECTOR` for dense retrieval.

The initial `000_orm_baseline` version records the schema created by
`Base.metadata.create_all()`. Migration files must be Oracle-compatible and
idempotent when the deployment contract requires reapplication.
