# Oracle Autonomous Database Migrations

Oracle uses the SQLAlchemy ORM metadata as the initial schema baseline.

Run:

```bash
python3 -m src.cli.apply_oracle_migrations
python3 -m src.cli.apply_oracle_migrations --check
```

The command requires `DATABASE_URL` to use the `oracle+oracledb://` dialect and
uses `TNS_ADMIN` for the Autonomous Database wallet. Add numbered `.sql` files
here for changes that cannot be represented safely by the ORM baseline.

The initial `000_orm_baseline` version records the schema created by
`Base.metadata.create_all()`. Migration files must be Oracle-compatible and
idempotent when the deployment contract requires reapplication.
