# Oracle Dialect Contract

Oracle Autonomous Database is the production store. The pull-request gate, however,
only has SQLite and PostgreSQL available, so nothing on a PR would notice when
Oracle-specific SQL drifts until the weekly `oci_live_verification.yml` run failed
against a real ADB — or worse, until production did.

This document defines the split between the two layers.

## Layer 1 — offline dialect contract (runs on every PR)

`tests/db/test_oracle_dialect_contract.py`

No database, no network. It pins the SQL that only Oracle would ever execute, using
`sqlalchemy.dialects.oracle` and the raw SQL builders:

| Contract | Why it is Oracle-only |
| --- | --- |
| `MERGE INTO ... USING (SELECT ... FROM DUAL UNION ALL ...)` shape | Oracle has no multi-row `VALUES` literal source. SQLite and PostgreSQL accept forms Oracle rejects. |
| `WHEN MATCHED` / `WHEN NOT MATCHED` ordering | A reordered MERGE is valid SQL that silently changes upsert semantics. |
| NULL-safe `ON` clause | Without `(t."PK" IS NULL AND s."PK" IS NULL)`, a NULL-keyed row re-inserts a duplicate on every sync. |
| Bind naming `:cN` in row-major order | The writer and its executemany call must agree on index ordering. |
| `CLOB -> VARCHAR2(4000)` cast in multi-row `USING` | Oracle cannot bind a CLOB inside a multi-row `USING` projection. |
| Cast table for numeric/character types | `FLOAT`, `BINARY_DOUBLE`, `NCHAR` and friends are not bindable as-is. |
| `TIMESTAMP WITH TIME ZONE` keeps its own cast | A blanket cast table would flatten a timezone-aware column. |
| `JSON -> CLOB` type compilation | Patched into the Oracle type compiler by `_install_oracle_json_compiler`. |
| `VECTOR(1536,FLOAT32,DENSE)` | A dimension mismatch is only observable at DDL time on Oracle. |

These carry no `slow`, `integration`, or `oci` marker, so they run in the default
`test` job. No workflow change is needed to put them on the merge path; the only
thing that would remove that is adding a marker.

### Verifying the contract still bites

A test that cannot fail is not a contract. To check coverage, break the writer and
confirm the suite goes red:

```bash
# 1. drop the NULL-safe ON branch
#    -> test_null_keys_match_null_to_null, test_single_row_uses_plain_binds fail
# 2. remove the "CLOB": "VARCHAR2(4000)" entry from _bulk_bind_expression
#    -> test_clob_columns_are_cast_in_multi_row_merges fails
```

Restore with `git checkout src/sync/oracle_writer.py` afterwards.

## Layer 2 — live ADB verification (weekly + manual)

`.github/workflows/oci_live_verification.yml`

Against `secrets.OCI_DB_URL`, which is a disposable schema and never the production
`DATABASE_URL`. It runs:

1. `apply_oracle_migrations` (apply, re-apply, `--check`)
2. `scripts/verification/audit_oracle_schema.py`
3. `pytest tests/test_oracle_smoke.py -m oci` (read-only smoke + repository rollback/upsert)
4. `scripts/verification/verify_oci_live_sync.py`

This layer owns everything the offline contract cannot reach: driver behaviour,
wallet/TLS negotiation, ID generators, transaction semantics, and the actual
`MERGE` execution plan.

## When you change either layer

| Change | Required action |
| --- | --- |
| A `MERGE`/bind change in `src/sync/oracle_writer.py` | Add or update a `TestMergeSqlContract` case. |
| A new column type reaching Oracle | Add it to the parameterized cast table test. |
| A new JSON-backed column | Confirm the `CLOB` case still holds. |
| A new RAG embedding dimension | Update the `VECTOR(...)` assertion and `RAG_*` version config together. |
| A new Oracle migration | Confirm Layer 2 still passes; the offline layer does not execute DDL. |
