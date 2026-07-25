# Oracle Design: 024 Deletion Anomaly Integrity

This migration is intentionally design-only. It performs data updates, trigger
disable/enable operations, foreign-key teardown/rebuild, and constraint
validation. It must not be generated as one automatic syntax translation.

## Planned Oracle Blocks

1. Add missing columns through `USER_TAB_COLUMNS` checks:
   `PLAYERS.PLAYER_BASIC_ID`, roster identity/person fields, and movement
   canonical/resolution fields.
2. Enumerate and drop matching single-column foreign keys through
   `USER_CONSTRAINTS` and `USER_CONS_COLUMNS`.
3. Backfill `PLAYERS.PLAYER_BASIC_ID` with `REGEXP_LIKE` and `TO_NUMBER`.
4. Backfill roster positions and `PERSON_TYPE` using correlated updates. Use
   `ALTER TABLE ... DISABLE ALL TRIGGERS` only after an explicit preflight.
5. Normalize `PLAYER_MOVEMENTS.CANONICAL_TEAM_ID` with a `CASE` expression and
   backfill player/team context using `MERGE` statements.
6. Add function-based indexes for nullable identity fields.
7. Rebuild the named foreign keys with `ENABLE NOVALIDATE`.
8. Run an explicit orphan census, then `ENABLE VALIDATE` each foreign key only
   when the census is clean.

## Required Preflight

- Count orphan rows for every planned foreign key.
- Count duplicate `PLAYER_BASIC_ID` values in `PLAYERS`.
- Count rows whose `PLAYER_MOVEMENTS.CANONICAL_TEAM_ID` cannot resolve to
  `TEAMS.TEAM_ID`.
- Record all trigger states before any disable operation.
- Take an Oracle schema backup or restore point.

## Blocking Risks

- Oracle DDL auto-commits, so the PostgreSQL transaction boundaries cannot be
  preserved.
- Existing orphan rows prevent final constraint validation.
- The movement backfill contains name-based identity resolution and must not
  silently choose among multiple candidates.
- The source migration uses PostgreSQL `UPDATE ... FROM`, regex operators,
  `IS DISTINCT FROM`, and `NOT VALID`; each needs a reviewed Oracle equivalent.

The file may be converted only after the preflight SQL and rollback procedure
are reviewed against the actual Oracle schema.
