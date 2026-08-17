-- Oracle intentionally does not apply the PostgreSQL 043 DDL.
--
-- The Oracle runner creates KBO_SCHEMA_MIGRATIONS with Oracle-native columns
-- before it reads or writes migration metadata. This no-op preserves the
-- source migration number and records that the requirement is satisfied by
-- the runner bootstrap.
BEGIN
    NULL;
END;
/
