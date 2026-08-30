# Phase 105 Gate 4P: Oracle Staging Rehearsal Preflight & Safety Architecture Specification

**Status**: `GATE_4P_DESIGN_SUBMITTED` (Pending Review / STRICT NO-GO on execution)
**Execution Guard**: `NETWORK_CONNECTIONS = 0`, `ORACLE_SELECTS = 0`, `ORACLE_DML = 0` (Design-Only Specification)

---

## 1. Deep Runtime Identity Verification Protocol

Relying on connection string / DSN substring matching alone (e.g. `_low`, `_medium`, `_high`) is insufficient to prevent accidental connection to production instances.
Prior to any DML or staging preparation, a **read-only identity probe** must execute and match all 5 runtime context descriptors:

```sql
SELECT
    SYS_CONTEXT('USERENV', 'DB_UNIQUE_NAME') AS db_unique_name,
    SYS_CONTEXT('USERENV', 'SERVICE_NAME')   AS service_name,
    SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA') AS current_schema,
    SYS_CONTEXT('USERENV', 'SESSION_USER')   AS session_user,
    SYS_CONTEXT('USERENV', 'SERVER_HOST')    AS server_host
FROM DUAL;
```

### Identity Allowlist Contract (Fail-Closed)
| Context Descriptor | Expected Value Contract | Action on Mismatch |
| :--- | :--- | :--- |
| `DB_UNIQUE_NAME` | Must match `*STAGING*` or configured disposable test instance | Immediate `sys.exit(1)` / Abort |
| `SERVICE_NAME` | Must match `*kbo_staging_*` | Immediate `sys.exit(1)` / Abort |
| `CURRENT_SCHEMA` | Must match `KBO_STAGING` | Immediate `sys.exit(1)` / Abort |
| `SESSION_USER` | Must match least-privilege rehearsal user `KBO_RAG_REHEARSAL_USER` | Immediate `sys.exit(1)` / Abort |
| `PRODUCTION_TAG` | Must NOT contain `PROD`, `PRODUCTION`, or `KBO_PROD` | Immediate `sys.exit(1)` / Abort |

---

## 2. Multi-Tier Rollback Architecture & Recovery Priority

`FLASHBACK TABLE` is an emergency fallback requiring specific prerequisites (row movement enabled, table lock acquisition, sufficient undo retention). It is **not** the primary rollback mechanism.

### Rollback Priority Matrix
```mermaid
graph TD
    A[Staging Rehearsal Execution] --> B{Canary Verification}
    B -- Failed during probe --> C[Priority 1: Session Rollback (Uncommitted)]
    B -- Failed post-commit --> D[Priority 2: Inverse Preimage Manifest Application]
    D -- Preimage unavailable / corrupted --> E[Priority 3: Isolated Staging Clone / Snapshot Restore]
    E -- Full instance recovery needed --> F[Priority 4: FLASHBACK TABLE TO SCN (Emergency Secondary)]
```

1. **Priority 1: In-Transaction `session.rollback()` (Canary Verification Phase)**
   - All canary operations run within an explicit transaction.
   - Postconditions and CAS checks are verified *before* `session.commit()`.
   - On any anomaly or assertion failure, immediate `session.rollback()` is issued (0 persistent changes).

2. **Priority 2: Deterministic Inverse Preimage Manifest**
   - Automatically generated alongside the apply manifest.
   - Encapsulates exact prior `source_row_id`, `index_status`, `content_hash`, `index_version`.
   - Re-application of preimage manifest with CAS verification restores original state.

3. **Priority 3: Staging PDB / Clone Snapshot Restore**
   - Oracle Autonomous Database disposable staging clone refreshed from point-in-time snapshot.

4. **Priority 4: Emergency `FLASHBACK TABLE` (Secondary Fallback Only)**
   - Pre-condition check: `ALTER TABLE rag_chunks ENABLE ROW MOVEMENT;`
   - SCN captured immediately prior to execution:
     ```sql
     SELECT CURRENT_SCN FROM V$DATABASE;
     ```
   - Rollback command:
     ```sql
     FLASHBACK TABLE rag_chunks TO SCN :PRE_REHEARSAL_SCN;
     ```

---

## 3. Deterministic 6-Archetype Multi-Source Canary Selection Matrix

The canary rehearsal must not be limited to awards data alone. It covers 6 distinct operational archetypes across multiple source domains:

| Archetype ID | Source Domain | Target Chunk ID | Test Condition | Expected Action | Verification Invariant |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `SAFE_REKEY` | `awards` | `chunk_101` | Numeric legacy ID `101` $\to$ Natural key `award:MVP:2024:101` | `UPDATE source_row_id` | CAS matches, exactly 1 row mutated |
| `SAFE_REKEY_STATS` | `player_season_batting` | `chunk_501` | Numeric legacy ID `501` $\to$ Natural key `batting:2024:62931:LT:REGULAR:1군` | `UPDATE source_row_id` | CAS matches, exactly 1 row mutated |
| `TARGET_COLLISION_TOMBSTONE` | `awards` | `chunk_102` | Natural target `award:ROOKIE:2024:202` already exists with same content | `UPDATE index_status = 'DELETED'` | Legacy chunk tombstoned, natural chunk untouched |
| `ALREADY_APPLIED_NOOP` | `awards` | `chunk_103` | Chunk already possesses target natural key | `SKIP` (0 mutation) | Reported as `ALREADY_APPLIED`, 0 rowcount |
| `STALE_CAS_REJECT` | `player_season_pitching` | `chunk_601` | DB content hash differs from manifest expected hash | `REJECT` (0 mutation) | Aborts chunk with `STALE_CONTENT_HASH`, Fail-Closed |
| `INVERSE_ROLLBACK_REPLAY` | `awards` | `chunk_101` | Apply preimage manifest onto rekeyed chunk | `RESTORE legacy_id` | Preimage verified, chunk restored to initial state |

---

## 4. Least-Privilege Rehearsal Account Matrix

The rehearsal must run under a dedicated, tightly scoped user account (`KBO_RAG_REHEARSAL_USER`) rather than an administrative or application owner account:

| Privilege | Scope | Rationale |
| :--- | :--- | :--- |
| `SELECT` | `KBO_STAGING.rag_chunks` | Read current index state |
| `UPDATE (source_row_id, index_status, updated_at)` | `KBO_STAGING.rag_chunks` | Execute CAS-guarded rekey and tombstone |
| `INSERT, DELETE, DROP, TRUNCATE` | `ALL TABLES` | **REVOKED / PROHIBITED** |
| `SELECT, UPDATE, INSERT, DELETE` | Other tables (`games`, `player_basic`, `player_season_*`) | **REVOKED / PROHIBITED** |
| `FLASHBACK` | `KBO_STAGING.rag_chunks` | Emergency recovery privilege only |

---

## 5. Preflight Readiness Checklist (Prerequisites before Gate 4 Rehearsal)

- [x] Gate 0 Baseline Clean Freeze: Verified (`519fa633`).
- [x] Gate 1 Formula Contract: Verified (45-season resolver, standard naming).
- [x] Gate 2E Source-Domain & 4-Way Parity: Verified (1,500 source rows, 16,500 evals, 0 divergence).
- [x] Gate 3E Exact Run & Crash Consistency: Verified (51 focused tests, directory fsync, postcondition recovery).
- [ ] Staging environment provisioning & identity allowlist registration.
- [ ] Least-privilege account creation & permission grant audit.
- [ ] Dry-run CLI execution under offline mock database.
- [ ] Formal rehearsal approval authorization from system administrator.
