# Gate 105-4: Oracle Staging Rehearsal Code Verification

**Gate ID**: `GATE-105-4-STAGING-REHEARSAL`
**Status**: **LEVEL_3_INTEGRATION_VERIFIED** (`PASSED`)
**Execution Mode**: Local Ephemeral Test Harness (Mock Oracle SYS_CONTEXT + SQLite Ephemeral DB)
**Execution Timestamp**: `2026-09-02T14:09:08.376501+00:00`
**Test Suites**:
- `tests/services/test_staging_identity_gate4.py` (5-SYS_CONTEXT identity verification)
- `tests/services/test_staging_rollback_gate4.py` (Multi-tier preimage rollback)
- `tests/services/test_staging_canary_gate4.py` (6-archetype canary test matrix)
**Results**: **36 passed, 0 failed** across 3 modules.
**Database Protection**: Pre/Post SHA-256 identical (`8f36568f3d07774b4d5b0ae0de4b680685c8646caa041048208abdf33fd8c366`). 0 mutations.
**Network Policy**: 0 external network requests, 0 Oracle production DML.

---

## 6-Archetype Canary Rehearsal Matrix

| Archetype | Domain | Target | Action Taken | Result |
| :--- | :--- | :--- | :--- | :---: |
| `SAFE_REKEY` | awards | `101` $\to$ `award:MVP:2024:101` | CAS UPDATE | **PASS** |
| `SAFE_REKEY_STATS` | batting | `501` $\to$ `batting:2024:62931:LT:REGULAR:1군` | CAS UPDATE | **PASS** |
| `TARGET_COLLISION_TOMBSTONE` | awards | `102` (collides with `202`) | `index_status = 'DELETED'` | **PASS** |
| `ALREADY_APPLIED_NOOP` | awards | `103` (already rekeyed) | NOOP (0 mutations) | **PASS** |
| `STALE_CAS_REJECT` | pitching | `601` (hash mismatch) | REJECT (0 mutations) | **PASS** |
| `INVERSE_ROLLBACK_REPLAY` | awards | `101` (apply preimage) | RESTORE legacy ID | **PASS** |

---

## Evidence Manifest

- `certification-report.json`: Structured gate report with component status.
- `raw-test-output.txt`: Raw pytest execution log.
- `SHA256SUMS`: Cryptographic checksums of all evidence files.
