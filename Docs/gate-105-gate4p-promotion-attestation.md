# Phase 105 — Gate 4P: Staging-to-Production Promotion Attestation

> [!WARNING]
> **STATUS: PRE-APPROVAL DRAFT**
> This document is a **promotion request**. It does **not** constitute production certification.
> No Oracle production DML has been executed by this work.

## 1. Purpose

This attestation requests **formal entry approval** to promote from
**Level 3 Integration Verification (Gate 4)** to **Phase 105 Gate 4P: Production
Staging Attestation**. Promotion is required before any Oracle production (write)
path is exercised.

## 2. Current Certification Status (as of this draft)

| Gate | Status | Evidence Location |
|------|--------|-------------------|
| Gate 0 | PASSED | `Docs/certification/phase-105/gate-0-observed-baseline/` |
| Gate 1 | PASSED | `Docs/certification/phase-105/gate-1-formula-contract/` |
| Gate 2 | PASSED | `Docs/certification/phase-105/gate-2-dual-path-audit/` |
| Gate 3 | PASSED | `Docs/certification/phase-105/gate-3-rag-rekey-safety/` |
| Gate 4 | LEVEL_3_INTEGRATION_VERIFIED | `Docs/certification/phase-105/gate-4-staging-rehearsal/` |
| **Gate 4P** | **PENDING ENTRY APPROVAL** | This document |

### Gate 4 Verification Summary

- **Identity probe**: 5 SYS_CONTEXT descriptors (`DB_UNIQUE_NAME`, `SERVICE_NAME`,
  `CURRENT_SCHEMA`, `SESSION_USER`, `CON_NAME`) validated via exact string match
  against an immutable allowlist (`StagingIdentityAllowlist`). Fails closed on any
  mismatch, missing descriptor, empty value, or probe error.
- **Approved staging instance**: `kbo_staging_iad` (disposable test schema also accepted).
- **Canary rehearsal**: 6-archetype matrix (`CanaryArchetype`) executed with
  **Priority 1 `session.rollback()`** guarantee — on any failure, all changes are
  rolled back; no net data persists. Verified: chunk 101 remains `"101"` after
  rollback.
- **Rollback engine**: Preimage manifest capture + verification
  (`src/services/staging_rollback.py`).
- **Test coverage**: 163 tests passing across `test_staging_canary_gate4.py`,
  `test_staging_identity_gate4.py`, `test_staging_rollback_gate4.py`,
  `tests/certification/test_certification_engine.py`,
  `tests/certification/test_certification_gates.py`,
  `tests/certification/test_synthetic_faults.py`, and
  `tests/services/test_rag_rekey_safety_gate3.py`.

## 3. Promotion Request — Required Resources

### 3.1 CI Secrets Required (for staging probe + canary)

| Secret | Purpose | Scope |
|--------|---------|-------|
| `OCI_DB_URL` | Read-only identity probe + canary rehearsal against staging | Staging-only (`kbo_staging_iad`) |
| `ORACLE_WALLET_B64` | TLS wallet for Oracle staging connection | Staging-only |
| `OCI_WALLET_PASSWORD` | Wallet decryption | Staging-only |
| `DATABASE_URL` | **NOT** used for staging verification; production gate | Production (held) |

> The staging identity probe is strictly **read-only** (`SELECT` from `DUAL` /
> `SYS_CONTEXT`). No `INSERT`/`UPDATE`/`DELETE`/`MERGE` is issued during Gate 4P.

### 3.2 Oracle Staging Verification Commands (Dry-Run Only)

```bash
# 1. Identity probe (read-only)
python3 -m src.cli.apply_oracle_migrations --check

# 2. Staging canary rehearsal (Priority 1 rollback)
python3 scripts/certification/phase105/run_staging_rehearsal_gate4.py --dry-run

# 3. Migration chain verification
psql "$OCI_DB_URL" --command "SELECT 0;"  # connectivity sanity (no schema mutation)
```

### 3.3 Oracle Migration Chain Status

All 33 canonical Oracle migrations (`000`–`074`, with `046`/`047` redundant-index
removal) are verified idempotent via the
`migration-apply` CI job (PostgreSQL baseline + OCI migration re-apply). Key
migrations:

| Migration | Purpose |
|-----------|---------|
| `059_rag_index_consistency.sql` | RAG chunk source identity UNIQUE constraint |
| `067_add_rag_vector_search.sql` | Native Oracle VECTOR column + metadata |
| `068`–`072` | Sparse term postings index (`RAG_CHUNK_TERMS`) |
| `073` / `074` | ID generator realignment (post-bulk-load) |

### 3.4 RAG Canary Status

- **Sparse term index**: `RAG_CHUNK_TERMS` 4,095,932 postings / 209,537 chunks; 0 orphan, 0 NULL source, 0 uncovered.
- **Hybrid retrieval**: BM25 Recall@5 = 0.6000 / MRR = 0.4667 (p95 ≤ 467ms);
  resolver-hybrid Recall@5 = 0.9485 / MRR = 0.8306 (p95 ≤ 694ms).
- **Staging gap**: 13,760 gap chunks reconciled (team-code drift + historical unindexed);
  `gap_resolution_summary.json` in artifacts.
- **Status**: quiet-instance evidence only — **NOT** a live traffic canary.

## 4. Post-Approval Production Readiness Checklist (Gate 4P → Production)

Upon formal entry approval, the following production steps are required:

1. **Oracle production sync**:
   ```bash
   python3 -m src.cli.sync_sqlite_to_oci \
     --source-url "sqlite:///./data/kbo_dev.db" \
     --target-url "$DATABASE_URL" \
     --dry-run  # ← Review row-count variance
   ```
   Followed by `--apply --mode incremental` (native `MERGE` bulk upsert).

2. **Scheduler restart** (`launchd`):
   ```bash
   launchctl unload /Library/LaunchDaemons/com.kbo.scheduler.plist
   launchctl load   /Library/LaunchDaemons/com.kbo.scheduler.plist
   python3 scripts/diagnose_scheduler_locks.py  # exit 0 = clean
   ```

3. **Phase 106 live smoke** (3 approved targets only):
   - `player-search-pagination-contract` (Playwright)
   - `player-stats-basic2-headers` (Playwright, read-only)
   - `wikipedia-awards-live` (httpx, read-only)

   See `Docs/certification/phase-106/gate-106d-live-smoke/` for network budget
   (87 requests budget: 52 allowed, 35 blocked, 0 unexpected hosts) and protected
   DB SHA-256 invariants.

## 5. Risk Assessment

| Risk | Mitigation | Residual |
|------|-----------|----------|
| Staging schema drift from production | `StagingIdentityAllowlist` strict exact-match | Low |
| Canary left in dirty state | Priority 1 `session.rollback()` + preimage manifest verify | Low |
| Production sync row-count variance | `--dry-run + verify` before `--apply` | Low |
| Stale scheduler lock on restart | `ForceProcessLock` auto-clear + `diagnose_scheduler_locks.py` | Low |

## 6. Approval Gate

- **Requested by**: <pending>
- **Reviewed by**: <pending>
- **Approved**: ❏ Yes / ❏ No / ❏ Conditional
- **Conditions** (if conditional):

---

*This document is updated on each promotion-attempt iteration. The last source-of-truth
commit is `1b977b18` (CI: 7/7 jobs green, 10,530 passed). Oracle production
promotion is blocked until this gate is explicitly approved.*
