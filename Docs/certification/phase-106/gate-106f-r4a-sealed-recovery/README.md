# Gate R4A Certification Report: Sealed-Snapshot Relay Replay Worker & Restart Recovery

**Gate ID**: `GATE-106F-R4A-SEALED-RECOVERY`
**Started At**: `2026-09-08T14:17:14.968354+00:00`
**Completed At**: `2026-09-08T14:17:38.840708+00:00`
**Target Game**: `20240930NCHT0` (NC Dinos vs KIA Tigers, 2024-09-30, Inning 9 top)
**Certification Status**: **`PASS`** (Level-3 Offline Integration Certified)
**Recovery Architecture Model**: `REPLAY_FROM_START_WITH_IDEMPOTENT_PERSISTENCE`

---

## 1. Executive Summary

Gate R4A certifies the crash recovery and restart resilience of the KBO text relay pipeline replay worker against hard process termination (`os._exit(137)`). Unlike synthetic test pipelines, this remediation strictly re-uses existing production parsing (`RelayCrawler._parse_naver_payload`, `PBPCrawler._update_out_base_state`), deduplication (`RelayDeduplicator`), and persistence (`save_relay_data`) paths without hardcoded answer tables.

### Core Certified Guarantees
1. **Production Pipeline Re-use & Context Provenance**: All event normalizations and DB writes execute through the canonical production codebase. Half-inning state is typed via `HalfInningContext` with explicit boxscore provenance.
2. **Zero External Network Requests**: Replay is 100% offline with worker subprocess socket connections blocked (`KBO_SEALED_REPLAY_OFFLINE=1`).
3. **Zero Protected Storage Mutations**: Bit-level SHA-256 of `data/kbo_dev.db` (`4be13e65d30621a8cc5d0134ba92b3fea9d56d3f6379d6b881c52a953cff229a`) is strictly unmutated.
4. **Transaction Boundary Crash Resilience**: Simulated hard termination (`os._exit(137)`) during active transactions (CP4, CP6, CP7) rolls back uncommitted writes cleanly.
5. **Permanent Revision Lineage**: Applied revisions are recorded in `_relay_revisions`. Re-applying identical payloads is a no-op (`mutations=0`), conflicting payloads raise `ValueError`, and regular replays preserve committed revisions without regression.
6. **4-Entity Domain Convergence**: Full state hash and diffs explicitly verify all 4 substantive entities: `GameEvent` (5 rows), `GamePlayByPlay` (47 rows), `GameValidationMetrics` (`dual_canonical` with cryptographic `observed_event_pbp_state_sha256`), and `RelayRevisionRecord` (with `target_provider_log_id` and `original_description` preimage).
7. **Strict Confinement & Lock Protection**: Ephemeral DBs and locks are strictly confined to the allocated temporary workspace root; `ForceProcessLock` protects active live PIDs while safely reclaiming stale dead PID locks.

---

## 2. Certified Scope & Disclosures

> [!IMPORTANT]
> **Scope & Provenance Disclosure**
> - **Certified**: Offline sealed snapshot replay worker (`SealedSnapshotRelayPipeline`), production parser execution, transaction-boundary crash recovery, process lock auto-healing, permanent revision lineage, and 4-entity convergence for game `20240930NCHT0`.
> - **Validation Hash Scope**: `observed_event_pbp_state_sha256` cryptographically verifies the state equivalence of normalized in-memory/replayed events & PBPs against the golden baseline; it does NOT assert coupling to historical database validation records.
> - **Test Suite Reconciliation**: All 51 selected unit/integration tests pass across `test_relay_recovery_r4a.py` (25), `test_relay_recovery.py` (13), and `test_lock.py` (13). 1 test (`tests/utils/test_lock.py::test_lock_cross_process`) is deselected by default due to `@pytest.mark.slow` filtering in `pytest.ini` (total collected: 52 items, 1 deselected, 51 passed).
> - **Untested**: Long-polling of live active games, multi-day daemon execution of APScheduler (`scripts/scheduler.py`), and direct writes to production Oracle databases.
> - **Fixture Provenance**: Naver raw JSON snapshot was obtained via a 1-time HTTP request on 2026-09-07 during fixture preparation; all certification runs execute with zero network connectivity.

---

## 3. Crash-Injection Recovery Matrix (CP1 ~ CP7)

| Crash Point ID | Transaction Boundary | Crash Code | Restart Code | Pre-Crash Events | Post-Restart Events | Rollback Verified | Monotonic | Status |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `CP1_FETCH_COMPLETE` | Pre-transaction | 137 | 0 | 0 | 5 | N/A | Yes | **PASS** |
| `CP2_DURING_NORMALIZATION` | Pre-transaction | 137 | 0 | 0 | 5 | N/A | Yes | **PASS** |
| `CP3_BEFORE_DEDUPLICATION_MERGE` | Pre-transaction | 137 | 0 | 0 | 5 | N/A | Yes | **PASS** |
| `CP4_IN_TRANSACTION_DURING_SAVE` | Inside `save_relay_data` transaction | 137 | 0 | 0 | 5 | Yes | Yes | **PASS** |
| `CP5_AFTER_COMMIT_BEFORE_CHECKPOINT` | Post-commit | 137 | 0 | 5 | 5 | N/A | Yes | **PASS** |
| `CP6_DURING_CHECKPOINT_RECORD` | Inside checkpoint transaction | 137 | 0 | 5 | 5 | Yes | Yes | **PASS** |
| `CP7_DURING_CORRECTION_UPDATE` | Inside revision transaction | 137 | 0 | 5 | 5 | Yes | Yes | **PASS** |

---

## 4. Negative Controls & Idempotency Evaluation

| Control Test | Configuration | Expected Outcome | Observed Outcome | Status |
|---|---|---|---|:---:|
| **Single-Source Fallback** | `empty_naver=True` | `source_used="kbo_single"`, 5 events, 5 pbps | Matched exactly | **PASS** |
| **Fixture Tamper Detection** | Corrupted KBO fixture bytes | Execution rejected with checksum mismatch | Rejected closed (code 1) | **PASS** |
| **Correction Idempotency** | Repeat `apply_event_correction` | `mutations=0`, `already_applied=True`, 1 tag | 0 mutations, single tag | **PASS** |
| **Revision Conflict Rejection** | Conflicting payload on same ID | Explicit `ValueError` raised | Rejected with conflict error | **PASS** |
| **Regular Replay Preservation** | Replay with `apply_correction=False` | Committed revision retained on event 3 & target PBP row 35 (Kim Hyeong-jun), row 3 (Kim Hwi-jip) untouched | Retained with zero regression | **PASS** |
| **Path Confinement Violation** | DB path outside temporary root | Explicit `ValueError` raised | Rejected with confinement error | **PASS** |
| **PBP Match Failure Safety** | Non-existent provider_log_id & description | 0 mutations, `status="PBP_MATCH_FAILED"`, 0 DB writes | Aborted safely with 0 mutations | **PASS** |
| **PBP Ambiguity Rejection** | Duplicate PBP rows with same provider_log_id | Explicit `ValueError` raised ("Ambiguous PBP match"), 0 mutations | Rejected with ambiguity error | **PASS** |

---

## 5. Artifact & Provenance Files

- `README.md`: This certification report.
- `tested-code-manifest.json`: Full manifest of tested code, test suites, and sealed fixtures.
- `crash-injection-ledger.jsonl`: Machine-readable ledger of all 7 crash and restart executions.
- `checkpoint-state-ledger.jsonl`: Complete audit trail of checkpoint transitions.
- `recovery-convergence-diff.json`: 4-entity field-by-field diff validating exact convergence to golden baseline.
- `negative-control-ledger.json`: Structured verification of 8 negative controls and idempotency.
- `domain-invariants-r4a.json`: Formal pass/fail evaluation of all 8 invariants.
- `protected-db-before-after.json`: Cryptographic proof of zero protected database mutation.
- `fixtures/kbo_sealed_dom_nodes_20240930NCHT0.json`: 41 sealed KBO DOM leaf nodes.
- `fixtures/naver_sealed_payload_20240930NCHT0.json`: 39 sealed Naver relay options.
- `SHA256SUMS`: Cryptographic checksums of all evidence files.
- `checksum-verification.txt`: Output of independent verification (`shasum -a 256 -c SHA256SUMS`).
