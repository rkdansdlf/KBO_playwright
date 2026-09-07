# Gate R4A Certification Report: Sealed-Snapshot Scheduler & Restart Recovery

**Gate ID**: `GATE-106F-R4A-SEALED-RECOVERY`
**Started At**: `2026-09-07T07:50:03.199528+00:00`
**Completed At**: `2026-09-07T07:50:13.845868+00:00`
**Target Game**: `20240930NCHT0` (NC Dinos vs KIA Tigers, 2024-09-30, Inning 9 top)
**Certification Status**: **`PASS`** (Level-3 Offline Integration Certified)

---

## 1. Executive Summary

Gate R4A establishes the operational reliability and restart recovery certification for the KBO text relay pipeline. Operating strictly offline on sealed snapshots obtained during Gate R2-R, this certification validates that unexpected process crashes at any stage of ingestion, normalization, database persistence, or checkpoint recording recover cleanly without human intervention.

### Core Certified Guarantees
1. **Zero External Network Requests**: 100% sealed snapshot replay with verified zero socket traffic.
2. **Zero Protected Storage Mutations**: Bit-level SHA-256 validation of `data/kbo_dev.db` (`4be13e65d30621a8cc5d0134ba92b3fea9d56d3f6379d6b881c52a953cff229a`) unchanged.
3. **Deterministic Crash Recovery**: All 7 pre-declared crash points (`CP1` ~ `CP7`) simulated via hard exit (`os._exit(137)`) successfully recover on subsequent invocation (`exit_code=0`).
4. **Zero Logical Event Loss & Duplication**: Exactly 5 canonical 9th-inning events preserved without duplication or partial-batch divergence.
5. **Strict Monotonicity**: Checkpoint ledger sequence numbers strictly increase monotonically across restarts.
6. **Automatic Lock Healing**: `ForceProcessLock` clears extinct dead PIDs without operator intervention or deadlock.

---

## 2. Certified Scope & Boundaries

> [!IMPORTANT]
> **Scope Qualification**
> This certification certifies **sealed-snapshot scheduler replay & restart recovery under process crash injection** for target game `20240930NCHT0`.
> It does **NOT** certify:
> - Live network long-polling over active ongoing games.
> - Direct write access to Oracle or production databases.
> - Multi-day continuous scheduling daemon operation.

---

## 3. Crash-Injection Recovery Matrix

| Crash Point ID | Description | Crash Exit Code | Restart Exit Code | Pre-Crash Events | Post-Restart Events | Checkpoint Monotonic | Convergence Diff | Status |
|---|---|:---:|:---:|:---:|:---:|:---:|:---:|:---:|
| `CP1_FETCH_COMPLETE` | Crash after reading sealed snapshots | 137 | 0 | 0 | 5 | Yes | 0 | **PASS** |
| `CP2_DURING_NORMALIZATION` | Crash during event normalization | 137 | 0 | 0 | 5 | Yes | 0 | **PASS** |
| `CP3_AFTER_KBO_BEFORE_NAVER` | Crash after KBO parse before Naver merge | 137 | 0 | 0 | 5 | Yes | 0 | **PASS** |
| `CP4_AFTER_EVENTS_BEFORE_PBP` | Crash after inserting GameEvents before PBP | 137 | 0 | 0 (rolled back) | 5 | Yes | 0 | **PASS** |
| `CP5_AFTER_COMMIT_BEFORE_CHECKPOINT` | Crash after DB commit before COMMITTED checkpoint | 137 | 0 | 5 | 5 | Yes | 0 | **PASS** |
| `CP6_DURING_CHECKPOINT_RECORD` | Crash during checkpoint ledger update | 137 | 0 | 5 | 5 | Yes | 0 | **PASS** |
| `CP7_DURING_CORRECTION_UPDATE` | Crash during in-place revision update | 137 | 0 | 5 | 5 | Yes | 0 (revised) | **PASS** |

---

## 4. Domain Invariants Evaluation

- **`INV_ZERO_EXTERNAL_NETWORK`**: **PASS** (Zero network sockets opened during entire evaluation).
- **`INV_ZERO_DB_MUTATION`**: **PASS** (`data/kbo_dev.db` pre-sha == post-sha == `4be13e65d30621a8cc5d0134ba92b3fea9d56d3f6379d6b881c52a953cff229a`).
- **`INV_ZERO_LOGICAL_EVENT_LOSS`**: **PASS** (5/5 canonical events intact across all restart runs).
- **`INV_ZERO_DUPLICATE_CANONICAL_EVENTS`**: **PASS** (Zero duplicate primary keys or event sequences).
- **`INV_ZERO_PARTIAL_BATCH`**: **PASS** (GameEvent count matches GamePlayByPlay count in all states).
- **`INV_CHECKPOINT_STRICT_MONOTONICITY`**: **PASS** (Checkpoint sequence strictly increasing).
- **`INV_CORRECTION_LINEAGE_PRESERVED`**: **PASS** (In-place revision preserves event identity).
- **`INV_ZERO_LOCK_COLLISION`**: **PASS** (ForceProcessLock dead-PID cleanup verified).

---

## 5. Artifact & Provenance Files

- `README.md`: This certification report.
- `tested-code-manifest.json`: Full manifest of tested code, test suites, and sealed fixtures.
- `crash-injection-ledger.jsonl`: Machine-readable ledger of all 7 crash and restart executions.
- `checkpoint-state-ledger.jsonl`: Complete audit trail of checkpoint transitions.
- `recovery-convergence-diff.json`: Field-by-field diff validating exact convergence to golden baseline.
- `domain-invariants-r4a.json`: Formal pass/fail evaluation of all 8 invariants.
- `protected-db-before-after.json`: Cryptographic proof of zero protected database mutation.
- `fixtures/kbo_sealed_dom_nodes_20240930NCHT0.json`: 41 sealed KBO DOM leaf nodes.
- `fixtures/naver_sealed_payload_20240930NCHT0.json`: 39 sealed Naver relay options.
- `SHA256SUMS`: Cryptographic checksums of all evidence files.
- `checksum-verification.txt`: Output of independent verification (`shasum -a 256 -c SHA256SUMS`).
