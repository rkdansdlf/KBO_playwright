# Gate R4A Certification Report: Sealed-Snapshot Scheduler & Restart Recovery

**Gate ID**: `GATE-106F-R4A-SEALED-RECOVERY`
**Started At**: `2026-09-07T08:59:16.329194+00:00`
**Completed At**: `2026-09-07T08:59:33.483477+00:00`
**Target Game**: `20240930NCHT0` (NC Dinos vs KIA Tigers, 2024-09-30, Inning 9 top)
**Certification Status**: **`PASS`** (Level-3 Offline Integration Certified)
**Recovery Architecture Model**: `REPLAY_FROM_START_WITH_IDEMPOTENT_PERSISTENCE`

---

## 1. Executive Summary

Gate R4A certifies the crash recovery and restart resilience of the KBO text relay pipeline against process termination (`os._exit(137)`). Unlike synthetic test pipelines, this remediation strictly re-uses existing production parsing (`RelayCrawler._parse_naver_payload`, `KBOTextParser`), deduplication (`RelayDeduplicator`), and persistence (`save_relay_data`) paths without hardcoded answer tables.

### Core Certified Guarantees
1. **Production Pipeline Re-use**: All event normalizations and DB writes execute through the canonical production codebase.
2. **Zero External Network Requests**: Replay is 100% offline with socket connections blocked (`KBO_SEALED_REPLAY_OFFLINE=1`).
3. **Zero Protected Storage Mutations**: Bit-level SHA-256 of `data/kbo_dev.db` (`4be13e65d30621a8cc5d0134ba92b3fea9d56d3f6379d6b881c52a953cff229a`) is strictly unmutated.
4. **Transaction Boundary Crash Resilience**: Simulated hard termination (`os._exit(137)`) during active transactions (CP4, CP6, CP7) rolls back uncommitted writes cleanly.
5. **Idempotent Revisions**: Repetitive execution of event corrections modifies 0 rows and avoids duplicate `[CORRECTED]` tags.
6. **Dual-Source Participation & Negative Controls**: Dual-source inputs establish `source_used="dual_canonical"`; empty Naver input gracefully yields `source_used="kbo_single"`.

---

## 2. Certified Scope & Disclosures

> [!IMPORTANT]
> **Scope & Provenance Disclosure**
> - **Certified**: Offline sealed snapshot replay, production parser execution, transaction-boundary crash recovery, process lock auto-healing, and idempotent revision application for game `20240930NCHT0`.
> - **Untested**: Long-polling of live active games, multi-day daemon execution, and direct writes to production Oracle databases.
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

---

## 5. Artifact & Provenance Files

- `README.md`: This certification report.
- `tested-code-manifest.json`: Full manifest of tested code, test suites, and sealed fixtures.
- `crash-injection-ledger.jsonl`: Machine-readable ledger of all 7 crash and restart executions.
- `checkpoint-state-ledger.jsonl`: Complete audit trail of checkpoint transitions.
- `recovery-convergence-diff.json`: Field-by-field diff validating exact convergence to golden baseline.
- `negative-control-ledger.json`: Structured verification of negative controls and idempotency.
- `domain-invariants-r4a.json`: Formal pass/fail evaluation of all 8 invariants.
- `protected-db-before-after.json`: Cryptographic proof of zero protected database mutation.
- `fixtures/kbo_sealed_dom_nodes_20240930NCHT0.json`: 41 sealed KBO DOM leaf nodes.
- `fixtures/naver_sealed_payload_20240930NCHT0.json`: 39 sealed Naver relay options.
- `SHA256SUMS`: Cryptographic checksums of all evidence files.
- `checksum-verification.txt`: Output of independent verification (`shasum -a 256 -c SHA256SUMS`).
