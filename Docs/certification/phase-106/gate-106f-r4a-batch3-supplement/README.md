# Batch-3 Supplement: R4A Relay Correction Target Identification & Ambiguous Matching Rejection

**Scope**: Source + offline ephemeral SQLite only. No Oracle/staging/production access. No live
collection, polling, or scheduler daemon. No commit/push/deploy/migration performed.

**Placement rationale**: `scripts/certification/phase106/run_gate_r4a_sealed_recovery.py` enumerates
the sealed bundle recursively (`TARGET_DIR.glob("**/*")`) when generating `SHA256SUMS`. Adding any
child path under `gate-106f-r4a-sealed-recovery/` would change the sealed inventory. This supplement
therefore lives as a **sibling** directory. Existing sealed files were **not** modified by this batch;
pre-existing working-tree modifications to the sealed bundle (multi-game runner extension and
regenerated ledgers from another session) were preserved untouched and are **not** claimed here.

## 1. Defect fixed (reproduced in current code)

`SealedSnapshotRelayPipeline._apply_revisions_to_staged` matched staged PBP rows with:

```python
if (pid and r.target_provider_log_id and pid == r.target_provider_log_id) or (
    desc and r.original_description and desc == r.original_description
):
```

Because `or` does not block the right side on ID mismatch, a row with a **different**
`provider_log_id` but the **same** description was accepted. An ambiguity-only guard (reject on
>1 candidates) cannot catch the single-wrong-row case, nor the case where the correct ID row plus a
same-description row are merged into a false "ambiguous" set.

## 2. Policy enforced (both correction and replay paths)

New helper `SealedSnapshotRelayPipeline._match_pbp_for_revision` (`src/services/relay_recovery_engine.py`):

| `target_provider_log_id` state | Matching path | 0 candidates | >1 candidates |
|---|---|---|---|
| Valid non-empty string | ID only, never description fallback | skip with warning (`NO_MATCH_ID`) | `ValueError` (ambiguous) |
| `None` (only genuine absence) | Contracted fallback: `play_description == original_description` | skip with warning | `ValueError` (ambiguous fallback) |
| Empty / whitespace-only / non-string | rejected as `INVALID_ID` | `ValueError`, never normalized to `None` | n/a |

`_apply_revisions_to_staged` now routes every revision through this helper. `INVALID_ID` and both
ambiguity states raise; no-match states skip that revision with a warning and zero mutation.
`apply_event_correction` / `_find_matching_pbp_row` (DB path) already implemented this policy and
were preserved unchanged. No `source_row_index == event_seq` mapping and no composite
batter/inning/outs key were introduced. No transaction ownership or commit/rollback changes.

## 3. Tests added (7, in `tests/test_relay_recovery_r4a.py`)

- `test_replay_revision_id_match_ignores_samedesc_other_row` — exact-ID row chosen although an
  unrelated staged row shares the description.
- `test_replay_revision_id_missing_returns_no_match` — valid but non-matching ID is skipped
  (`NO_MATCH_ID`), never falls back to the unique description match.
- `test_replay_revision_id_genuine_none_fallback_unique` — `None` ID uses the contracted
  description fallback when unique.
- `test_replay_revision_id_genuine_none_fallback_multiple_rejected` — `None` ID with two
  same-description rows raises `ValueError`.
- `test_correction_kbo_single_verifies_mapping_or_rejects` — `kbo_single` (PBP rows without
  `provider_log_id`) corrects nothing: event 3 left uncorrected instead of guessed.
- `test_late_revision_failure_no_partial_application` — a revision with empty-string ID raises
  `ValueError`; earlier committed correction preserved, unrelated event untouched.
- `test_independent_revisions_no_interference` — two revisions on different targets coexist;
  unrelated event untouched.

Pre-existing suites reused unchanged: 25 original R4A tests + 9 batch-1/2 uniqueness tests
(`tests/test_relay_correction_uniqueness.py`, preserved as-is).

## 4. Verification (offline, ephemeral SQLite, sealed fixtures read-only)

- `python3 -m pytest tests/test_relay_recovery_r4a.py tests/test_relay_correction_uniqueness.py -v`
  → **41 passed** (32 R4A incl. 7 new + 9 uniqueness), exit 0. Full log: `batch3-test-output.txt`.
- `python3 -m pytest tests/test_relay_recovery.py tests/services/test_relay_recovery*.py`
  → **138 passed** (no regression in relay service/recovery paths).
- `ruff check` + `ruff format --check` on both touched files → clean.
- Sealed fixtures verified read-only (hashes match sealed checksums):
  - `kbo_sealed_dom_nodes_20240930NCHT0.json`: `5d040108…199eb7c95`
  - `naver_sealed_payload_20240930NCHT0.json`: `aa3a45fd…11d88233ad`
- External network block: `KBO_SEALED_REPLAY_OFFLINE=1` honored by existing tests
  (`test_zero_network_invariance` passes); no live/OCI/protected-DB access performed.
- Baseline commit `6c2470f3`, working tree dirty (external-session files preserved; see §5).
- 2026-09-13 worktree cleanup: R4A rescoped to the certified single game
  (`20240930NCHT0`). The second-game (`20230501LGWO0`) fixtures and registry
  entries were removed from the sealed bundle, runner, and engine; this
  supplement's test log (§4) remains valid as it exercises the single-game
  fixtures only.

## 5. Working-tree note (preservation, not authorship)

At batch start the tree already contained unrelated modifications which were
**not** reverted at the time:
modified sealed-bundle ledgers + `run_gate_r4a_sealed_recovery.py` (multi-game registry work),
modified `tests/test_relay_correction_uniqueness.py` (batch-1/2), and untracked `20230501LGWO0`
fixtures. This batch touches only:
- `src/services/relay_recovery_engine.py` (+98/−15 approx: helper + replay routing)
- `tests/test_relay_recovery_r4a.py` (+367: 7 tests + `datetime` import)
- this supplement directory (new).

2026-09-13 cleanup resolution: the pre-existing multi-game registry work was
**superseded** — R4A is certified SCOPED single-game (`20240930NCHT0` only), so
the `20230501LGWO0` fixtures and all second-game registry entries were removed
(sealed `SHA256SUMS`/`checksum-verification.txt` re-verified 10/10 after removal).
The batch-1/2 `test_relay_correction_uniqueness.py` modifications were already
absorbed into HEAD (`eec206a6`, `1c49fbed`).

## 6. Explicit non-claims

Verified only: source-level ID-priority behavior and the listed offline ephemeral-DB tests.
**Not** newly acquired: full-suite green, Oracle atomicity, long-run operation, production
approval, or carry-over of the prior R4A PASS to the changed code.
