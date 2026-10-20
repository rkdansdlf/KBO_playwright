# Notification Incident Recovery — Design Draft

Last updated: 2026-10-20

Status: **DRAFT — nothing here is implemented.** The runbook
(`Docs/runbooks/NOTIFICATIONS.md`, §1.4) describes the behaviour that exists
today; this note is the design space for the gap it lists first.

## 1. What already works

- Incidents open and clear on their own. `bridge.apply_incidents` resolves the
  keys it is told have recovered (`resolve_keys=`, Section 4.1 of the runbook),
  so the normal lifecycle needs no operator. `kbo incidents ack|resolve` are a
  correction path for a check that stopped running or for a false positive.
- The ledger keeps `last_seen_at`, `occurrence_count` and `notification_count`.
- Two CLI columns are already **derived** from durable values rather than
  stored: `NOTICE` (`notified` / `partly` / `silent`) and `SILENCED_BY`
  (`floor` / `acknowledged` / `cooldown` / `-`, via
  `src.notifications.incident.classify_suppression`). The reason for deriving
  rather than storing is written up on that function and in `NOTIFICATIONS.md`
  §1.4.

## 2. The gap

If a check stops running, its incident stays `OPEN` forever. Nothing moves:
`occurrence_count` does not rise because the check never calls `process()`, and
the row keeps the `last_seen_at` it had when the check last ran.

From the CLI, "a real problem that is still present" and "a check that stopped
reporting" look identical:

| Observation | Real problem | Check silent |
| --- | --- | --- |
| `state` | `OPEN` | `OPEN` |
| `occurrence_count` | unchanged | unchanged |
| `last_seen_at` | old | old |
| `SILENCED_BY` | `-` once the cooldown lapses | `-` |

`SILENCED_BY` does not help here. It answers "would the *next* occurrence be
suppressed", computed from `severity`, `state` and `last_notified_at` — a
different question from "is anything still happening". `last_seen_at` is
durable but the CLI never derives an age from it, so the operator does the
arithmetic by eye.

## 3. Options

### A. Auto-resolve stale incidents — rejected

Close `OPEN` incidents whose `last_seen_at` is older than a threshold.

Rejected because it manufactures a recovery that never happened:

- `RECOVERED` means a check reported the condition cleared
  (`apply_incidents(resolve_keys=...)`). A timeout is not a recovery.
- If the check comes back and the problem is still there, the ledger gets a
  fresh incident and the history splits at the timeout, so the duration and
  occurrence count of one long outage are lost.
- The Phase 108 lesson applies in reverse: a state change nobody observed is
  worse than a visible unknown.

### B. Derive staleness and show it — recommended

Add a derived `AGE`/`STALE` surface and a `list --stale` filter, following the
`SILENCED_BY` precedent:

- No schema change, no migration on three dialects, and no backfill problem:
  the input (`last_seen_at`) is already durable.
- The threshold stays a policy, re-evaluated on read. Raising or lowering
  `INCIDENT_STALE_HOURS` changes which rows read `stale` without rewriting a
  row — the same reason `SILENCED_BY` is computed rather than stored.
- No state transition. The operator decides; the CLI only makes the question
  askable.
- It composes with what exists: `list --state OPEN --stale` becomes the first
  command to run when a check is suspected dead.

Sketch (not implemented):

| Piece | Shape |
| --- | --- |
| Threshold | `INCIDENT_STALE_HOURS` (env, conservative default such as 24) |
| Derivation | `_incident_staleness(incident, now)` next to `_delivery_notice_state` in `src/cli/incidents.py` |
| Column | `STALE` in `_COLUMNS` (`-` fresh / `stale`), `age_hours` in `--json` |
| Filter | `--stale` on `list`, alongside the existing `--notice silent` |
| Tests | derivation unit tests (boundary at the threshold), filter contract, JSON shape, env parsing |
| Runbook | §1.4 loses the item, §3 gains "is the check still running?" |

The threshold is an approximation on purpose: the CLI does not know each
check's cadence, so it must not act on the answer — only surface it.

### C. Watch for silent checks — follow-up, different problem

`last_seen_at` only ages an incident that *exists*. A check that never opened
one is invisible to B: there is no row to age. Detecting "the check did not run
at all" needs a last-success timestamp per check, outside the incident ledger
(for example the run-marker files the scheduler already writes for some jobs).
That is where the real root cause lives and it is deliberately out of scope
here.

## 4. What this draft does not change

- `apply_incidents` remains the only normal recovery path.
- `resolve` stays a recovery signal, not a timeout.
- No new table, column, or migration.
