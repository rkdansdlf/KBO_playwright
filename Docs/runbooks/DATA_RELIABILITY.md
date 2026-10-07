# KBO Data Reliability Runbook — Crawl Ledger, DLQ, Snapshot Replay

Last updated: 2026-10-03

This runbook covers the data-reliability subsystem: the **crawl execution
ledger**, the **failure taxonomy**, the **dead letter queue (DLQ)** with
automatic retry/recovery, and **offline snapshot replay / validation /
persistence**.

> **Mutation rule** — every write path is double-gated by an explicit
> `--apply` / `--persist` flag **and** a `KBO_ALLOW_*` environment variable.
> Read-only commands never need a guard. When unsure, run **without** the flag:
> the command validates and previews, then exits without writing anything.

---

## 1. Command surface

### 1.1 Read-only (always safe)

```bash
python3 -m src.cli.kbo dlq status   [--json]   # state summary
python3 -m src.cli.kbo dlq stats    [--json]   # per-status counters + oldest due age
python3 -m src.cli.kbo dlq list     [--status {pending,retrying,resolved,exhausted,ignored}]
                                    [--crawler NAME] [--error-code CODE] [--limit N] [--json]
python3 -m src.cli.kbo dlq show     <dlq_id> [--json]   # letter + replay lineage

python3 -m src.cli.kbo snapshot validate [--snapshot-id N | --limit N] [--fail-on-drift] [--json]
python3 -m src.cli.kbo snapshot replay   [--snapshot-id N | --limit N] [--strict] [--json]
python3 -m src.cli.kbo crawl replay      --run-id <RUN-ID> [--json]
```

### 1.2 Guarded mutations

| Command | Flag | Environment | Denied ⇒ |
| --- | --- | --- | --- |
| `kbo dlq retry\|requeue\|ignore <dlq_id>` | `--apply` | `KBO_ALLOW_DLQ_MUTATION=1` | exit 3, no change |
| `kbo crawl replay --run-id <ID>` | `--apply` | `KBO_ALLOW_CRAWL_REPLAY=1` | exit 3, no change |
| `kbo snapshot replay` (ledger run) | `--apply` | `KBO_ALLOW_SNAPSHOT_REPLAY=1` | exit 3, no change |
| `kbo snapshot replay` (domain tables) | `--persist` | `KBO_ALLOW_SNAPSHOT_PERSIST=1` | exit 3, no change |

`--persist --apply` together requires **both** guards; they are validated
*before* any mutation, so a partial write cannot happen on a guard failure.

### 1.3 Exit codes

| Command | Codes |
| --- | --- |
| `kbo dlq retry\|requeue\|ignore` | `0` ok/preview · `1` not found · `2` invalid state / not due · `3` guard denied |
| `kbo crawl replay` | `0` ok · `1` not found · `2` invalid state · `3` guard denied |
| `kbo snapshot replay` | `0` ok · `1` not found · `2` replay error · `3` guard denied · `4` `--strict` failure/skip |
| `kbo snapshot validate` | `0` ok · `1` not found · `2` replay error · `3` drift (only with `--fail-on-drift`) |

---

## 2. Crawl execution ledger

Every tracked crawl records one `crawl_execution_runs` row via
`track_crawl_run` (`src/services/crawl_run_service.py`), finalized as
`success` / `partial` / `failed` with the counters `records_read`,
`records_written`, `records_failed` and a taxonomy `error_code`.

- The service owns the transaction boundary. Callers working against a
  non-default database must pass `session_factory=` so the ledger row lands in
  the same database as the data.
- Replaying a past run is **independent** of the DLQ: `kbo crawl replay` does
  not mutate DLQ state (`kbo dlq retry` does). Use `crawl replay` to reproduce a
  run, `dlq retry` to work the failure queue.

---

## 3. Dead letter queue

### 3.1 State machine

```text
pending ──retry──▶ retrying ──success──▶ resolved            (terminal)
   │                  │
   │                  ├──retryable failure──▶ pending        (next_retry_at set)
   │                  └──budget exhausted───▶ exhausted ──ignore──▶ ignored (terminal)
   └──ignore──▶ ignored (terminal)

requeue (operator override): ignored | exhausted ──▶ pending   (retry_count preserved)
```

Transitions are enforced in `src/services/crawl_dead_letter_state.py`
(`ALLOWED_TRANSITIONS`); an illegal change raises
`InvalidDlqTransitionError` before any row is mutated. `requeue` is the only
operator path that revives a terminal letter, and it deliberately keeps
`retry_count` so the audit trail counts automatic **and** forced attempts.

### 3.2 Backlog triage

```bash
python3 -m src.cli.kbo dlq status --json          # is anything pending/retrying?
python3 -m src.cli.kbo dlq stats  --json          # oldest due age, per-status counts
python3 -m src.cli.kbo dlq list --status pending --limit 20 --json
python3 -m src.cli.kbo dlq show <dlq_id> --json   # error_code, retry_count, replay lineage

export KBO_ALLOW_DLQ_MUTATION=1
python3 -m src.cli.kbo dlq retry   <dlq_id> --apply                     # pending + due only
python3 -m src.cli.kbo dlq requeue <dlq_id> --apply                     # ignored/exhausted → pending
python3 -m src.cli.kbo dlq ignore  <dlq_id> --reason "…" --apply        # no longer reproducible
```

Decision guide:

| Observation | Action |
| --- | --- |
| `pending`, `next_retry_at` in the future | wait — the retry job owns it |
| `pending` and due, transient code (`FETCH_TIMEOUT`, `RATE_LIMITED`, …) | `retry --apply` (or let the job pick it up) |
| `exhausted` but the root cause is fixed | `requeue --apply` |
| `exhausted`/`pending`, root cause permanent (removed page, retired player) | `ignore --reason … --apply` |
| `retrying` and stale (older than `DLQ_STALE_RETRYING_SECONDS`) | recovery job finalizes it; re-check `dlq stats` after the next 30-min tick |

### 3.2b DLQ alerting

Two Prometheus rules cover the queue. Both live in
`monitoring/prometheus/alert_rules_crawler.yml` and read the gauges
`publish_dlq_state_metrics()` refreshes after each worker, recovery and operator
batch.

| Alert | Condition | Severity | First response |
| --- | --- | --- | --- |
| `KboDlqRecoveryStalled` | `kbo_crawl_dlq_stale_retrying_letters > 0` for 1m | critical | a letter outlived the staleness cutoff — recovery itself stopped. Check the `crawl_dead_letter_recovery` job ran, then `dlq list --status retrying` |
| `KboDlqBacklogAgeHigh` | `kbo_crawl_dlq_oldest_due_age_seconds > 86400` for 30m | warning | the drain is not keeping up. `dlq stats` for queue depth, then raise the retry batch limit or fix the failing crawler |

The two answer different questions and need different responses: a *stranded*
letter means a worker died mid-replay, while an *aged* backlog means the worker
is fine and the queue is growing faster than one pass per 10 minutes drains it.

Thresholds, and what they are worth:

- `> 0` for stranded letters is not a tunable. One stranded letter is already a
  stopped recovery; there is no healthy amount of stranded work. A rate-based
  threshold would need a baseline, and the baseline for this condition is the
  thing being investigated.
- The 24h backlog age was derived from the retry schedule
  (`RETRY_SCHEDULE = (60, 300, 900, 3600)` across a 5-attempt budget), **not**
  from production data — none was available when the rule was written (BUG-002).
  Re-measure `kbo_crawl_dlq_oldest_due_age_seconds` over a real week before
  treating it as calibrated.

Three further DLQ series are exported and deliberately unread by any rule —
`kbo_crawl_dlq_letters`, `..._due_letters`, `..._failures_total`,
`..._retry_attempts_total`, `..._retry_outcomes_total`,
`..._recovery_actions_total`. Each is registered as an exemption with a reason
in `tests/monitoring/test_crawler_alert_rules_contract.py`, so adding a new
unread DLQ series fails CI instead of joining the silent ones.

Firing behaviour is asserted, not assumed:
`monitoring/prometheus/tests/crawler_alert_dlq_test.yml` (30s interval, for the
1m rule) and `crawler_alert_dlq_backlog_test.yml` (30m interval, for the 30m
rule). Each has a firing case and quiet cases, including the exact-threshold
boundary. Run either with:

```bash
promtool test rules monitoring/prometheus/tests/crawler_alert_dlq_test.yml
```

### 3.2c Sustained-partial alerting

One rule covers the quietest failure mode in the crawl family: a crawler that
keeps recording `partial` -- some sources failing while others succeed. It is
quiet by construction for the other rules, because `partial` is in
`SUCCESS_STATUSES` (it advances `kbo_crawl_last_success_timestamp`, so
`KboCrawlerNoRecentSuccess` stays healthy) and the surviving sources keep
writing (so `KboCrawlerWriteDrop` usually stays healthy). "One source of three
quietly fails" is exactly that shape.

| Alert | Condition | Severity | First response |
| --- | --- | --- | --- |
| `KboCrawlerSustainedPartial` | `increase(kbo_crawl_runs_total{status="partial"}[24h]) > 0` unless a success increased in the same window, for 30m | warning | a crawler landed runs but none completed. Check that crawler's per-source results; if it enqueues DLQ letters, `python3 -m src.cli.kbo dlq list --status pending` shows what is still failing |

Why the shape is what it is:

- The success side uses `unless`, not `== 0`: a crawler that has never recorded
  a success has no success series, and `increase()` over a missing series yields
  no sample to compare -- the longest-running degradation would be the one the
  rule stays silent for.
- The 24h window follows the daily pipeline cadence (03:00-06:45 KST), the same
  schedule-derived choice as `KboDlqBacklogAgeHigh`. It also gives a daily
  crawler one full cycle of grace: after a single partial day, yesterday's
  success is still inside the window and nothing pages. No production sample was
  available when the rule was written (BUG-001) -- re-measure before treating
  the window as calibrated.
- Dropping `partial` from `SUCCESS_STATUSES` was deliberately **not** done: it
  would reclassify a working crawler as an outage and change the meaning of the
  existing critical. The detection is an added warning instead (BUG-001 in
  `Docs/certification/bug-hunt/BH0_CONTRACTS.md`).

Firing behaviour is asserted, not assumed:
`monitoring/prometheus/tests/crawler_alert_partial_test.yml` (1m interval, for
the 30m rule) covers sustained partials firing, the missing-success-series case,
the `for:` hold-back, and mixed/healthy staying quiet:

```bash
promtool test rules monitoring/prometheus/tests/crawler_alert_partial_test.yml
```

### 3.3 Retry policy

`src/services/crawl_retry_policy.py`:

- backoff schedule `RETRY_SCHEDULE = (60, 300, 900, 3600)` seconds;
- `DEFAULT_MAX_RETRIES = 5` (a letter may carry its own budget); the schedule
  index is clamped, so a larger budget repeats the final delay instead of
  raising;
- only codes in `RETRYABLE_CODES` are retried — a non-retryable code goes
  straight to the terminal path. Do **not** widen this set casually.

### 3.4 Coverage caveat (important)

An empty DLQ does **not** mean "no failures". Dead letters are enqueued only by:

- `src/crawlers/award_crawler.py`
- `src/crawlers/roster_transaction_crawler.py`
- `src/crawlers/schedule_crawler.py`
- `src/crawlers/team_history_crawler.py`
- `src/crawlers/player_movement_crawler.py`
- `src/crawlers/kbo_event_crawler.py`
- `src/crawlers/parking_crawler.py`
- `src/crawlers/food_crawler.py`
- `src/services/game_collection_service.py`

Other crawlers' failures are recorded in the ledger but never enter the DLQ.
`src/crawlers/adoption_matrix.py` reports per-module `dead_letter` ownership —
use it to see which crawlers are actually covered before trusting DLQ silence.

---

## 4. Snapshot replay, validation, and drift

Snapshots are re-parsed from the **content-addressed artifact** recorded at
crawl time — no network access. Raw artifacts are read-only evidence.

### 4.1 Pre-check before trusting any drift result

1. **Evidence root** — artifacts must live under `evidence_root()`
   (`CRAWL_EVIDENCE_DIR`, default `data/crawl_evidence`). Replay refuses any
   stored path outside that root, so a misconfigured root shows up as
   `success=false` for **every** snapshot — not as drift. Verify before
   concluding "the parser broke":

   ```bash
   python3 -c "from src.repositories.crawl_evidence_repository import evidence_root as e; r=e(); print(r, r.exists(), len([p for p in r.rglob('*') if p.is_file()]))"
   ```

2. **Artifact freshness** — compare the newest artifact mtime with the crawl
   cadence. If nothing has been captured recently, the drift check has nothing
   meaningful to say, and a silent capture outage is the real incident.

### 4.2 Drift

`kbo snapshot validate` compares the re-parsed record count against the
`parsed_records` baseline captured at crawl time — i.e. it detects **parser
regression against unchanged input**.

```bash
python3 -m src.cli.kbo snapshot validate --limit 100 --json
python3 -m src.cli.kbo snapshot validate --snapshot-id 123 --json
python3 -m src.cli.kbo snapshot validate --limit 100 --fail-on-drift   # gate: exit 3
```

Fields: `total`, `with_baseline`, `drifted`, `matched`, `unknown_baseline`,
`failed`, `drifted_ids`, `failed_ids`.

- `unknown_baseline` means the snapshot predates baseline capture — it is
  reported but **never** counted as drift.
- `drifted` means the parser produced a different record count for identical
  bytes → fix the parser, then re-validate.
- `failed` means replay itself could not run (missing artifact, no registered
  parser, path outside the evidence root). Investigate `error` first; drift
  numbers are meaningless while `failed` is high.
- Gate semantics: `ok = drifted <= SNAPSHOT_DRIFT_MAX and failed <= SNAPSHOT_DRIFT_FAIL_MAX`.

### 4.3 Replay and persist

```bash
# read-only: show what the current parser produces
python3 -m src.cli.kbo snapshot replay --snapshot-id 123 --json

# record ledger runs for recent snapshots (ledger only, no domain writes)
export KBO_ALLOW_SNAPSHOT_REPLAY=1
python3 -m src.cli.kbo snapshot replay --limit 50 --apply

# write parsed records into the domain tables — HIGHEST RISK, idempotent upsert
export KBO_ALLOW_SNAPSHOT_PERSIST=1
python3 -m src.cli.kbo snapshot replay --snapshot-id 123 --persist --strict
```

- `--strict` exits `4` when any snapshot fails or is skipped — use it in gates.
- Records are committed **one record per transaction**. A row that violates a
  constraint is counted as `failed` while its clean siblings are still saved,
  and `parse_status` becomes `partial` (not `done`). Re-running is idempotent,
  so a `partial` result is safe to retry.

---

## 5. Scheduled jobs

| Job id | Trigger | Lock | `max_instances` | Notes |
| --- | --- | --- | --- | --- |
| `crawl_dead_letter_retry` | every 10 min | `MAINTENANCE_LOCK` | 1 | works due letters; alerts on `exhausted`/`errored` |
| `crawl_dead_letter_recovery` | every 30 min | `MAINTENANCE_LOCK` | 1 | rescues stale `retrying` letters, finalizes interrupted replay runs |
| `snapshot_drift_check` | daily 06:45 | `MAINTENANCE_LOCK` | 1 | validates `SNAPSHOT_DRIFT_SAMPLE_LIMIT` snapshots; opens a single `drift:snapshot` incident |

The three jobs are registered in `src/scheduler/registry.py`; confirm registration
in `logs/scheduler.launchd.err.log` (`Registered job: crawl_dead_letter_retry`).

---

## 6. Behaviour during a database outage

Observed on 2026-10-03 (remote PostgreSQL unreachable for several hours while
the scheduler stayed up). Expected behaviour, not a bug list:

- **Advisory locks degrade to local file locks** —
  `Failed to acquire PostgreSQL advisory lock …, falling back to local lock`.
  Cross-host mutual exclusion is therefore *lost* during an outage: run the
  scheduler on exactly one host.
- **The reliability jobs do not pile up** — `max_instances=1` makes APScheduler
  skip an overlapping run (`maximum number of running instances reached (1)`).
- **The reliability jobs fail fast when the database is dead.** `crawl_dead_letter_retry`,
  `crawl_dead_letter_recovery`, and `snapshot_drift_check` probe the database with a short
  connect timeout (`DB_PROBE_CONNECT_TIMEOUT_SECONDS`, default 3s) **before** taking
  `MAINTENANCE_LOCK` and return early when it does not answer. Without the gate a single
  job spent ~150s on connect retries *while holding the lock* (measured 2026-10-03:
  `@retry(wait_exponential(min=120))` on top of the connect timeout), and other
  maintenance jobs were skipped after their bounded 60s lock wait. The gate is silent by
  design: it does not raise, so the job's tenacity retry and `alert_failure` callback stay
  quiet — the Prometheus `kbo_db_available` gauge owns the alerting.
- **Jobs without the gate still hold the lock while connecting.** Most maintenance jobs
  keep the older behaviour: they block on connect and other jobs sharing their tier lock
  skip via the bounded timeout.
- **A job blocked on connect still holds `MAINTENANCE_LOCK`** when it has no gate, so
  unrelated maintenance jobs skip via the bounded lock timeout (default 60s) and log
  `lock_skip` warnings. This is the designed fail-fast path, not a crash.
- **Alerting keeps working** — Telegram/Slack delivery is HTTP and independent
  of the database.
- **Expect a lot of noise** in `logs/scheduler.launchd.err.log`
  (`connection to server … Operation timed out`).

Diagnostics (all read-only):

```bash
python3 scripts/diagnose_scheduler_locks.py         # exit 0 clean, 1 stale lock / duplicate scheduler
python3 -m src.cli.apply_postgres_migrations --check # exit non-zero while migrations are pending
```

During an outage `diagnose_scheduler_locks.py` reports locks "held by a live
PID" — that is normal, not a stale lock. DB reachability itself is covered by the
Prometheus `db_availability` collector and the alert rules under `monitoring/`.

---

## 7. Environment reference

| Variable | Default | Purpose |
| --- | --- | --- |
| `KBO_ALLOW_DLQ_MUTATION` | unset | opt-in for `dlq retry/requeue/ignore --apply` |
| `KBO_ALLOW_CRAWL_REPLAY` | unset | opt-in for `crawl replay --apply` |
| `KBO_ALLOW_SNAPSHOT_REPLAY` | unset | opt-in for `snapshot replay --apply` |
| `KBO_ALLOW_SNAPSHOT_PERSIST` | unset | opt-in for `snapshot replay --persist` |
| `CRAWL_EVIDENCE_DIR` | `data/crawl_evidence` | evidence root; replay rejects paths outside it |
| `DLQ_STALE_RETRYING_SECONDS` | `1800` | a `retrying` letter older than this is rescued |
| `DLQ_RUN_STALE_SECONDS` | `3600` | an interrupted replay run older than this is finalized |
| `SNAPSHOT_DRIFT_SAMPLE_LIMIT` | `100` | snapshots validated per drift run |
| `SNAPSHOT_DRIFT_MAX` | `0` | drifted snapshots tolerated before the gate fails |
| `SNAPSHOT_DRIFT_FAIL_MAX` | `5` | failed replays tolerated before the gate fails |

---

## 8. Verification

```bash
# read-only sanity: the CLI surface exists and parses
python3 -m src.cli.kbo dlq --help
python3 -m src.cli.kbo snapshot validate --help

# targeted tests
nice -n 19 ./venv/bin/python -m pytest tests/services/test_snapshot_persist.py \
    tests/services/test_crawl_run_service.py tests/scheduler/test_snapshot_drift_job.py -q
```
