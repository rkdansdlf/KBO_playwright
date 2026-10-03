# KBO Notification Runbook — Incident Ledger, Delivery Audit, Alert Rules

Last updated: 2026-10-04

This runbook covers the notification path: the **incident ledger**
(`notification_incidents`), the **delivery audit** (`notification_deliveries`),
the **Prometheus alert rules** that watch both, and the **retention job** that
prunes them.

It matters more than a normal runbook because this subsystem is the delivery
path for every other subsystem's alerts. When notification is down, the rest of
the platform goes quiet — a failure that looks identical to "nothing is wrong".
Treat "no alerts" as a symptom to verify, not as evidence of health.

> **Mutation rule** — there is no guarded-write surface here. Everything in this
> runbook is **read-only** except Section 4.2, which is a last resort and is
> labelled as such. The normal way an incident closes is that the check which
> opened it runs again, passes, and reports recovery itself.

---

## 1. Command surface

### 1.1 What exists

```bash
# Send an ad-hoc message through the configured transports.
python3 -m src.cli.kbo notify --channel telegram --title "Title" --body "Body"

# Inspect the alert rules and prove they still parse and still fire.
promtool check rules monitoring/prometheus/alert_rules_notifications.yml
promtool test  rules monitoring/prometheus/tests/notification_alert_delivery_test.yml

# Prove the transport boundary and the package layering still hold.
python3 scripts/lint_alert_transport_bypass.py
python3 scripts/lint_notification_layering.py
```

The metrics exporter is started by `start_metrics_server(port)`
(`src/utils/metrics.py`) and scraped by Prometheus from `scheduler:8000`
(`monitoring/prometheus/prometheus.yml`).

### 1.2 What does **not** exist (important)

There is **no CLI to list, acknowledge, or close incidents**, and no CLI to read
the delivery audit. That is a genuine operational gap, not an oversight in this
document. Consequences:

- Triage is done with SQL (Sections 3–4) and with the metrics in Section 5.
- An incident closes when its originating check recovers
  (`bridge.apply_incidents`, Section 4.1). There is no "close button".
- `kbo notify` sends a message; it does not touch the ledger.

Do not assume a missing incident means a healthy one — check
`occurrence_count` and `notification_count` (Section 3.2).

---

## 2. How an alert reaches a human

```
call site (CLI job / service / crawler)
        │
        ├─ stateful alert ──► AlertPublisher        src/notifications/publisher.py
        │                       └─ IncidentManager  src/notifications/incident.py
        │                            └─ NotificationIncident   (ledger row, unique incident_key)
        │
        └─ stateless message ─► NotificationDispatcher  src/notifications/dispatcher.py
                                   (no ledger row)

both paths ──► transport            src/utils/alerting.py   (Telegram / Slack / GenericWebhook)
           ──► DeliveryRecorder     src/notifications/recorder.py
                   └─ NotificationDelivery   (audit row, own short transaction)

makes it visible ──► Prometheus (kbo_notification_*) ──► Prometheus alert rules
                                                        ──► Alertmanager ──► Telegram
```

Two properties are load-bearing and must not be broken:

1. **The delivery audit owns its own transaction.** A successful external send is
   a real side effect, so its audit row must survive even if the caller's
   business transaction rolls back. Audit failures are swallowed, counted, and
   logged — they must never turn a successful send into a failure.
2. **`SUPPRESSED` is not written to the audit.** Only `SENT`, `FAILED`,
   `SKIPPED_UNCONFIGURED`, and `DRY_RUN` are recorded. A severity below
   `ALERT_MIN_SEVERITY` (default `WARNING`) is suppressed by policy, so its
   absence from the audit is expected, not a bug.

---

## 3. Triage: "alerts are not arriving"

Work outward from the transport, then inward to the ledger. Each step rules out
one layer.

### 3.1 Step 1 — is anything being dispatched?

```bash
curl -s localhost:8000/metrics | grep '^kbo_notification_'
```

```promql
# Attempts and their outcomes by channel, last 15 minutes.
sum by (channel, status) (rate(kbo_notification_dispatch_total[15m]))
```

| Observation | Meaning |
| --- | --- |
| `status="FAILED"` rising | The transport is reachable but rejecting. Go to Section 3.3 for `error_code`. |
| `status="SKIPPED_UNCONFIGURED"` | The channel has no credentials. Expected in dev; a misconfiguration in production. |
| `status="DRY_RUN"` only | A dry-run flag is set somewhere in the call path. |
| No series at all | Nothing is calling the notification path, or the exporter is not being scraped. Check `kbo_notification_delivery_audit_failures_total` and the scrape target first. |

`kbo_notification_dispatch_failures_total` counts failures only; the ratio rule
uses `dispatch_total{status="FAILED"}` so numerator and denominator share one
series. Prefer the ratio when reasoning about health.

### 3.2 Step 2 — is the ledger accumulating?

```sql
-- Everything not yet recovered, worst first.
SELECT severity, state, source, component, incident_key,
       occurrence_count, notification_count,
       first_opened_at, last_seen_at, last_notified_at
FROM notification_incidents
WHERE state <> 'RECOVERED'
ORDER BY severity DESC, last_seen_at DESC;
```

```promql
# What the alert rules actually read.
sum by (severity) (kbo_notification_open_incidents)
```

Read it as follows:

- `occurrence_count` climbing with a low `notification_count` means the event is
  being seen but **suppressed** — cooldown or `ALERT_MIN_SEVERITY`. This is by
  design, not a delivery failure.
- `notification_count` climbing means the pipeline is working and your problem
  is downstream (transport or chat routing).
- **No rows at all** while you expect alerts means the call site never published
  an event. Check the call site, not the notification stack. Since the pipeline
  converged, a call site that still imports `src.utils.alerting` directly
  bypasses the ledger entirely — see Section 7.

### 3.3 Step 3 — what did delivery actually do?

```sql
-- Outcome mix, last 7 days.
SELECT channel, status, count(*)
FROM notification_deliveries
WHERE dispatched_at > now() - interval '7 days'
GROUP BY channel, status
ORDER BY channel, status;

-- Recent failures with the transport's own reason.
SELECT channel, destination, error_code, left(error_message, 160) AS reason,
       dispatched_at, latency_ms
FROM notification_deliveries
WHERE status = 'FAILED' AND dispatched_at > now() - interval '7 days'
ORDER BY dispatched_at DESC
LIMIT 50;

-- One incident's delivery history (fan-out shares a batch_id).
SELECT batch_id, channel, status, attempt_count, dispatched_at, error_code
FROM notification_deliveries
WHERE incident_id = <incident_id>
ORDER BY dispatched_at DESC;
```

`kbo_notification_delivery_audit_failures_total` is the only signal that
distinguishes "delivery failed" from **"we could not even write the audit row"**.
`DeliveryRecorder` swallows persistence errors by design, so an unwritable audit
looks like missing rows. If that counter is rising, investigate the database, not
the transport.

### 3.4 Step 4 — audit unwritable?

```promql
increase(kbo_notification_delivery_audit_failures_total[15m])
```

Any non-zero value here is a database problem. A known cause is a SAVEPOINT-based
insert on SQLite: pysqlite's default transaction handling does not make a
`SAVEPOINT` participate in the outer transaction, so a row survives a rollback.
The insert path therefore uses dialect-native insert-if-absent and the
`begin_nested()` fallback is **forbidden repository-wide**
(`tests/notifications/test_incident_manager.py` is the regression that catches a
reintroduction).

---

## 4. Triage: "an incident stays open"

### 4.1 Normal recovery (the only supported path)

Recovery is reported by the check that opened the incident, through
`bridge.apply_incidents`. Two modes:

| Mode | Use for |
| --- | --- |
| `resolve_keys=[key]` | A specific check that now passes. Used by `freshness_gate` (`freshness:gate`) and `sqlite_integrity_guard` (per-database keys). |
| `reconcile_prefix="<ns>:"` | A recurring threshold sweep where a passing run simply reports fewer keys. |

**Prefer `resolve_keys`.** A prefix reconcile resolves *every* active incident
under the namespace that is absent from the batch, which is how a check can
resolve a different subject than the one it just verified. `sqlite_integrity_guard`
uses explicit keys for exactly this reason.

If an incident will not close:

1. Confirm the check is actually running (`grep` the scheduler log for the job).
2. Confirm it is passing. A check that no longer runs cannot report recovery, and
   its incident will sit `OPEN` forever.
3. Confirm the key matches exactly. A renamed key orphans the old incident.

```bash
# Which check owns this key?
grep -rn "<incident_key>" src/ | head
```

### 4.2 Last resort: closing an incident by hand

There is no CLI for this. Do it only after the root cause is fixed, otherwise the
next run reopens it and the incident history becomes noise.

```sql
-- DANGEROUS: writes to the ledger. Fix the cause first.
UPDATE notification_incidents
SET state = 'RECOVERED', resolved_at = now()
WHERE incident_key = '<key>' AND state <> 'RECOVERED';
```

`ACKNOWLEDGED` exists and stops re-notification until the severity escalates, but
it is only reachable in code (`IncidentManager.acknowledge`). If you need it from
the operator side, that is a feature request against Section 1.2, not a SQL
recipe to improvise.

### 4.3 Too much noise

Re-notification is throttled per severity, not per incident:

| Severity | Default cooldown | Override |
| --- | --- | --- |
| `CRITICAL` | 5 min | `ALERT_COOLDOWN_CRITICAL_SECONDS` |
| `ERROR` | 10 min | `ALERT_COOLDOWN_ERROR_SECONDS` |
| `WARNING` | 30 min | `ALERT_COOLDOWN_WARNING_SECONDS` |
| `INFO` | 6 h | `ALERT_COOLDOWN_INFO_SECONDS` |

`ALERT_MIN_SEVERITY` (default `WARNING`) drops everything below the floor to
`SUPPRESSED`. Raising it is the blunt instrument; raising a specific cooldown is
usually the right one. Both are read at call time, so a change takes effect on the
next dispatch without a restart.

---

## 5. Prometheus alert rules

Rules live in `monitoring/prometheus/alert_rules_notifications.yml` (group
`kbo_notification_alerts`), loaded via `rule_files` in `prometheus.yml` and
mounted in both compose files. A rule file that is not mounted never loads.

| Alert | Severity | Fires when | First response |
| --- | --- | --- | --- |
| `NotificationDeliveryFailureRateHigh` | warning | A channel fails > 20 % of its dispatches over 15 min **and** has ≥ 5 attempts in that window | Section 3.3 — read `error_code`, check credentials/network for that channel |
| `CriticalIncidentDeliveryFailed` | critical | A `CRITICAL` incident is open **and** any dispatch failed in the last 15 min | The page never arrived. Check the transport first, then Section 4 |

The volume guard on the first rule is deliberate: a ratio alone pages on one
failed send against one attempt on a quiet channel. The second rule requires the
open `CRITICAL` incident because delivery failure alone is too noisy to page on.

Alertmanager routes `severity: critical` to `kbo-alerts-critical` and everything
else to `kbo-alerts-default` (`monitoring/alertmanager/alertmanager.yml`). Both
receivers currently use the same Telegram credentials, so the split is a routing
seam, not a different destination.

Changes to a rule must survive both gates:

```bash
promtool check rules monitoring/prometheus/alert_rules_notifications.yml
promtool test  rules monitoring/prometheus/tests/notification_alert_delivery_test.yml
python3 -m pytest tests/monitoring/test_notification_alert_rules_contract.py -q
```

The contract test also fails if a referenced `kbo_notification_*` metric no
longer exists, if an exported notification metric is neither read by a rule nor
listed with a reason, or if a rule has no firing fixture. A renamed metric would
otherwise leave a rule permanently silent.

---

## 6. Scheduled jobs and retention

| Job | Schedule (KST) | Lock | Purpose |
| --- | --- | --- | --- |
| `notification_retention_weekly` | Sun 03:00 | `MAINTENANCE_LOCK` | Prune `notification_deliveries` and recovered `notification_incidents` |

The two windows are independent because a delivery is an external side effect
worth keeping even after the incident it announced has closed.

| Variable | Default | Applies to |
| --- | --- | --- |
| `NOTIFICATION_DELIVERY_RETENTION_DAYS` | 90 | `notification_deliveries` |
| `NOTIFICATION_INCIDENT_RETENTION_DAYS` | 30 | Recovered incidents only |

Only incidents in state `RECOVERED` **with** a `resolved_at` are pruned. An
`OPEN` incident is never deleted by age, so a check that stopped running leaves a
permanent row — which is why Section 4.1 step 2 exists.

Verify a run:

```bash
grep "Notification Retention Completed" logs/scheduler.launchd.err.log | tail -3
```

```sql
-- Growth check: a healthy ledger should not grow without bound.
SELECT count(*) FILTER (WHERE state <> 'RECOVERED') AS active,
       count(*) FILTER (WHERE state =  'RECOVERED') AS recovered,
       min(first_opened_at) AS oldest
FROM notification_incidents;

SELECT count(*) AS deliveries, min(dispatched_at) AS oldest
FROM notification_deliveries;
```

Scheduler process control (launchd, label `com.kbo-playwright.scheduler`):

```bash
launchctl print   gui/$(id -u)/com.kbo-playwright.scheduler | head -20
launchctl kickstart -k gui/$(id -u)/com.kbo-playwright.scheduler
```

Before restarting, confirm only one instance is live — a second scheduler
contends for the same tier locks. `python3 scripts/diagnose_scheduler_locks.py`
exits `0` when clean and `1` on a stale lock or duplicate process.

---

## 7. Boundary lint failures

Two custom lints guard this subsystem. Both fail the commit and the CI lint job,
so a failure means someone routed around the pipeline.

| Lint | Blocks | Fix |
| --- | --- | --- |
| `scripts/lint_alert_transport_bypass.py` | Application code importing `src.utils.alerting` directly, which bypasses incident lifecycle, dedup, cooldown, severity routing and delivery metrics | Publish an `AlertEvent` through `AlertPublisher` (stateful) or `NotificationDispatcher` (stateless). Only `src/utils/alerting.py` and `src/notifications/dispatcher.py` may import the transport. |
| `scripts/lint_notification_layering.py` | Layer inversions inside `src/notifications/`, plus the transport and service boundaries | Import downward. See the rank table below. |

Package layers (higher may import lower, never the reverse):

| Rank | Modules |
| --- | --- |
| 0 | `alert_dto`, `dto` (pure contracts) |
| 1 | `formatter`, `policy` |
| 2 | `incident`, `recorder`, `retention` (state) |
| 3 | `dispatcher` |
| 4 | `publisher` (composition root) |
| 5 | `bridge`, `standalone` |

Two boundaries are frozen: the transport (`src/utils/alerting.py`) may import
only the pure contracts (`alert_dto`, `policy`), and
`src/services/notification_service.py` must delegate delivery rather than import
the ledger (`incident`). A new module without a declared rank fails, so it cannot
silently join at the wrong level.

The transport bypass lint has a shrinking `GRANDFATHERED` set. It must empty by
**removing the last real violation**, not by clearing the list: the list is only
safe to delete once the repository has zero actual bypasses, which
`lint_alert_transport_bypass.py` itself reports.

---

## 8. During a database outage

The notification path is database-backed (ledger and audit), so an outage
degrades it in a specific and dangerous way.

1. **Delivery still succeeds; the audit does not.** `DeliveryRecorder` contains
   persistence errors, so messages go out while `notification_deliveries` stays
   empty and `kbo_notification_delivery_audit_failures_total` climbs. Do not read
   an empty audit as "nothing was sent".
2. **No new incidents.** `IncidentManager` needs the database, so events cannot be
   opened. Whether the alert still reaches a human depends on the call site's
   failure handling, which is why the transport bypass lint matters during an
   outage specifically.
3. **The retention job blocks while holding `MAINTENANCE_LOCK`.** During an
   outage, run a single scheduler host: PostgreSQL advisory locks fall back to
   local file locks, so two hosts are not mutually excluded.
4. **Wait for the database, then reconcile.** Re-run the owning checks so they
   report recovery through `apply_incidents` (Section 4.1). Do not mass-close
   incidents by hand — the ones whose checks still fail would immediately reopen
   and the history would be lost.

For crawl-ledger and DLQ behaviour during the same outage, see
`Docs/runbooks/DATA_RELIABILITY.md` §6.
