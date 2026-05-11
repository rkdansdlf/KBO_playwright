# KBO Crawler Operational Guide

This guide provides instructions for managing and troubleshooting the daily data pipeline.

## 1. Environment Variables

Throttling and stability can be tuned via `.env`:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KBO_REQUEST_DELAY_MIN` | `1.5` | Minimum delay (seconds) between requests. |
| `KBO_REQUEST_DELAY_MAX` | `2.5` | Maximum delay (seconds) between requests. |
| `TELEGRAM_BOT_TOKEN` | - | Token for Telegram Bot notifications (Recommended). |
| `TELEGRAM_CHAT_ID` | - | Chat ID to send Telegram notifications to. |
| `SLACK_WEBHOOK_URL` | - | Webhook URL for Slack alerts (Legacy fallback). |
| `NOTIFY_SUCCESS` | `0` | Set to `1` to receive success alerts. |

## 2. Manual Data Recovery

If a specific day fails to crawl or has data inconsistencies:

### Re-run Daily Finalize for a specific date
```bash
./.venv/bin/python3 -m src.cli.run_daily_update --date YYYYMMDD --sync
```

### Run Quality Gate Check
To verify statistical consistency manually:
```bash
./.venv/bin/python3 -m src.cli.quality_gate_check --year 2024
```

### Run Crawler Stability Gate
Run this before crawler/publish releases or after selector, retry, relay, or OCI eligibility changes:
```bash
./scripts/verification/crawler_stability_gate.sh
```

### Run Crawler Release Check
Run the deterministic release gate first:
```bash
./scripts/verification/crawler_release_check.sh
```

Optionally add one live KBO/Naver smoke after choosing a known completed date:
```bash
KBO_LIVE_SMOKE=1 KBO_LIVE_SMOKE_DATE=YYYYMMDD ./scripts/verification/crawler_release_check.sh
```

The live smoke is opt-in only. It reads schedule/detail/relay sources without saving to the database, and `crawler_stability_gate.sh` remains network-free by default.

### Release Readiness Checklist
Run these before promoting crawler stability changes:
```bash
./scripts/verification/crawler_stability_gate.sh
./scripts/verification/crawler_release_check.sh
git diff --check
```

Then review the release notes and untracked debug artifacts before staging:
```bash
cat Docs/release_notes/crawler_stability_20260511.md
git status --short
```

If a known completed KBO date is available, run the opt-in live smoke separately:
```bash
KBO_LIVE_SMOKE=1 KBO_LIVE_SMOKE_DATE=YYYYMMDD ./scripts/verification/crawler_release_check.sh
```

Do not include local investigation files such as `inspect_*.py`, `find_boxscore.py`, `gamecenter_*.png`, or `gamecenter_page.html` in the release commit unless they are promoted to fixtures deliberately.

### Recover an `incomplete_detail` game
`incomplete_detail` means the GameCenter page loaded, but the full-detail payload did not include both teams' hitter and pitcher rows after direct HITTER/PITCHER extraction and REVIEW fallback.

```bash
./.venv/bin/python3 -m src.cli.crawl_game_details --year YYYY --month M --force --concurrency 1
```

If the same game still fails, wait for the KBO box score to finish publishing and rerun the daily finalize command for that date. Do not manually save partial hitter/pitcher payloads as completed detail data.

## 3. Monitoring

- **Logs:** Located in `logs/scheduler.log`. These are automatically rotated (max 10MB, 5 backups).
- **Alerts:** Critical errors in the scheduler are sent to the configured Telegram bot (or Slack as fallback) after 3 failed attempts.
- **Daily summary:** `run_daily_update` prints detail failure reason counts, relay recovery target counts, and OCI publish skip counts at the end of each run. The same stability payload is written to `logs/daily_update_summary/YYYYMMDD.json` and embedded in the refresh manifest.

### Read Daily Stability Summary
```bash
cat logs/daily_update_summary/YYYYMMDD.json
```

Use `stability.detail.failure_game_ids.incomplete_detail` to rerun full-detail recovery targets, and `stability.oci.skip_game_ids.skipped_empty_relay` to rerun relay/PBP recovery targets. The `retry_candidates` section contains the same operational shortlist without requiring operators to remember each failure key.

Success notifications include the compact stability summary when `NOTIFY_SUCCESS=1`, so soft failures such as `incomplete_detail` or `skipped_empty_relay` are visible even when the scheduler job itself exits successfully.

### Retry Daily Soft Failures
Preview retry commands first:
```bash
./.venv/bin/python -m src.cli.retry_daily_failures --date YYYYMMDD --dry-run
```

Execute only after reviewing the targets:
```bash
./.venv/bin/python -m src.cli.retry_daily_failures --date YYYYMMDD --apply
```

Add `--sync` to publish retried game IDs to OCI after all retry commands succeed:
```bash
./.venv/bin/python -m src.cli.retry_daily_failures --date YYYYMMDD --apply --sync
```

This CLI reads `logs/daily_update_summary/YYYYMMDD.json`, retries only `retry_candidates.detail` and `retry_candidates.relay`, and requires `--apply` before it runs any recovery command.

## 4. Failure Reason Reference

| Reason | Scope | Meaning | Action |
| :--- | :--- | :--- | :--- |
| `schedule_empty` | Schedule | The schedule page loaded but no games were extracted. | Retry the monthly schedule crawl after checking the KBO schedule page. |
| `invalid_game_id`, `game_id_date_mismatch`, `missing_stadium` | Schedule | A schedule row failed structural validation. | Inspect the source row before detail recovery; do not hand it to full-detail crawl manually. |
| `schedule_payload_filtered` | Schedule service | A schedule payload was rejected before DB save. | Review the attached validation reason and rerun schedule crawl if the source row changed. |
| `incomplete_detail`, `detail_payload_filtered` | Detail | Completed game detail is missing required hitter/pitcher rows. | Retry full-detail recovery; do not publish partial child datasets. |
| `relay_not_found`, `invalid_relay_match`, `relay_api_error`, `relay_empty` | Relay | Relay source did not produce a trusted match or payload. | Verify date/team/doubleheader matching, then rerun relay recovery. |
| `skipped_filtered`, `partial_relay` | Relay service | Malformed relay rows were filtered; `partial_relay` saved valid rows. | Inspect relay report rows when all rows are filtered or partial data is unexpected. |
| `skipped_schedule_only`, `skipped_incomplete_detail`, `skipped_empty_relay`, `skipped_cancelled` | OCI publish | Dataset-level publish eligibility filtered the game. | Treat as publish filters; repair detail/relay first where applicable. |

## 5. Troubleshooting Common Issues

### Timeout Errors
- **Symptom:** `❌ Basic2 크롤링 중 오류: Timeout 30000ms exceeded.`
- **Action:** The system now includes automated retries. If persistent, check your network connection or increase `KBO_REQUEST_DELAY_MIN` to reduce load on the KBO site.

### Incomplete Detail Payloads
- **Symptom:** `reason=incomplete_detail` or `Detail payload is missing required hitter/pitcher rows`.
- **Action:** Treat this as a retryable missing-detail state. Re-run the target date after the official KBO GameCenter HITTER/PITCHER tables are available. Past-date failures are marked unresolved so auto-healer/backfill can retry later.

### Filtered Schedule Rows
- **Symptom:** `Filtered schedule game` or `Filtered schedule row` with reasons such as `missing_stadium`, `invalid_game_id`, or `game_id_date_mismatch`.
- **Action:** Re-run the monthly schedule crawl first. If the same row remains filtered, inspect the KBO schedule page for malformed links, missing stadium cells, or date/month drift before attempting detail recovery.

### Relay/PBP Recovery Misses
- **Symptom:** relay recovery report rows show `relay_not_found`, `invalid_relay_match`, `relay_api_error`, `skipped_filtered`, or `partial_relay`.
- **Action:** For `invalid_relay_match`, verify the Naver schedule entry matches the KBO date, teams, and doubleheader number. For `skipped_filtered`, inspect the source relay payload because all rows were missing required inning/half/description state. `partial_relay` means valid rows were saved after malformed rows were discarded.

### OCI Sync Skips
- **Symptom:** `sync_oci --game-details --unsynced-only` reports `skipped_schedule_only`, `skipped_incomplete_detail`, `skipped_empty_relay`, or `skipped_cancelled`.
- **Action:** Treat these as publish filters, not transport failures. `skipped_schedule_only` rows should be synced through parent game schedule sync only. For `skipped_incomplete_detail`, rerun detail recovery first. For `skipped_empty_relay`, rerun relay/PBP recovery or verify the source has no PBP.

### Statistical Mismatches
- **Symptom:** Quality Gate fails with `Transactional PA > Cumulative PA`.
- **Action:** Usually caused by KBO site's internal sync delay. Re-running `run_daily_update` for that date after a few hours often resolves the issue.
