# KBO Crawler Operational Guide

This guide provides instructions for managing and troubleshooting the daily data pipeline.

## 1. Environment Variables

Throttling and stability can be tuned via `.env`:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KBO_REQUEST_DELAY_MIN` | `1.5` | Minimum delay (seconds) between requests. |
| `KBO_REQUEST_DELAY_MAX` | `2.5` | Maximum delay (seconds) between requests. |
| `SLACK_WEBHOOK_URL` | - | Webhook URL for critical failure alerts. |
| `NOTIFY_SUCCESS` | `0` | Set to `1` to receive Slack alerts on successful job completion. |

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

## 3. Monitoring

- **Logs:** Located in `logs/scheduler.log`. These are automatically rotated (max 10MB, 5 backups).
- **Alerts:** Critical errors in the scheduler are sent to the configured Slack webhook after 3 failed attempts.

## 4. Troubleshooting Common Issues

### Timeout Errors
- **Symptom:** `❌ Basic2 크롤링 중 오류: Timeout 30000ms exceeded.`
- **Action:** The system now includes automated retries. If persistent, check your network connection or increase `KBO_REQUEST_DELAY_MIN` to reduce load on the KBO site.

### Statistical Mismatches
- **Symptom:** Quality Gate fails with `Transactional PA > Cumulative PA`.
- **Action:** Usually caused by KBO site's internal sync delay. Re-running `run_daily_update` for that date after a few hours often resolves the issue.
