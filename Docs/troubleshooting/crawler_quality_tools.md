# Crawler Quality Tools Runbook

This runbook covers the lightweight crawler quality tools used for selector drift checks, crawler failure triage, and compact data quality regression checks.

## Selector Gate

Purpose: validate crawler selector contracts against fixture or live pages before a crawler change is considered safe.

Command:

```bash
python3 -m src.cli.crawler_selector_gate --config Docs/references/crawler_selector_gate.json
```

JSON output:

```bash
python3 -m src.cli.crawler_selector_gate --config Docs/references/crawler_selector_gate.json --json
```

Artifacts:

```bash
python3 -m src.cli.crawler_selector_gate --config Docs/references/crawler_selector_gate.json --output-dir logs/selector_gate
```

Exit codes:

- `0`: all configured targets passed.
- `1`: at least one target failed, usually selector drift, navigation failure, or missing required DOM.

Operational guidance:

- Run this before changing crawler selectors or URLs.
- Prefer `--output-dir` when diagnosing live selector drift so screenshots and JSON artifacts are retained.
- Keep `Docs/references/crawler_selector_gate.json` small and focused on high-value selectors to avoid brittle CI.

## Failure Diagnosis

Purpose: classify crawler logs and suggest recovery commands for common failure modes.

Command with file input:

```bash
python3 -m src.cli.diagnose_crawler_failure logs/daily_update.log
```

Command with stdin:

```bash
python3 -m src.cli.diagnose_crawler_failure < logs/daily_update.log
```

JSON output:

```bash
python3 -m src.cli.diagnose_crawler_failure --json logs/daily_update.log
```

Exit codes:

- `0`: no actionable failure detected.
- Non-zero: one or more failure categories were detected.

Operational guidance:

- Use this first when a scheduled crawler fails and the root cause is unclear.
- Treat the output as triage guidance, not as an automatic repair plan.
- Pair the diagnosis with the narrowest relevant recovery command, then run the data quality regression pack.

## Data Quality Regression Pack

Purpose: run compact database invariants that catch common crawler or parser regressions.

Default database resolution:

- `--database-url` when provided.
- `DATABASE_URL` environment variable.
- `OCI_DB_URL` environment variable.

Command:

```bash
python3 -m src.cli.data_quality_regression_pack
```

Explicit database:

```bash
python3 -m src.cli.data_quality_regression_pack --database-url sqlite:///data/kbo_dev.db
```

JSON output:

```bash
python3 -m src.cli.data_quality_regression_pack --json
```

Exit codes:

- `0`: all invariants passed.
- `1`: at least one invariant failed.

Operational guidance:

- Run after parser, repository, or sync changes.
- Run after targeted repair commands before marking the issue closed.
- Use JSON output for CI or alert routing.

## Suggested CI Placement

Recommended low-cost checks:

```bash
python3 -m src.cli.data_quality_regression_pack --json
python3 -m src.cli.crawler_selector_gate --config Docs/references/crawler_selector_gate.json --json
```

Do not make live selector checks blocking until the selector config is stable enough for CI conditions.
