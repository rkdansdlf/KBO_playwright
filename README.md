# KBO Playwright Crawler

Korean Baseball Organization (KBO) data collection system using Playwright, with CLI orchestration, SQLite local storage, and OCI PostgreSQL sync.

## Architecture

Two-track data pipeline:
- **Core (src/)**: CLI entrypoints (`src/cli/`), Playwright crawlers (`src/crawlers/`), parsers, SQLAlchemy ORM models, repositories (UPSERT), OCI sync.
- **Scripts (scripts/)**: Batch utilities for crawling, maintenance, verification, and historical backfill.

**Database**: SQLite (local), PostgreSQL (OCI). All save logic uses **UPSERT** for idempotency.

## Quick Start

```bash
python3 -m venv venv && source venv/bin/activate
pip3 install -r requirements.txt
playwright install chromium
cp env.example .env
# Edit .env as needed, then:
python3 -c "from src.db.engine import init_db; init_db()"
```

## CLI Commands

All operational commands use `python3 -m src.cli.*`. See [`AGENTS.md`](AGENTS.md) for the full reference.

```bash
# Daily postgame orchestration
python3 -m src.cli.run_daily_update --date 20251015 --sync

# Manual game detail collection
python3 -m src.cli.collect_games --year 2025 --month 10

# Schedule crawl
python3 -m src.cli.crawl_schedule --year 2025 --month 10

# Futures League stats
python3 -m src.cli.crawl_futures --season 2025 --concurrency 3

# OCI sync
python3 -m src.cli.sync_oci --truncate

# Quality gates
python3 -m src.cli.quality_gate_check --year 2025
python3 -m src.cli.monthly_unified_audit --year 2025
```

## CI/CD Pipeline

14 GitHub Actions workflows under `.github/workflows/`:

| Workflow | Schedule (KST) | Purpose |
|---|---|---|
| `daily_kbo_sync.yml` | 03:00 daily | Postgame finalize, quality report, advanced sync, OCI publish |
| `backfill.yml` | Sun 04:00-06:00 | 6 matrix jobs: missed crawls, stats, SH/SF, advanced stats, player IDs, roster |
| `daily_preview.yml` | 05:00 daily | Pregame preview + live data refresh |
| `weekly_maintenance.yml` | Sun 05:00 | Futures profiles, player enrichment |
| `full_recalculation.yml` | Manual | Full season stat recalc + OCI sync |
| `kbo_automation.yml` | Manual | 8-phase: pregame, live, finalize, freshness, quality, gap, backfill, recalc |
| `test_suite.yml` | CI on push | Ruff lint + pytest (3.12) |
| `periodic_extras.yml` | Monthly 1st | Periodic data sync |

See [`AGENTS.md`](AGENTS.md) for required secrets and composite actions.

## Project Structure

```
KBO_playwright/
├── src/
│   ├── cli/               # CLI entrypoints (65+ commands)
│   ├── crawlers/          # Playwright crawlers (game, player, schedule, etc.)
│   ├── db/                # Engine, session management
│   ├── models/            # 40 ORM model files (game, player, team, etc.)
│   ├── parsers/           # HTML/JSON parsing
│   ├── repositories/      # UPSERT-based data storage
│   ├── services/          # Business logic (matchup, P0 readiness, etc.)
│   ├── sync/              # OCI sync engine
│   ├── aggregators/       # Season stats, rankings, standings computation
│   └── validators/        # Quality gate checks
├── scripts/
│   ├── archived/          # Legacy/unreferenced scripts (preserved for history)
│   ├── crawling/          # Historical batch crawling
│   ├── verification/      # Data quality verification
│   ├── legacy/maintenance/# Active maintenance utilities
│   └── scheduler.py       # APScheduler job definitions
├── .github/
│   ├── workflows/         # 14 CI/CD workflow files
│   └── actions/           # 3 composite actions (python-env, kbo-job-setup, notify)
├── tests/                 # Pytest suite (1400+ tests)
│   ├── factories/         # Model factory helpers (DB + model builders)
│   └── fixtures/          # Static test data (HTML, JSON)
├── migrations/            # Schema migrations (SQLite + OCI)
├── Docs/                  # Runbooks, URL references, schemas
├── data/                  # SQLite database location
└── logs/                  # Runtime logs and reports
```

## Rate Limiting & Compliance

- Centralized `AsyncThrottle` with configurable delay + random jitter
- User-Agent rotation and Playwright stealth injection
- `robots.txt` compliance checks

Configure via `.env`:
| Variable | Default | Description |
|---|---|---|
| `KBO_REQUEST_DELAY_MIN` | `1.5` | Base delay between requests (seconds) |
| `KBO_REQUEST_JITTER` | `0.3` | Random variance |
| `KBO_UA_ROTATION` | `true` | Auto User-Agent rotation |

## Documentation

- **[AGENTS.md](AGENTS.md)**: Full command reference, CI/CD secrets, workflow docs
- **[Docs/](Docs/)**: Runbooks, URL patterns, limitations
- **[Docs/references/COMMAND_REFERENCE.md](Docs/references/COMMAND_REFERENCE.md)**: CLI command guide
- **[Docs/schema/](Docs/schema/)**: Database schema documentation
- **[tests/factories/](tests/factories/)**: Model factory helpers for test development

## License

This project is for educational and research purposes. Please respect KBO's terms of service.
