# Repository Guidelines

## Project Structure & Module Organization
This repository is a Playwright-based KBO data crawler with a two-track pipeline:
- `src/`: Core application code (crawlers, parsers, models, repositories, CLI).
- `scripts/`: Maintenance and batch utilities (crawling, oci, maintenance).
- `tests/`: Pytest tests and debug scripts (`test_*.py` run by default).
- `Docs/`: Runbooks, URL references, limitations, and schemas.
- `migrations/`: Schema migrations (including OCI).
- `data/`, `logs/`: Local SQLite DB and runtime logs.

## Build, Test, and Development Commands
- `python3 -m venv venv && source venv/bin/activate`: Create and activate virtual environment.
- `pip3 install -r requirements.txt`: Install Python dependencies.
- `playwright install chromium`: Install Playwright browser binaries.
- `python3 -m src.cli.crawl_schedule --year 2025 --month 3`: Crawl schedule data.
- `python3 -m src.cli.crawl_game_details --date 20241015` or `python3 -m src.cli.collect_games --year 2024 --month 10`: Collect game details.
- `python3 -m src.cli.crawl_futures --season 2025 --concurrency 3`: Crawl Futures stats.
- `python3 -m src.cli.sync_oci --truncate`: Sync local SQLite to OCI.
- `pytest`: Run the test suite.

## Coding Style & Naming Conventions
- Python, 4-space indentation, type hints encouraged but not required.
- Module names are `snake_case.py`; classes are `CamelCase`.
- Crawler classes live in `src/crawlers/`, parsers in `src/parsers/`, data access in `src/repositories/`.
- Prefer structured payloads (dicts with explicit keys) between crawlers and repositories.

## Testing Guidelines
- Test framework: `pytest`.
- Test files follow `test_*.py` naming in `tests/`.
- Debug utilities (e.g., `debug_*.py`) are not part of the default test run.
- Example: `pytest tests/test_player_profile_parser.py`.

## Commit & Pull Request Guidelines
- Commit history uses short, imperative messages (e.g., “Add ...”, “Implement ...”).
- PRs should include: a clear summary, affected modules, test commands run, and any selector/URL changes.

## Configuration & Secrets
- Use `.env` for `DATABASE_URL`, `OCI_DB_URL`, and request throttling (e.g., `KBO_REQUEST_DELAY_MIN`).
- Crawler stability depends on consistent delays; avoid reducing throttling without review.
