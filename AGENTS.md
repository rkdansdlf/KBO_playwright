# Repository Guidelines

## Project Structure & Module Organization
This repository is a Playwright-based KBO data crawler with a two-track pipeline:
- `src/`: Core application code (crawlers, parsers, models, repositories, CLI).
- `scripts/`: Maintenance and batch utilities (crawling, supabase, maintenance).
- `tests/`: Pytest tests and debug scripts (`test_*.py` run by default).
- `Docs/`: Runbooks, URL references, limitations, and schemas.
- `migrations/`: Schema migrations (including Supabase).
- `data/`, `logs/`: Local SQLite DB and runtime logs.

## Build, Test, and Development Commands
- `pip install -r requirements.txt`: Install Python dependencies.
- `playwright install chromium`: Install Playwright browser binaries.
- `python -m src.cli.crawl_schedule --year 2025 --month 3`: Crawl schedule data.
- `python -m src.cli.crawl_game_details --date 20241015` or `python -m src.cli.collect_games --year 2024 --month 10`: Collect game details.
- `python -m src.cli.crawl_futures --season 2025 --concurrency 3`: Crawl Futures stats.
- `python -m src.cli.sync_supabase --truncate`: Sync local SQLite to Supabase.
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
- Use `.env` for `DATABASE_URL`, `SUPABASE_DB_URL`, and request throttling (e.g., `KBO_REQUEST_DELAY_MIN`).
- Crawler stability depends on consistent delays; avoid reducing throttling without review.
