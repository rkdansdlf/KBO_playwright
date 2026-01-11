# Repository Guidelines

## Project Structure & Module Organization
Source code lives in `src/`, with `cli/` entry points, `crawlers/` for Playwright automation, `models/` and `repositories/` for SQLAlchemy data access, and `sync/` utilities for Supabase transfers. Operational scripts sit in `scripts/` (grouped by crawling, Supabase, and maintenance helpers), while Docker assets are in `docker/` plus `docker-compose.yml`. Test suites and HTML fixtures live under `tests/`, and SQLite artifacts are stored in `data/` (keep large DB files out of commits). Documentation references sit in `Docs/` for deeper architecture notes.

## Build, Test, and Development Commands
Set up dependencies with `pip install -r requirements.txt` and install browsers via `playwright install chromium`. Use `python -m src.db.engine` or `python init_db.py` to bootstrap schemas, and `python init_data_collection.py` to run the default crawl flow end to end. Focused crawls can be triggered with commands like `python -m src.cli.crawl_game_details --date 20241015` or `python -m src.cli.crawl_futures`. Synchronize to Supabase using `python -m src.cli.sync_supabase --truncate` after exporting `TARGET_DATABASE_URL`. For containerized workflows, run `docker-compose build && docker-compose up -d scheduler` and watch logs with `docker-compose logs -f scheduler`.

## Coding Style & Naming Conventions
Code is Python 3.10+, formatted with 4-space indentation and standard PEP 8 spacing. Keep modules, functions, and variables snake_case; reserve PascalCase for classes (e.g., `RelayCrawler`). Use descriptive typing (`Dict[str, Any]`, `Optional[...]`) and keep async functions explicit when Playwright is involved. Place CLI scripts under `src/cli/` with `if __name__ == "__main__":` entry points, and store shared helpers in `src/utils/`. Favor small, composable functions and guarded network retries to keep crawlers resilient.

## Testing Guidelines
Pytest is the canonical framework. Add deterministic fixtures in `tests/fixtures/` and reference them via pathlib to keep paths portable. Name tests after the parser or crawler they cover (e.g., `test_futures_stats_parser.py`) and mark async cases with `@pytest.mark.asyncio`. Run the whole suite via `pytest` or target suites such as `pytest tests/test_futures_e2e.py -k relay`. New crawler logic should include fixture-backed unit tests plus at least one integration test that simulates a full CLI invocation.

## Commit & Pull Request Guidelines
Recent history uses concise date-like commit messages (`251021`, `251020`). Stick to that convention or extend it with a short suffix (`251021 relay tab fix`). Ensure commits stay focused and lint/tests pass locally. PRs should describe the scope, list primary commands run (e.g., `pytest`, `python init_data_collection.py`), link to any Supabase dashboards or issues touched, and include logs or screenshots when UI scraping changed. Mention new environment variables or migration steps explicitly.

## Security & Configuration Tips
Never commit `.env`, Supabase credentials, or SQLite databases larger than the sample in `data/`. Rotate API keys used for Supabase syncs and keep `TARGET_DATABASE_URL` only in local shells or secure CI secrets. When sharing logs, redact player PII beyond IDs already published by the KBO site.
