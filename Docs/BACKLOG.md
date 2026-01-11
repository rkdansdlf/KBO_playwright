# Backlog

## Adaptive Rate-Limiting & User-Agent Rotation
- **Goal**: prevent throttling/blocks while crawling hitter/pitcher/relay flows (`src/crawlers/*.py`).
- **Scope**: introduce a shared throttle service (e.g., `src/utils/throttle.py`) to enforce per-host request spacing (default 3s) and jitter, plus a User-Agent pool applied inside `config/browser_config.py`.
- **Tasks**:
  1. Design throttle API usable by sync + async crawlers (context manager or decorator).
  2. Update high-volume crawlers (`player_batting_all_series_crawler.py`, `player_pitching_all_series_crawler.py`, `relay_crawler.py`) to call the throttle and randomize UA per session.
  3. Add configuration knobs via `.env` (e.g., `KBO_REQUEST_DELAY`, `KBO_UA_ROTATION=true`) and document them in `README.md`.
  4. Provide logging hooks to detect when throttling kicks in (so scheduler jobs can alert before hitting site guardrails).

## Robots.txt & Compliance Checks
- **Goal**: ensure long-running crawls remain within koreabaseball.com's published rules.
- **Tasks**:
  1. Automate fetching/parsing `https://www.koreabaseball.com/robots.txt` (store snapshot under `Docs/robots/` with timestamp) before each crawl batch.
  2. Extend `scripts/check_crawl_progress.sh` to abort when a disallow rule touches our target paths.
  3. Add CI/unit test that mocks a restrictive robots file and verifies the crawler driver refuses to run.
  4. Document the compliance workflow (where snapshots live, how to update) in `Docs/CRAWLING_LIMITATIONS.md`.
