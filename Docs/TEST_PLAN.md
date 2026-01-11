# Parser Test Plan

This plan covers the fixtures and pytest suites required before expanding crawls to every season (1982–present).

## Team Batting Parser
- **Fixture**: capture HTML from `Record/Team/Hitter/BasicOld.aspx` for seasons 1982, 1999, 2024; store under `tests/fixtures/team_batting/{year}.html`.
- **Tests**:
  - `tests/test_team_batting_parser.py` validates header detection across legacy vs modern layouts.
  - Verify cumulative stats (`R`, `TB`, `OPS`) sum correctly and honor team aliases defined in `src/utils/team_mapping.py`.
  - Ensure pagination logic requests every page and logs when totals shift between snapshots (use `monkeypatch` on Playwright page object).

## Team Pitching Parser
- **Fixture**: HTML dumps from `Record/Team/Pitcher/Basic.aspx` for representative eras (e.g., 1985 side-by-side with 2024).
- **Tests**:
  - Confirm ERA/WHIP rounding uses the same helper as player-level stats.
  - Validate bullpen/starting splits if extra columns appear, guarding with fallbacks for missing values.
  - Add regression test for tie situations (when two teams share a rank) to ensure ordering is stable.

## Fielding & Baserunning Enhancements
- Add fixture variants for multi-page responses (Fielding Basic, Baserunning Basic) with odd pagination counts.
- Create `tests/test_fielding_rank_parser.py` and `tests/test_baserunning_parser.py` that assert:
  - pagination loops stop at the last real page even when site renders hidden anchors.
  - stats normalize position labels (e.g., "중견수" -> "CF") before writing to repositories.

## Ranking/Leaderboard Pages
- Build fixtures for ranking endpoints (e.g., Defense rankings already leveraged in `src/crawlers/fielding_stats_crawler.py`).
- Tests must ensure overall rank is saved even if players have incomplete stats, and verify sorting integrity by re-computing the metric inside the test.

## Integration Hooks
- Add `pytest` markers (`@pytest.mark.integration`) to simulate CLI executions:
  - `python -m src.crawlers.team_batting_crawler --year 1995 --series regular`
  - use temporary SQLite DBs (tmp_path) to assert repository writes succeed.
- Wire these tests into CI before allowing scheduler jobs to run the new crawlers.
