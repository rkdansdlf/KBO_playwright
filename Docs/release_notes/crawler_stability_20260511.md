# Crawler Stability Release Notes - 2026-05-11

## Summary
- Stabilized the schedule -> detail -> relay -> OCI publish path for completed KBO games.
- Prevented incomplete completed-game detail payloads from being saved or published as success.
- Added deterministic regression gates, opt-in live smoke, structured daily stability summaries, and summary-driven retry commands.

## Release Contents
- **Schedule/detail handoff:** schedule rows are validated before save/detail handoff, and cancelled/postponed/future games are excluded from full-detail collection.
- **Game detail crawler:** section navigation now uses shared compliance/delay/retry handling, HITTER/PITCHER direct tables fall back to REVIEW extraction, roster map player-id recovery is preserved, and incomplete completed-game payloads are marked `incomplete_detail`.
- **Relay/PBP recovery:** relay source matching is validated by date/team/doubleheader, malformed rows are filtered before save, and empty completed-game relay payloads are not treated as success.
- **OCI publish guard:** parent game, detail child datasets, and relay/PBP datasets are evaluated separately so schedule-only, incomplete detail, empty relay, and cancelled rows are skipped deliberately.
- **Operations:** `run_daily_update` writes `logs/daily_update_summary/YYYYMMDD.json`, `retry_daily_failures` converts summary retry candidates into dry-run/apply commands, and `crawler_release_check.sh` wraps the deterministic gate plus opt-in live smoke.

## Verification Commands
```bash
./scripts/verification/crawler_stability_gate.sh
./scripts/verification/crawler_release_check.sh
git diff --check
```

Optional live smoke, after choosing a known completed KBO date:
```bash
KBO_LIVE_SMOKE=1 KBO_LIVE_SMOKE_DATE=YYYYMMDD ./scripts/verification/crawler_release_check.sh
```

## Operational Commands
```bash
./.venv/bin/python -m src.cli.run_daily_update --date YYYYMMDD --sync
./.venv/bin/python -m src.cli.retry_daily_failures --date YYYYMMDD --dry-run
./.venv/bin/python -m src.cli.retry_daily_failures --date YYYYMMDD --apply --sync
```

## Rollback And Recovery
- If release validation fails before deploy, do not run live smoke or daily finalize; inspect the failing gate output first.
- If a production finalize run reports `incomplete_detail`, wait for KBO GameCenter tables to publish and rerun `retry_daily_failures --dry-run` before applying.
- If OCI reports `skipped_empty_relay`, rerun relay recovery through `retry_daily_failures --apply --sync` or `scripts/fetch_kbo_pbp.py --game-ids ... --force`.
- If live smoke fails but deterministic gate passes, treat it as source/network evidence and do not roll back local code until the source page/API has been inspected.

## File Hygiene Notes
- Keep as intentional new release assets:
  - `scripts/verification/crawler_stability_gate.sh`
  - `scripts/verification/crawler_live_smoke.sh`
  - `scripts/verification/crawler_release_check.sh`
  - `src/cli/retry_daily_failures.py`
  - `src/cli/crawler_live_smoke.py`
  - `src/utils/schedule_validation.py`
  - `tests/test_crawler_*`, `tests/test_retry_daily_failures.py`, `tests/test_refresh_manifest.py`, `tests/test_schedule_crawler_stability.py`, `tests/test_game_detail_crawler_stability.py`
- Debug/local investigation artifacts should not be included in the release commit unless explicitly needed as fixtures:
  - `inspect_*.py`, `find_boxscore.py`
  - `gamecenter_*.png`, `hitter_direct.png`, `review_page.png`
  - `gamecenter_page.html`
