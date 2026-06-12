# Player Name Issue: Diagnostic Report

## Issue Summary
All 2,710 players in the `player_basic` table had "Unknown Player" as their name instead of actual Korean player names. **This has been resolved.**

## Current State (as of 2026-06-08)

```sql
SELECT COUNT(*) FROM player_basic WHERE name LIKE '%Unknown%';
-- Result: 0 (was 2710)

SELECT COUNT(*) FROM player_basic WHERE name = 'Unknown' AND status = 'PSEUDO';
-- Result: 3
```

The 3 remaining "Unknown" records are all `PSEUDO` status placeholders:
- Player IDs: 900970, 901083, 901312
- These players do NOT exist on the KBO website
- Status `PSEUDO` is explicitly excluded from gap reports (`src/cli/gap_report.py:67`)
- They are synthetic records that don't correspond to real KBO players

## Resolution

### Prevention Measures (already in place)
1. **`src/utils/player_validation.py`**: `INVALID_PLAYER_NAMES` set includes "Unknown Player", "Unknown", and variants. `filter_valid_player_payloads()` blocks all of them before save.
2. **`src/repositories/player_basic_repository.py`**: Both `upsert_players()` and `_upsert_one()` call `validate_player_payload()` / `filter_valid_player_payloads()` before any write.
3. **Tests pass**: 6/6 validation tests confirm the guard works.

### Fix Script
```bash
# Re-crawl all players and save correct names
python3 -m src.cli.fix_player_names --crawl --save

# Re-crawl and sync to OCI
python3 -m src.cli.fix_player_names --crawl --save --sync-oci

# Quick test (1 page)
python3 -m src.cli.fix_player_names --crawl --save --max-pages 1
```

### Investigation History
1. Verified issue existed in SQLite (2,710 "Unknown Player" records)
2. Confirmed "Unknown Player" not in source code as default value
3. Checked database schema - no default values
4. Added validation in `player_validation.py` to filter invalid names
5. Players with bad names were re-crawled and replaced via upsert
6. 3 remaining `PSEUDO` records are confirmed placeholders (don't exist on KBO website)

## Relevant Files
- `src/cli/fix_player_names.py` - CLI command to re-crawl and fix
- `src/crawlers/player_search_crawler.py` - Main crawler logic
- `src/repositories/player_basic_repository.py` - Database save logic with validation
- `src/utils/player_validation.py` - Name validation and filtering
