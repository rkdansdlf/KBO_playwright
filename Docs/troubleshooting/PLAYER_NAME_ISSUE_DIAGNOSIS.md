# Player Name Issue: Diagnostic Report

## Issue Summary
All 2,710 players in the `player_basic` table have "Unknown Player" as their name instead of actual Korean player names.

## Investigation Findings

### Database Status (as of 2025-10-26)
```sql
-- SQLite Database: data/kbo_dev.db
-- Last Modified: October 21, 2025

SELECT COUNT(*) FROM player_basic WHERE name LIKE '%Unknown%';
-- Result: 2710 (100% of all players)

SELECT MIN(player_id), MAX(player_id), COUNT(*) FROM player_basic;
-- Result: 10005, 99810, 2710
```

### Sample Data
```
player_id | name           | team | position
----------|----------------|------|----------
99810     | Unknown Player | SK   | 타자
99742     | Unknown Player | HH   | 투타겸용
99737     | Unknown Player | HH   | 투타겸용
10005     | Unknown Player | HT   | 타자
```

### Key Findings

1. **Issue Location**: The problem exists in the SQLite database itself, NOT in Supabase sync
2. **String Pattern**: The exact string is "Unknown Player" (capital U, capital P, with space)
3. **No Default Value**: The string "Unknown Player" does NOT appear anywhere in the source code
4. **Database Schema**: No DEFAULT value is set for the `name` column
5. **Data Age**: Database was last modified on October 21, 2025

## Possible Root Causes

### Hypothesis 1: Website HTML Structure Change ⚠️ MOST LIKELY
The KBO website may have changed its HTML structure, causing the name selector to fail:

**Current crawler logic** ([src/crawlers/player_search_crawler.py:164](src/crawlers/player_search_crawler.py#L164)):
```python
name_el = tds.nth(1).locator("a")
name = (await name_el.inner_text()).strip()
```

If the website changed and:
- The link at `tds.nth(1).locator("a")` no longer contains the player name
- The element returns an empty string or placeholder text
- The parsing logic somewhere converts empty/missing names to "Unknown Player"

### Hypothesis 2: Previous Import with Bad Data
A previous bulk import or migration script may have populated the database with placeholder data.

### Hypothesis 3: Encoding/Parsing Issue
Korean character encoding might have failed during a previous crawl, causing names to be lost.

## Investigation Steps Completed

1. ✅ Verified issue exists in SQLite (not Supabase sync)
2. ✅ Confirmed "Unknown Player" not in source code
3. ✅ Checked database schema - no default values
4. ✅ Reviewed all player-saving repositories
5. ✅ Examined data flow: Crawler → Parser → Repository → SQLite
6. ⏳ Attempted live crawl test (process hanging/slow)

## Recommended Solution Steps

### Step 1: Verify Live Website Scraping
```bash
# Test if current crawler can extract names from live website
venv/bin/python test_simple_crawl.py
```

**Expected outcome**:
- If names are extracted: The old data is bad, need to re-crawl
- If names are NOT extracted: Website HTML changed, need to fix selectors

### Step 2: Inspect Live Website HTML
Visit: https://www.koreabaseball.com/Player/Search.aspx?searchWord=%25

Check if the table structure matches the expected format:
```html
<table class="tEx">
  <tbody>
    <tr>
      <td>등번호</td>
      <td><a href="...?playerId=...">선수명</a></td>  <!-- Column 1 - Player name -->
      <td>팀명</td>
      <td>포지션</td>
      ...
    </tr>
  </tbody>
</table>
```

### Step 3: Fix Crawler if Needed
If website structure changed, update selectors in [src/crawlers/player_search_crawler.py](src/crawlers/player_search_crawler.py).

### Step 4: Re-crawl All Players
```bash
# Backup current database
cp data/kbo_dev.db data/kbo_dev.db.backup_$(date +%Y%m%d)

# Re-initialize and crawl
venv/bin/python init_db.py
venv/bin/python -m src.crawlers.player_search_crawler --save

# Verify results
sqlite3 data/kbo_dev.db "SELECT player_id, name, team FROM player_basic WHERE name != 'Unknown Player' LIMIT 10;"
```

### Step 5: Verify and Sync
```bash
# Verify SQLite data quality
venv/bin/python verify_sqlite_data.py

# Sync to Supabase
export SUPABASE_DB_URL='postgresql://...'
venv/bin/python -m src.crawlers.player_search_crawler --save --sync-supabase
```

## Prevention Measures

### 1. Add Name Validation
Add validation in [src/repositories/player_basic_repository.py](src/repositories/player_basic_repository.py):

```python
def _upsert_one(self, session: Session, player_data: Dict[str, Any]):
    # Validate player name
    name = player_data.get('name', '').strip()
    if not name or name in ['Unknown Player', 'Unknown', '-', 'N/A']:
        raise ValueError(f"Invalid player name for player_id {player_data['player_id']}: '{name}'")

    data = {
        'player_id': player_data['player_id'],
        'name': name,
        # ...
    }
```

### 2. Add Integration Test
Create test to verify actual names are crawled:

```python
# tests/test_player_crawler.py
async def test_player_names_not_unknown():
    players = await crawl_all_players(max_pages=1)
    assert len(players) > 0
    for player in players:
        assert player.name not in ['Unknown Player', 'Unknown', '', None]
        assert len(player.name) >= 2  # Korean names are at least 2 characters
```

### 3. Add Monitoring
Log warnings when suspicious data is encountered:

```python
if not name or name == 'Unknown Player':
    logger.warning(f"Suspicious player name for ID {player_id}: '{name}'")
```

## Next Actions

1. **URGENT**: Complete the live crawl test to confirm current scraping works
2. Inspect live website HTML if test fails
3. Fix crawler selectors if needed
4. Re-crawl all 2,710 players with corrected logic
5. Verify data quality before Supabase sync
6. Add validation and tests to prevent recurrence

## Files to Monitor

- [src/crawlers/player_search_crawler.py](src/crawlers/player_search_crawler.py) - Main crawler logic
- [src/repositories/player_basic_repository.py](src/repositories/player_basic_repository.py) - Database save logic
- [data/kbo_dev.db](data/kbo_dev.db) - SQLite database
- [Docs/URL_REFERENCE.md](Docs/URL_REFERENCE.md) - URL patterns and selectors

## Contact & Resources

- KBO Player Search URL: https://www.koreabaseball.com/Player/Search.aspx
- Documentation: [Docs/PLAYERID_CRAWLING.md](Docs/PLAYERID_CRAWLING.md)
- Crawler Design: [Docs/projectOverviewGuid.md](Docs/projectOverviewGuid.md)
