# Phase 3 Implementation Plan: Medium Module Coverage (100-500 stmts)

**Goal**: Raise 14 medium modules from <70% to ≥70% coverage
**Duration**: 5-7 days
**Approach**: Reuse Phase 1-2 patterns — parser extraction + sync/async Playwright mocks

---

## Priority 1: High ROI (Days 1-3)

### 1. `src/crawlers/fielding_stats_crawler.py` (18.6%, 231 stmts)
**Existing tests**: `tests/crawlers/test_fielding_stats_crawler.py`, `test_fielding_stats_pure.py` — only `build_fielding_crawl_summary`

**Target helpers to test**:
- `_init_fielding_page(page, url, year, policy)` → mock sync PW chain
- `_get_team_list(page)` → fake `ElementHandle` dropdown options
- `_parse_fielding_row(row, year, position_mapping, map)` → fake `ElementHandle` row cells
- `_parse_catcher_detail_row(row, year, map)` → same pattern
- `crawl_all_fielding_stats(year)` → full sync orchestration with `sync_playwright` mock
- `_crawl_team_fielding_basic(ctx)` → loop pages, call parser
- `_crawl_catcher_fielding_details(page, url, year, map, policy)` → separate URL flow

**Fixtures**: Reuse `FIELDING_STATS` selectors; fake `ElementHandle` with `query_selector_all("td")` returning cell text arrays

---

### 2. `src/crawlers/futures/profile.py` (22.4%, 156 stmts)
**Existing tests**: None dedicated (only `test_futures_crawler_stability.py` — skipped)

**Target helpers to test**:
- `_extract_profile_text(page)` → multiple selector fallbacks
- `_click_futures_tab(page)` → tab selector sequence
- `_extract_known_futures_tables(soup)` → BS4 parsing with `tblHitterRecord`, `tblPitcherRecord`
- `_extract_fallback_futures_tables(soup)` → div[id*="Futures"] search
- `_parse_table_with_bs4(table_elem)` → caption/headers/rows extraction
- `fetch_player_futures(player_id)` → dual URL (hitter/pitcher) with pool lifecycle

**Fixtures**: HTML snippets for profile text, futures tables; mock `AsyncPlaywrightPool` acquire/release

---

### 3. `src/crawlers/team_info_crawler.py` (23.4%, 124 stmts)
**Existing tests**: `tests/crawlers/test_team_info_crawler.py` — covers `start/close`, `crawl` happy paths, `save` with mock Franchise

**Missing branches to cover**:
- Modal link missing (`link.count() == 0`) — already has test
- Modal visible timeout / exception path in `_extract_modal_fields`
- `_get_modal_field` xpath returns None
- `_close_modal` close button absent, Escape fallback
- `_save_raw_snapshots` with empty `_raw_pages`
- `save()` Franchise not found branch
- Network error in `crawl()` page.goto

**Fixtures**: Mock `AsyncPlaywright`, `Page`, `Locator` chains with `nth()`, `locator()`, `count()`, `click()`, `wait_for()`, `inner_text()`

---

## Priority 2: Similar Patterns (Days 4-5)

| Module | Coverage | Stmts | Key Helpers |
|--------|----------|-------|-------------|
| `team_history_crawler.py` | 24.4% | 135 | `_crawl_team`, `_parse_history_table`, `run()` loop |
| `player_movement_crawler.py` | 25.2% | 115 | `_extract_table`, `crawl_year`, `crawl_years` pagination |
| `game_mvp_crawler.py` | 32.4% | 111 | `_extract_mvp_data`, `run()` with PW |
| `retire/detail.py` | 32.7% | 107 | `_extract_player_detail`, profile parsing |
| `futures/futures_pitching.py` | 39.8% | 103 | Similar to `futures_batting` pure helpers |

**Pattern**: Each uses sync Playwright → extract parser → mock `ElementHandle` DOM → test orchestration with `sync_playwright()` double

---

## Priority 3: Complex Orchestration (Days 6-7)

| Module | Coverage | Stmts | Strategy |
|--------|----------|-------|----------|
| `smart_polling_gate.py` | 40.4% | 141 | Mock `httpx.AsyncClient` → test gate logic (no games / live / finished) |
| `live_crawler.py` | 40.5% | 447 | Mock phase methods (`_crawl_preview`, `_refresh_live`, `_finalize`) |
| `player_search_crawler.py` | 42.2% | 408 | Extract pagination helpers; mock search API |
| `ticket_crawler.py` | 46.7% | 152 | Mock PW price extraction; test save |
| `verify_chunk_quality.py` | 47.0% | 149 | Mock DB queries; test quality thresholds |

---

## Reusable Test Patterns (from Phase 1-2)

### Sync Playwright Double
```python
browser = MagicMock()
context = MagicMock()
context.new_page.return_value = page
browser.new_context.return_value = context
playwright = MagicMock()
playwright.chromium.launch.return_value = browser
manager = MagicMock()
manager.__enter__.return_value = playwright
manager.__exit__.return_value = False

with patch("src.crawlers.module.sync_playwright", return_value=manager):
    result = crawl_function(...)
```

### Async Pool Double
```python
pool = MagicMock()
pool.acquire = AsyncMock(return_value=page)
pool.release = AsyncMock()
pool.start = AsyncMock()
pool.close = AsyncMock()

with patch("src.module.AsyncPlaywrightPool", return_value=pool):
    await async_function(...)
```

### Fake DOM Element
```python
class Node:
    def __init__(self, text="", *, children=None):
        self.text = text
        self.children = children or {}
    def query_selector(self, sel): return self.children.get(sel)
    def query_selector_all(self, sel): return self.children.get(sel, [])
    def inner_text(self): return self.text
    def get_attribute(self, n): return self.children.get(n)
```

---

## Verification Checklist per Module

- [ ] Parser pure functions: 100% coverage
- [ ] Sync/async orchestration: all branches (success, timeout, missing element, exception)
- [ ] Save paths: commit, rollback, empty input
- [ ] Pagination loops: first page, middle, last, empty
- [ ] Configuration: headless, delay, limit, team_filter
- [ ] Compliance/blocked paths

---

## Commands

```bash
# Single module verification
venv/bin/python -m pytest -q tests/crawlers/test_fielding_stats_crawler.py tests/crawlers/test_fielding_stats_pure.py
venv/bin/ruff check tests/crawlers/test_fielding_stats_crawler.py

# Targeted coverage
COVERAGE_FILE="/tmp/.cov-fielding" venv/bin/python -m pytest --cov=src.crawlers.fielding_stats_crawler --cov-report=term ...

# Full Phase 3 check
venv/bin/python -m pytest -q --ignore=tests/test_kbo_event_crawler.py
```

---

## Output

17 test files created/extended across 3 priority batches, targeting ≥70% on all 14 modules.
