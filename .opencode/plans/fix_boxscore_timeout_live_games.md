# Fix: Boxscore Timeout During LIVE Games

## Root Cause

`_wait_for_boxscore` in `game_detail_crawler.py` waits for **13 CSS selectors** to appear before proceeding with data extraction:

```python
await page.wait_for_selector(", ".join(GAME_DETAIL.boxscore_presence_selectors), timeout=SEL_TIMEOUT)
```

The `boxscore_presence_selectors` include:
- `#tblAwayHitter1`, `#tblHomeHitter1` etc. — per-player boxscore tables (ONLY rendered after game finishes)
- `.sms-score`, `.score-board` — CSS classes that do NOT match the KBO GameCenter DOM

During a LIVE game, **none of these 13 selectors match**. The scoreboard that IS visible uses table IDs `#tblScordboard1`/`#tblScoreboard1` (used by `_extract_team_info` JS), but these IDs aren't in the presence selector list.

The 15-second timeout causes `_crawl_single` to return `None`, which means:
1. No `game_inning_scores` saved (scoreboard data is lost)
2. No `game_metadata` saved
3. No `game_summary` saved
4. The live crawler wastes 15 seconds per game every cycle

## Changes Required

### 1. `src/crawlers/selectors.py` — Add scoreboard table selectors

Add two scoreboard-related selectors to `GameDetailSelectors`:

```python
# After line 66
scoreboard_primary: str = "#tblScordboard1"
scoreboard_fallback: str = "#tblScoreboard1"
```

These match the actual table IDs that the KBO site renders for the live scoreboard. The `_extract_team_info` JS code already uses these IDs as its primary extraction strategy.

Add them to `boxscore_presence_selectors` property:

```python
@property
def boxscore_presence_selectors(self) -> tuple[str, ...]:
    return (
        # ... existing 13 selectors ...
        self.scoreboard_primary,   # new
        self.scoreboard_fallback,  # new
    )
```

This alone would likely fix the timeout for most cases since the scoreboard table IS present during live games.

### 2. `src/crawlers/game_detail_crawler.py` — Make timeout non-fatal in lightweight mode

Even with the selector fix, there could be edge cases (e.g., network delay, KBO site changes) where the presence check still times out. In lightweight mode, the scoreboard extraction (`_extract_team_info`, `_extract_metadata`, `_extract_game_summary`) can succeed even without the presence check passing — they use JS `page.evaluate()` with their own fallback logic.

**In `_crawl_single`** (line 470–473):

Change:

```python
is_ready, failure_reason = await self._wait_for_boxscore(page, game_id=game_id, lightweight=lightweight)
if not is_ready:
    self._last_failure_reason[game_id] = failure_reason
    return None
```

To:

```python
is_ready, failure_reason = await self._wait_for_boxscore(page, game_id=game_id, lightweight=lightweight)
if not is_ready:
    if lightweight:
        logger.warning(
            "⚠️ Boxscore presence check failed in lightweight mode for %s: %s. "
            "Proceeding with partial extraction...",
            game_id,
            failure_reason,
        )
    else:
        self._last_failure_reason[game_id] = failure_reason
        return None
```

This way, if the presence check fails in lightweight mode, we log a warning but continue to extract whatever data is available (scoreboard, metadata).

### 3. `src/crawlers/game_detail_crawler.py` — Add lightweight-only selectors in `_wait_for_boxscore`

In `_wait_for_boxscore`, when `lightweight=True`, add scoreboard-specific selectors to the wait list so the check can pass quickly (sub-second) when the scoreboard IS present:

```python
async def _wait_for_boxscore(self, page: Page, *, game_id: str, lightweight: bool = False) -> tuple[bool, str]:
    if await self._is_cancelled_boxscore_page(page):
        return False, "cancelled"

    selectors = list(GAME_DETAIL.boxscore_presence_selectors)
    if lightweight:
        # Add scoreboard-only selectors for fast presence check during live games
        selectors.extend([
            "#tblScordboard1",
            "#tblScoreboard1",
            "#tblScordboard2",
            "#tblScoreboard2",
        ])

    try:
        await page.wait_for_selector(", ".join(selectors), timeout=SEL_TIMEOUT)
    except PlaywrightError:
        ...
```

This ensures that during lightweight mode, the presence check can succeed as soon as the scoreboard tables appear (which happens immediately during live games).

## Verification Plan

1. Run the live crawler while games are in progress:
   ```bash
   python3 -m src.cli.live_crawler --run-once --no-sync
   ```
2. Check that `game_inning_scores` are saved for active games:
   ```sql
   SELECT * FROM game_inning_scores WHERE game_id LIKE '20260627%';
   ```
3. Check that no timeout warnings appear in logs
4. Run the full crawl for a completed game (non-lightweight) to verify no regression:
   ```bash
   python3 -m src.cli.collect_games --year 2026 --month 6
   ```

## Risk Assessment

| Change | Risk | Mitigation |
|--------|------|------------|
| Add scoreboard selectors to presence list | Low — new selectors are the same IDs used by `_extract_team_info` JS | If they match during finished games too, presence check passes faster (good) |
| Make timeout non-fatal in lightweight mode | Low — `_extract_team_info` has its own ID→header fallback logic | If page hasn't loaded at all, `team_info` will be partial; `save_game_snapshot` handles None gracefully |
| Add lightweight-only selectors | Very low — only extends selector list for lightweight calls | Full (`lightweight=False`) path unchanged |
