# Team Stats Fallback Extension Plan

## Objective
Implement a fallback system for team-level season statistics (`TeamSeasonBatting`, `TeamSeasonPitching`) that aggregates data from individual player season statistics. This ensures that even if KBO's team record pages are unavailable, we can still generate accurate team-level summaries.

## Proposed Solution
1. **TeamStatAggregator Service**: Create a new service `src/aggregators/team_stat_aggregator.py` that aggregates player season stats into team season stats.
2. **Crawler Integration**: Update `TeamBattingStatsCrawler` and `TeamPitchingStatsCrawler` to trigger the fallback when KBO pages fail.
3. **CLI Utility**: Create a CLI tool to manually recalculate team stats.

## Detailed Design

### 1. TeamStatAggregator (`src/aggregators/team_stat_aggregator.py`)
- `aggregate_team_batting(session, season, league) -> List[Dict]`:
    - Query `PlayerSeasonBatting` grouped by `team_id`.
    - Sum up counting stats (G, PA, AB, R, H, 2B, 3B, HR, RBI, SB, CS, BB, SO).
    - Use `BattingStatCalculator` to recompute team-level AVG, OBP, SLG, OPS.
- `aggregate_team_pitching(session, season, league) -> List[Dict]`:
    - Query `PlayerSeasonPitching` grouped by `team_id`.
    - Sum up counting stats (G, W, L, T, SV, HD, IP_outs, R, ER, H, HR, BB, SO).
    - Use `PitchingStatCalculator` to recompute team-level ERA, WHIP, AVG_AGAINST.

### 2. Crawler Modifications
- **TeamBattingStatsCrawler**: Wrap `_collect_from_site` in a try-except. If it returns empty or fails, call `TeamStatAggregator`.
- **TeamPitchingStatsCrawler**: Same for pitching.

### 3. CLI Tool (`src/cli/recalc_team_stats.py`)
- Similar to `recalc_season_stats.py` but for teams.
- Arguments: `--year`, `--league`, `--type`, `--save`.

## Implementation Roadmap
1. **Phase 1: Aggregator Logic**
    - Implement `TeamStatAggregator` with SQLAlchemy group-by queries.
2. **Phase 2: Integration**
    - Update team crawlers to use the aggregator as a fallback.
3. **Phase 3: Validation**
    - Run an audit comparing official team stats with calculated ones for 2025/2026.
