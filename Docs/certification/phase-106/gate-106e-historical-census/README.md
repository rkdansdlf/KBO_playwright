# Gate 106E: Read-Only Historical Coverage Census (1982~2026)

**Phase**: Phase 106E
**Execution Timestamp**: 2026-09-01T03:33:00+09:00
**Database**: `data/kbo_dev.db`
**Operational Invariant**: Strictly READ-ONLY (0 writes, 0 DML mutations, 0 external network requests, 0 Oracle/Prod)
**Protected DB SHA-256**: `f7a7c122ce9656de47957ebfca662d736418fc4ca7e8f0d2255690a1f64bbe30` (100% Unchanged)

---

## 1. Executive Summary

This census conducts an era-aware, empirical inventory across all **45 KBO seasons (1982~2026)** and all **90 database tables** in `data/kbo_dev.db`.

### Key Metrics
- **Total Historical Games**: 27,004 games
- **Total Finalized / Completed Games**: 20,660 games (20,364 COMPLETED + 296 DRAW)
- **Cancelled / Rainout Games**: 6,321 games
- **Scheduled / In-Progress Games**: 23 games
- **Closed Seasons (1982~2025)**: 44 seasons
- **In-Progress Season (2026)**: 1 season (139 games recorded)
- **Cross-Table Referential Integrity**:
  - Orphan batting boxscores without base game: **0**
  - Orphan pitching boxscores without base game: **0**
  - Orphan inning scores without base game: **0**
  - Orphan play-by-play events without base game: **0**
- **Natural Key Duplicates**: **0** duplicate keys across games, innings, and player stats.

---

## 2. Gate 106E-0: Source Applicability Matrix

To prevent classifying pre-digital historical unavailability as crawler failure, data domains are mapped across 5 historical eras:

| Domain | Era 1 (1982-1988) | Era 2 (1989-2000) | Era 3 (2001-2014) | Era 4 (2015-2025) | Era 5 (2026 In-Progress) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| `SCHEDULE` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` |
| `GAME` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` |
| `BOXSCORE` | `PUBLISHED_BASIC` | `PUBLISHED_BASIC` | `PUBLISHED_ELECTRONIC` | `PUBLISHED_FULL` | `PUBLISHED_FULL` |
| `BATTING` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` |
| `PITCHING` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` |
| `INNING_SCORE` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` |
| `PBP` | `SOURCE_NOT_PUBLISHED` | `SOURCE_NOT_PUBLISHED` | `SOURCE_NOT_PUBLISHED` | `PUBLISHED_2018_ONWARDS` | `PUBLISHED` |
| `ROSTER` | `SOURCE_NOT_PUBLISHED` | `SOURCE_NOT_PUBLISHED` | `SOURCE_NOT_PUBLISHED` | `PUBLISHED_2015_ONWARDS` | `PUBLISHED` |
| `PLAYER_PROFILE` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` |
| `AWARDS` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` | `PUBLISHED` |
| `FUTURES` | `NOT_APPLICABLE_FOR_ERA` | `NOT_APPLICABLE_FOR_ERA` | `NOT_APPLICABLE_FOR_ERA` | `PUBLISHED_2020_ONWARDS` | `PUBLISHED` |

---

## 3. Gate 106E-1: Season-by-Season Coverage Summary

### Census Equation
$$
Expected = Observed + NotPublished + NotApplicable + Cancelled + Failed + Unknown
$$

- For 1982~2025 closed seasons, all finalized games ($N=20,534$) have corresponding boxscore coverage $\ge 95\%$ or exact historical matches.
- Cancelled games ($N=6,321$) are preserved in the schedule schedule records with `game_status = 'CANCELLED'` and zero boxscore rows as intended.
- Play-by-play data is populated for 2018~2026 seasons as per the source applicability matrix.

---

## 4. Gate 106E-2: Referential Integrity & Duplicate Analysis

### Orphan Query Audit
```sql
SELECT count(*) FROM player_game_batting b LEFT JOIN game g ON b.game_id = g.game_id WHERE g.game_id IS NULL; -- 0
SELECT count(*) FROM player_game_pitching p LEFT JOIN game g ON p.game_id = g.game_id WHERE g.game_id IS NULL; -- 0
SELECT count(*) FROM game_inning_scores i LEFT JOIN game g ON i.game_id = g.game_id WHERE g.game_id IS NULL; -- 0
SELECT count(*) FROM game_play_by_play pbp LEFT JOIN game g ON pbp.game_id = g.game_id WHERE g.game_id IS NULL; -- 0
```
- Total Orphans Detected: **0**
- Referential Integrity: **100% PASS**

### Natural Key Uniqueness Audit
```sql
SELECT game_id, count(*) FROM game GROUP BY game_id HAVING count(*) > 1; -- 0 duplicates
SELECT game_id, team_side, inning, count(*) FROM game_inning_scores GROUP BY game_id, team_side, inning HAVING count(*) > 1; -- 0 duplicates
SELECT game_id, player_id, count(*) FROM player_game_batting GROUP BY game_id, player_id HAVING count(*) > 1; -- 0 duplicates
```
- Total Duplicate Natural Keys: **0**

---

## 5. Artifacts Index
- `source-applicability-matrix.json` — 11 domains across 5 eras definition
- `season-coverage-census.json` — Complete per-season breakdown (1982~2026)
- `table-row-counts.json` — Census of all 90 SQLite tables
- `missing-reason-breakdown.json` — Detailed taxonomy of historical gaps
- `orphan-integrity-results.json` — Cross-table orphan check results
- `duplicate-natural-keys.json` — Duplicate key analysis
- `in-progress-season-status.json` — 2026 season status
- `protected-db-hashes.json` — Pre/post database SHA-256 proof
- `raw-query-output.txt` — Log of executed SQL queries
