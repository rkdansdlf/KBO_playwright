-- SQLite Composite Indexes for Stadium Real-Time & Fact Tables, plus Stat Recalc View

CREATE INDEX IF NOT EXISTS idx_sc_stadium_measured ON stadium_congestion(stadium_code, measured_at);
CREATE INDEX IF NOT EXISTS idx_sc_stadium_game_date ON stadium_congestion(stadium_code, game_date);

CREATE INDEX IF NOT EXISTS idx_stt_stadium_measured ON stadium_transit_times(stadium_code, measured_at);
CREATE INDEX IF NOT EXISTS idx_stt_stadium_game_date ON stadium_transit_times(stadium_code, game_date);

CREATE INDEX IF NOT EXISTS idx_pbp_game_inning ON game_play_by_play(game_id, inning, inning_half);
CREATE INDEX IF NOT EXISTS idx_pbp_player_game ON game_play_by_play(player_id, game_id);

CREATE INDEX IF NOT EXISTS idx_ge_game_event_type ON game_events(game_id, event_type);
CREATE INDEX IF NOT EXISTS idx_ge_batter_game ON game_events(batter_id, game_id);

-- Realtime Recalculated Season Batting View
CREATE VIEW IF NOT EXISTS vw_player_season_batting_recalc AS
SELECT
    b.player_id,
    b.player_name,
    g.season_id AS season,
    b.team_code,
    COUNT(DISTINCT b.game_id) AS games,
    SUM(COALESCE(b.plate_appearances, 0)) AS plate_appearances,
    SUM(COALESCE(b.at_bats, 0)) AS at_bats,
    SUM(COALESCE(b.runs, 0)) AS runs,
    SUM(COALESCE(b.hits, 0)) AS hits,
    SUM(COALESCE(b.doubles, 0)) AS doubles,
    SUM(COALESCE(b.triples, 0)) AS triples,
    SUM(COALESCE(b.home_runs, 0)) AS home_runs,
    SUM(COALESCE(b.rbi, 0)) AS rbi,
    SUM(COALESCE(b.walks, 0)) AS walks,
    SUM(COALESCE(b.strikeouts, 0)) AS strikeouts,
    CASE WHEN SUM(COALESCE(b.at_bats, 0)) > 0
         THEN ROUND(CAST(SUM(COALESCE(b.hits, 0)) AS FLOAT) / SUM(COALESCE(b.at_bats, 0)), 3)
         ELSE 0.0 END AS avg
FROM game_batting_stats b
JOIN game g ON b.game_id = g.game_id
WHERE b.player_id IS NOT NULL
GROUP BY b.player_id, b.player_name, g.season_id, b.team_code;
