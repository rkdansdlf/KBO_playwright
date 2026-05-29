-- Phase 2a: Extend team_standings_daily with new columns
ALTER TABLE team_standings_daily ADD COLUMN rank INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN top_5 INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN recent_10_wins INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN recent_10_losses INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN recent_10_draws INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN weekly_win_pcts JSON;
ALTER TABLE team_standings_daily ADD COLUMN home_wins INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN home_losses INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN away_wins INTEGER NOT NULL DEFAULT 0;
ALTER TABLE team_standings_daily ADD COLUMN away_losses INTEGER NOT NULL DEFAULT 0;

-- Phase 2d: Team fielding & baserunning tables
CREATE TABLE IF NOT EXISTS team_season_fielding (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season INTEGER NOT NULL,
    team_code VARCHAR(10) NOT NULL,
    errors INTEGER NOT NULL DEFAULT 0,
    double_plays INTEGER NOT NULL DEFAULT 0,
    triple_plays INTEGER NOT NULL DEFAULT 0,
    total_chances INTEGER NOT NULL DEFAULT 0,
    putouts INTEGER NOT NULL DEFAULT 0,
    assists INTEGER NOT NULL DEFAULT 0,
    def_innings INTEGER,
    fielding_pct FLOAT,
    range_factor_per_game FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, team_code)
);

CREATE TABLE IF NOT EXISTS team_season_baserunning (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    season INTEGER NOT NULL,
    team_code VARCHAR(10) NOT NULL,
    stolen_bases INTEGER NOT NULL DEFAULT 0,
    caught_stealing INTEGER NOT NULL DEFAULT 0,
    sb_success_rate FLOAT,
    extra_bases_taken INTEGER NOT NULL DEFAULT 0,
    out_on_base INTEGER NOT NULL DEFAULT 0,
    sacrifice_hits INTEGER NOT NULL DEFAULT 0,
    sacrifice_flies INTEGER NOT NULL DEFAULT 0,
    bunt_hits INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, team_code)
);
