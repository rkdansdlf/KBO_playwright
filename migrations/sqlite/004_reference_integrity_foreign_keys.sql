-- Rebuild legacy SQLite tables to add declared foreign keys that cannot be
-- added with ALTER TABLE. Run only after check_orphan_data.py passes.

PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

DROP TABLE IF EXISTS game_batting_stats_new;
CREATE TABLE game_batting_stats_new (
    id INTEGER NOT NULL,
    game_id VARCHAR(20) NOT NULL,
    team_side VARCHAR(5) NOT NULL,
    team_code VARCHAR(10),
    player_id INTEGER,
    player_name VARCHAR(64) NOT NULL,
    batting_order INTEGER,
    is_starter BOOLEAN,
    appearance_seq INTEGER NOT NULL,
    position VARCHAR(8),
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    rbi INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    hbp INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    sacrifice_hits INTEGER,
    sacrifice_flies INTEGER,
    gdp INTEGER,
    avg FLOAT,
    obp FLOAT,
    slg FLOAT,
    ops FLOAT,
    iso FLOAT,
    babip FLOAT,
    extra_stats JSON,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    uniform_no VARCHAR(10),
    standard_position VARCHAR(10),
    franchise_id INTEGER,
    canonical_team_code VARCHAR(10),
    PRIMARY KEY (id),
    CONSTRAINT uq_game_batting_player UNIQUE (game_id, player_id, appearance_seq),
    FOREIGN KEY(game_id) REFERENCES game (game_id),
    FOREIGN KEY(player_id) REFERENCES player_basic (player_id)
);
INSERT INTO game_batting_stats_new (
    id, game_id, team_side, team_code, player_id, player_name, batting_order,
    is_starter, appearance_seq, position, plate_appearances, at_bats, runs,
    hits, doubles, triples, home_runs, rbi, walks, intentional_walks, hbp,
    strikeouts, stolen_bases, caught_stealing, sacrifice_hits, sacrifice_flies,
    gdp, avg, obp, slg, ops, iso, babip, extra_stats, created_at, updated_at,
    uniform_no, standard_position, franchise_id, canonical_team_code
)
SELECT
    id, game_id, team_side, team_code, player_id, player_name, batting_order,
    is_starter, appearance_seq, position, plate_appearances, at_bats, runs,
    hits, doubles, triples, home_runs, rbi, walks, intentional_walks, hbp,
    strikeouts, stolen_bases, caught_stealing, sacrifice_hits, sacrifice_flies,
    gdp, avg, obp, slg, ops, iso, babip, extra_stats, created_at, updated_at,
    uniform_no, standard_position, franchise_id, canonical_team_code
FROM game_batting_stats;
DROP TABLE game_batting_stats;
ALTER TABLE game_batting_stats_new RENAME TO game_batting_stats;

DROP TABLE IF EXISTS game_pitching_stats_new;
CREATE TABLE game_pitching_stats_new (
    id INTEGER NOT NULL,
    game_id VARCHAR(20) NOT NULL,
    team_side VARCHAR(5) NOT NULL,
    team_code VARCHAR(10),
    player_id INTEGER,
    player_name VARCHAR(64) NOT NULL,
    is_starting BOOLEAN,
    appearance_seq INTEGER NOT NULL,
    innings_outs INTEGER,
    innings_pitched NUMERIC(5, 3),
    batters_faced INTEGER,
    pitches INTEGER,
    hits_allowed INTEGER,
    runs_allowed INTEGER,
    earned_runs INTEGER,
    home_runs_allowed INTEGER,
    walks_allowed INTEGER,
    strikeouts INTEGER,
    hit_batters INTEGER,
    wild_pitches INTEGER,
    balks INTEGER,
    wins INTEGER,
    losses INTEGER,
    saves INTEGER,
    holds INTEGER,
    decision VARCHAR(2),
    era FLOAT,
    whip FLOAT,
    k_per_nine FLOAT,
    bb_per_nine FLOAT,
    kbb FLOAT,
    extra_stats JSON,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    uniform_no VARCHAR(10),
    standard_position VARCHAR(10),
    franchise_id INTEGER,
    canonical_team_code VARCHAR(10),
    PRIMARY KEY (id),
    CONSTRAINT uq_game_pitching_player UNIQUE (game_id, player_id, appearance_seq),
    FOREIGN KEY(game_id) REFERENCES game (game_id),
    FOREIGN KEY(player_id) REFERENCES player_basic (player_id)
);
INSERT INTO game_pitching_stats_new (
    id, game_id, team_side, team_code, player_id, player_name, is_starting,
    appearance_seq, innings_outs, innings_pitched, batters_faced, pitches,
    hits_allowed, runs_allowed, earned_runs, home_runs_allowed, walks_allowed,
    strikeouts, hit_batters, wild_pitches, balks, wins, losses, saves, holds,
    decision, era, whip, k_per_nine, bb_per_nine, kbb, extra_stats, created_at,
    updated_at, uniform_no, standard_position, franchise_id, canonical_team_code
)
SELECT
    id, game_id, team_side, team_code, player_id, player_name, is_starting,
    appearance_seq, innings_outs, innings_pitched, batters_faced, pitches,
    hits_allowed, runs_allowed, earned_runs, home_runs_allowed, walks_allowed,
    strikeouts, hit_batters, wild_pitches, balks, wins, losses, saves, holds,
    decision, era, whip, k_per_nine, bb_per_nine, kbb, extra_stats, created_at,
    updated_at, uniform_no, standard_position, franchise_id, canonical_team_code
FROM game_pitching_stats;
DROP TABLE game_pitching_stats;
ALTER TABLE game_pitching_stats_new RENAME TO game_pitching_stats;

DROP TABLE IF EXISTS game_lineups_new;
CREATE TABLE game_lineups_new (
    id INTEGER NOT NULL,
    game_id VARCHAR(20) NOT NULL,
    team_side VARCHAR(5) NOT NULL,
    team_code VARCHAR(10),
    player_id INTEGER,
    player_name VARCHAR(64) NOT NULL,
    batting_order INTEGER,
    position VARCHAR(8),
    is_starter BOOLEAN,
    appearance_seq INTEGER NOT NULL,
    notes VARCHAR(64),
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    uniform_no VARCHAR(10),
    standard_position VARCHAR(10),
    franchise_id INTEGER,
    canonical_team_code VARCHAR(10),
    PRIMARY KEY (id),
    CONSTRAINT uq_game_lineup_entry UNIQUE (game_id, team_side, appearance_seq),
    FOREIGN KEY(game_id) REFERENCES game (game_id),
    FOREIGN KEY(player_id) REFERENCES player_basic (player_id)
);
INSERT INTO game_lineups_new (
    id, game_id, team_side, team_code, player_id, player_name, batting_order,
    position, is_starter, appearance_seq, notes, created_at, updated_at,
    uniform_no, standard_position, franchise_id, canonical_team_code
)
SELECT
    id, game_id, team_side, team_code, player_id, player_name, batting_order,
    position, is_starter, appearance_seq, notes, created_at, updated_at,
    uniform_no, standard_position, franchise_id, canonical_team_code
FROM game_lineups;
DROP TABLE game_lineups;
ALTER TABLE game_lineups_new RENAME TO game_lineups;

DROP TABLE IF EXISTS player_season_batting_new;
CREATE TABLE player_season_batting_new (
    id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL,
    level VARCHAR(16) NOT NULL,
    source VARCHAR(16) NOT NULL,
    team_code VARCHAR(10),
    games INTEGER,
    plate_appearances INTEGER,
    at_bats INTEGER,
    runs INTEGER,
    hits INTEGER,
    doubles INTEGER,
    triples INTEGER,
    home_runs INTEGER,
    rbi INTEGER,
    walks INTEGER,
    intentional_walks INTEGER,
    hbp INTEGER,
    strikeouts INTEGER,
    stolen_bases INTEGER,
    caught_stealing INTEGER,
    sacrifice_hits INTEGER,
    sacrifice_flies INTEGER,
    gdp INTEGER,
    avg FLOAT,
    obp FLOAT,
    slg FLOAT,
    ops FLOAT,
    iso FLOAT,
    babip FLOAT,
    extra_stats JSON,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    franchise_id INTEGER,
    canonical_team_code VARCHAR(10),
    PRIMARY KEY (id),
    CONSTRAINT uq_player_season_batting UNIQUE (player_id, season, league, level),
    FOREIGN KEY(player_id) REFERENCES player_basic (player_id),
    FOREIGN KEY(team_code) REFERENCES teams (team_id)
);
INSERT INTO player_season_batting_new (
    id, player_id, season, league, level, source, team_code, games,
    plate_appearances, at_bats, runs, hits, doubles, triples, home_runs, rbi,
    walks, intentional_walks, hbp, strikeouts, stolen_bases, caught_stealing,
    sacrifice_hits, sacrifice_flies, gdp, avg, obp, slg, ops, iso, babip,
    extra_stats, created_at, updated_at, franchise_id, canonical_team_code
)
SELECT
    id, player_id, season, league, level, source, team_code, games,
    plate_appearances, at_bats, runs, hits, doubles, triples, home_runs, rbi,
    walks, intentional_walks, hbp, strikeouts, stolen_bases, caught_stealing,
    sacrifice_hits, sacrifice_flies, gdp, avg, obp, slg, ops, iso, babip,
    extra_stats, created_at, updated_at, franchise_id, canonical_team_code
FROM player_season_batting;
DROP TABLE player_season_batting;
ALTER TABLE player_season_batting_new RENAME TO player_season_batting;
CREATE INDEX idx_psb_player ON player_season_batting (player_id, season);

DROP TABLE IF EXISTS player_season_pitching_new;
CREATE TABLE player_season_pitching_new (
    id INTEGER NOT NULL,
    player_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    league VARCHAR(16) NOT NULL,
    level VARCHAR(16) NOT NULL,
    source VARCHAR(16) NOT NULL,
    team_code VARCHAR(10),
    games INTEGER,
    games_started INTEGER,
    wins INTEGER,
    losses INTEGER,
    saves INTEGER,
    holds INTEGER,
    innings_pitched FLOAT,
    innings_outs INTEGER,
    hits_allowed INTEGER,
    runs_allowed INTEGER,
    earned_runs INTEGER,
    home_runs_allowed INTEGER,
    walks_allowed INTEGER,
    intentional_walks INTEGER,
    hit_batters INTEGER,
    strikeouts INTEGER,
    wild_pitches INTEGER,
    balks INTEGER,
    era FLOAT,
    whip FLOAT,
    fip FLOAT,
    k_per_nine FLOAT,
    bb_per_nine FLOAT,
    kbb FLOAT,
    complete_games INTEGER,
    shutouts INTEGER,
    quality_starts INTEGER,
    blown_saves INTEGER,
    tbf INTEGER,
    np INTEGER,
    avg_against FLOAT,
    doubles_allowed INTEGER,
    triples_allowed INTEGER,
    sacrifices_allowed INTEGER,
    sacrifice_flies_allowed INTEGER,
    extra_stats JSON,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    franchise_id INTEGER,
    canonical_team_code VARCHAR(10),
    PRIMARY KEY (id),
    CONSTRAINT uq_player_season_pitching UNIQUE (player_id, season, league, level),
    FOREIGN KEY(player_id) REFERENCES player_basic (player_id),
    FOREIGN KEY(team_code) REFERENCES teams (team_id)
);
INSERT INTO player_season_pitching_new (
    id, player_id, season, league, level, source, team_code, games,
    games_started, wins, losses, saves, holds, innings_pitched, innings_outs,
    hits_allowed, runs_allowed, earned_runs, home_runs_allowed, walks_allowed,
    intentional_walks, hit_batters, strikeouts, wild_pitches, balks, era, whip,
    fip, k_per_nine, bb_per_nine, kbb, complete_games, shutouts,
    quality_starts, blown_saves, tbf, np, avg_against, doubles_allowed,
    triples_allowed, sacrifices_allowed, sacrifice_flies_allowed, extra_stats,
    created_at, updated_at, franchise_id, canonical_team_code
)
SELECT
    id, player_id, season, league, level, source, team_code, games,
    games_started, wins, losses, saves, holds, innings_pitched, innings_outs,
    hits_allowed, runs_allowed, earned_runs, home_runs_allowed, walks_allowed,
    intentional_walks, hit_batters, strikeouts, wild_pitches, balks, era, whip,
    fip, k_per_nine, bb_per_nine, kbb, complete_games, shutouts,
    quality_starts, blown_saves, tbf, np, avg_against, doubles_allowed,
    triples_allowed, sacrifices_allowed, sacrifice_flies_allowed, extra_stats,
    created_at, updated_at, franchise_id, canonical_team_code
FROM player_season_pitching;
DROP TABLE player_season_pitching;
ALTER TABLE player_season_pitching_new RENAME TO player_season_pitching;
CREATE INDEX idx_psp_player_season ON player_season_pitching (player_id, season);

COMMIT;
PRAGMA foreign_keys = ON;
PRAGMA foreign_key_check;
