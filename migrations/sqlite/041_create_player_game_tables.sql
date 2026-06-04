-- Player-game-level batting and pitching aggregation tables.
-- Mirror of OCI migration 041, adapted for SQLite.

CREATE TABLE IF NOT EXISTS player_game_batting (
    id                   INTEGER  PRIMARY KEY AUTOINCREMENT,
    game_id              TEXT     NOT NULL,
    player_id            INTEGER  NOT NULL,
    player_name          TEXT     NOT NULL,
    team_side            TEXT     NOT NULL,
    team_code            TEXT,
    batting_order        INTEGER,
    appearance_seq       INTEGER,
    position             TEXT,
    is_starter           INTEGER  NOT NULL DEFAULT 0,
    source               TEXT,
    plate_appearances    INTEGER  NOT NULL DEFAULT 0,
    at_bats              INTEGER  NOT NULL DEFAULT 0,
    runs                 INTEGER  NOT NULL DEFAULT 0,
    hits                 INTEGER  NOT NULL DEFAULT 0,
    doubles              INTEGER  NOT NULL DEFAULT 0,
    triples              INTEGER  NOT NULL DEFAULT 0,
    home_runs            INTEGER  NOT NULL DEFAULT 0,
    rbi                  INTEGER  NOT NULL DEFAULT 0,
    walks                INTEGER  NOT NULL DEFAULT 0,
    intentional_walks    INTEGER  NOT NULL DEFAULT 0,
    hbp                  INTEGER  NOT NULL DEFAULT 0,
    strikeouts           INTEGER  NOT NULL DEFAULT 0,
    stolen_bases         INTEGER  NOT NULL DEFAULT 0,
    caught_stealing      INTEGER  NOT NULL DEFAULT 0,
    sacrifice_hits       INTEGER  NOT NULL DEFAULT 0,
    sacrifice_flies      INTEGER  NOT NULL DEFAULT 0,
    gdp                  INTEGER  NOT NULL DEFAULT 0,
    avg                  REAL,
    obp                  REAL,
    slg                  REAL,
    ops                  REAL,
    iso                  REAL,
    babip                REAL,
    extra_stats          TEXT,
    created_at           TEXT     NOT NULL DEFAULT (datetime('now')),
    updated_at           TEXT     NOT NULL DEFAULT (datetime('now')),
    UNIQUE(game_id, player_id)
);

CREATE TABLE IF NOT EXISTS player_game_pitching (
    id                   INTEGER  PRIMARY KEY AUTOINCREMENT,
    game_id              TEXT     NOT NULL,
    player_id            INTEGER  NOT NULL,
    player_name          TEXT     NOT NULL,
    team_side            TEXT     NOT NULL,
    team_code            TEXT,
    is_starting          INTEGER  NOT NULL DEFAULT 0,
    appearance_seq       INTEGER,
    source               TEXT,
    innings_outs         INTEGER  NOT NULL DEFAULT 0,
    hits_allowed         INTEGER  NOT NULL DEFAULT 0,
    runs_allowed         INTEGER  NOT NULL DEFAULT 0,
    earned_runs          INTEGER  NOT NULL DEFAULT 0,
    home_runs_allowed    INTEGER  NOT NULL DEFAULT 0,
    walks_allowed        INTEGER  NOT NULL DEFAULT 0,
    strikeouts           INTEGER  NOT NULL DEFAULT 0,
    hit_batters          INTEGER  NOT NULL DEFAULT 0,
    wild_pitches         INTEGER  NOT NULL DEFAULT 0,
    balks                INTEGER  NOT NULL DEFAULT 0,
    wins                 INTEGER  NOT NULL DEFAULT 0,
    losses               INTEGER  NOT NULL DEFAULT 0,
    saves                INTEGER  NOT NULL DEFAULT 0,
    holds                INTEGER  NOT NULL DEFAULT 0,
    decision             TEXT,
    batters_faced        INTEGER  NOT NULL DEFAULT 0,
    era                  REAL,
    whip                 REAL,
    fip                  REAL,
    k_per_nine           REAL,
    bb_per_nine          REAL,
    kbb                  REAL,
    extra_stats          TEXT,
    created_at           TEXT     NOT NULL DEFAULT (datetime('now')),
    updated_at           TEXT     NOT NULL DEFAULT (datetime('now')),
    UNIQUE(game_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_game_batting_game_id
    ON player_game_batting (game_id);
CREATE INDEX IF NOT EXISTS idx_player_game_batting_player_id
    ON player_game_batting (player_id);
CREATE INDEX IF NOT EXISTS idx_player_game_pitching_game_id
    ON player_game_pitching (game_id);
CREATE INDEX IF NOT EXISTS idx_player_game_pitching_player_id
    ON player_game_pitching (player_id);
