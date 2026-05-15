-- Add declared foreign keys for relationships now covered by logical gates.
-- Data is validated before release by scripts/verification/check_orphan_data.py.

CREATE OR REPLACE FUNCTION pg_temp.has_single_column_fk(
    child_table text,
    child_column text,
    parent_table text,
    parent_column text
)
RETURNS boolean
LANGUAGE sql
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM pg_constraint c
        JOIN pg_class child ON child.oid = c.conrelid
        JOIN pg_class parent ON parent.oid = c.confrelid
        JOIN pg_attribute child_attr
          ON child_attr.attrelid = child.oid
         AND child_attr.attnum = c.conkey[1]
        JOIN pg_attribute parent_attr
          ON parent_attr.attrelid = parent.oid
         AND parent_attr.attnum = c.confkey[1]
        WHERE c.contype = 'f'
          AND child.oid = to_regclass(child_table)
          AND parent.oid = to_regclass(parent_table)
          AND array_length(c.conkey, 1) = 1
          AND array_length(c.confkey, 1) = 1
          AND child_attr.attname = child_column
          AND parent_attr.attname = parent_column
    );
$$;

CREATE INDEX IF NOT EXISTS idx_game_metadata_game_id
    ON game_metadata (game_id);
CREATE INDEX IF NOT EXISTS idx_game_batting_stats_game_id
    ON game_batting_stats (game_id);
CREATE INDEX IF NOT EXISTS idx_game_batting_stats_player_id
    ON game_batting_stats (player_id)
    WHERE player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_game_pitching_stats_game_id
    ON game_pitching_stats (game_id);
CREATE INDEX IF NOT EXISTS idx_game_pitching_stats_player_id
    ON game_pitching_stats (player_id)
    WHERE player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_game_lineups_game_id
    ON game_lineups (game_id);
CREATE INDEX IF NOT EXISTS idx_game_lineups_player_id
    ON game_lineups (player_id)
    WHERE player_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_player_season_batting_player_id
    ON player_season_batting (player_id);
CREATE INDEX IF NOT EXISTS idx_player_season_batting_team_code
    ON player_season_batting (team_code)
    WHERE team_code IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_player_season_pitching_player_id
    ON player_season_pitching (player_id);
CREATE INDEX IF NOT EXISTS idx_player_season_pitching_team_code
    ON player_season_pitching (team_code)
    WHERE team_code IS NOT NULL;

DO $$
BEGIN
    IF NOT pg_temp.has_single_column_fk('game_metadata', 'game_id', 'game', 'game_id') THEN
        ALTER TABLE game_metadata
            ADD CONSTRAINT fk_refint_game_metadata_game
            FOREIGN KEY (game_id) REFERENCES game (game_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('game_batting_stats', 'game_id', 'game', 'game_id') THEN
        ALTER TABLE game_batting_stats
            ADD CONSTRAINT fk_refint_game_batting_stats_game
            FOREIGN KEY (game_id) REFERENCES game (game_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('game_batting_stats', 'player_id', 'player_basic', 'player_id') THEN
        ALTER TABLE game_batting_stats
            ADD CONSTRAINT fk_refint_game_batting_stats_player
            FOREIGN KEY (player_id) REFERENCES player_basic (player_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('game_pitching_stats', 'game_id', 'game', 'game_id') THEN
        ALTER TABLE game_pitching_stats
            ADD CONSTRAINT fk_refint_game_pitching_stats_game
            FOREIGN KEY (game_id) REFERENCES game (game_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('game_pitching_stats', 'player_id', 'player_basic', 'player_id') THEN
        ALTER TABLE game_pitching_stats
            ADD CONSTRAINT fk_refint_game_pitching_stats_player
            FOREIGN KEY (player_id) REFERENCES player_basic (player_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('game_lineups', 'game_id', 'game', 'game_id') THEN
        ALTER TABLE game_lineups
            ADD CONSTRAINT fk_refint_game_lineups_game
            FOREIGN KEY (game_id) REFERENCES game (game_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('game_lineups', 'player_id', 'player_basic', 'player_id') THEN
        ALTER TABLE game_lineups
            ADD CONSTRAINT fk_refint_game_lineups_player
            FOREIGN KEY (player_id) REFERENCES player_basic (player_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('player_season_batting', 'player_id', 'player_basic', 'player_id') THEN
        ALTER TABLE player_season_batting
            ADD CONSTRAINT fk_refint_player_season_batting_player
            FOREIGN KEY (player_id) REFERENCES player_basic (player_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('player_season_batting', 'team_code', 'teams', 'team_id') THEN
        ALTER TABLE player_season_batting
            ADD CONSTRAINT fk_refint_player_season_batting_team
            FOREIGN KEY (team_code) REFERENCES teams (team_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('player_season_pitching', 'player_id', 'player_basic', 'player_id') THEN
        ALTER TABLE player_season_pitching
            ADD CONSTRAINT fk_refint_player_season_pitching_player
            FOREIGN KEY (player_id) REFERENCES player_basic (player_id) NOT VALID;
    END IF;

    IF NOT pg_temp.has_single_column_fk('player_season_pitching', 'team_code', 'teams', 'team_id') THEN
        ALTER TABLE player_season_pitching
            ADD CONSTRAINT fk_refint_player_season_pitching_team
            FOREIGN KEY (team_code) REFERENCES teams (team_id) NOT VALID;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'game_metadata'::regclass AND conname = 'fk_refint_game_metadata_game' AND NOT convalidated) THEN
        ALTER TABLE game_metadata VALIDATE CONSTRAINT fk_refint_game_metadata_game;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'game_batting_stats'::regclass AND conname = 'fk_refint_game_batting_stats_game' AND NOT convalidated) THEN
        ALTER TABLE game_batting_stats VALIDATE CONSTRAINT fk_refint_game_batting_stats_game;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'game_batting_stats'::regclass AND conname = 'fk_refint_game_batting_stats_player' AND NOT convalidated) THEN
        ALTER TABLE game_batting_stats VALIDATE CONSTRAINT fk_refint_game_batting_stats_player;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'game_pitching_stats'::regclass AND conname = 'fk_refint_game_pitching_stats_game' AND NOT convalidated) THEN
        ALTER TABLE game_pitching_stats VALIDATE CONSTRAINT fk_refint_game_pitching_stats_game;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'game_pitching_stats'::regclass AND conname = 'fk_refint_game_pitching_stats_player' AND NOT convalidated) THEN
        ALTER TABLE game_pitching_stats VALIDATE CONSTRAINT fk_refint_game_pitching_stats_player;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'game_lineups'::regclass AND conname = 'fk_refint_game_lineups_game' AND NOT convalidated) THEN
        ALTER TABLE game_lineups VALIDATE CONSTRAINT fk_refint_game_lineups_game;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'game_lineups'::regclass AND conname = 'fk_refint_game_lineups_player' AND NOT convalidated) THEN
        ALTER TABLE game_lineups VALIDATE CONSTRAINT fk_refint_game_lineups_player;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'player_season_batting'::regclass AND conname = 'fk_refint_player_season_batting_player' AND NOT convalidated) THEN
        ALTER TABLE player_season_batting VALIDATE CONSTRAINT fk_refint_player_season_batting_player;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'player_season_batting'::regclass AND conname = 'fk_refint_player_season_batting_team' AND NOT convalidated) THEN
        ALTER TABLE player_season_batting VALIDATE CONSTRAINT fk_refint_player_season_batting_team;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'player_season_pitching'::regclass AND conname = 'fk_refint_player_season_pitching_player' AND NOT convalidated) THEN
        ALTER TABLE player_season_pitching VALIDATE CONSTRAINT fk_refint_player_season_pitching_player;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid = 'player_season_pitching'::regclass AND conname = 'fk_refint_player_season_pitching_team' AND NOT convalidated) THEN
        ALTER TABLE player_season_pitching VALIDATE CONSTRAINT fk_refint_player_season_pitching_team;
    END IF;
END $$;
