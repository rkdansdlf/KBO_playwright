-- Deletion-anomaly integrity hardening.
-- Adds canonical player/team links and enforces game-child cascade semantics.

ALTER TABLE players
    ADD COLUMN IF NOT EXISTS player_basic_id INTEGER;

ALTER TABLE team_daily_roster
    ADD COLUMN IF NOT EXISTS player_basic_id INTEGER,
    ADD COLUMN IF NOT EXISTS person_type VARCHAR(16) DEFAULT 'player' NOT NULL;

ALTER TABLE player_movements
    ADD COLUMN IF NOT EXISTS canonical_team_id VARCHAR(10),
    ADD COLUMN IF NOT EXISTS player_basic_id INTEGER,
    ADD COLUMN IF NOT EXISTS resolution_status VARCHAR(24) DEFAULT 'unresolved' NOT NULL;

-- team_code is retained as the raw source snapshot. Rewriting it in a trigger
-- can collide with historical duplicate snapshots; use canonical_team_id for
-- normalized joins instead.
DROP TRIGGER IF EXISTS trg_normalize_player_movements_team_code ON player_movements;

UPDATE players
SET player_basic_id = kbo_person_id::INTEGER
WHERE kbo_person_id ~ '^[0-9]+$'
  AND EXISTS (
      SELECT 1 FROM player_basic
      WHERE player_basic.player_id = players.kbo_person_id::INTEGER
  )
  AND (player_basic_id IS NULL OR player_basic_id <> kbo_person_id::INTEGER);

BEGIN;
ALTER TABLE team_daily_roster DISABLE TRIGGER USER;

UPDATE team_daily_roster
SET position = COALESCE(
    (
        SELECT NULLIF(player_basic.position, '')
        FROM player_basic
        WHERE player_basic.player_id = team_daily_roster.player_id
          AND player_basic.position IN ('투수', '포수', '내야수', '외야수')
    ),
    CASE
        WHEN EXISTS (
            SELECT 1 FROM player_basic
            WHERE player_basic.player_id = team_daily_roster.player_id
        )
        THEN '선수'
        ELSE '코치'
    END
)
WHERE position = '포지션';

UPDATE team_daily_roster
SET person_type = CASE
    WHEN position IN ('투수', '포수', '내야수', '외야수', '선수') THEN 'player'
    WHEN position IN ('감독', '코치') THEN 'staff'
    ELSE 'unknown'
END
WHERE person_type IS DISTINCT FROM CASE
    WHEN position IN ('투수', '포수', '내야수', '외야수', '선수') THEN 'player'
    WHEN position IN ('감독', '코치') THEN 'staff'
    ELSE 'unknown'
END;

UPDATE team_daily_roster
SET player_basic_id = player_id
WHERE person_type = 'player'
  AND player_basic_id IS DISTINCT FROM player_id
  AND EXISTS (SELECT 1 FROM player_basic WHERE player_basic.player_id = team_daily_roster.player_id);

UPDATE team_daily_roster
SET player_basic_id = NULL
WHERE person_type <> 'player'
  AND player_basic_id IS NOT NULL;

ALTER TABLE team_daily_roster ENABLE TRIGGER USER;
COMMIT;

UPDATE player_movements
SET canonical_team_id = CASE TRIM(team_code)
    WHEN 'KIA' THEN 'KIA'
    WHEN '기아' THEN 'KIA'
    WHEN '두산' THEN 'DB'
    WHEN 'DB' THEN 'DB'
    WHEN 'OB' THEN 'OB'
    WHEN '롯데' THEN 'LT'
    WHEN 'LT' THEN 'LT'
    WHEN '삼성' THEN 'SS'
    WHEN 'SS' THEN 'SS'
    WHEN '한화' THEN 'HH'
    WHEN 'HH' THEN 'HH'
    WHEN '키움' THEN 'KH'
    WHEN 'KH' THEN 'KH'
    WHEN '넥센' THEN 'NX'
    WHEN 'NX' THEN 'NX'
    WHEN '우리' THEN 'WO'
    WHEN 'WO' THEN 'WO'
    WHEN 'SSG' THEN 'SSG'
    WHEN 'SK' THEN 'SK'
    WHEN 'LG' THEN 'LG'
    WHEN 'KT' THEN 'KT'
    WHEN 'kt' THEN 'KT'
    WHEN 'NC' THEN 'NC'
    WHEN '현대' THEN 'HU'
    WHEN 'HU' THEN 'HU'
    WHEN 'HD' THEN 'HU'
    WHEN '해태' THEN 'HT'
    WHEN 'HT' THEN 'HT'
    WHEN '쌍방울' THEN 'SL'
    WHEN 'SL' THEN 'SL'
    WHEN '태평양' THEN 'TP'
    WHEN 'TP' THEN 'TP'
    WHEN '청보' THEN 'CB'
    WHEN 'CB' THEN 'CB'
    WHEN '삼미' THEN 'SM'
    WHEN 'SM' THEN 'SM'
    WHEN '빙그레' THEN 'BE'
    WHEN 'BE' THEN 'BE'
    WHEN 'MBC' THEN 'MBC'
    ELSE TRIM(team_code)
END;

UPDATE player_movements
SET canonical_team_id = NULL
WHERE canonical_team_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM teams WHERE teams.team_id = player_movements.canonical_team_id);

WITH movement_scope AS (
    SELECT
        id,
        canonical_team_id,
        EXTRACT(YEAR FROM movement_date)::INTEGER AS movement_year,
        regexp_replace(COALESCE(player_name, ''), '\s*\([^)]*\)\s*$', '') AS normalized_name
    FROM player_movements
    WHERE canonical_team_id IS NULL
),
history_teams AS (
    SELECT movement_scope.id, player_season_batting.team_code
    FROM movement_scope
    JOIN player_basic ON player_basic.name = movement_scope.normalized_name
    JOIN player_season_batting ON player_season_batting.player_id = player_basic.player_id
    WHERE player_season_batting.team_code IS NOT NULL
      AND player_season_batting.team_code <> ''
      AND player_season_batting.season <= movement_scope.movement_year
    UNION ALL
    SELECT movement_scope.id, player_season_pitching.team_code
    FROM movement_scope
    JOIN player_basic ON player_basic.name = movement_scope.normalized_name
    JOIN player_season_pitching ON player_season_pitching.player_id = player_basic.player_id
    WHERE player_season_pitching.team_code IS NOT NULL
      AND player_season_pitching.team_code <> ''
      AND player_season_pitching.season <= movement_scope.movement_year
),
unique_history_team AS (
    SELECT history_teams.id, MIN(history_teams.team_code) AS team_code
    FROM history_teams
    JOIN teams ON teams.team_id = history_teams.team_code
    GROUP BY history_teams.id
    HAVING COUNT(DISTINCT history_teams.team_code) = 1
)
UPDATE player_movements
SET canonical_team_id = unique_history_team.team_code
FROM unique_history_team
WHERE player_movements.id = unique_history_team.id
  AND player_movements.canonical_team_id IS NULL;

WITH movement_scope AS (
    SELECT
        id,
        canonical_team_id,
        EXTRACT(YEAR FROM movement_date)::INTEGER AS movement_year,
        regexp_replace(COALESCE(player_name, ''), '\s*\([^)]*\)\s*$', '') AS normalized_name
    FROM player_movements
    WHERE canonical_team_id IS NOT NULL
),
unique_name AS (
    SELECT movement_scope.id, MIN(player_basic.player_id) AS player_id
    FROM movement_scope
    JOIN player_basic ON player_basic.name = movement_scope.normalized_name
    GROUP BY movement_scope.id
    HAVING COUNT(DISTINCT player_basic.player_id) = 1
),
team_context AS (
    SELECT movement_scope.id, MIN(player_basic.player_id) AS player_id
    FROM movement_scope
    JOIN player_basic ON player_basic.name = movement_scope.normalized_name
    LEFT JOIN teams ON teams.team_id = movement_scope.canonical_team_id
    WHERE COALESCE(player_basic.team, '') LIKE '%' || movement_scope.canonical_team_id || '%'
       OR (teams.team_short_name IS NOT NULL AND COALESCE(player_basic.team, '') LIKE '%' || teams.team_short_name || '%')
       OR (teams.team_name IS NOT NULL AND COALESCE(player_basic.team, '') LIKE '%' || teams.team_name || '%')
    GROUP BY movement_scope.id
    HAVING COUNT(DISTINCT player_basic.player_id) = 1
),
season_context AS (
    SELECT id, MIN(player_id) AS player_id
    FROM (
        SELECT movement_scope.id, player_season_batting.player_id
        FROM movement_scope
        JOIN player_basic ON player_basic.name = movement_scope.normalized_name
        JOIN player_season_batting
          ON player_season_batting.player_id = player_basic.player_id
         AND player_season_batting.season = movement_scope.movement_year
         AND player_season_batting.team_code = movement_scope.canonical_team_id
        UNION ALL
        SELECT movement_scope.id, player_season_pitching.player_id
        FROM movement_scope
        JOIN player_basic ON player_basic.name = movement_scope.normalized_name
        JOIN player_season_pitching
          ON player_season_pitching.player_id = player_basic.player_id
         AND player_season_pitching.season = movement_scope.movement_year
         AND player_season_pitching.team_code = movement_scope.canonical_team_id
    ) AS season_candidates
    GROUP BY id
    HAVING COUNT(DISTINCT player_id) = 1
),
resolved AS (
    SELECT
        movement_scope.id,
        COALESCE(unique_name.player_id, team_context.player_id, season_context.player_id) AS player_id
    FROM movement_scope
    LEFT JOIN unique_name ON unique_name.id = movement_scope.id
    LEFT JOIN team_context ON team_context.id = movement_scope.id
    LEFT JOIN season_context ON season_context.id = movement_scope.id
)
UPDATE player_movements
SET player_basic_id = resolved.player_id
FROM resolved
WHERE player_movements.id = resolved.id
  AND resolved.player_id IS NOT NULL;

UPDATE player_movements
SET resolution_status = CASE
    WHEN canonical_team_id IS NULL THEN 'unresolved_team'
    WHEN player_basic_id IS NULL THEN 'unresolved_player'
    ELSE 'resolved'
END;

CREATE UNIQUE INDEX IF NOT EXISTS uq_players_player_basic_id
    ON players (player_basic_id)
    WHERE player_basic_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_players_player_basic_id
    ON players (player_basic_id)
    WHERE player_basic_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_team_daily_roster_player_basic_id
    ON team_daily_roster (player_basic_id)
    WHERE player_basic_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_player_movements_player_basic_id
    ON player_movements (player_basic_id)
    WHERE player_basic_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_player_movements_canonical_team_id
    ON player_movements (canonical_team_id)
    WHERE canonical_team_id IS NOT NULL;

CREATE OR REPLACE FUNCTION pg_temp.drop_single_column_fk(
    child_table text,
    child_column text,
    parent_table text,
    parent_column text
)
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    constraint_name text;
BEGIN
    FOR constraint_name IN
        SELECT c.conname
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
    LOOP
        EXECUTE format('ALTER TABLE %I DROP CONSTRAINT %I', child_table, constraint_name);
    END LOOP;
END $$;

DO $$
BEGIN
    PERFORM pg_temp.drop_single_column_fk('players', 'player_basic_id', 'player_basic', 'player_id');
    ALTER TABLE players
        ADD CONSTRAINT fk_players_player_basic
        FOREIGN KEY (player_basic_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('team_daily_roster', 'team_code', 'teams', 'team_id');
    ALTER TABLE team_daily_roster
        ADD CONSTRAINT fk_team_daily_roster_team
        FOREIGN KEY (team_code) REFERENCES teams (team_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('team_daily_roster', 'player_basic_id', 'player_basic', 'player_id');
    ALTER TABLE team_daily_roster
        ADD CONSTRAINT fk_team_daily_roster_player_basic
        FOREIGN KEY (player_basic_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('player_movements', 'canonical_team_id', 'teams', 'team_id');
    ALTER TABLE player_movements
        ADD CONSTRAINT fk_player_movements_canonical_team
        FOREIGN KEY (canonical_team_id) REFERENCES teams (team_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('player_movements', 'player_basic_id', 'player_basic', 'player_id');
    ALTER TABLE player_movements
        ADD CONSTRAINT fk_player_movements_player_basic
        FOREIGN KEY (player_basic_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_id_aliases', 'canonical_game_id', 'game', 'game_id');
    ALTER TABLE game_id_aliases
        ADD CONSTRAINT fk_game_id_aliases_game
        FOREIGN KEY (canonical_game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_metadata', 'game_id', 'game', 'game_id');
    ALTER TABLE game_metadata
        ADD CONSTRAINT fk_game_metadata_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_inning_scores', 'game_id', 'game', 'game_id');
    ALTER TABLE game_inning_scores
        ADD CONSTRAINT fk_game_inning_scores_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_lineups', 'game_id', 'game', 'game_id');
    ALTER TABLE game_lineups
        ADD CONSTRAINT fk_game_lineups_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_lineups', 'player_id', 'player_basic', 'player_id');
    ALTER TABLE game_lineups
        ADD CONSTRAINT fk_game_lineups_player
        FOREIGN KEY (player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_batting_stats', 'game_id', 'game', 'game_id');
    ALTER TABLE game_batting_stats
        ADD CONSTRAINT fk_game_batting_stats_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_batting_stats', 'player_id', 'player_basic', 'player_id');
    ALTER TABLE game_batting_stats
        ADD CONSTRAINT fk_game_batting_stats_player
        FOREIGN KEY (player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_pitching_stats', 'game_id', 'game', 'game_id');
    ALTER TABLE game_pitching_stats
        ADD CONSTRAINT fk_game_pitching_stats_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_pitching_stats', 'player_id', 'player_basic', 'player_id');
    ALTER TABLE game_pitching_stats
        ADD CONSTRAINT fk_game_pitching_stats_player
        FOREIGN KEY (player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_events', 'game_id', 'game', 'game_id');
    ALTER TABLE game_events
        ADD CONSTRAINT fk_game_events_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_events', 'batter_id', 'player_basic', 'player_id');
    ALTER TABLE game_events
        ADD CONSTRAINT fk_game_events_batter
        FOREIGN KEY (batter_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_events', 'pitcher_id', 'player_basic', 'player_id');
    ALTER TABLE game_events
        ADD CONSTRAINT fk_game_events_pitcher
        FOREIGN KEY (pitcher_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_summary', 'game_id', 'game', 'game_id');
    ALTER TABLE game_summary
        ADD CONSTRAINT fk_game_summary_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_summary', 'player_id', 'player_basic', 'player_id');
    ALTER TABLE game_summary
        ADD CONSTRAINT fk_game_summary_player
        FOREIGN KEY (player_id) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID;

    PERFORM pg_temp.drop_single_column_fk('game_play_by_play', 'game_id', 'game', 'game_id');
    ALTER TABLE game_play_by_play
        ADD CONSTRAINT fk_game_play_by_play_game
        FOREIGN KEY (game_id) REFERENCES game (game_id) ON DELETE CASCADE NOT VALID;
END $$;

DO $$
DECLARE
    target record;
BEGIN
    FOR target IN
        SELECT * FROM (VALUES
            ('matchup_bvp', 'batter_id', 'fk_matchup_bvp_batter'),
            ('matchup_bvp', 'pitcher_id', 'fk_matchup_bvp_pitcher'),
            ('matchup_batter_splits', 'player_id', 'fk_matchup_batter_splits_player'),
            ('matchup_pitcher_splits', 'player_id', 'fk_matchup_pitcher_splits_player'),
            ('matchup_batter_team_split', 'player_id', 'fk_matchup_batter_team_split_player'),
            ('matchup_pitcher_team_split', 'player_id', 'fk_matchup_pitcher_team_split_player'),
            ('matchup_batter_stadium_split', 'player_id', 'fk_matchup_batter_stadium_split_player'),
            ('matchup_batter_vs_starter', 'player_id', 'fk_matchup_batter_vs_starter_player')
        ) AS v(table_name, column_name, constraint_name)
    LOOP
        IF to_regclass(target.table_name) IS NOT NULL THEN
            PERFORM pg_temp.drop_single_column_fk(target.table_name, target.column_name, 'player_basic', 'player_id');
            EXECUTE format(
                'ALTER TABLE %I ADD CONSTRAINT %I FOREIGN KEY (%I) REFERENCES player_basic (player_id) ON DELETE RESTRICT NOT VALID',
                target.table_name,
                target.constraint_name,
                target.column_name
            );
        END IF;
    END LOOP;
END $$;

DO $$
DECLARE
    target record;
BEGIN
    FOR target IN
        SELECT conrelid::regclass::text AS table_name, conname
        FROM pg_constraint
        WHERE contype = 'f'
          AND conname IN (
              'fk_players_player_basic',
              'fk_team_daily_roster_team',
              'fk_team_daily_roster_player_basic',
              'fk_player_movements_canonical_team',
              'fk_player_movements_player_basic',
              'fk_game_id_aliases_game',
              'fk_game_metadata_game',
              'fk_game_inning_scores_game',
              'fk_game_lineups_game',
              'fk_game_lineups_player',
              'fk_game_batting_stats_game',
              'fk_game_batting_stats_player',
              'fk_game_pitching_stats_game',
              'fk_game_pitching_stats_player',
              'fk_game_events_game',
              'fk_game_events_batter',
              'fk_game_events_pitcher',
              'fk_game_summary_game',
              'fk_game_summary_player',
              'fk_game_play_by_play_game',
              'fk_matchup_bvp_batter',
              'fk_matchup_bvp_pitcher',
              'fk_matchup_batter_splits_player',
              'fk_matchup_pitcher_splits_player',
              'fk_matchup_batter_team_split_player',
              'fk_matchup_pitcher_team_split_player',
              'fk_matchup_batter_stadium_split_player',
              'fk_matchup_batter_vs_starter_player'
          )
          AND NOT convalidated
    LOOP
        EXECUTE format('ALTER TABLE %s VALIDATE CONSTRAINT %I', target.table_name, target.conname);
    END LOOP;
END $$;
