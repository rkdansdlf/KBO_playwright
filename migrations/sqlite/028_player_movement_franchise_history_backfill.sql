-- Resolve player_movements using same-franchise season history.
-- Covers historical team-code aliases (HT/KIA, OB/DB, SK/SSG, NX/KH).

UPDATE player_movements
SET player_basic_id = resolved.player_basic_id,
    resolution_status = 'resolved'
FROM (
    WITH movement_scope AS (
        SELECT
            pm.id,
            pm.canonical_team_id,
            CAST(strftime('%Y', pm.movement_date) AS INTEGER) AS movement_year,
            regexp_replace(COALESCE(pm.player_name, ''), '\s*\([^)]*\)\s*$', '') AS normalized_name
        FROM player_movements pm
        WHERE pm.player_basic_id IS NULL
          AND pm.resolution_status IN ('unresolved', 'unresolved_player')
          AND pm.canonical_team_id IS NOT NULL
    ),
    franchise_teams AS (
        SELECT movement_scope.id, teams_in_franchise.team_id
        FROM movement_scope
        JOIN teams movement_team
          ON movement_team.team_id = movement_scope.canonical_team_id
         AND movement_team.franchise_id IS NOT NULL
        JOIN teams teams_in_franchise
          ON teams_in_franchise.franchise_id = movement_team.franchise_id
    ),
    season_candidates AS (
        SELECT movement_scope.id, player_season_batting.player_id
        FROM movement_scope
        JOIN player_basic
          ON player_basic.name = movement_scope.normalized_name
        JOIN franchise_teams
          ON franchise_teams.id = movement_scope.id
        JOIN player_season_batting
          ON player_season_batting.player_id = player_basic.player_id
         AND player_season_batting.team_code = franchise_teams.team_id
         AND player_season_batting.season IN (movement_scope.movement_year - 1, movement_scope.movement_year)
        UNION ALL
        SELECT movement_scope.id, player_season_pitching.player_id
        FROM movement_scope
        JOIN player_basic
          ON player_basic.name = movement_scope.normalized_name
        JOIN franchise_teams
          ON franchise_teams.id = movement_scope.id
        JOIN player_season_pitching
          ON player_season_pitching.player_id = player_basic.player_id
         AND player_season_pitching.team_code = franchise_teams.team_id
         AND player_season_pitching.season IN (movement_scope.movement_year - 1, movement_scope.movement_year)
    ),
    franchise_resolved AS (
        SELECT id, MIN(player_id) AS player_basic_id
        FROM season_candidates
        GROUP BY id
        HAVING COUNT(DISTINCT player_id) = 1
    )
    SELECT * FROM franchise_resolved
) AS resolved
WHERE player_movements.id = resolved.id;
