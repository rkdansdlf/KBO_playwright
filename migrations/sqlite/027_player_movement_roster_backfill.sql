-- Resolve player_movements using verified same-season roster entries.

UPDATE player_movements
SET player_basic_id = resolved.player_basic_id,
    resolution_status = 'resolved'
FROM (
    WITH movement_scope AS (
        SELECT
            pm.id,
            pm.movement_date,
            pm.canonical_team_id,
            regexp_replace(COALESCE(pm.player_name, ''), '\s*\([^)]*\)\s*$', '') AS normalized_name
        FROM player_movements pm
        WHERE pm.player_basic_id IS NULL
          AND pm.resolution_status IN ('unresolved', 'unresolved_player')
          AND pm.canonical_team_id IS NOT NULL
    ),
    roster_resolved AS (
        SELECT
            movement_scope.id,
            MIN(team_daily_roster.player_basic_id) AS player_basic_id
        FROM movement_scope
        JOIN team_daily_roster
          ON team_daily_roster.team_code = movement_scope.canonical_team_id
         AND team_daily_roster.player_name = movement_scope.normalized_name
         AND CAST(strftime('%Y', team_daily_roster.roster_date) AS INTEGER) =
             CAST(strftime('%Y', movement_scope.movement_date) AS INTEGER)
         AND team_daily_roster.person_type = 'player'
         AND team_daily_roster.player_basic_id IS NOT NULL
        GROUP BY movement_scope.id
        HAVING COUNT(DISTINCT team_daily_roster.player_basic_id) = 1
    )
    SELECT * FROM roster_resolved
) AS resolved
WHERE player_movements.id = resolved.id;
