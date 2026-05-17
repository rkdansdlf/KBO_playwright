-- Resolve player_movements rows using same-season team_daily_roster canonical links.
-- The roster table is still a source snapshot, but player rows now carry a
-- verified player_basic_id, making a same-team/same-name/same-year singleton a
-- safe movement identity hint.

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
     AND EXTRACT(YEAR FROM team_daily_roster.roster_date)::INTEGER =
         EXTRACT(YEAR FROM movement_scope.movement_date)::INTEGER
     AND team_daily_roster.person_type = 'player'
     AND team_daily_roster.player_basic_id IS NOT NULL
    GROUP BY movement_scope.id
    HAVING COUNT(DISTINCT team_daily_roster.player_basic_id) = 1
)
UPDATE player_movements
SET player_basic_id = roster_resolved.player_basic_id,
    resolution_status = 'resolved'
FROM roster_resolved
WHERE player_movements.id = roster_resolved.id;
