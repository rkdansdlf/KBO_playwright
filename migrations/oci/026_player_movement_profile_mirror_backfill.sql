-- Resolve player_movements rows where duplicate player_basic name candidates
-- are narrowed to exactly one canonical profile mirror.

WITH movement_scope AS (
    SELECT
        pm.id,
        regexp_replace(COALESCE(pm.player_name, ''), '\s*\([^)]*\)\s*$', '') AS normalized_name,
        substring(pm.player_name from '\(([^)]*)\)') AS raw_position
    FROM player_movements pm
    WHERE pm.player_basic_id IS NULL
      AND pm.resolution_status IN ('unresolved', 'unresolved_player')
      AND pm.canonical_team_id IS NOT NULL
),
candidate_scope AS (
    SELECT
        movement_scope.id,
        player_basic.player_id,
        player_basic.position,
        movement_scope.raw_position,
        BOOL_OR(player_basic.position = movement_scope.raw_position) OVER (
            PARTITION BY movement_scope.id
        ) AS has_position_candidates
    FROM movement_scope
    JOIN player_basic
      ON player_basic.name = movement_scope.normalized_name
),
profile_mirror_resolved AS (
    SELECT candidate_scope.id, MIN(candidate_scope.player_id) AS player_basic_id
    FROM candidate_scope
    JOIN players
      ON players.player_basic_id = candidate_scope.player_id
    WHERE candidate_scope.raw_position IS NULL
       OR candidate_scope.raw_position = ''
       OR NOT candidate_scope.has_position_candidates
       OR candidate_scope.position = candidate_scope.raw_position
    GROUP BY candidate_scope.id
    HAVING COUNT(DISTINCT candidate_scope.player_id) = 1
)
UPDATE player_movements
SET player_basic_id = profile_mirror_resolved.player_basic_id,
    resolution_status = 'resolved'
FROM profile_mirror_resolved
WHERE player_movements.id = profile_mirror_resolved.id;
