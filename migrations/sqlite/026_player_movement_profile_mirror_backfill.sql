-- Resolve player_movements where duplicate name candidates are narrowed
-- to a single player_basic via the profile mirror (players table).

UPDATE player_movements
SET player_basic_id = resolved.player_basic_id,
    resolution_status = 'resolved'
FROM (
    WITH movement_scope AS (
        SELECT
            pm.id,
            regexp_replace(COALESCE(pm.player_name, ''), '\s*\([^)]*\)\s*$', '') AS normalized_name,
            substr(pm.player_name, instr(pm.player_name, '(') + 1,
                   length(pm.player_name) - instr(pm.player_name, '(') -
                   CASE WHEN instr(pm.player_name, ')') > 0
                        THEN length(pm.player_name) - instr(pm.player_name, ')')
                        ELSE 0 END - 1) AS raw_position
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
            movement_scope.raw_position
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
           OR candidate_scope.position = candidate_scope.raw_position
        GROUP BY candidate_scope.id
        HAVING COUNT(DISTINCT candidate_scope.player_id) = 1
    )
    SELECT * FROM profile_mirror_resolved
) AS resolved
WHERE player_movements.id = resolved.id;
