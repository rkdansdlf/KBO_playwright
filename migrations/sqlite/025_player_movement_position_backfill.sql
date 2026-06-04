-- Resolve player_movements rows where parenthesized position in player_name
-- uniquely identifies the player_basic record.

UPDATE player_movements
SET player_basic_id = resolved.player_basic_id,
    resolution_status = 'resolved'
FROM (
    WITH unresolved AS (
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
    )
    SELECT unresolved.id, MIN(player_basic.player_id) AS player_basic_id
    FROM unresolved
    JOIN player_basic
      ON player_basic.name = unresolved.normalized_name
     AND player_basic.position = unresolved.raw_position
    WHERE unresolved.raw_position IS NOT NULL
      AND unresolved.raw_position <> ''
    GROUP BY unresolved.id
    HAVING COUNT(DISTINCT player_basic.player_id) = 1
) AS resolved
WHERE player_movements.id = resolved.id;
