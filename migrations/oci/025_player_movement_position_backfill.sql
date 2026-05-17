-- Resolve player_movements rows that are ambiguous by name alone but unique
-- when the source snapshot's parenthesized position is included.

WITH unresolved AS (
    SELECT
        pm.id,
        regexp_replace(COALESCE(pm.player_name, ''), '\s*\([^)]*\)\s*$', '') AS normalized_name,
        substring(pm.player_name from '\(([^)]*)\)') AS raw_position
    FROM player_movements pm
    WHERE pm.player_basic_id IS NULL
      AND pm.resolution_status IN ('unresolved', 'unresolved_player')
),
position_resolved AS (
    SELECT unresolved.id, MIN(player_basic.player_id) AS player_basic_id
    FROM unresolved
    JOIN player_basic
      ON player_basic.name = unresolved.normalized_name
     AND player_basic.position = unresolved.raw_position
    WHERE unresolved.raw_position IS NOT NULL
      AND unresolved.raw_position <> ''
    GROUP BY unresolved.id
    HAVING COUNT(DISTINCT player_basic.player_id) = 1
)
UPDATE player_movements
SET player_basic_id = position_resolved.player_basic_id,
    resolution_status = 'resolved'
FROM position_resolved
WHERE player_movements.id = position_resolved.id;
