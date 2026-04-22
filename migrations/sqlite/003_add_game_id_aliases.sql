-- 003_add_game_id_aliases.sql
-- SQLite identity repair schema. Add game.is_primary manually only when absent:
-- ALTER TABLE game ADD COLUMN is_primary BOOLEAN DEFAULT 1;

CREATE TABLE IF NOT EXISTS game_id_aliases (
    alias_game_id VARCHAR(20) PRIMARY KEY,
    canonical_game_id VARCHAR(20) NOT NULL,
    source VARCHAR(50),
    reason VARCHAR(120),
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY(canonical_game_id) REFERENCES game(game_id)
);

CREATE INDEX IF NOT EXISTS idx_game_id_aliases_canonical_game_id
ON game_id_aliases (canonical_game_id);
