-- Resolver-driven scalar filters (team, season, player) had no B-tree indexes,
-- forcing full rag_chunks scans before every filtered vector search.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNKS_TEAM_ID';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE INDEX IDX_RAG_CHUNKS_TEAM_ID ON RAG_CHUNKS (TEAM_ID)';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNKS_SEASON_YEAR';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE INDEX IDX_RAG_CHUNKS_SEASON_YEAR ON RAG_CHUNKS (SEASON_YEAR)';
    END IF;
END;
/

DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNKS_PLAYER_ID';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE INDEX IDX_RAG_CHUNKS_PLAYER_ID ON RAG_CHUNKS (PLAYER_ID)';
    END IF;
END;
/
