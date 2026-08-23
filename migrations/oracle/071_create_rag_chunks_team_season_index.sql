-- Team+season candidate lookups scan every team row and filter season per row
-- with the single-column indexes, costing ~850ms before vector distance.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'IDX_RAG_CHUNKS_TEAM_SEASON';

    IF v_exists = 0 THEN
        EXECUTE IMMEDIATE 'CREATE INDEX IDX_RAG_CHUNKS_TEAM_SEASON ON RAG_CHUNKS (TEAM_ID, SEASON_YEAR) ONLINE';
    END IF;
END;
/
