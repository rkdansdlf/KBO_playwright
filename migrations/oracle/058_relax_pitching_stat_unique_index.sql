-- Preserve multiple pitching appearances by one player in a game.
-- The ORM/source contract includes appearance_seq in the unique key.
DECLARE
    v_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_exists
      FROM user_indexes
     WHERE index_name = 'UQ_GAME_PITCHING_STATS_GAME_PLAYER_NONNULL';

    IF v_exists = 1 THEN
        EXECUTE IMMEDIATE 'DROP INDEX UQ_GAME_PITCHING_STATS_GAME_PLAYER_NONNULL';
    END IF;
END;
/
