-- Oracle port of 025_increase_lineup_notes_length.sql.
ALTER TABLE GAME_LINEUPS MODIFY (NOTES VARCHAR2(512));
