-- Oracle port of 024_increase_pitching_string_lengths.sql.
ALTER TABLE PLAYER_SEASON_PITCHING
    MODIFY (
        LEAGUE VARCHAR2(50),
        LEVEL VARCHAR2(50),
        SOURCE VARCHAR2(50)
    );
