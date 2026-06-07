-- Create awards table conditionally
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'awards') THEN
        CREATE TABLE awards (
            id SERIAL PRIMARY KEY,
            year INTEGER NOT NULL,
            award_type VARCHAR(50) NOT NULL,
            category VARCHAR(50),
            player_name VARCHAR(100) NOT NULL,
            team_name VARCHAR(50) NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

            CONSTRAINT uq_award_record UNIQUE (year, award_type, category, player_name, team_name)
        );
        CREATE INDEX idx_award_year ON awards(year);
    ELSE
        -- Table already exists. Create index on whichever column exists (year or award_year)
        IF EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'awards' AND column_name = 'year') THEN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_award_year ON awards(year)';
        ELSIF EXISTS (SELECT FROM information_schema.columns WHERE table_name = 'awards' AND column_name = 'award_year') THEN
            EXECUTE 'CREATE INDEX IF NOT EXISTS idx_award_year ON awards(award_year)';
        END IF;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_award_type ON awards(award_type);
CREATE INDEX IF NOT EXISTS idx_award_player ON awards(player_name);

COMMENT ON TABLE awards IS 'KBO Award History (MVP, Golden Glove, etc.)';
