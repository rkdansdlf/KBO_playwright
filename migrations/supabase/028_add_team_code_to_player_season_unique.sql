-- Preserve separate season rows when a player represents multiple teams.
ALTER TABLE IF EXISTS public.player_season_batting DROP CONSTRAINT IF EXISTS uq_player_season_batting;
ALTER TABLE IF EXISTS public.player_season_batting DROP CONSTRAINT IF EXISTS uq_player_season_batting_new;
ALTER TABLE IF EXISTS public.player_season_batting DROP CONSTRAINT IF EXISTS uq_player_season_batting_team;
ALTER TABLE IF EXISTS public.player_season_batting
    ADD CONSTRAINT uq_player_season_batting_team UNIQUE (player_id, season, league, level, team_code);

ALTER TABLE IF EXISTS public.player_season_pitching DROP CONSTRAINT IF EXISTS uq_player_season_pitching;
ALTER TABLE IF EXISTS public.player_season_pitching DROP CONSTRAINT IF EXISTS uq_player_season_pitching_team;
ALTER TABLE IF EXISTS public.player_season_pitching
    ADD CONSTRAINT uq_player_season_pitching_team UNIQUE (player_id, season, league, level, team_code);
