
-- Migration: Add franchises table and link to teams
-- Description: Formalizes the technical game_id segments (SK, HT, etc.) as Franchises

-- 1. Create franchises table
CREATE TABLE IF NOT EXISTS public.franchises (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,              -- Representative name (e.g. "SSG Landers Franchise")
    original_code VARCHAR(10) NOT NULL,     -- The technical code used in game_id (e.g. "SK", "WO", "HT")
    current_code VARCHAR(10) NOT NULL,       -- The current canonical code (e.g. "SSG", "KI", "KIA")
    created_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc'::text, now()) NOT NULL
);

-- 2. Add Unique constraint on original_code
ALTER TABLE public.franchises ADD CONSTRAINT uq_franchises_original_code UNIQUE (original_code);

-- 3. Update teams table
ALTER TABLE public.teams ADD COLUMN IF NOT EXISTS franchise_id INTEGER;

-- 4. Add FK constraint
ALTER TABLE public.teams 
    ADD CONSTRAINT fk_teams_franchise 
    FOREIGN KEY (franchise_id) 
    REFERENCES public.franchises(id);

-- 5. Seed initial franchise data (Idempotent)
INSERT INTO public.franchises (name, original_code, current_code) VALUES
('삼성 라이온즈', 'SS', 'SS'),
('롯데 자이언츠', 'LT', 'LT'),
('LG 트윈스', 'LG', 'LG'),
('두산 베어스', 'OB', 'OB'),
('KIA 타이거즈', 'HT', 'KIA'),
('키움 히어로즈', 'WO', 'WO'),
('한화 이글스', 'HH', 'HH'),
('SSG 랜더스', 'SK', 'SSG'),
('NC 다이노스', 'NC', 'NC'),
('KT 위즈', 'KT', 'KT')
ON CONFLICT (original_code) DO NOTHING;
