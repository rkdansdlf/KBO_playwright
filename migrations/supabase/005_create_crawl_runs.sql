CREATE TABLE IF NOT EXISTS public.crawl_runs (
    id SERIAL PRIMARY KEY,
    label text,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    active_count integer NOT NULL DEFAULT 0,
    retired_count integer NOT NULL DEFAULT 0,
    staff_count integer NOT NULL DEFAULT 0,
    confirmed_profiles integer NOT NULL DEFAULT 0,
    heuristic_only integer NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now()
);
