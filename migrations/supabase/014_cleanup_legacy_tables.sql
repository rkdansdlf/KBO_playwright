
-- Migration: Cleanup Legacy Tables
-- Description: Drops tables that have been replaced by the optimized schema.
-- WARNING: This is destructive. Ensure data has been migrated.

-- 1. Drop renamed legacy history table
DROP TABLE IF EXISTS public.team_history_legacy CASCADE;

-- 2. Drop legacy mapping tables if they exist (based on plan, these were "to be deleted")
DROP TABLE IF EXISTS public.team_name_mapping CASCADE;
DROP TABLE IF EXISTS public.team_profiles CASCADE;
