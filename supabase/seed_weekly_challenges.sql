-- Weekly Challenges seed.
-- This seed is intentionally idempotent because the migration uses
-- `on conflict (week_number) do update`.
\ir migrations/20260514120000_add_weekly_challenges.sql
