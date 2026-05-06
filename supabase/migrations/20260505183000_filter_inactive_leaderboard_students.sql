create or replace function public.get_leaderboard_students(p_studio_id uuid)
returns table (
  id uuid,
  "firstName" text,
  "lastName" text,
  "avatarUrl" text,
  points integer,
  level integer
)
language sql
security definer
set search_path = public
as $$
  with caller_access as (
    select
      sm.studio_id,
      coalesce(sm.roles, '{}'::text[]) && array['admin', 'teacher', 'owner']::text[] as can_see_names
    from public.studio_members sm
    where sm.studio_id = p_studio_id
      and sm.user_id = auth.uid()
    limit 1
  )
  select
    u.id,
    case when ca.can_see_names or u.id = auth.uid() then u."firstName" else null end as "firstName",
    case when ca.can_see_names or u.id = auth.uid() then u."lastName" else null end as "lastName",
    u."avatarUrl",
    coalesce(u.points, 0)::integer as points,
    coalesce(u.level, 1)::integer as level
  from public.users u
  join caller_access ca
    on ca.studio_id = u.studio_id
  where u.studio_id = p_studio_id
    and 'student' = any (coalesce(u.roles, '{}'::text[]))
    and u.active = true
    and u.deactivated_at is null
    and coalesce(u.showonleaderboard, true) = true
  order by coalesce(u.points, 0) desc, u."lastName" asc, u."firstName" asc;
$$;

revoke all on function public.get_leaderboard_students(uuid) from public;
grant execute on function public.get_leaderboard_students(uuid) to authenticated;
