drop function if exists public.get_manage_users_for_studio(uuid);

create or replace function public.get_manage_users_for_studio(
  p_studio_id uuid
)
returns table (
  id uuid,
  "firstName" text,
  "lastName" text,
  email text,
  "avatarUrl" text,
  "teacherIds" text[],
  instrument text[],
  points integer,
  level integer,
  active boolean,
  roles text[],
  studio_id uuid,
  membership_roles text[],
  identity_roles text[],
  parent_uuid uuid,
  deactivated_at timestamptz
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_allowed boolean := false;
begin
  if auth.uid() is null then
    raise exception 'not_authenticated' using errcode = '28000';
  end if;

  if p_studio_id is null then
    raise exception 'missing_studio_id' using errcode = '22023';
  end if;

  select exists (
    select 1
    from public.studio_members sm
    where sm.studio_id = p_studio_id
      and sm.user_id = auth.uid()
      and coalesce(sm.roles, '{}'::text[]) && array['owner', 'admin', 'teacher']::text[]
  ) or exists (
    select 1
    from public.studios s
    where s.id = p_studio_id
      and s.account_holder_user_id = auth.uid()
  )
  into v_allowed;

  if not v_allowed then
    raise exception 'not_authorized' using errcode = '42501';
  end if;

  return query
  select
    u.id,
    u."firstName"::text,
    u."lastName"::text,
    u.email::text,
    u."avatarUrl"::text,
    coalesce(u."teacherIds", '{}'::text[]),
    coalesce(u.instrument, '{}'::text[]),
    coalesce(u.points, 0)::integer,
    coalesce(u.level, 1)::integer,
    case when u.deactivated_at is null then coalesce(u.active, true) else false end,
    coalesce(u.roles, '{}'::text[]),
    u.studio_id,
    coalesce(sm.roles, '{}'::text[]),
    coalesce(u.roles, '{}'::text[]),
    u.parent_uuid,
    u.deactivated_at
  from public.users u
  left join public.studio_members sm
    on sm.user_id = u.id
   and sm.studio_id = u.studio_id
  where u.studio_id = p_studio_id
  order by lower(coalesce(u."lastName"::text, '')), lower(coalesce(u."firstName"::text, '')), lower(coalesce(u.email::text, ''));
end;
$$;

revoke all on function public.get_manage_users_for_studio(uuid) from public;
grant execute on function public.get_manage_users_for_studio(uuid) to authenticated;
