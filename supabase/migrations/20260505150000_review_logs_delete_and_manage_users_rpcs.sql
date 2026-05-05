create or replace function public.delete_logs_for_studio(
  p_studio_id uuid,
  p_log_ids bigint[]
)
returns table (
  id bigint,
  "userId" uuid,
  studio_id uuid
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

  if coalesce(array_length(p_log_ids, 1), 0) = 0 then
    return;
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
  delete from public.logs l
  where l.studio_id = p_studio_id
    and l.id = any(p_log_ids)
  returning l.id, l."userId", l.studio_id;
end;
$$;

revoke all on function public.delete_logs_for_studio(uuid, bigint[]) from public;
grant execute on function public.delete_logs_for_studio(uuid, bigint[]) to authenticated;

create or replace function public.get_manage_users_for_studio(
  p_studio_id uuid
)
returns table (
  id uuid,
  studio_id uuid,
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
    u.studio_id,
    u."firstName",
    u."lastName",
    u.email,
    u."avatarUrl",
    u."teacherIds",
    u.instrument,
    u.points,
    u.level,
    coalesce(u.active, true) as active,
    u.roles,
    sm.roles as membership_roles,
    u.roles as identity_roles,
    u.parent_uuid,
    u.deactivated_at
  from public.users u
  left join public.studio_members sm
    on sm.user_id = u.id
   and sm.studio_id = u.studio_id
  where u.studio_id = p_studio_id
  order by lower(coalesce(u."lastName", '')), lower(coalesce(u."firstName", '')), lower(coalesce(u.email, ''));
end;
$$;

revoke all on function public.get_manage_users_for_studio(uuid) from public;
grant execute on function public.get_manage_users_for_studio(uuid) to authenticated;
