create or replace function public.set_manage_user_active(
  p_studio_id uuid,
  p_user_id uuid,
  p_active boolean
)
returns table (
  id uuid,
  active boolean,
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

  if p_studio_id is null or p_user_id is null then
    raise exception 'missing_studio_or_user_id' using errcode = '22023';
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

  if not exists (
    select 1
    from public.users u
    left join public.studio_members sm
      on sm.user_id = u.id
     and sm.studio_id = p_studio_id
    where u.id = p_user_id
      and u.studio_id = p_studio_id
      and (
        'student' = any(coalesce(u.roles, '{}'::text[]))
        or 'student' = any(coalesce(sm.roles, '{}'::text[]))
      )
  ) then
    raise exception 'target_user_is_not_a_student_in_this_studio' using errcode = '42501';
  end if;

  return query
  update public.users u
  set
    active = coalesce(p_active, true),
    deactivated_at = case when coalesce(p_active, true) then null else now() end,
    deactivated_reason = case when coalesce(p_active, true) then null else u.deactivated_reason end
  where u.id = p_user_id
    and u.studio_id = p_studio_id
  returning u.id, coalesce(u.active, true), u.deactivated_at;
end;
$$;

revoke all on function public.set_manage_user_active(uuid, uuid, boolean) from public;
grant execute on function public.set_manage_user_active(uuid, uuid, boolean) to authenticated;
