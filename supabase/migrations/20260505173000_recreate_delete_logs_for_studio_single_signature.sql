drop function if exists public.delete_logs_for_studio(bigint[], uuid);
drop function if exists public.delete_logs_for_studio(uuid, bigint[]);

create function public.delete_logs_for_studio(
  p_studio_id uuid,
  p_log_ids bigint[]
)
returns table (
  id bigint,
  "userId" uuid,
  studio_id uuid,
  affected_user_ids uuid[]
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
  with targets as materialized (
    select l.id, l."userId", l.studio_id
    from public.logs l
    where l.studio_id = p_studio_id
      and l.id = any(p_log_ids)
  ),
  affected as (
    select coalesce(array_agg(distinct t."userId") filter (where t."userId" is not null), '{}'::uuid[]) as user_ids
    from targets t
  ),
  deleted as (
    delete from public.logs l
    using targets t
    where l.id = t.id
      and l.studio_id = t.studio_id
    returning l.id, l."userId", l.studio_id
  )
  select d.id, d."userId", d.studio_id, a.user_ids
  from deleted d
  cross join affected a;
end;
$$;

revoke all on function public.delete_logs_for_studio(uuid, bigint[]) from public;
grant execute on function public.delete_logs_for_studio(uuid, bigint[]) to authenticated;
