create or replace function public.repair_level_completed_notification_timestamps_for_studio(
  p_studio_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_attempted integer := 0;
  v_updated integer := 0;
  v_unmatched_student integer := 0;
  v_unmatched_level integer := 0;
  v_no_crossing_log integer := 0;
  v_duplicate_groups integer := 0;
begin
  if auth.uid() is null then
    raise exception 'not_authenticated' using errcode = '28000';
  end if;

  if p_studio_id is null then
    raise exception 'missing_studio_id' using errcode = '22023';
  end if;

  if not (
    exists (
      select 1
      from public.studio_members sm
      where sm.studio_id = p_studio_id
        and sm.user_id = auth.uid()
        and coalesce(sm.roles, '{}'::text[]) && array['owner', 'admin']::text[]
    )
    or exists (
      select 1
      from public.studios s
      where s.id = p_studio_id
        and s.account_holder_user_id = auth.uid()
    )
  ) then
    raise exception 'not_authorized' using errcode = '42501';
  end if;

  with notification_targets as (
    select
      n.id,
      n.studio_id,
      coalesce(n.user_id, n."userId") as recipient_id,
      coalesce(
        n.completed_level_start,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
      ) as completed_level_id,
      coalesce(
        n.completed_level_end,
        n.completed_level_start,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+[0-9]+[[:space:]]*[-–][[:space:]]*([0-9]+)', 'i'))[1], '')::integer,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
      ) as completed_level_end_id,
      nullif(btrim((regexp_match(coalesce(n.message, ''), '^[[:space:]]*(.*?)[[:space:]]+completed[[:space:]]+Level', 'i'))[1]), '') as parsed_student_name,
      regexp_replace(
        lower(nullif(btrim((regexp_match(coalesce(n.message, ''), '^[[:space:]]*(.*?)[[:space:]]+completed[[:space:]]+Level', 'i'))[1]), '')),
        '[[:space:]]+',
        ' ',
        'g'
      ) as normalized_student_name
    from public.notifications n
    where n.studio_id = p_studio_id
      and lower(coalesce(n.type, '')) = 'level_completed'
  ),
  target_levels as (
    select
      nt.*,
      (coalesce(nt.completed_level_end_id, nt.completed_level_id) + 1)::integer as target_level_id
    from notification_targets nt
  ),
  user_name_candidates as (
    select
      u.id as student_id,
      u.studio_id,
      regexp_replace(lower(btrim(candidate.name_value)), '[[:space:]]+', ' ', 'g') as normalized_name
    from public.users u
    cross join lateral (
      values
        (to_jsonb(u)->>'full_name'),
        (to_jsonb(u)->>'name'),
        (to_jsonb(u)->>'display_name'),
        (btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", '')))
    ) as candidate(name_value)
    where u.studio_id = p_studio_id
      and nullif(btrim(coalesce(candidate.name_value, '')), '') is not null
  ),
  student_matches as (
    select distinct on (tl.id)
      tl.id as notification_id,
      tl.studio_id,
      u.id as student_id,
      tl.target_level_id
    from target_levels tl
    join public.users u
      on u.studio_id = tl.studio_id
     and (
       u.id = tl.recipient_id
       or exists (
         select 1
         from user_name_candidates unc
         where unc.student_id = u.id
           and unc.normalized_name = tl.normalized_student_name
       )
     )
    where tl.target_level_id is not null
      and tl.target_level_id > 1
    order by
      tl.id,
      case when u.id = tl.recipient_id then 0 else 1 end,
      u.id
  ),
  approved_logs as (
    select
      sm.notification_id,
      sm.studio_id,
      sm.student_id,
      sm.target_level_id,
      l.id as log_id,
      coalesce(l.points, 0)::integer as points,
      coalesce(
        l.date::timestamp at time zone 'UTC',
        nullif(to_jsonb(l)->>'created_at', '')::timestamptz,
        nullif(to_jsonb(l)->>'approved_at', '')::timestamptz,
        now()
      ) as log_event_at
    from student_matches sm
    join public.logs l
      on l.studio_id = sm.studio_id
     and l."userId" = sm.student_id
     and lower(coalesce(l.status, '')) = 'approved'
  ),
  running as (
    select
      al.*,
      coalesce(sum(al.points) over (
        partition by al.notification_id
        order by al.log_event_at, al.log_id
        rows between unbounded preceding and 1 preceding
      ), 0)::integer as previous_points,
      sum(al.points) over (
        partition by al.notification_id
        order by al.log_event_at, al.log_id
        rows between unbounded preceding and current row
      )::integer as running_points
    from approved_logs al
  ),
  crossings as (
    select distinct on (r.notification_id)
      r.notification_id,
      r.log_event_at
    from running r
    join public.levels lv
      on lv.id = r.target_level_id
     and coalesce(lv."minPoints", 0) > r.previous_points
     and coalesce(lv."minPoints", 0) <= r.running_points
    order by r.notification_id, r.log_event_at, r.log_id
  ),
  updated as (
    update public.notifications n
    set created_at = c.log_event_at
    from crossings c
    where n.id = c.notification_id
      and n.created_at is distinct from c.log_event_at
    returning n.id
  )
  select
    (select count(*)::integer from notification_targets),
    (select count(*)::integer from updated),
    (select count(*)::integer from target_levels where target_level_id is null or target_level_id <= 1),
    (
      select count(*)::integer
      from target_levels tl
      where tl.target_level_id is not null
        and tl.target_level_id > 1
        and not exists (
          select 1
          from student_matches sm
          where sm.notification_id = tl.id
        )
    ),
    (
      select count(*)::integer
      from student_matches sm
      where not exists (
        select 1
        from crossings c
        where c.notification_id = sm.notification_id
      )
    )
  into
    v_attempted,
    v_updated,
    v_unmatched_level,
    v_unmatched_student,
    v_no_crossing_log;

  with keyed as (
    select
      n.studio_id,
      lower(coalesce(n.type, '')) as type_key,
      coalesce(
        n.completed_level_start,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
      ) as level_id,
      regexp_replace(
        lower(nullif(btrim((regexp_match(coalesce(n.message, ''), '^[[:space:]]*(.*?)[[:space:]]+completed[[:space:]]+Level', 'i'))[1]), '')),
        '[[:space:]]+',
        ' ',
        'g'
      ) as normalized_student_name
    from public.notifications n
    where n.studio_id = p_studio_id
      and lower(coalesce(n.type, '')) = 'level_completed'
  ),
  duplicate_groups as (
    select studio_id, type_key, level_id, normalized_student_name
    from keyed
    where level_id is not null
      and normalized_student_name is not null
    group by 1, 2, 3, 4
    having count(*) > 1
  )
  select count(*)::integer into v_duplicate_groups
  from duplicate_groups;

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
    'attempted', v_attempted,
    'updated', v_updated,
    'unmatched_student', v_unmatched_student,
    'unmatched_level', v_unmatched_level,
    'no_crossing_log', v_no_crossing_log,
    'timestampUpdatesAttempted', v_attempted,
    'timestampUpdatesCompleted', v_updated,
    'updatedTimestamps', v_updated,
    'updatedNotificationTimestamps', v_updated,
    'duplicateGroupsFound', v_duplicate_groups
  );
end;
$$;

revoke all on function public.repair_level_completed_notification_timestamps_for_studio(uuid) from public;
grant execute on function public.repair_level_completed_notification_timestamps_for_studio(uuid) to authenticated;
