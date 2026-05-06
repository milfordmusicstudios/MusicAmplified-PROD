-- Harden level-completed timestamp repair so existing notifications are matched
-- from their message text, not primarily from notification.user_id.

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
      ) as level_id,
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
    select distinct on (nt.id)
      nt.id as notification_id,
      nt.studio_id,
      u.id as student_id,
      nt.level_id
    from notification_targets nt
    join public.users u
      on u.studio_id = nt.studio_id
     and (
       u.id = nt.recipient_id
       or exists (
         select 1
         from user_name_candidates unc
         where unc.student_id = u.id
           and unc.normalized_name = nt.normalized_student_name
       )
     )
    where nt.level_id is not null
      and nt.level_id > 0
    order by
      nt.id,
      case when u.id = nt.recipient_id then 0 else 1 end,
      u.id
  ),
  approved_logs as (
    select
      sm.notification_id,
      sm.studio_id,
      sm.student_id,
      sm.level_id,
      l.id as log_id,
      coalesce(l.points, 0)::integer as points,
      coalesce(
        nullif(to_jsonb(l)->>'approved_at', '')::timestamptz,
        l.date::timestamp at time zone 'UTC',
        nullif(to_jsonb(l)->>'created_at', '')::timestamptz,
        now()
      ) as log_crossing_at
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
        order by al.log_crossing_at, al.log_id
        rows between unbounded preceding and 1 preceding
      ), 0)::integer as previous_points,
      sum(al.points) over (
        partition by al.notification_id
        order by al.log_crossing_at, al.log_id
        rows between unbounded preceding and current row
      )::integer as running_points
    from approved_logs al
  ),
  crossings as (
    select distinct on (r.notification_id)
      r.notification_id,
      r.log_crossing_at
    from running r
    join public.levels lv
      on lv.id = r.level_id
     and (
       (coalesce(lv."minPoints", 0) <= 0 and r.running_points >= coalesce(lv."minPoints", 0))
       or (coalesce(lv."minPoints", 0) > r.previous_points and coalesce(lv."minPoints", 0) <= r.running_points)
     )
    order by r.notification_id, r.log_crossing_at, r.log_id
  ),
  updated as (
    update public.notifications n
    set created_at = c.log_crossing_at
    from crossings c
    where n.id = c.notification_id
      and n.created_at is distinct from c.log_crossing_at
    returning n.id
  )
  select
    (select count(*)::integer from notification_targets),
    (select count(*)::integer from updated),
    (select count(*)::integer from notification_targets where level_id is null or level_id <= 0),
    (
      select count(*)::integer
      from notification_targets nt
      where nt.level_id is not null
        and nt.level_id > 0
        and not exists (
          select 1
          from student_matches sm
          where sm.notification_id = nt.id
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

create or replace function public.backfill_level_notifications_for_studio(
  p_studio_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_students_checked integer := 0;
  v_levels_checked integer := 0;
  v_inserted_total integer := 0;
  v_repair jsonb := '{}'::jsonb;
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

  with active_students as (
    select
      u.id as student_id,
      trim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", '')) as student_name,
      coalesce(u."teacherIds", '{}'::text[]) as teacher_ids
    from public.users u
    join public.studio_members sm
      on sm.user_id = u.id
     and sm.studio_id = p_studio_id
     and coalesce(sm.roles, '{}'::text[]) @> array['student']::text[]
    where u.studio_id = p_studio_id
      and coalesce(u.active, true) = true
      and u.deactivated_at is null
  ),
  approved_logs as (
    select
      s.student_id,
      s.student_name,
      s.teacher_ids,
      l.id as log_id,
      coalesce(l.points, 0)::integer as points,
      coalesce(
        nullif(to_jsonb(l)->>'approved_at', '')::timestamptz,
        l.date::timestamp at time zone 'UTC',
        nullif(to_jsonb(l)->>'created_at', '')::timestamptz,
        now()
      ) as crossing_at
    from active_students s
    join public.logs l
      on l.studio_id = p_studio_id
     and l."userId" = s.student_id
     and lower(coalesce(l.status, '')) = 'approved'
  ),
  running as (
    select
      al.*,
      coalesce(sum(al.points) over (
        partition by al.student_id
        order by al.crossing_at, al.log_id
        rows between unbounded preceding and 1 preceding
      ), 0)::integer as previous_points,
      sum(al.points) over (
        partition by al.student_id
        order by al.crossing_at, al.log_id
        rows between unbounded preceding and current row
      )::integer as running_points
    from approved_logs al
  ),
  crossings as (
    select distinct on (r.student_id, lv.id)
      r.student_id,
      coalesce(nullif(r.student_name, ''), 'Student') as student_name,
      r.teacher_ids,
      lv.id::integer as level_id,
      r.crossing_at
    from running r
    join public.levels lv
      on lv.id > 0
     and (
       (coalesce(lv."minPoints", 0) <= 0 and r.running_points >= coalesce(lv."minPoints", 0))
       or (coalesce(lv."minPoints", 0) > r.previous_points and coalesce(lv."minPoints", 0) <= r.running_points)
     )
    order by r.student_id, lv.id, r.crossing_at, r.log_id
  ),
  recipients as (
    select c.*, c.student_id as recipient_id
    from crossings c
    union
    select c.*, sm.user_id as recipient_id
    from crossings c
    join public.studio_members sm
      on sm.studio_id = p_studio_id
     and coalesce(sm.roles, '{}'::text[]) @> array['admin']::text[]
    union
    select c.*, sm.user_id as recipient_id
    from crossings c
    join public.studio_members sm
      on sm.studio_id = p_studio_id
     and coalesce(sm.roles, '{}'::text[]) @> array['teacher']::text[]
     and sm.user_id::text = any (c.teacher_ids)
  ),
  inserted as (
    insert into public.notifications (
      "userId",
      user_id,
      title,
      message,
      type,
      completed_level_start,
      completed_level_end,
      read,
      studio_id,
      created_by,
      created_at
    )
    select
      r.recipient_id,
      r.recipient_id,
      'Level Completed',
      r.student_name || ' completed Level ' || r.level_id::text || '.',
      'level_completed',
      r.level_id,
      r.level_id,
      false,
      p_studio_id,
      auth.uid(),
      r.crossing_at
    from recipients r
    where r.recipient_id is not null
      and not exists (
        select 1
        from public.notifications n
        where n.studio_id = p_studio_id
          and lower(coalesce(n.type, '')) = 'level_completed'
          and coalesce(n.user_id, n."userId") = r.recipient_id
          and coalesce(
            n.completed_level_start,
            nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
          ) = r.level_id
      )
    returning id
  )
  select
    (select count(*)::integer from active_students),
    (select count(*)::integer from crossings),
    (select count(*)::integer from inserted)
  into v_students_checked, v_levels_checked, v_inserted_total;

  v_repair := public.repair_level_completed_notification_timestamps_for_studio(p_studio_id);

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
    'studentsChecked', v_students_checked,
    'levelsChecked', v_levels_checked,
    'insertedNotifications', v_inserted_total,
    'attempted', coalesce((v_repair->>'attempted')::integer, 0),
    'updated', coalesce((v_repair->>'updated')::integer, 0),
    'unmatched_student', coalesce((v_repair->>'unmatched_student')::integer, 0),
    'unmatched_level', coalesce((v_repair->>'unmatched_level')::integer, 0),
    'no_crossing_log', coalesce((v_repair->>'no_crossing_log')::integer, 0),
    'timestampUpdatesAttempted', coalesce((v_repair->>'timestampUpdatesAttempted')::integer, 0),
    'timestampUpdatesCompleted', coalesce((v_repair->>'timestampUpdatesCompleted')::integer, 0),
    'updatedTimestamps', coalesce((v_repair->>'updatedTimestamps')::integer, 0),
    'updatedNotificationTimestamps', coalesce((v_repair->>'updatedNotificationTimestamps')::integer, 0),
    'duplicateGroupsFound', coalesce((v_repair->>'duplicateGroupsFound')::integer, 0)
  );
end;
$$;

revoke all on function public.backfill_level_notifications_for_studio(uuid) from public;
grant execute on function public.backfill_level_notifications_for_studio(uuid) to authenticated;
