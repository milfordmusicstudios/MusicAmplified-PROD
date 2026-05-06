-- Harden level-completion notifications:
-- - canonical type/message is level_completed / "completed Level X."
-- - completed Level X is emitted only when logs cross into Level X + 1
-- - invalid old dates are ignored for notification timestamps
-- - Milford Music test-account recognition notifications are removed/excluded

create or replace function public.valid_level_notification_timestamp(p_value timestamptz)
returns timestamptz
language sql
stable
set search_path = public
as $$
  select case
    when p_value is null then null
    when extract(year from p_value) < 2020 then null
    when extract(year from p_value) > extract(year from now()) + 1 then null
    else p_value
  end;
$$;

create or replace function public.level_notification_log_event_at(p_log jsonb)
returns timestamptz
language sql
stable
set search_path = public
as $$
  select coalesce(
    public.valid_level_notification_timestamp(nullif(p_log->>'approved_at', '')::timestamptz),
    public.valid_level_notification_timestamp(nullif(p_log->>'created_at', '')::timestamptz),
    public.valid_level_notification_timestamp((nullif(p_log->>'date', '')::date)::timestamp at time zone 'UTC')
  );
$$;

drop function if exists public.insert_level_completed_notifications(uuid, uuid, text, integer, integer, uuid, uuid[]);

create or replace function public.insert_level_completed_notifications(
  p_studio_id uuid,
  p_student_id uuid,
  p_student_name text,
  p_completed_level_start integer,
  p_completed_level_end integer,
  p_created_by uuid,
  p_recipient_ids uuid[],
  p_created_at timestamptz default null
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_message text;
  v_created_at timestamptz;
  v_inserted integer := 0;
begin
  if p_studio_id is null then
    raise exception 'Missing studio id';
  end if;
  if p_student_id is null then
    raise exception 'Missing student id';
  end if;
  if p_completed_level_start is null
    or p_completed_level_end is null
    or p_completed_level_start <= 0
    or p_completed_level_end < p_completed_level_start then
    return 0;
  end if;

  if regexp_replace(lower(coalesce(p_student_name, '')), '[[:space:]]+', ' ', 'g') = 'milford music' then
    return 0;
  end if;

  v_message := public.format_level_completed_notification_message(
    p_student_name,
    p_completed_level_start,
    p_completed_level_end
  );
  if nullif(v_message, '') is null then
    return 0;
  end if;

  with approved_logs as (
    select
      l.id as log_id,
      coalesce(l.points, 0)::integer as points,
      coalesce(
        public.level_notification_log_event_at(to_jsonb(l)),
        public.valid_level_notification_timestamp(l.created_at)
      ) as event_at
    from public.logs l
    where l.studio_id = p_studio_id
      and l."userId" = p_student_id
      and lower(coalesce(l.status, '')) = 'approved'
  ),
  running as (
    select
      al.*,
      coalesce(sum(al.points) over (
        order by al.event_at, al.log_id
        rows between unbounded preceding and 1 preceding
      ), 0)::integer as previous_points,
      sum(al.points) over (
        order by al.event_at, al.log_id
        rows between unbounded preceding and current row
      )::integer as running_points
    from approved_logs al
  ),
  crossing as (
    select r.event_at
    from running r
    join public.levels target_level
      on target_level.id = p_completed_level_end + 1
     and coalesce(target_level."minPoints", 0) > r.previous_points
     and coalesce(target_level."minPoints", 0) <= r.running_points
    where r.event_at is not null
    order by r.event_at, r.log_id
    limit 1
  )
  select coalesce(public.valid_level_notification_timestamp(p_created_at), (select event_at from crossing), now())
  into v_created_at;

  with recipient_ids as (
    select distinct unnest(coalesce(p_recipient_ids, array[]::uuid[])) as user_id
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
      r.user_id,
      r.user_id,
      'Level Completed',
      v_message,
      'level_completed',
      p_completed_level_start,
      p_completed_level_end,
      false,
      p_studio_id,
      p_created_by,
      v_created_at
    from recipient_ids r
    where r.user_id is not null
    on conflict (
      studio_id,
      user_id,
      type,
      completed_level_start,
      completed_level_end
    )
    where type = 'level_completed'
    do update set
      message = excluded.message,
      title = excluded.title,
      created_at = least(public.notifications.created_at, excluded.created_at)
    where public.notifications.created_at is distinct from least(public.notifications.created_at, excluded.created_at)
       or public.notifications.message is distinct from excluded.message
       or public.notifications.title is distinct from excluded.title
    returning id
  )
  select count(*)::integer into v_inserted from inserted;

  return v_inserted;
end;
$$;

grant execute on function public.insert_level_completed_notifications(uuid, uuid, text, integer, integer, uuid, uuid[], timestamptz) to authenticated;
grant execute on function public.insert_level_completed_notifications(uuid, uuid, text, integer, integer, uuid, uuid[], timestamptz) to service_role;

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
        n.completed_level_end,
        n.completed_level_start,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+[0-9]+[[:space:]]*[-–][[:space:]]*([0-9]+)', 'i'))[1], '')::integer,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
      ) as completed_level_id,
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
    select nt.*, (nt.completed_level_id + 1)::integer as target_level_id
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
      and regexp_replace(lower(btrim(coalesce(candidate.name_value, ''))), '[[:space:]]+', ' ', 'g') <> 'milford music'
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
    order by tl.id, case when u.id = tl.recipient_id then 0 else 1 end, u.id
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
        public.level_notification_log_event_at(to_jsonb(l)),
        public.valid_level_notification_timestamp(l.created_at)
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
    where al.log_event_at is not null
  ),
  crossings as (
    select distinct on (r.notification_id)
      r.notification_id,
      r.log_event_at
    from running r
    join public.levels target_level
      on target_level.id = r.target_level_id
     and coalesce(target_level."minPoints", 0) > r.previous_points
     and coalesce(target_level."minPoints", 0) <= r.running_points
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
        and not exists (select 1 from student_matches sm where sm.notification_id = tl.id)
    ),
    (
      select count(*)::integer
      from student_matches sm
      where not exists (select 1 from crossings c where c.notification_id = sm.notification_id)
    )
  into v_attempted, v_updated, v_unmatched_level, v_unmatched_student, v_no_crossing_log;

  with keyed as (
    select
      n.studio_id,
      lower(coalesce(n.type, '')) as type_key,
      n.completed_level_start as level_id,
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
      and regexp_replace(lower(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", ''))), '[[:space:]]+', ' ', 'g') <> 'milford music'
  ),
  approved_logs as (
    select
      s.student_id,
      s.student_name,
      s.teacher_ids,
      l.id as log_id,
      coalesce(l.points, 0)::integer as points,
      coalesce(
        public.level_notification_log_event_at(to_jsonb(l)),
        public.valid_level_notification_timestamp(l.created_at)
      ) as event_at
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
        order by al.event_at, al.log_id
        rows between unbounded preceding and 1 preceding
      ), 0)::integer as previous_points,
      sum(al.points) over (
        partition by al.student_id
        order by al.event_at, al.log_id
        rows between unbounded preceding and current row
      )::integer as running_points
    from approved_logs al
    where al.event_at is not null
  ),
  crossings as (
    select distinct on (r.student_id, completed_level.completed_level_id)
      r.student_id,
      coalesce(nullif(r.student_name, ''), 'Student') as student_name,
      r.teacher_ids,
      completed_level.completed_level_id,
      r.event_at
    from running r
    join public.levels target_level
      on target_level.id > 1
     and coalesce(target_level."minPoints", 0) > r.previous_points
     and coalesce(target_level."minPoints", 0) <= r.running_points
    cross join lateral (
      select (target_level.id - 1)::integer as completed_level_id
    ) completed_level
    where completed_level.completed_level_id > 0
    order by r.student_id, completed_level.completed_level_id, r.event_at, r.log_id
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
      public.format_level_completed_notification_message(r.student_name, r.completed_level_id, r.completed_level_id),
      'level_completed',
      r.completed_level_id,
      r.completed_level_id,
      false,
      p_studio_id,
      auth.uid(),
      r.event_at
    from recipients r
    where r.recipient_id is not null
    on conflict (
      studio_id,
      user_id,
      type,
      completed_level_start,
      completed_level_end
    )
    where type = 'level_completed'
    do update set
      message = excluded.message,
      title = excluded.title,
      created_at = least(public.notifications.created_at, excluded.created_at)
    where public.notifications.created_at is distinct from least(public.notifications.created_at, excluded.created_at)
       or public.notifications.message is distinct from excluded.message
       or public.notifications.title is distinct from excluded.title
    returning id
  )
  select
    (select count(*)::integer from active_students),
    (select count(*)::integer from crossings),
    (select count(*)::integer from inserted)
  into v_students_checked, v_levels_checked, v_inserted_total;

  v_repair := public.repair_level_completed_notification_timestamps_for_studio(p_studio_id);

  delete from public.notifications n
  where n.studio_id = p_studio_id
    and lower(coalesce(n.type, '')) in ('level_completed', 'level_up')
    and coalesce(n.message, '') ~* '^[[:space:]]*Milford[[:space:]]+Music[[:space:]]+';

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

create or replace function public.backfill_level_up_notifications_for_studio(
  p_studio_id uuid
)
returns jsonb
language sql
security definer
set search_path = public
as $$
  select public.backfill_level_notifications_for_studio(p_studio_id);
$$;

revoke all on function public.backfill_level_up_notifications_for_studio(uuid) from public;
grant execute on function public.backfill_level_up_notifications_for_studio(uuid) to authenticated;

delete from public.notifications n
where lower(coalesce(n.type, '')) in ('level_completed', 'level_up')
  and coalesce(n.message, '') ~* '^[[:space:]]*Milford[[:space:]]+Music[[:space:]]+';
