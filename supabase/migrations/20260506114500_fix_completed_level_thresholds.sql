-- Level threshold interpretation:
-- public.levels.minPoints/maxPoints define the student's current working level range.
-- A completed level is not the user's stored current level. A level is completed
-- only when total approved points are >= that level row's maxPoints.
-- Example with Level 1 minPoints=0 and maxPoints=1200:
--   0-1199 points => completed_level null
--   1200 points   => completed_level 1
--   Level 2 completes at the Level 2 maxPoints threshold, and so on.

create or replace function public.get_completed_level_from_points(p_points integer)
returns integer
language sql
stable
set search_path = public
as $$
  select max(lv.id)::integer
  from public.levels lv
  where coalesce(p_points, 0) >= coalesce(lv."maxPoints", 2147483647);
$$;

create or replace function public.get_level_completion_threshold(p_level integer)
returns integer
language sql
stable
set search_path = public
as $$
  select nullif(coalesce(lv."maxPoints", 0), 0)::integer
  from public.levels lv
  where lv.id = p_level;
$$;

create or replace function public.find_invalid_level_completion_notifications(
  p_studio_id uuid default null
)
returns table (
  id bigint,
  studio_id uuid,
  notification_user_id uuid,
  student_id uuid,
  student_name text,
  notification_level integer,
  calculated_completed_level integer,
  total_points integer,
  required_points integer,
  created_at timestamptz,
  message text,
  reason text
)
language sql
stable
set search_path = public
as $$
  with notification_targets as (
    select
      n.id,
      n.studio_id,
      coalesce(n.user_id, n."userId") as notification_user_id,
      n.created_at,
      n.message,
      coalesce(
        n.completed_level_end,
        n.completed_level_start,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+[0-9]+[[:space:]]*[-â€“][[:space:]]*([0-9]+)', 'i'))[1], '')::integer,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
      ) as notification_level,
      regexp_replace(
        lower(nullif(btrim((regexp_match(coalesce(n.message, ''), '^[[:space:]]*(.*?)[[:space:]]+completed[[:space:]]+Level', 'i'))[1]), '')),
        '[[:space:]]+',
        ' ',
        'g'
      ) as normalized_student_name
    from public.notifications n
    where (p_studio_id is null or n.studio_id = p_studio_id)
      and lower(coalesce(n.type, '')) = 'level_completed'
  ),
  students as (
    select
      u.id as student_id,
      u.studio_id,
      nullif(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", '')), '') as student_name,
      regexp_replace(lower(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", ''))), '[[:space:]]+', ' ', 'g') as normalized_student_name
    from public.users u
    where (p_studio_id is null or u.studio_id = p_studio_id)
      and regexp_replace(lower(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", ''))), '[[:space:]]+', ' ', 'g') <> 'milford music'
  ),
  point_totals as (
    select
      s.student_id,
      coalesce(sum(coalesce(l.points, 0)) filter (where lower(coalesce(l.status, '')) = 'approved'), 0)::integer as total_points
    from students s
    left join public.logs l
      on l.studio_id = s.studio_id
     and l."userId" = s.student_id
    group by s.student_id
  ),
  evaluated as (
    select
      nt.id,
      nt.studio_id,
      nt.notification_user_id,
      s.student_id,
      coalesce(s.student_name, nt.normalized_student_name) as student_name,
      nt.notification_level,
      public.get_completed_level_from_points(pt.total_points) as calculated_completed_level,
      coalesce(pt.total_points, 0)::integer as total_points,
      public.get_level_completion_threshold(nt.notification_level) as required_points,
      nt.created_at,
      nt.message
    from notification_targets nt
    left join students s
      on s.studio_id = nt.studio_id
     and s.normalized_student_name = nt.normalized_student_name
    left join point_totals pt
      on pt.student_id = s.student_id
  )
  select
    e.id,
    e.studio_id,
    e.notification_user_id,
    e.student_id,
    e.student_name,
    e.notification_level,
    e.calculated_completed_level,
    e.total_points,
    e.required_points,
    e.created_at,
    e.message,
    concat_ws(
      ', ',
      case when e.student_id is null then 'unmatched student' end,
      case when e.notification_level is null then 'missing notification level' end,
      case when e.required_points is null then 'unknown level threshold' end,
      case when e.total_points = 0 then 'zero points with completed-level notification' end,
      case when e.notification_level is not null and coalesce(e.calculated_completed_level, 0) < e.notification_level then 'points below completion threshold' end,
      case when e.notification_level is distinct from e.calculated_completed_level then 'notification level does not match calculated completed level' end
    ) as reason
  from evaluated e
  where e.student_id is null
     or e.notification_level is null
     or e.required_points is null
     or e.total_points = 0
     or coalesce(e.calculated_completed_level, 0) < coalesce(e.notification_level, 2147483647)
     or e.notification_level is distinct from e.calculated_completed_level
  order by e.student_name nulls last, e.created_at desc, e.id;
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
  v_required_points integer;
  v_total_points integer := 0;
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

  v_required_points := public.get_level_completion_threshold(p_completed_level_end);
  if v_required_points is null then
    return 0;
  end if;

  select coalesce(sum(coalesce(l.points, 0)), 0)::integer
  into v_total_points
  from public.logs l
  where l.studio_id = p_studio_id
    and l."userId" = p_student_id
    and lower(coalesce(l.status, '')) = 'approved';

  if v_total_points < v_required_points then
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
    where al.event_at is not null
  ),
  crossing as (
    select r.event_at
    from running r
    where r.previous_points < v_required_points
      and r.running_points >= v_required_points
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
begin
  if auth.uid() is null then
    raise exception 'not_authenticated' using errcode = '28000';
  end if;

  with notification_targets as (
    select
      n.id,
      n.studio_id,
      coalesce(n.completed_level_end, n.completed_level_start) as completed_level_id,
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
  students as (
    select
      u.id as student_id,
      u.studio_id,
      regexp_replace(lower(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", ''))), '[[:space:]]+', ' ', 'g') as normalized_student_name
    from public.users u
    where u.studio_id = p_studio_id
  ),
  approved_logs as (
    select
      nt.id as notification_id,
      nt.completed_level_id,
      public.get_level_completion_threshold(nt.completed_level_id) as required_points,
      l.id as log_id,
      coalesce(l.points, 0)::integer as points,
      coalesce(
        public.level_notification_log_event_at(to_jsonb(l)),
        public.valid_level_notification_timestamp(l.created_at)
      ) as event_at
    from notification_targets nt
    join students s
      on s.studio_id = nt.studio_id
     and s.normalized_student_name = nt.normalized_student_name
    join public.logs l
      on l.studio_id = nt.studio_id
     and l."userId" = s.student_id
     and lower(coalesce(l.status, '')) = 'approved'
    where nt.completed_level_id is not null
  ),
  running as (
    select
      al.*,
      coalesce(sum(al.points) over (
        partition by al.notification_id
        order by al.event_at, al.log_id
        rows between unbounded preceding and 1 preceding
      ), 0)::integer as previous_points,
      sum(al.points) over (
        partition by al.notification_id
        order by al.event_at, al.log_id
        rows between unbounded preceding and current row
      )::integer as running_points
    from approved_logs al
    where al.event_at is not null
      and al.required_points is not null
  ),
  crossings as (
    select distinct on (r.notification_id)
      r.notification_id,
      r.event_at
    from running r
    where r.previous_points < r.required_points
      and r.running_points >= r.required_points
    order by r.notification_id, r.event_at, r.log_id
  ),
  updated as (
    update public.notifications n
    set created_at = c.event_at
    from crossings c
    where n.id = c.notification_id
      and n.created_at is distinct from c.event_at
    returning n.id
  )
  select
    (select count(*)::integer from notification_targets),
    (select count(*)::integer from updated)
  into v_attempted, v_updated;

  return jsonb_build_object(
    'ok', true,
    'attempted', v_attempted,
    'updated', v_updated,
    'updatedTimestamps', v_updated,
    'updatedNotificationTimestamps', v_updated
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
  v_deleted_invalid integer := 0;
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

  delete from public.notifications n
  using public.find_invalid_level_completion_notifications(p_studio_id) bad
  where n.id = bad.id;
  get diagnostics v_deleted_invalid = row_count;

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
    select distinct on (r.student_id, lv.id)
      r.student_id,
      coalesce(nullif(r.student_name, ''), 'Student') as student_name,
      r.teacher_ids,
      lv.id::integer as completed_level_id,
      r.event_at
    from running r
    join public.levels lv
      on public.get_level_completion_threshold(lv.id) is not null
     and r.previous_points < public.get_level_completion_threshold(lv.id)
     and r.running_points >= public.get_level_completion_threshold(lv.id)
    order by r.student_id, lv.id, r.event_at, r.log_id
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

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
    'studentsChecked', v_students_checked,
    'levelsChecked', v_levels_checked,
    'insertedNotifications', v_inserted_total,
    'deletedInvalidNotifications', v_deleted_invalid,
    'updatedTimestamps', coalesce((v_repair->>'updatedTimestamps')::integer, 0)
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

revoke all on function public.get_completed_level_from_points(integer) from public;
grant execute on function public.get_completed_level_from_points(integer) to authenticated;
grant execute on function public.get_completed_level_from_points(integer) to service_role;

revoke all on function public.get_level_completion_threshold(integer) from public;
grant execute on function public.get_level_completion_threshold(integer) to authenticated;
grant execute on function public.get_level_completion_threshold(integer) to service_role;

revoke all on function public.find_invalid_level_completion_notifications(uuid) from public;
grant execute on function public.find_invalid_level_completion_notifications(uuid) to authenticated;
