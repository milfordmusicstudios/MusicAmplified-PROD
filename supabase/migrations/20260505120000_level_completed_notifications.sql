alter table public.notifications add column if not exists completed_level_start integer;
alter table public.notifications add column if not exists completed_level_end integer;

update public.notifications
set message = replace(message, 'Level ' || 'Level', 'Level')
where message like '%' || 'Level ' || 'Level' || '%';

create unique index if not exists notifications_level_completed_unique
on public.notifications (
  studio_id,
  user_id,
  type,
  completed_level_start,
  completed_level_end
)
where type = 'level_completed';

create or replace function public.format_level_completed_notification_message(
  p_student_name text,
  p_completed_level_start integer,
  p_completed_level_end integer
)
returns text
language sql
immutable
set search_path = public
as $$
  select
    case
      when p_completed_level_start is null
        or p_completed_level_end is null
        or p_completed_level_start <= 0
        or p_completed_level_end <= 0
        or p_completed_level_end < p_completed_level_start
      then ''
      when p_completed_level_start = p_completed_level_end
      then coalesce(nullif(trim(p_student_name), ''), 'Student')
        || ' completed Level '
        || p_completed_level_start::text
        || '.'
      else coalesce(nullif(trim(p_student_name), ''), 'Student')
        || ' completed Levels '
        || p_completed_level_start::text
        || '–'
        || p_completed_level_end::text
        || '.'
    end;
$$;

create or replace function public.insert_level_completed_notifications(
  p_studio_id uuid,
  p_student_id uuid,
  p_student_name text,
  p_completed_level_start integer,
  p_completed_level_end integer,
  p_created_by uuid,
  p_recipient_ids uuid[]
)
returns integer
language plpgsql
security definer
set search_path = public
as $$
declare
  v_message text;
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

  v_message := public.format_level_completed_notification_message(
    p_student_name,
    p_completed_level_start,
    p_completed_level_end
  );
  if nullif(v_message, '') is null then
    return 0;
  end if;

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
      created_by
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
      p_created_by
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
    do nothing
    returning id
  )
  select count(*)::integer into v_inserted from inserted;

  return v_inserted;
end;
$$;

grant execute on function public.format_level_completed_notification_message(text, integer, integer) to authenticated;
grant execute on function public.insert_level_completed_notifications(uuid, uuid, text, integer, integer, uuid, uuid[]) to authenticated;
grant execute on function public.insert_level_completed_notifications(uuid, uuid, text, integer, integer, uuid, uuid[]) to service_role;

create or replace function public.backfill_level_up_notifications_for_student(
  p_studio_id uuid,
  p_user_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_total_points integer := 0;
  v_level_id integer := null;
  v_student_name text := 'Student';
  v_teacher_ids text[] := array[]::text[];
  v_completed_level integer := null;
  v_message text;
  v_recipient_ids uuid[] := array[]::uuid[];
  v_inserted integer := 0;
begin
  if auth.uid() is null then
    raise exception 'Not authenticated';
  end if;

  if p_studio_id is null or p_user_id is null then
    raise exception 'Missing studio or user id';
  end if;

  if not public.can_backfill_notifications_for_student(p_studio_id, p_user_id) then
    raise exception 'Not allowed to backfill notifications for this student';
  end if;

  if not exists (
    select 1
    from public.studio_members sm
    where sm.studio_id = p_studio_id
      and sm.user_id = p_user_id
      and coalesce(sm.roles, '{}'::text[]) @> array['student']::text[]
  ) then
    raise exception 'Target user is not a student in this studio';
  end if;

  select
    trim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", '')),
    coalesce(u."teacherIds", '{}'::text[])
  into v_student_name, v_teacher_ids
  from public.users u
  where u.id = p_user_id;

  v_student_name := coalesce(nullif(v_student_name, ''), 'Student');

  select coalesce(sum(coalesce(l.points, 0)), 0)::integer
  into v_total_points
  from public.logs l
  where l.studio_id = p_studio_id
    and l."userId" = p_user_id
    and lower(coalesce(l.status, '')) = 'approved';

  select lv.id
  into v_level_id
  from public.levels lv
  where v_total_points >= coalesce(lv."minPoints", 0)
  order by coalesce(lv."minPoints", 0) desc
  limit 1;

  v_completed_level := coalesce(v_level_id, 0) - 1;
  if v_completed_level <= 0 then
    return jsonb_build_object(
      'ok', true,
      'studentUserId', p_user_id,
      'studioId', p_studio_id,
      'totalPoints', v_total_points,
      'level', v_level_id,
      'insertedNotifications', 0,
      'reason', 'student has not completed a level beyond level 0'
    );
  end if;

  select array_agg(distinct recipient_id)
  into v_recipient_ids
  from (
    select p_user_id as recipient_id
    union
    select sm.user_id
    from public.studio_members sm
    where sm.studio_id = p_studio_id
      and coalesce(sm.roles, '{}'::text[]) @> array['admin']::text[]
    union
    select sm.user_id
    from public.studio_members sm
    where sm.studio_id = p_studio_id
      and coalesce(sm.roles, '{}'::text[]) @> array['teacher']::text[]
      and sm.user_id::text = any (v_teacher_ids)
  ) recipients
  where recipient_id is not null;

  v_inserted := public.insert_level_completed_notifications(
    p_studio_id,
    p_user_id,
    v_student_name,
    v_completed_level,
    v_completed_level,
    auth.uid(),
    coalesce(v_recipient_ids, array[]::uuid[])
  );
  v_message := public.format_level_completed_notification_message(v_student_name, v_completed_level, v_completed_level);

  return jsonb_build_object(
    'ok', true,
    'studentUserId', p_user_id,
    'studioId', p_studio_id,
    'totalPoints', v_total_points,
    'level', v_level_id,
    'completedLevelStart', v_completed_level,
    'completedLevelEnd', v_completed_level,
    'message', v_message,
    'insertedNotifications', v_inserted
  );
end;
$$;

grant execute on function public.backfill_level_up_notifications_for_student(uuid, uuid) to authenticated;
