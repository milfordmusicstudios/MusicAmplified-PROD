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
        l.date::timestamp at time zone 'UTC',
        nullif(to_jsonb(l)->>'created_at', '')::timestamptz,
        nullif(to_jsonb(l)->>'approved_at', '')::timestamptz,
        now()
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
    join public.levels lv
      on lv.id = p_completed_level_end + 1
     and coalesce(lv."minPoints", 0) > r.previous_points
     and coalesce(lv."minPoints", 0) <= r.running_points
    order by r.event_at, r.log_id
    limit 1
  )
  select coalesce(p_created_at, (select event_at from crossing), now())
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
      created_at = excluded.created_at
    where public.notifications.created_at is distinct from excluded.created_at
    returning id
  )
  select count(*)::integer into v_inserted from inserted;

  return v_inserted;
end;
$$;

grant execute on function public.insert_level_completed_notifications(uuid, uuid, text, integer, integer, uuid, uuid[], timestamptz) to authenticated;
grant execute on function public.insert_level_completed_notifications(uuid, uuid, text, integer, integer, uuid, uuid[], timestamptz) to service_role;
