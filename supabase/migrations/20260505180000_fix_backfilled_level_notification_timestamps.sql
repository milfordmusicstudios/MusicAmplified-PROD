-- Backfilled level-completed notifications should use the log timestamp that
-- caused the level threshold crossing, not the recalculation timestamp.

create or replace function public.backfill_level_notifications_for_studio(
  p_studio_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_student record;
  v_crossing record;
  v_message text;
  v_students_checked integer := 0;
  v_levels_checked integer := 0;
  v_inserted integer := 0;
  v_inserted_total integer := 0;
  v_updated integer := 0;
  v_updated_total integer := 0;
  v_recipient_ids uuid[] := array[]::uuid[];
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

  for v_student in
    select
      u.id,
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
  loop
    v_students_checked := v_students_checked + 1;

    select array_agg(distinct recipient_id)
    into v_recipient_ids
    from (
      select v_student.id as recipient_id
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
        and sm.user_id::text = any (v_student.teacher_ids)
    ) recipients
    where recipient_id is not null;

    for v_crossing in
      with approved_logs as (
        select
          l.id,
          coalesce(l.points, 0)::integer as points,
          coalesce(
            nullif(to_jsonb(l)->>'approved_at', '')::timestamptz,
            l.date::timestamp at time zone 'UTC',
            nullif(to_jsonb(l)->>'created_at', '')::timestamptz,
            now()
          ) as crossing_at
        from public.logs l
        where l.studio_id = p_studio_id
          and l."userId" = v_student.id
          and lower(coalesce(l.status, '')) = 'approved'
      ),
      running as (
        select
          al.*,
          coalesce(sum(al.points) over (
            order by al.crossing_at, al.id
            rows between unbounded preceding and 1 preceding
          ), 0)::integer as previous_points,
          sum(al.points) over (
            order by al.crossing_at, al.id
            rows between unbounded preceding and current row
          )::integer as running_points
        from approved_logs al
      )
      select distinct on (lv.id)
        lv.id::integer as level_id,
        r.crossing_at
      from running r
      join public.levels lv
        on coalesce(lv."minPoints", 0) > r.previous_points
       and coalesce(lv."minPoints", 0) <= r.running_points
       and lv.id > 0
      order by lv.id, r.crossing_at, r.id
    loop
      v_levels_checked := v_levels_checked + 1;
      v_message := coalesce(nullif(v_student.student_name, ''), 'Student')
        || ' completed Level '
        || v_crossing.level_id::text
        || '.';

      with recipient_ids as (
        select distinct unnest(coalesce(v_recipient_ids, array[]::uuid[])) as user_id
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
          v_crossing.level_id,
          v_crossing.level_id,
          false,
          p_studio_id,
          auth.uid(),
          v_crossing.crossing_at
        from recipient_ids r
        where r.user_id is not null
          and not exists (
            select 1
            from public.notifications n
            where n.studio_id = p_studio_id
              and coalesce(n.user_id, n."userId") = r.user_id
              and lower(coalesce(n.type, '')) = 'level_completed'
              and n.completed_level_start = v_crossing.level_id
              and n.completed_level_end = v_crossing.level_id
              and n.message = v_message
          )
        returning id
      )
      select count(*)::integer into v_inserted from inserted;

      v_inserted_total := v_inserted_total + coalesce(v_inserted, 0);

      with updated as (
        update public.notifications n
        set created_at = v_crossing.crossing_at
        where n.studio_id = p_studio_id
          and lower(coalesce(n.type, '')) = 'level_completed'
          and n.completed_level_start = v_crossing.level_id
          and n.completed_level_end = v_crossing.level_id
          and n.message = v_message
          and n.created_at is distinct from v_crossing.crossing_at
        returning n.id
      )
      select count(*)::integer into v_updated from updated;

      v_updated_total := v_updated_total + coalesce(v_updated, 0);
    end loop;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
    'studentsChecked', v_students_checked,
    'levelsChecked', v_levels_checked,
    'insertedNotifications', v_inserted_total,
    'updatedTimestamps', v_updated_total,
    'updatedNotificationTimestamps', v_updated_total
  );
end;
$$;

revoke all on function public.backfill_level_notifications_for_studio(uuid) from public;
grant execute on function public.backfill_level_notifications_for_studio(uuid) to authenticated;

with active_students as (
  select
    u.id as student_id,
    u.studio_id,
    trim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", '')) as student_name
  from public.users u
  join public.studio_members sm
    on sm.user_id = u.id
   and sm.studio_id = u.studio_id
   and coalesce(sm.roles, '{}'::text[]) @> array['student']::text[]
  where coalesce(u.active, true) = true
    and u.deactivated_at is null
),
approved_logs as (
  select
    s.student_id,
    s.studio_id,
    s.student_name,
    l.id,
    coalesce(l.points, 0)::integer as points,
    coalesce(
      nullif(to_jsonb(l)->>'approved_at', '')::timestamptz,
      l.date::timestamp at time zone 'UTC',
      nullif(to_jsonb(l)->>'created_at', '')::timestamptz,
      now()
    ) as crossing_at
  from active_students s
  join public.logs l
    on l.studio_id = s.studio_id
   and l."userId" = s.student_id
   and lower(coalesce(l.status, '')) = 'approved'
),
running as (
  select
    al.*,
    coalesce(sum(al.points) over (
      partition by al.studio_id, al.student_id
      order by al.crossing_at, al.id
      rows between unbounded preceding and 1 preceding
    ), 0)::integer as previous_points,
    sum(al.points) over (
      partition by al.studio_id, al.student_id
      order by al.crossing_at, al.id
      rows between unbounded preceding and current row
    )::integer as running_points
  from approved_logs al
),
crossings as (
  select distinct on (r.studio_id, r.student_id, lv.id)
    r.studio_id,
    r.student_id,
    coalesce(nullif(r.student_name, ''), 'Student')
      || ' completed Level '
      || lv.id::text
      || '.' as message,
    lv.id::integer as level_id,
    r.crossing_at
  from running r
  join public.levels lv
    on coalesce(lv."minPoints", 0) > r.previous_points
   and coalesce(lv."minPoints", 0) <= r.running_points
   and lv.id > 0
  order by r.studio_id, r.student_id, lv.id, r.crossing_at, r.id
)
update public.notifications n
set created_at = c.crossing_at
from crossings c
where n.studio_id = c.studio_id
  and lower(coalesce(n.type, '')) = 'level_completed'
  and n.completed_level_start = c.level_id
  and n.completed_level_end = c.level_id
  and n.message = c.message
  and n.created_at is distinct from c.crossing_at;
