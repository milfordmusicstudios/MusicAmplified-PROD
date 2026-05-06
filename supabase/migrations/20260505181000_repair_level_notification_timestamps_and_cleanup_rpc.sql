-- Repair existing level-completed notification timestamps by reconstructing the
-- exact approved log that crossed each level threshold. Also provides an
-- explicit cleanup RPC for duplicate level-completed rows.

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
      nullif(btrim((regexp_match(coalesce(n.message, ''), '^[[:space:]]*(.*?)[[:space:]]+completed[[:space:]]+Level', 'i'))[1]), '') as student_name,
      n.created_at
    from public.notifications n
    where n.studio_id = p_studio_id
      and lower(coalesce(n.type, '')) = 'level_completed'
  ),
  student_matches as (
    select distinct on (nt.id)
      nt.id as notification_id,
      nt.studio_id,
      u.id as student_id,
      nt.level_id,
      nt.created_at
    from notification_targets nt
    join public.users u
      on u.studio_id = nt.studio_id
     and coalesce(u.active, true) = true
     and u.deactivated_at is null
     and (
       u.id = nt.recipient_id
       or lower(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", ''))) = lower(btrim(coalesce(nt.student_name, '')))
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
      sm.created_at as notification_created_at,
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
     and coalesce(lv."minPoints", 0) > r.previous_points
     and coalesce(lv."minPoints", 0) <= r.running_points
    order by r.notification_id, r.log_crossing_at, r.log_id
  ),
  attempted as (
    select count(*)::integer as total
    from crossings
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
    coalesce((select total from attempted), 0),
    count(updated.id)::integer
  into v_attempted, v_updated
  from updated;

  with duplicate_groups as (
    select
      n.studio_id,
      coalesce(n.user_id, n."userId") as recipient_id,
      lower(coalesce(n.type, '')) as type_key,
      coalesce(
        n.completed_level_start,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
      ) as level_id
    from public.notifications n
    where n.studio_id = p_studio_id
      and lower(coalesce(n.type, '')) = 'level_completed'
    group by 1, 2, 3, 4
    having count(*) > 1
  )
  select count(*)::integer into v_duplicate_groups
  from duplicate_groups;

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
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

create or replace function public.cleanup_duplicate_level_completed_notifications_for_studio(
  p_studio_id uuid
)
returns jsonb
language plpgsql
security definer
set search_path = public
as $$
declare
  v_duplicate_groups integer := 0;
  v_deleted integer := 0;
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

  with keyed as (
    select
      n.id,
      n.studio_id,
      coalesce(n.user_id, n."userId") as recipient_id,
      lower(coalesce(n.type, '')) as type_key,
      coalesce(
        n.completed_level_start,
        nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
      ) as level_id,
      row_number() over (
        partition by
          n.studio_id,
          coalesce(n.user_id, n."userId"),
          lower(coalesce(n.type, '')),
          coalesce(
            n.completed_level_start,
            nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
          )
        order by n.created_at asc, n.id asc
      ) as duplicate_rank,
      count(*) over (
        partition by
          n.studio_id,
          coalesce(n.user_id, n."userId"),
          lower(coalesce(n.type, '')),
          coalesce(
            n.completed_level_start,
            nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
          )
      ) as duplicate_count,
      coalesce(n.recognition_given, false) as recognition_given,
      nullif(btrim(coalesce(n.recognition_note, '')), '') as recognition_note
    from public.notifications n
    where n.studio_id = p_studio_id
      and lower(coalesce(n.type, '')) = 'level_completed'
  ),
  groups as (
    select count(*)::integer as total
    from (
      select studio_id, recipient_id, type_key, level_id
      from keyed
      where level_id is not null
      group by 1, 2, 3, 4
      having count(*) > 1
    ) duplicate_groups
  ),
  deleted as (
    delete from public.notifications n
    using keyed k
    where n.id = k.id
      and k.level_id is not null
      and k.duplicate_count > 1
      and k.duplicate_rank > 1
      and k.recognition_given = false
      and k.recognition_note is null
    returning n.id
  )
  select
    coalesce((select total from groups), 0),
    count(deleted.id)::integer
  into v_duplicate_groups, v_deleted
  from deleted;

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
    'duplicateGroupsFound', v_duplicate_groups,
    'deletedDuplicates', v_deleted
  );
end;
$$;

revoke all on function public.cleanup_duplicate_level_completed_notifications_for_studio(uuid) from public;
grant execute on function public.cleanup_duplicate_level_completed_notifications_for_studio(uuid) to authenticated;

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
  v_recipient_ids uuid[] := array[]::uuid[];
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
              and coalesce(
                n.completed_level_start,
                nullif((regexp_match(coalesce(n.message, ''), 'Levels?[[:space:]]+([0-9]+)', 'i'))[1], '')::integer
              ) = v_crossing.level_id
          )
        returning id
      )
      select count(*)::integer into v_inserted from inserted;

      v_inserted_total := v_inserted_total + coalesce(v_inserted, 0);
    end loop;
  end loop;

  v_repair := public.repair_level_completed_notification_timestamps_for_studio(p_studio_id);

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
    'studentsChecked', v_students_checked,
    'levelsChecked', v_levels_checked,
    'insertedNotifications', v_inserted_total,
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
