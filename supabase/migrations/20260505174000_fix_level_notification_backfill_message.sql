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
  v_level record;
  v_message text;
  v_students_checked integer := 0;
  v_levels_checked integer := 0;
  v_inserted integer := 0;
  v_inserted_total integer := 0;
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
      coalesce(u."teacherIds", '{}'::text[]) as teacher_ids,
      coalesce(sum(coalesce(l.points, 0)) filter (where lower(coalesce(l.status, '')) = 'approved'), 0)::integer as total_points
    from public.users u
    join public.studio_members sm
      on sm.user_id = u.id
     and sm.studio_id = p_studio_id
     and coalesce(sm.roles, '{}'::text[]) @> array['student']::text[]
    left join public.logs l
      on l."userId" = u.id
     and l.studio_id = p_studio_id
    where u.studio_id = p_studio_id
      and coalesce(u.active, true) = true
      and u.deactivated_at is null
    group by u.id, u."firstName", u."lastName", u."teacherIds"
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

    for v_level in
      select lv.id::integer as id
      from public.levels lv
      where v_student.total_points >= coalesce(lv."minPoints", 0)
        and lv.id > 0
      order by lv.id
    loop
      v_levels_checked := v_levels_checked + 1;
      v_message := coalesce(nullif(v_student.student_name, ''), 'Student')
        || ' completed Level '
        || v_level.id::text
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
          v_level.id,
          v_level.id,
          false,
          p_studio_id,
          auth.uid(),
          now()
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

      v_inserted_total := v_inserted_total + coalesce(v_inserted, 0);
    end loop;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'studioId', p_studio_id,
    'studentsChecked', v_students_checked,
    'levelsChecked', v_levels_checked,
    'insertedNotifications', v_inserted_total
  );
end;
$$;

revoke all on function public.backfill_level_notifications_for_studio(uuid) from public;
grant execute on function public.backfill_level_notifications_for_studio(uuid) to authenticated;
