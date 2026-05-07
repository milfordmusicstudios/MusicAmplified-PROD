-- Remove invalid level-completion notifications created from current/started level
-- and rebuild canonical current completed-level notifications from total approved points.

delete from public.notifications n
using public.find_invalid_level_completion_notifications(null::uuid) bad
where n.id = bad.id;

delete from public.notifications n
where lower(coalesce(n.type, '')) in ('level_completed', 'level_up')
  and (
    coalesce(n.message, '') ~* '(advanced[[:space:]]+to|reached[[:space:]]+Level|Level[[:space:]]+Level)'
    or coalesce(n.message, '') ~* '^[[:space:]]*Milford[[:space:]]+Music[[:space:]]+'
    or extract(year from n.created_at) < 2024
  );

with students as (
  select
    u.id as student_id,
    u.studio_id,
    nullif(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", '')), '') as student_name,
    coalesce(u."teacherIds", '{}'::text[]) as teacher_ids
  from public.users u
  join public.studio_members sm
    on sm.user_id = u.id
   and sm.studio_id = u.studio_id
   and coalesce(sm.roles, '{}'::text[]) @> array['student']::text[]
  where coalesce(u.active, true) = true
    and u.deactivated_at is null
    and regexp_replace(lower(btrim(coalesce(u."firstName", '') || ' ' || coalesce(u."lastName", ''))), '[[:space:]]+', ' ', 'g') <> 'milford music'
),
approved_logs as (
  select
    s.student_id,
    s.studio_id,
    coalesce(nullif(s.student_name, ''), 'Student') as student_name,
    s.teacher_ids,
    l.id as log_id,
    coalesce(l.points, 0)::integer as points,
    coalesce(
      public.level_notification_log_event_at(to_jsonb(l)),
      public.valid_level_notification_timestamp(l.created_at)
    ) as event_at
  from students s
  join public.logs l
    on l.studio_id = s.studio_id
   and l."userId" = s.student_id
   and lower(coalesce(l.status, '')) = 'approved'
),
point_totals as (
  select
    al.student_id,
    al.studio_id,
    max(al.student_name) as student_name,
    max(al.teacher_ids) as teacher_ids,
    coalesce(sum(al.points), 0)::integer as total_points,
    public.get_completed_level_from_points(coalesce(sum(al.points), 0)::integer) as completed_level
  from approved_logs al
  group by al.student_id, al.studio_id
),
eligible as (
  select
    pt.*,
    public.get_level_completion_threshold(pt.completed_level) as required_points
  from point_totals pt
  where pt.completed_level is not null
),
running as (
  select
    al.*,
    e.completed_level,
    e.required_points,
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
  join eligible e
    on e.student_id = al.student_id
   and e.studio_id = al.studio_id
  where al.event_at is not null
),
crossings as (
  select distinct on (r.student_id)
    r.student_id,
    r.studio_id,
    r.student_name,
    r.teacher_ids,
    r.completed_level,
    r.event_at
  from running r
  where r.required_points is not null
    and r.previous_points < r.required_points
    and r.running_points >= r.required_points
  order by r.student_id, r.event_at, r.log_id
),
recipients as (
  select c.*, c.student_id as recipient_id
  from crossings c
  union
  select c.*, sm.user_id as recipient_id
  from crossings c
  join public.studio_members sm
    on sm.studio_id = c.studio_id
   and coalesce(sm.roles, '{}'::text[]) @> array['admin']::text[]
  union
  select c.*, sm.user_id as recipient_id
  from crossings c
  join public.studio_members sm
    on sm.studio_id = c.studio_id
   and coalesce(sm.roles, '{}'::text[]) @> array['teacher']::text[]
   and sm.user_id::text = any (c.teacher_ids)
),
deduped_recipients as (
  select distinct on (r.studio_id, r.recipient_id, r.completed_level)
    r.*
  from recipients r
  where r.recipient_id is not null
  order by r.studio_id, r.recipient_id, r.completed_level, r.event_at
)
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
  public.format_level_completed_notification_message(r.student_name, r.completed_level, r.completed_level),
  'level_completed',
  r.completed_level,
  r.completed_level,
  false,
  r.studio_id,
  null,
  r.event_at
from deduped_recipients r
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
   or public.notifications.title is distinct from excluded.title;
