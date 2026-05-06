-- Standardize milestone notifications on type = level_completed and
-- "Student completed Level X." copy. Legacy "reached/advanced to Level Y"
-- rows are transition events, so they map to completed Level Y - 1.

create or replace function public.normalize_level_completed_notification()
returns trigger
language plpgsql
set search_path = public
as $$
declare
  v_match text[];
  v_entered_level integer;
  v_completed_level integer;
  v_student_name text;
begin
  if lower(coalesce(new.type, '')) <> 'level_up' then
    return new;
  end if;

  v_match := regexp_match(
    coalesce(new.message, ''),
    '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$',
    'i'
  );

  if v_match is null then
    return new;
  end if;

  v_student_name := coalesce(nullif(trim(v_match[1]), ''), 'Student');
  v_entered_level := nullif(v_match[2], '')::integer;
  v_completed_level := v_entered_level - 1;

  -- Entering Level 1 is the baseline, not a completed-level milestone.
  if v_completed_level <= 0 then
    return null;
  end if;

  new."userId" := coalesce(new."userId", new.user_id);
  new.user_id := coalesce(new.user_id, new."userId");
  new.type := 'level_completed';
  new.title := 'Level Completed';
  new.completed_level_start := coalesce(new.completed_level_start, v_completed_level);
  new.completed_level_end := coalesce(new.completed_level_end, v_completed_level);
  new.message := public.format_level_completed_notification_message(
    v_student_name,
    new.completed_level_start,
    new.completed_level_end
  );

  if exists (
    select 1
    from public.notifications n
    where n.studio_id is not distinct from new.studio_id
      and coalesce(n.user_id, n."userId") is not distinct from coalesce(new.user_id, new."userId")
      and lower(coalesce(n.type, '')) = 'level_completed'
      and n.completed_level_start is not distinct from new.completed_level_start
      and n.completed_level_end is not distinct from new.completed_level_end
  ) then
    return null;
  end if;

  return new;
end;
$$;

drop trigger if exists trg_normalize_level_completed_notification on public.notifications;
create trigger trg_normalize_level_completed_notification
before insert or update on public.notifications
for each row
execute function public.normalize_level_completed_notification();

update public.notifications
set
  "userId" = coalesce("userId", user_id),
  user_id = coalesce(user_id, "userId")
where "userId" is distinct from coalesce("userId", user_id)
   or user_id is distinct from coalesce(user_id, "userId");

with legacy as (
  select
    n.id,
    n.studio_id,
    coalesce(n.user_id, n."userId") as recipient_id,
    (regexp_match(n.message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[1] as student_name,
    ((regexp_match(n.message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1) as completed_level,
    row_number() over (
      partition by
        n.studio_id,
        coalesce(n.user_id, n."userId"),
        ((regexp_match(n.message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1)
      order by n.created_at asc, n.id asc
    ) as target_rank
  from public.notifications n
  where lower(coalesce(n.type, '')) = 'level_up'
    and n.message ~* '^\s*.*\s+(reached|advanced to)\s+Level\s+(Level\s+)?[0-9]+\.?\s*$'
),
updatable as (
  select l.*
  from legacy l
  where l.completed_level > 0
    and l.target_rank = 1
    and not exists (
      select 1
      from public.notifications existing
      where existing.studio_id is not distinct from l.studio_id
        and coalesce(existing.user_id, existing."userId") is not distinct from l.recipient_id
        and lower(coalesce(existing.type, '')) = 'level_completed'
        and existing.completed_level_start is not distinct from l.completed_level
        and existing.completed_level_end is not distinct from l.completed_level
    )
)
update public.notifications n
set
  type = 'level_completed',
  title = 'Level Completed',
  completed_level_start = u.completed_level,
  completed_level_end = u.completed_level,
  message = public.format_level_completed_notification_message(u.student_name, u.completed_level, u.completed_level)
from updatable u
where n.id = u.id;

with legacy as (
  select
    n.id,
    n.studio_id,
    coalesce(n.user_id, n."userId") as recipient_id,
    ((regexp_match(n.message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1) as completed_level,
    row_number() over (
      partition by
        n.studio_id,
        coalesce(n.user_id, n."userId"),
        ((regexp_match(n.message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1)
      order by n.created_at asc, n.id asc
    ) as target_rank
  from public.notifications n
  where lower(coalesce(n.type, '')) = 'level_up'
    and n.message ~* '^\s*.*\s+(reached|advanced to)\s+Level\s+(Level\s+)?[0-9]+\.?\s*$'
)
delete from public.notifications n
using legacy l
where n.id = l.id
  and (
    l.completed_level <= 0
    or l.target_rank > 1
    or exists (
      select 1
      from public.notifications existing
      where existing.studio_id is not distinct from l.studio_id
        and coalesce(existing.user_id, existing."userId") is not distinct from l.recipient_id
        and lower(coalesce(existing.type, '')) = 'level_completed'
        and existing.completed_level_start is not distinct from l.completed_level
        and existing.completed_level_end is not distinct from l.completed_level
    )
  );

with ranked as (
  select
    id,
    row_number() over (
      partition by
        studio_id,
        coalesce(user_id, "userId"),
        lower(coalesce(type, '')),
        completed_level_start,
        completed_level_end
      order by created_at asc, id asc
    ) as duplicate_rank
  from public.notifications
  where lower(coalesce(type, '')) = 'level_completed'
    and coalesce(user_id, "userId") is not null
)
delete from public.notifications n
using ranked r
where n.id = r.id
  and r.duplicate_rank > 1;
