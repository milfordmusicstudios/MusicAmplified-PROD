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

  if v_completed_level <= 0 then
    return new;
  end if;

  new.type := 'level_completed';
  new.title := coalesce(nullif(new.title, ''), 'Level Completed');
  new.completed_level_start := coalesce(new.completed_level_start, v_completed_level);
  new.completed_level_end := coalesce(new.completed_level_end, v_completed_level);
  new.message := public.format_level_completed_notification_message(
    v_student_name,
    new.completed_level_start,
    new.completed_level_end
  );

  if tg_op = 'INSERT' and exists (
    select 1
    from public.notifications n
    where n.studio_id is not distinct from new.studio_id
      and n.user_id is not distinct from new.user_id
      and n.type = 'level_completed'
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
  type = 'level_completed',
  title = coalesce(nullif(title, ''), 'Level Completed'),
  completed_level_start = (regexp_match(message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1,
  completed_level_end = (regexp_match(message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1,
  message = public.format_level_completed_notification_message(
    (regexp_match(message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[1],
    (regexp_match(message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1,
    (regexp_match(message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer - 1
  )
where lower(coalesce(type, '')) = 'level_up'
  and message ~* '^\s*.*\s+(reached|advanced to)\s+Level\s+(Level\s+)?[0-9]+\.?\s*$'
  and (regexp_match(message, '^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$', 'i'))[2]::integer > 1;
