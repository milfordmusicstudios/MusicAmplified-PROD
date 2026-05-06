alter table public.notifications
  add column if not exists recognition_given boolean not null default false,
  add column if not exists recognition_given_at timestamptz,
  add column if not exists recognition_given_by uuid,
  add column if not exists recognition_note text;

create index if not exists notifications_level_completed_unrecognized_idx
on public.notifications (studio_id, created_at desc)
where lower(coalesce(type, '')) = 'level_completed'
  and coalesce(recognition_given, false) = false;

create index if not exists notifications_level_completed_recognition_idx
on public.notifications (studio_id, recognition_given, created_at desc)
where lower(coalesce(type, '')) = 'level_completed';

revoke update on public.notifications from authenticated;
grant update (
  read,
  recognition_given,
  recognition_given_at,
  recognition_given_by,
  recognition_note
) on public.notifications to authenticated;

create or replace function public.set_notification_recognition(
  p_notification_id bigint,
  p_recognition_given boolean,
  p_recognition_note text default null
)
returns table (
  id bigint,
  recognition_given boolean,
  recognition_given_at timestamptz,
  recognition_given_by uuid,
  recognition_note text
)
language plpgsql
security definer
set search_path = public
as $$
begin
  if auth.uid() is null then
    raise exception 'not_authenticated' using errcode = '28000';
  end if;

  return query
  update public.notifications n
  set
    recognition_given = coalesce(p_recognition_given, false),
    recognition_given_at = case when coalesce(p_recognition_given, false) then now() else null end,
    recognition_given_by = case when coalesce(p_recognition_given, false) then auth.uid() else null end,
    recognition_note = nullif(btrim(coalesce(p_recognition_note, '')), '')
  where n.id = p_notification_id
    and lower(coalesce(n.type, '')) = 'level_completed'
    and (
      coalesce(n.user_id, n."userId") = auth.uid()
      or exists (
        select 1
        from public.studio_members sm
        where sm.studio_id = n.studio_id
          and sm.user_id = auth.uid()
          and coalesce(sm.roles, '{}'::text[]) && array['admin', 'teacher']::text[]
      )
      or exists (
        select 1
        from public.studios s
        where s.id = n.studio_id
          and s.account_holder_user_id = auth.uid()
      )
    )
  returning
    n.id,
    n.recognition_given,
    n.recognition_given_at,
    n.recognition_given_by,
    n.recognition_note;
end;
$$;

revoke all on function public.set_notification_recognition(bigint, boolean, text) from public;
grant execute on function public.set_notification_recognition(bigint, boolean, text) to authenticated;
