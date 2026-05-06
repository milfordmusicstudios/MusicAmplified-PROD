-- Prevent future duplicate level notifications when the existing data is already clean.
-- This intentionally does not delete or merge existing duplicate rows.

do $$
begin
  update public.notifications
  set
    "userId" = coalesce("userId", user_id),
    user_id = coalesce(user_id, "userId")
  where "userId" is distinct from coalesce("userId", user_id)
     or user_id is distinct from coalesce(user_id, "userId");

  if to_regclass('public.notifications_level_display_dedupe_idx') is not null then
    return;
  end if;

  if exists (
    select 1
    from (
      select
        coalesce(studio_id, '00000000-0000-0000-0000-000000000000'::uuid) as studio_key,
        coalesce(user_id, "userId") as recipient_key,
        lower(coalesce(type, '')) as type_key,
        coalesce(
          completed_level_start::text,
          substring(coalesce(message, '') from 'Levels?[[:space:]]+([0-9]+)'),
          lower(btrim(coalesce(message, '')))
        ) as level_or_message_key,
        count(*) as duplicate_count
      from public.notifications
      where lower(coalesce(type, '')) in ('level_completed', 'level_up')
        and coalesce(user_id, "userId") is not null
      group by 1, 2, 3, 4
      having count(*) > 1
    ) duplicate_groups
  ) then
    raise notice 'Skipping notifications_level_display_dedupe_idx because duplicate level notifications already exist.';
  else
    create unique index notifications_level_display_dedupe_idx
    on public.notifications (
      (coalesce(studio_id, '00000000-0000-0000-0000-000000000000'::uuid)),
      (coalesce(user_id, "userId")),
      (lower(coalesce(type, ''))),
      (coalesce(
        completed_level_start::text,
        substring(coalesce(message, '') from 'Levels?[[:space:]]+([0-9]+)'),
        lower(btrim(coalesce(message, '')))
      ))
    )
    where lower(coalesce(type, '')) in ('level_completed', 'level_up')
      and coalesce(user_id, "userId") is not null;
  end if;
end $$;
