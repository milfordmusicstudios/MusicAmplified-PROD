create or replace function public.recalculate_user_points_and_level(
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
  v_level_id integer;
  v_updated_points integer;
  v_updated_level integer;
begin
  if p_studio_id is null then
    raise exception 'Missing studio id';
  end if;

  if p_user_id is null then
    raise exception 'Missing user id';
  end if;

  select coalesce(sum(coalesce(l.points, 0)), 0)::integer
  into v_total_points
  from public.logs l
  where l."userId" = p_user_id
    and l.studio_id = p_studio_id
    and l.status = 'approved';

  select lv.id
  into v_level_id
  from public.levels lv
  where v_total_points between coalesce(lv."minPoints", 0)
    and coalesce(lv."maxPoints", 2147483647)
  order by coalesce(lv."minPoints", 0) desc
  limit 1;

  if v_level_id is null then
    raise exception 'No matching level found for % points', v_total_points;
  end if;

  update public.users
  set
    points = v_total_points,
    level = v_level_id
  where id = p_user_id
    and studio_id = p_studio_id
  returning points, level
  into v_updated_points, v_updated_level;

  if not found then
    raise exception 'User % was not found in studio %', p_user_id, p_studio_id;
  end if;

  return jsonb_build_object(
    'points', v_updated_points,
    'totalPoints', v_updated_points,
    'level', v_updated_level
  );
end;
$$;
