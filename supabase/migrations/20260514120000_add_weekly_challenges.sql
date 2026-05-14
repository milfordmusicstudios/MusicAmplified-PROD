create extension if not exists pgcrypto;

create table if not exists public.weekly_challenges (
  id uuid primary key default gen_random_uuid(),
  week_number integer not null check (week_number between 1 and 52),
  title text not null,
  description text,
  points integer null check (points is null or points >= 0),
  point_type text null check (point_type in ('fixed', 'memorization', 'precision', 'performance', 'practice')),
  has_levels boolean not null default false,
  beginner text null,
  intermediate text null,
  advanced text null,
  challenge text null,
  notes_instruction text not null,
  active boolean not null default true,
  start_month integer null check (start_month between 1 and 12),
  challenge_type text,
  created_at timestamptz not null default now()
);

create unique index if not exists weekly_challenges_week_number_key
  on public.weekly_challenges (week_number);

create index if not exists weekly_challenges_active_week_idx
  on public.weekly_challenges (active, week_number);

create table if not exists public.weekly_challenge_completions (
  id uuid primary key default gen_random_uuid(),
  studio_id uuid not null,
  user_id uuid not null,
  challenge_id uuid not null references public.weekly_challenges(id) on delete restrict,
  selected_level text null check (selected_level in ('beginner', 'intermediate', 'advanced')),
  notes text not null,
  quantity integer null check (quantity is null or quantity > 0),
  calculated_points integer not null check (calculated_points >= 0),
  log_id bigint null,
  completed_at timestamptz not null default now(),
  created_at timestamptz not null default now(),
  unique (user_id, challenge_id)
);

create index if not exists weekly_challenge_completions_studio_user_idx
  on public.weekly_challenge_completions (studio_id, user_id, completed_at desc);

create index if not exists weekly_challenge_completions_challenge_idx
  on public.weekly_challenge_completions (challenge_id);

alter table public.weekly_challenges enable row level security;
alter table public.weekly_challenge_completions enable row level security;

drop policy if exists weekly_challenges_read_active on public.weekly_challenges;
create policy weekly_challenges_read_active
on public.weekly_challenges
for select
using (active = true);

drop policy if exists weekly_challenge_completions_select_own_family_staff on public.weekly_challenge_completions;
create policy weekly_challenge_completions_select_own_family_staff
on public.weekly_challenge_completions
for select
using (
  user_id = auth.uid()
  or public.has_any_studio_role(studio_id, auth.uid(), array['owner', 'admin', 'teacher']::text[])
  or exists (
    select 1
    from public.users u
    where u.id = weekly_challenge_completions.user_id
      and u.parent_uuid = auth.uid()
  )
  or exists (
    select 1
    from public.parent_student_links psl
    where psl.student_id = weekly_challenge_completions.user_id
      and psl.parent_id = auth.uid()
      and psl.studio_id = weekly_challenge_completions.studio_id
  )
);

create or replace function public.get_current_weekly_challenge_number(p_at timestamptz default now())
returns integer
language sql
stable
as $$
  select (((extract(week from coalesce(p_at, now()))::integer - 1) % 52) + 1);
$$;

create or replace function public.get_current_weekly_challenge()
returns setof public.weekly_challenges
language sql
stable
security definer
set search_path = public
as $$
  select wc.*
  from public.weekly_challenges wc
  where wc.active = true
    and wc.week_number = public.get_current_weekly_challenge_number(now())
  order by wc.week_number
  limit 1;
$$;

grant execute on function public.get_current_weekly_challenge_number(timestamptz) to authenticated;
grant execute on function public.get_current_weekly_challenge() to authenticated;

create or replace function public.complete_weekly_challenge(
  p_studio_id uuid,
  p_student_id uuid,
  p_challenge_id uuid,
  p_selected_level text default null,
  p_notes text default '',
  p_quantity integer default null
)
returns table (
  completion_id uuid,
  log_id bigint,
  calculated_points integer
)
language plpgsql
security definer
set search_path = public
as $$
declare
  v_caller_id uuid := auth.uid();
  v_challenge public.weekly_challenges%rowtype;
  v_can_submit boolean := false;
  v_instrument_text text := '';
  v_points integer := 0;
  v_log_id bigint;
  v_completion_id uuid;
  v_point_type text;
  v_category text := 'weekly challenge';
  v_status text := 'pending';
begin
  if v_caller_id is null then
    raise exception 'not_authenticated' using errcode = '28000';
  end if;

  if p_studio_id is null or p_student_id is null or p_challenge_id is null then
    raise exception 'missing_required_fields' using errcode = '22023';
  end if;

  v_can_submit := v_caller_id = p_student_id
    or public.has_any_studio_role(p_studio_id, v_caller_id, array['owner', 'admin', 'teacher']::text[])
    or exists (
      select 1
      from public.users u
      where u.id = p_student_id
        and u.parent_uuid = v_caller_id
    )
    or exists (
      select 1
      from public.parent_student_links psl
      where psl.student_id = p_student_id
        and psl.parent_id = v_caller_id
        and psl.studio_id = p_studio_id
    );

  if not v_can_submit then
    raise exception 'not_authorized' using errcode = '42501';
  end if;

  if not exists (
    select 1
    from public.studio_members sm
    where sm.studio_id = p_studio_id
      and sm.user_id = p_student_id
      and coalesce(sm.roles, '{}'::text[]) @> array['student']::text[]
  ) then
    raise exception 'student_not_in_studio' using errcode = '42501';
  end if;

  select *
  into v_challenge
  from public.weekly_challenges wc
  where wc.id = p_challenge_id
    and wc.active = true
  for update;

  if not found then
    raise exception 'challenge_not_found' using errcode = '22023';
  end if;

  if v_challenge.week_number <> public.get_current_weekly_challenge_number(now()) then
    raise exception 'challenge_not_current' using errcode = '22023';
  end if;

  if v_challenge.has_levels then
    if p_selected_level not in ('beginner', 'intermediate', 'advanced') then
      raise exception 'selected_level_required' using errcode = '22023';
    end if;
  elsif p_selected_level is not null then
    raise exception 'selected_level_not_allowed' using errcode = '22023';
  end if;

  if nullif(trim(coalesce(p_notes, '')), '') is null then
    raise exception 'notes_required' using errcode = '22023';
  end if;

  v_point_type := coalesce(v_challenge.point_type, case when v_challenge.points is not null then 'fixed' else null end);

  if v_point_type = 'memorization' or v_point_type = 'precision' then
    if p_quantity is null or p_quantity <= 0 then
      raise exception 'quantity_required' using errcode = '22023';
    end if;

    select lower(array_to_string(coalesce(u.instrument, '{}'::text[]), ' '))
    into v_instrument_text
    from public.users u
    where u.id = p_student_id;

    v_points := p_quantity * case
      when v_instrument_text ~ '(voice|vocal|singer|singing)' then 1
      else 2
    end;
    v_category := 'proficiency';
  elsif v_point_type = 'performance' then
    v_points := 100;
    v_category := 'performance';
  elsif v_point_type = 'practice' then
    v_points := 5;
    v_category := 'practice';
  else
    v_points := coalesce(v_challenge.points, 0);
    v_category := 'weekly challenge';
  end if;

  insert into public.logs (
    "userId",
    date,
    category,
    points,
    notes,
    status,
    source,
    studio_id,
    created_by
  )
  values (
    p_student_id,
    current_date,
    v_category,
    v_points,
    'Weekly Challenge: ' || coalesce(v_challenge.title, 'Challenge') || ' - ' || trim(coalesce(p_notes, '')),
    v_status,
    'weekly_challenge',
    p_studio_id,
    v_caller_id
  )
  returning id into v_log_id;

  insert into public.weekly_challenge_completions (
    studio_id,
    user_id,
    challenge_id,
    selected_level,
    notes,
    quantity,
    calculated_points,
    log_id
  )
  values (
    p_studio_id,
    p_student_id,
    p_challenge_id,
    p_selected_level,
    trim(coalesce(p_notes, '')),
    p_quantity,
    v_points,
    v_log_id
  )
  returning id into v_completion_id;

  return query select v_completion_id, v_log_id, v_points;
exception
  when unique_violation then
    raise exception 'weekly_challenge_already_completed' using errcode = '23505';
end;
$$;

grant execute on function public.complete_weekly_challenge(uuid, uuid, uuid, text, text, integer) to authenticated;

insert into public.weekly_challenges (
  week_number, title, description, points, point_type, has_levels,
  beginner, intermediate, advanced, challenge, notes_instruction, start_month, challenge_type
)
values
  (1, 'Practice Streak', 'Build consistency by spreading your practice across the week.', 15, 'fixed', false, null, null, null, 'Practice on 5 different days this week.', 'Write how many days you practiced and total minutes.', null, 'habits'),
  (2, 'Listen & Repeat', 'Train your ear by copying short musical ideas.', 5, 'fixed', false, null, null, null, 'Listen to and repeat 3 musical patterns.', 'Write what you copied.', null, 'listening'),
  (3, 'Memory Mission', 'Strengthen musical memory one section at a time.', null, 'memorization', true, 'Memorize 4 bars', 'Memorize 8 bars', 'Memorize 16+ bars', null, 'Write song title and number of bars memorized.', null, 'memorization'),
  (4, 'Dynamic Explorer', 'Use volume changes to make your music more expressive.', 10, 'fixed', true, 'Perform one section soft and loud', 'Add medium volume', 'Add crescendos', null, 'Write the song title used.', null, 'musicality'),
  (5, 'Rhythm Builder', 'Make rhythm steady and confident.', 10, 'fixed', true, 'Clap and count 8 measures', 'Play 8 measures with a metronome', 'Record 16 measures with steady pulse', null, 'Write the rhythm or song you practiced.', null, 'rhythm'),
  (6, 'Creative Ending', 'Create your own ending for a piece or exercise.', 10, 'fixed', false, null, null, null, 'Compose a new 4-bar ending.', 'Write what piece inspired your ending.', null, 'creativity'),
  (7, 'Courage Take', 'Practice performing even when it feels imperfect.', null, 'performance', false, null, null, null, 'Perform one piece for someone at home.', 'Write who listened and what you noticed.', null, 'courage'),
  (8, 'Precision Pass', 'Clean up the details in a short section.', null, 'precision', true, 'Polish 4 bars', 'Polish 8 bars', 'Polish 16+ bars', null, 'Write the song and number of bars polished.', null, 'precision'),
  (9, 'Practice Plan', 'Plan before you play.', 10, 'fixed', false, null, null, null, 'Write a 3-step practice plan and follow it twice.', 'Write your three steps and what improved.', null, 'habits'),
  (10, 'Melody Detective', 'Listen closely for melodic shape.', 10, 'fixed', false, null, null, null, 'Find the highest and lowest notes in one piece.', 'Write the piece and where the high and low points happen.', null, 'listening'),
  (11, 'Spring Soundtrack', 'Connect music to a season or mood.', 10, 'fixed', false, null, null, null, 'Play a piece with a spring mood or create a short spring melody.', 'Write the title or describe the melody.', 3, 'seasonal'),
  (12, 'Left Hand Lead', 'Give attention to the part that usually supports.', 10, 'fixed', true, 'Practice left hand alone for 5 minutes', 'Play left hand with steady rhythm', 'Bring out the left hand melody or bass line', null, 'Write what section you practiced.', null, 'technique'),
  (13, 'Tempo Ladder', 'Control tempo instead of rushing.', 10, 'fixed', true, 'Play slowly and evenly', 'Play slow, medium, and goal tempo', 'Record all three tempos', null, 'Write the tempos or describe the speed changes.', null, 'rhythm'),
  (14, 'Composer Copycat', 'Learn by imitating a style.', 10, 'fixed', false, null, null, null, 'Create a 4-bar idea that sounds like a piece you are studying.', 'Write the piece or composer that inspired you.', null, 'composition'),
  (15, 'Kindness Concert', 'Share music with someone as a gift.', null, 'performance', false, null, null, null, 'Perform one song for a family member, friend, or neighbor.', 'Write who you played for and how it felt.', null, 'community'),
  (16, 'Articulation Quest', 'Make notes speak clearly.', 10, 'fixed', true, 'Find and play staccato or legato markings', 'Contrast staccato and legato in one section', 'Add clear articulation throughout a full piece', null, 'Write the piece and articulation you focused on.', null, 'musicality'),
  (17, 'Memory Mission II', 'Keep building memorized repertoire.', null, 'memorization', true, 'Memorize 4 new bars', 'Memorize 8 new bars', 'Memorize 16+ new bars', null, 'Write song title and number of bars memorized.', null, 'memorization'),
  (18, 'Listening Journal', 'Notice details in music you hear.', 10, 'fixed', false, null, null, null, 'Listen to one full piece and name 3 things you heard.', 'Write the title and your three observations.', null, 'listening'),
  (19, 'May Momentum', 'Refresh your routine before summer.', 15, 'fixed', false, null, null, null, 'Practice at least 60 total minutes this week.', 'Write your total minutes and what you practiced most.', 5, 'habits'),
  (20, 'Steady Start', 'Begin with focus and control.', 10, 'fixed', false, null, null, null, 'Play the first line of a piece perfectly 3 times in a row.', 'Write the piece and what made the start stronger.', null, 'precision'),
  (21, 'Improvisation Minute', 'Create music in the moment.', 10, 'fixed', true, 'Improvise for 30 seconds', 'Improvise for 1 minute', 'Improvise using a planned rhythm or mood', null, 'Write what notes, mood, or pattern you used.', null, 'creativity'),
  (22, 'Performance Posture', 'Prepare your body for confident playing.', null, 'performance', false, null, null, null, 'Perform a piece while focusing on posture and setup.', 'Write what you adjusted before performing.', null, 'performance'),
  (23, 'Rhythm Remix', 'Change rhythm while keeping control.', 10, 'fixed', false, null, null, null, 'Take a short melody and play it with a new rhythm.', 'Write the melody and describe the rhythm change.', null, 'rhythm'),
  (24, 'Summer Kickoff', 'Start summer with a musical win.', 15, 'fixed', false, null, null, null, 'Choose one summer music goal and complete the first step.', 'Write your goal and the step you completed.', 6, 'seasonal'),
  (25, 'Practice Power-Up', 'Use focused repetition.', null, 'practice', false, null, null, null, 'Complete one focused practice session of at least 20 minutes.', 'Write what you practiced and for how long.', null, 'practice'),
  (26, 'Half-Year Checkpoint', 'Look back and notice growth.', 10, 'fixed', false, null, null, null, 'Replay something from earlier this year and compare it to now.', 'Write what improved since the first time you played it.', null, 'reflection'),
  (27, 'Precision Pass II', 'Make a difficult section cleaner.', null, 'precision', true, 'Polish 4 bars', 'Polish 8 bars', 'Polish 16+ bars', null, 'Write the song and number of bars polished.', null, 'precision'),
  (28, 'Sound Color', 'Explore tone and character.', 10, 'fixed', true, 'Play one section with a gentle tone', 'Play the same section with two tone colors', 'Match tone color to the mood of the piece', null, 'Write the tone colors you tried.', null, 'musicality'),
  (29, 'Duet Day', 'Make music with another person or recording.', 10, 'fixed', false, null, null, null, 'Play along with a duet partner, backing track, or recording.', 'Write what you played with.', null, 'community'),
  (30, 'Mini Composition', 'Build a short original piece.', 15, 'fixed', true, 'Compose 4 bars', 'Compose 8 bars', 'Compose 16 bars or add an ending', null, 'Write the title or describe your composition.', null, 'composition'),
  (31, 'Brave Recording', 'Use recording as a practice tool.', null, 'performance', false, null, null, null, 'Record one full take of a piece without stopping.', 'Write the title and one thing you liked about the take.', null, 'courage'),
  (32, 'Back-to-Basics', 'Review fundamentals with care.', 10, 'fixed', false, null, null, null, 'Spend 10 minutes on scales, warmups, or technique.', 'Write what fundamentals you practiced.', 8, 'technique'),
  (33, 'Memory Mission III', 'Memorize with accuracy and confidence.', null, 'memorization', true, 'Memorize 4 bars', 'Memorize 8 bars', 'Memorize 16+ bars', null, 'Write song title and number of bars memorized.', null, 'memorization'),
  (34, 'Teacher Tip Replay', 'Apply feedback from your lesson.', 10, 'fixed', false, null, null, null, 'Practice one specific teacher tip at least 3 times.', 'Write the tip and what changed.', null, 'habits'),
  (35, 'Rhythm Without Notes', 'Separate rhythm from pitch.', 10, 'fixed', true, 'Clap one line', 'Count and clap one full section', 'Clap, count, then play the section', null, 'Write the section you used.', null, 'rhythm'),
  (36, 'Fall Focus', 'Settle into a strong routine.', 15, 'fixed', false, null, null, null, 'Practice on 4 different days and include one review piece.', 'Write the days practiced and review piece.', 9, 'seasonal'),
  (37, 'Expression Marks', 'Turn written markings into sound.', 10, 'fixed', false, null, null, null, 'Find 3 markings in your music and perform them clearly.', 'Write the markings and the piece.', null, 'musicality'),
  (38, 'Precision Pass III', 'Polish carefully before moving on.', null, 'precision', true, 'Polish 4 bars', 'Polish 8 bars', 'Polish 16+ bars', null, 'Write the song and number of bars polished.', null, 'precision'),
  (39, 'Audience Choice', 'Let someone choose what you play.', null, 'performance', false, null, null, null, 'Ask someone to choose a piece and perform it for them.', 'Write who chose and what you played.', null, 'community'),
  (40, 'Spooky Sounds', 'Use music to create character.', 10, 'fixed', false, null, null, null, 'Play or create a short spooky, mysterious, or dramatic sound.', 'Write what made it sound that way.', 10, 'seasonal'),
  (41, 'Metronome Match', 'Lock in with a steady beat.', 10, 'fixed', true, 'Play 8 bars with metronome', 'Play 16 bars with metronome', 'Change tempo and stay steady', null, 'Write the piece and metronome setting.', null, 'rhythm'),
  (42, 'Gratitude Song', 'Connect music with gratitude.', 10, 'fixed', false, null, null, null, 'Play a song for someone you appreciate or choose a grateful mood.', 'Write who or what inspired the song.', 11, 'seasonal'),
  (43, 'Memory Mission IV', 'Add another memorized section.', null, 'memorization', true, 'Memorize 4 bars', 'Memorize 8 bars', 'Memorize 16+ bars', null, 'Write song title and number of bars memorized.', null, 'memorization'),
  (44, 'Mistake Recovery', 'Practice continuing with confidence.', 10, 'fixed', false, null, null, null, 'Play through a piece without stopping, even after mistakes.', 'Write what helped you keep going.', null, 'courage'),
  (45, 'Theme and Variation', 'Change one musical idea creatively.', 15, 'fixed', true, 'Change the rhythm', 'Change the mood', 'Change rhythm, mood, and ending', null, 'Write the theme and variation you made.', null, 'composition'),
  (46, 'Practice Sprint', 'Use a short focused session well.', null, 'practice', false, null, null, null, 'Complete a 15-minute practice sprint with one clear goal.', 'Write your goal and what improved.', null, 'practice'),
  (47, 'Holiday Preview', 'Prepare something seasonal or celebratory.', null, 'performance', false, null, null, null, 'Perform or record a holiday, winter, or celebration piece.', 'Write the title and who heard it.', 12, 'seasonal'),
  (48, 'Listening Compare', 'Hear differences between performances.', 10, 'fixed', false, null, null, null, 'Listen to two versions of the same piece and compare them.', 'Write two differences you noticed.', null, 'listening'),
  (49, 'Precision Finale', 'Finish the year with clean details.', null, 'precision', true, 'Polish 4 bars', 'Polish 8 bars', 'Polish 16+ bars', null, 'Write the song and number of bars polished.', null, 'precision'),
  (50, 'Year-End Showcase', 'Share your progress with confidence.', null, 'performance', false, null, null, null, 'Perform one favorite piece from this year.', 'Write the title and why you chose it.', 12, 'performance'),
  (51, 'Reflection Remix', 'Use reflection to guide practice.', 10, 'fixed', false, null, null, null, 'Name one musical skill that grew this year and practice it again.', 'Write the skill and what you practiced.', 12, 'reflection'),
  (52, 'New Year Launch', 'Set up your next musical chapter.', 15, 'fixed', false, null, null, null, 'Choose one goal for next year and complete a first musical step.', 'Write your goal and the first step you completed.', 1, 'seasonal')
on conflict (week_number) do update set
  title = excluded.title,
  description = excluded.description,
  points = excluded.points,
  point_type = excluded.point_type,
  has_levels = excluded.has_levels,
  beginner = excluded.beginner,
  intermediate = excluded.intermediate,
  advanced = excluded.advanced,
  challenge = excluded.challenge,
  notes_instruction = excluded.notes_instruction,
  active = true,
  start_month = excluded.start_month,
  challenge_type = excluded.challenge_type;
