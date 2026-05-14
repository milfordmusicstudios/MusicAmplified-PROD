import { supabase } from "./supabaseClient.js";

export async function getCurrentChallenge() {
  const { data, error } = await supabase.rpc("get_current_weekly_challenge");
  if (error) throw error;
  return Array.isArray(data) ? data[0] || null : data || null;
}

export async function fetchWeeklyChallengeCompletion(studioId, studentId, challengeId) {
  const targetStudioId = String(studioId || "").trim();
  const targetStudentId = String(studentId || "").trim();
  const targetChallengeId = String(challengeId || "").trim();
  if (!targetStudioId || !targetStudentId || !targetChallengeId) return null;

  const { data, error } = await supabase
    .from("weekly_challenge_completions")
    .select("id, selected_level, notes, quantity, calculated_points, completed_at, challenge_id")
    .eq("studio_id", targetStudioId)
    .eq("user_id", targetStudentId)
    .eq("challenge_id", targetChallengeId)
    .maybeSingle();

  if (error) throw error;
  return data || null;
}

export async function completeWeeklyChallenge(payload) {
  const { data, error } = await supabase.rpc("complete_weekly_challenge", {
    p_studio_id: payload?.studioId ?? null,
    p_student_id: payload?.studentId ?? null,
    p_challenge_id: payload?.challengeId ?? null,
    p_selected_level: payload?.selectedLevel ?? null,
    p_notes: payload?.notes ?? "",
    p_quantity: payload?.quantity ?? null
  });

  if (error) throw error;
  return Array.isArray(data) ? data[0] || null : data || null;
}
