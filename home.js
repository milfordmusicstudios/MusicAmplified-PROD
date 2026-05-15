import { supabase } from './supabaseClient.js';
import { ensureStudioContextAndRoute } from './studio-routing.js';
import { clearAppSessionCache, ensureUserRow, fetchStudentLevelSnapshots, getAuthUserId, getCategoryDefaultPoints, getViewerContext, recalculateUsersAfterApprovedBatch, renderActiveStudentHeader } from './utils.js';
import { getActiveProfileId, setActiveProfileId, persistLastActiveStudent, getLastActiveStudent, clearLastActiveStudent } from './active-profile.js';
import { getAccountProfiles, renderAccountProfileList, hasRole, loadLinkedStudentsForParent } from './account-profiles.js';
import { initStaffChallengesUI } from './challenges-ui.js';
import { initStudentChallengesUI } from './challenges-student-ui.js';
import { queueCelebrations } from './celebrations.js';
import { selectHeroBadge, normalizeHeroBadge } from './badgeHero.js';
import { renderBadgeHeroHalfPanel } from './BadgeHeroHalfPanel.js';
import { initBadgeHeroModal, openBadgeHeroModal } from './BadgeHeroModal.js';
import { createStudentHomeTutorial, createTeacherAdminTutorial } from './student-tutorial.js';
import { getStudentLogPrompts, getTeacherLogPrompts, getTeacherPointCategories } from './log-prompts.js';

const qs = id => document.getElementById(id);
const dispatchTutorialAction = (action) => {
  if (!action) return;
  window.dispatchEvent(new CustomEvent(String(action)));
};
const safeParse = value => {
  try {
    return JSON.parse(value);
  } catch {
    return null;
  }
};

let AVAILABLE_PROFILES = [];
let ACTIVE_PROFILE_ID = null;
let avatarMenuHandler = null;
let avatarMenuEventsBound = false;
let studentTutorial = null;
let teacherAdminTutorial = null;
const HOME_INIT_LOADING_CLASS = "home-init-loading";
function revealHomeAfterIdentity() {
  document.body.classList.remove(HOME_INIT_LOADING_CLASS);
}

function isStudentTutorialEligible({ viewerContext, profile } = {}) {
  if (!viewerContext || !profile) return false;
  if (viewerContext.isAdmin || viewerContext.isTeacher) return false;
  if (!hasRole(profile, "student")) return false;
  if (document.querySelector("#quickPracticeBtn")?.offsetParent === null) return false;
  return true;
}

function scheduleStudentTutorial({ viewerContext, profileId, profile } = {}) {
  if (!isStudentTutorialEligible({ viewerContext, profile })) return;
  if (!profileId) return;

  window.setTimeout(async () => {
    if (!isStudentTutorialEligible({ viewerContext, profile })) return;
    try {
      if (!studentTutorial) {
        studentTutorial = createStudentHomeTutorial({ profileId });
      } else {
        studentTutorial.profileId = profileId;
      }
      window.AA_replayStudentTutorial = () => studentTutorial.start({ force: true });
      window.AA_replayTutorial = () => studentTutorial.start({ force: true });
      await studentTutorial.maybeStart();
    } catch (error) {
      console.warn("[StudentTutorial] failed to start", error);
    }
  }, 180);
}

function isTeacherAdminTutorialEligible({ viewerContext } = {}) {
  if (!viewerContext) return false;
  if (!(viewerContext.accountIsAdmin || viewerContext.accountIsTeacher)) return false;
  if (!homeIdentityContext?.showStaffUI) return false;
  if (document.querySelector("#staffQuickLogForm")?.offsetParent === null) return false;
  return true;
}

function scheduleTeacherAdminTutorial({ viewerContext } = {}) {
  if (!isTeacherAdminTutorialEligible({ viewerContext })) return;
  const profileId = viewerContext?.viewerUserId || viewerContext?.activeProfileId || null;
  if (!profileId) return;

  window.setTimeout(async () => {
    if (!isTeacherAdminTutorialEligible({ viewerContext })) return;
    try {
      if (!teacherAdminTutorial) {
        teacherAdminTutorial = createTeacherAdminTutorial({ profileId });
      } else {
        teacherAdminTutorial.profileId = profileId;
      }
      window.AA_replayTeacherAdminTutorial = () => teacherAdminTutorial.start({ force: true });
      window.AA_replayTutorial = () => teacherAdminTutorial.start({ force: true });
      await teacherAdminTutorial.maybeStart({ startOnlyOnStartPath: true });
    } catch (error) {
      console.warn("[TeacherTutorial] failed to start", error);
    }
  }, 220);
}

function loginRedirectUrl() {
  return "login.html?returnTo=/index.html";
}

function setActiveProfileIdContext(id) {
  ACTIVE_PROFILE_ID = id != null ? String(id) : null;
}

function setAvailableProfilesContext(profiles) {
  AVAILABLE_PROFILES = Array.isArray(profiles) ? profiles : [];
}

function getProfileAvatarUrl(profile) {
  if (!profile) return "";
  return profile.avatarUrl || profile.avatar_url || profile.avatar || "";
}

async function loadStudioQuickLogTypeSettings() {
  try {
    const { data, error } = await supabase.rpc("get_my_studio");
    if (error) throw error;
    const studio = Array.isArray(data) ? data[0] : data;
    const rawSettings = studio?.settings;
    if (!rawSettings) return {};
    if (typeof rawSettings === "string") {
      try {
        return JSON.parse(rawSettings || "{}") || {};
      } catch {
        return {};
      }
    }
    if (typeof rawSettings === "object") return rawSettings;
    return {};
  } catch (err) {
    console.warn("[Home] failed to load studio quick log type settings", err);
    return {};
  }
}

function getEnabledStudentQuickLogTypes(studioSettings = {}) {
  const savedPresets = studioSettings?.studentLogTypes?.presets || {};
  const enabledPresets = getStudentLogPrompts().filter((preset) => {
    if (!(preset.key in savedPresets)) return true;
    return Boolean(savedPresets[preset.key]);
  });

  const custom = Array.isArray(studioSettings?.studentLogTypes?.custom)
    ? studioSettings.studentLogTypes.custom
    : [];
  const customTypes = custom
    .map((row, index) => {
      if (row?.enabled === false) return null;
      const label = String(row?.label || "").trim();
      const category = String(row?.category || "").trim().toLowerCase();
      const points = Number(row?.points);
      if (!label || !category || !Number.isFinite(points) || points <= 0) return null;
      return {
        key: `custom-${index}`,
        label,
        icon: "Custom",
        logType: "fixed",
        points: Math.round(points),
        notesPrompt: `Describe: ${label}`,
        notesRequired: true,
        category
      };
    })
    .filter(Boolean);

  return enabledPresets.concat(customTypes);
}

function renderStudentQuickLogTypeChips(types) {
  const grid = document.querySelector(".action-grid");
  if (!grid) return;
  const chips = Array.isArray(types) && types.length ? types : getStudentLogPrompts();
  const chipsMarkup = chips.map((item) => {
    const pointsAttr = Number.isFinite(item.points) ? ` data-points="${item.points}"` : "";
    const promptAttr = item.notesPrompt ? ` data-notes-prompt="${item.notesPrompt.replace(/"/g, "&quot;")}"` : "";
    const requiredAttr = typeof item.notesRequired === "boolean" ? ` data-notes-required="${item.notesRequired ? "true" : "false"}"` : "";
    return `<button class="chip" data-log-type="${item.logType}"${pointsAttr}${requiredAttr}${promptAttr} data-category="${item.category}" data-hint="${item.label}">${item.icon || ""} ${item.label}</button>`;
  }).join("");

  grid.innerHTML = `
    ${chipsMarkup}
    <button id="teacherGoalChip" class="chip chip-teacher" data-log-type="discretionary" data-points="5" data-notes-required="true" data-notes-prompt="Describe the goal." data-category="personal" data-hint="Teacher goal" style="display:none;">
      ⭐ Teacher goal
    </button>
  `;
}

async function safeViewerContext() {
  try {
    return await getViewerContext();
  } catch (err) {
    console.warn("[Home] viewer context unavailable; continuing without it", err);
    return null;
  }
}

function renderHomeAvatarFromActiveProfile() {
  const img = document.querySelector("#avatarImg, #homeAvatar, #profileAvatar, .home-avatar img, img[data-home-avatar]");
  if (!img) return;
  const active = (AVAILABLE_PROFILES || []).find(p =>
    (p?.id && ACTIVE_PROFILE_ID && String(p.id) === ACTIVE_PROFILE_ID) ||
    (p?.user_id && ACTIVE_PROFILE_ID && String(p.user_id) === ACTIVE_PROFILE_ID)
  ) || null;
  const url = getProfileAvatarUrl(active);
  if (url) {
    img.src = url;
  } else if (img.dataset?.placeholder) {
    img.src = img.dataset.placeholder;
  }
  const first = active ? String(active.firstName || active.first_name || "").trim() : "";
  const last = active ? String(active.lastName || active.last_name || "").trim() : "";
  const name = last && first ? `${last}, ${first}` : (last || first);
  img.alt = name || "Profile";
}

function bindAvatarMenu() {
  const button = document.querySelector("#avatarSwitcher, #homeAvatarBtn, #profileAvatarBtn, .home-avatar, #homeAvatar");
  const menu = qs("avatarMenu");
  if (!button || !menu) return;
  const canSwitch = Array.isArray(AVAILABLE_PROFILES) && AVAILABLE_PROFILES.length > 1;
  button.classList.toggle("no-switch", !canSwitch);
  button.style.cursor = canSwitch ? "pointer" : "default";

  if (avatarMenuHandler) {
    button.removeEventListener("click", avatarMenuHandler);
    avatarMenuHandler = null;
  }
  if (canSwitch) {
    avatarMenuHandler = (e) => {
      e.preventDefault();
      e.stopPropagation();
      const isOpen = !menu.hidden;
      if (isOpen) {
        closeAvatarMenu();
        return;
      }
      menu.hidden = false;
      button.setAttribute("aria-expanded", "true");
    };
    button.addEventListener("click", avatarMenuHandler);
  }

  if (!avatarMenuEventsBound) {
    document.addEventListener("click", (e) => {
      if (!menu.hidden && !menu.contains(e.target) && !button.contains(e.target)) {
        closeAvatarMenu();
      }
    });
    document.addEventListener("keydown", (e) => {
      if (e.key === "Escape") closeAvatarMenu();
    });
    avatarMenuEventsBound = true;
  }
}

function resolveAvatarSrc(user) {
  const candidates = [user?.avatarUrl, user?.avatar_url, user?.avatar];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim()) {
      return candidate.trim();
    }
  }
  return "images/icons/default.png";
}

function normalizeUserRow(row) {
  if (!row) return null;
  return {
    ...row,
    firstName: row.firstName ?? row.first_name ?? null,
    lastName: row.lastName ?? row.last_name ?? null,
    avatarUrl: row.avatarUrl ?? row.avatar_url ?? null
  };
}

let currentProfile = null;
let availableUsers = [];
let pendingPointsTotal = 0;
let currentLevelRow = null;
let isStaffUser = false;
let isParentReadOnly = false;
let practiceLoggedToday = false;
let studentCelebrationsLoadedFor = "";
let lastNextUpDebugState = null;
let homeShowStaffUI = false;
let homeIdentityContext = null;

const ADMIN_MODE_KEY = "aa.adminModeEnabled";
const ACTIVE_STUDENT_STORAGE_KEY = "aa.activeStudentId";

function getAdminModeEnabled() {
  if (typeof window.adminModeEnabled === "boolean") return window.adminModeEnabled;
  return localStorage.getItem(ADMIN_MODE_KEY) === "1";
}

function setAdminModeEnabled(enabled) {
  const next = Boolean(enabled);
  window.adminModeEnabled = next;
  localStorage.setItem(ADMIN_MODE_KEY, next ? "1" : "0");
}

function getStoredActiveStudentId() {
  const raw = localStorage.getItem(ACTIVE_STUDENT_STORAGE_KEY);
  const id = String(raw || "").trim();
  return id || null;
}

function persistActiveStudentId(studentId) {
  const id = String(studentId || "").trim();
  if (!id) {
    localStorage.removeItem(ACTIVE_STUDENT_STORAGE_KEY);
    return;
  }
  localStorage.setItem(ACTIVE_STUDENT_STORAGE_KEY, id);
}

function getParentSelectionKey(studioId, viewerUserId) {
  return studioId && viewerUserId ? `aa.activeStudent.${studioId}.${viewerUserId}` : null;
}

function clearParentSelection(viewerUserId, studioId) {
  const key = getParentSelectionKey(studioId, viewerUserId);
  if (key) localStorage.removeItem(key);
}

function persistParentSelection(viewerUserId, studioId, studentId) {
  const key = getParentSelectionKey(studioId, viewerUserId);
  if (key && studentId) {
    localStorage.setItem(key, String(studentId));
  }
  persistActiveStudentId(studentId);
  persistLastActiveStudent(viewerUserId, studioId, studentId);
}

const SWITCH_STUDENT_TIP_KEY = "aa_switch_student_tip_shown";

function maybeShowSwitchStudentTip(mode, hasMultipleUsers = false) {
  const tip = qs("switchStudentTip");
  if (!tip) return;
  if (mode !== "student" || !hasMultipleUsers) {
    tip.style.display = "none";
    return;
  }
  const alreadyShown = localStorage.getItem(SWITCH_STUDENT_TIP_KEY) === "true";
  if (alreadyShown) {
    tip.style.display = "none";
    return;
  }
  tip.textContent = "Click the avatar to switch users.";
  tip.style.display = "";
  localStorage.setItem(SWITCH_STUDENT_TIP_KEY, "true");
}

function updateViewModeToggle(result) {
  const toggle = qs("viewModeToggle");
  if (!toggle) return;
  const view = (result?.view) ||
    (typeof window.AA_getActiveView === "function" ? window.AA_getActiveView() : "teacher");
  const buttons = toggle.querySelectorAll("[data-view-mode]");
  buttons.forEach(btn => {
    const mode = btn.dataset.viewMode;
    const active = mode === view;
    btn.classList.toggle("is-active", active);
    btn.setAttribute("aria-pressed", active ? "true" : "false");
  });
  toggle.dataset.allowToggle = result?.allowToggle ? "true" : "false";
}

function initViewModeToggle() {
  const toggle = qs("viewModeToggle");
  if (!toggle) return;
  toggle.style.display = "none";
  toggle.dataset.allowToggle = "false";
  return;
  const buttons = toggle.querySelectorAll("[data-view-mode]");
  buttons.forEach(btn => {
    btn.addEventListener("click", () => {
      const mode = btn.dataset.viewMode;
      if (!mode || typeof window.AA_setActiveView !== "function") return;
      const viewResult = window.AA_setActiveView(mode);
      updateViewModeToggle(viewResult);
    });
  });
  updateViewModeToggle();
}

function getStudioRoleContext() {
  const rolesRaw = localStorage.getItem("activeStudioRoles");
  let roles = [];
  try {
    roles = JSON.parse(rolesRaw || "[]");
  } catch {
    roles = [];
  }
  return {
    roles,
    studioId: localStorage.getItem("activeStudioId")
  };
}

function logEmptyFetch(label, userId) {
  const ctx = getStudioRoleContext();
  console.log(`[Home] empty ${label}`, {
    activeStudentId: userId,
    roles: ctx.roles,
    studioId: ctx.studioId
  });
}

function getActiveStudentIdForContext(ctx) {
  const stored = getStoredActiveStudentId();
  if (stored) return stored;
  if (!ctx) return null;
  if (ctx.activeProfileId) {
    persistActiveStudentId(ctx.activeProfileId);
    return String(ctx.activeProfileId);
  }
  return null;
}

function resolveHomeIdentityContext(viewerContext = null) {
  const fallbackStudioId = localStorage.getItem("activeStudioId") || null;
  const authUserId = String(viewerContext?.viewerUserId || homeIdentityContext?.authUserId || "").trim() || null;
  const viewerUserId = String(viewerContext?.viewerUserId || homeIdentityContext?.viewerUserId || authUserId || "").trim() || null;
  const activeStudentId = String(getActiveStudentIdForContext(viewerContext) || homeIdentityContext?.activeStudentId || "").trim() || null;
  const accountIsStaff = Boolean(homeIdentityContext?.accountIsStaff);
  const activeIsStudent = Boolean(homeIdentityContext?.activeIsStudent);
  const adminMode = Boolean(homeIdentityContext?.adminMode);
  const showStaffUI = Boolean(homeIdentityContext?.showStaffUI);
  const staffLoggingMode = accountIsStaff && showStaffUI;
  const isParentMode = isParentReadOnly || viewerContext?.mode === "parent";
  const mode = isParentMode ? "parent" : (staffLoggingMode ? "staff" : "student");
  const studioId = String(viewerContext?.studioId || homeIdentityContext?.studioId || fallbackStudioId || "").trim() || null;
  return {
    authUserId,
    viewerUserId,
    activeStudentId,
    accountIsStaff,
    activeIsStudent,
    adminMode,
    showStaffUI,
    staffLoggingMode,
    mode,
    studioId
  };
}


async function loadLevel(levelId) {
  const { data, error } = await supabase
    .from("levels")
    .select("*")
    .eq("id", levelId)
    .single();

  if (error) {
    console.error("Failed to load level", error);
    return null;
  }
  return data;
}

async function calculateXpAndLevel(userId) {
  if (!userId) return { totalPoints: 0, currentLevel: null };

  const { data: logs, error: logsError } = await supabase
    .from("logs")
    .select("points")
    .eq("userId", userId)
    .eq("status", "approved");
  if (logsError) {
    console.error("[Home] XP logs fetch failed", logsError);
    return { totalPoints: 0, currentLevel: null };
  }

  const totalPoints = (logs || []).reduce((sum, log) => sum + (log.points || 0), 0);

  const { data: levels, error: levelsError } = await supabase
    .from("levels")
    .select("*")
    .order("minPoints", { ascending: true });
  if (levelsError) {
    console.error("[Home] levels fetch failed", levelsError);
    return { totalPoints, currentLevel: null };
  }

  const currentLevel =
    (levels || []).find(l => totalPoints >= l.minPoints && totalPoints <= l.maxPoints) ||
    (levels || [])[levels?.length - 1] ||
    null;

  return { totalPoints, currentLevel };
}

function renderIdentity(profile, level, showWelcome = true) {
if (showWelcome) {
  qs('welcomeText').textContent = `Welcome, ${profile.firstName || 'Student'}!`;
}
const avatarImg = document.getElementById("avatarImg");
if (avatarImg) {
  avatarImg.src = resolveAvatarSrc(profile);
}
qs('levelBadgeImg').src = level.badge;

  const pct = Math.min(
    100,
    Math.round(
((profile.points - level.minPoints) /
  (level.maxPoints - level.minPoints)) *
  100
    )
  );

  qs('progressFill').style.width = `${pct}%`;
  qs('progressText').textContent = `${profile.points} XP`;
  qs('progressPercent').textContent = `${pct}% complete`;
}

function getUserLabel(user) {
  const first = String(user.firstName || "").trim();
  const last = String(user.lastName || "").trim();
  const name = last && first ? `${last}, ${first}` : (last || first);
  return name || "Student";
}

function closeAvatarMenu() {
  const menu = qs("avatarMenu");
  const button = qs("avatarSwitcher");
  if (!menu || !button) return;
  menu.hidden = true;
  button.setAttribute("aria-expanded", "false");
}

function syncParentViewerSelector(activeId) {
  const select = qs("parentStudentSelect");
  if (!select) return;
  const value = String(activeId || "");
  if (value && select.value !== value) select.value = value;
  updateParentProgressState({ hasSelection: !!value });
}

function initParentViewerSelector(users, activeProfile, viewerUserId, studioId) {
  const row = qs("parentViewerRow");
  const select = qs("parentStudentSelect");
  if (!row || !select) return;

  if (!isParentReadOnly || !Array.isArray(users)) {
    row.style.display = "none";
    return;
  }

  const studentProfiles = users.filter(user => hasRole(user, "student"));
  if (studentProfiles.length === 0) {
    select.innerHTML = "";
    const option = document.createElement("option");
    option.value = "";
    option.textContent = "No students found";
    select.appendChild(option);
    select.disabled = true;
    row.style.display = "";
    updateParentProgressState({ hasSelection: false });
    return;
  }

  select.innerHTML = "";
  const key = getParentSelectionKey(studioId, viewerUserId);
  const stored = key ? localStorage.getItem(key) : null;
  if (studentProfiles.length > 1 && !stored) {
    const placeholder = document.createElement("option");
    placeholder.value = "";
    placeholder.textContent = "Select a student";
    select.appendChild(placeholder);
  }
  studentProfiles.forEach(user => {
    const option = document.createElement("option");
    option.value = user.id;
    option.textContent = getUserLabel(user);
    select.appendChild(option);
  });

  const firstStudentId = studentProfiles[0]?.id;
  const activeId = stored || activeProfile?.id || firstStudentId;
  if (activeId && !(users.length > 1 && !stored)) select.value = activeId;
  select.disabled = studentProfiles.length <= 1;
  row.style.display = "";

  select.onchange = async () => {
    if (!select.value) return;
    const nextUser = studentProfiles.find(u => String(u.id) === String(select.value));
    if (nextUser) {
      persistParentSelection(viewerUserId, studioId, nextUser.id);
      updateParentProgressState({ hasSelection: true });
      await switchUser(nextUser);
    }
  };
}

function applyParentReadOnlyUI() {
  const notice = qs("parentReadOnlyNotice");
  const controls = qs("studentLoggingControls");
  const staffMount = qs("staffQuickLogMount");
  const staffRibbon = qs("staffChallengesRibbon");
  if (notice) notice.style.display = isParentReadOnly ? "block" : "none";
  if (controls) controls.style.display = isParentReadOnly ? "none" : "";
  if (staffMount) staffMount.style.display = isParentReadOnly ? "none" : "";
  if (staffRibbon) staffRibbon.style.display = isParentReadOnly ? "none" : "";

  const modalLogPractice = qs("modalLogPractice");
  const modalLogOther = qs("modalLogOther");
  if (modalLogPractice) {
    modalLogPractice.disabled = isParentReadOnly;
    modalLogPractice.style.display = isParentReadOnly ? "none" : "";
  }
  if (modalLogOther) {
    modalLogOther.disabled = isParentReadOnly;
    modalLogOther.style.display = isParentReadOnly ? "none" : "";
  }
}

function updateParentProgressState({ hasSelection }) {
  const progressWrap = qs("identityProgress");
  const notice = qs("parentProgressNotice");
  if (!isParentReadOnly) {
    if (progressWrap) progressWrap.style.display = "";
    if (notice) notice.style.display = "none";
    return;
  }
  if (progressWrap) progressWrap.style.display = hasSelection ? "" : "none";
  if (notice) notice.style.display = hasSelection ? "none" : "block";
}

function setParentNotice(text) {
  const notice = qs("parentProgressNotice");
  if (notice && typeof text === "string") {
    notice.textContent = text;
  }
}

function setHomeMode(mode) {
  const studentEls = document.querySelectorAll(".student-only");
  const staffEls = document.querySelectorAll(".staff-only");
  const header = qs("homeHeader");
  const staffMount = qs("staffQuickLogMount");
  const parentViewer = qs("parentViewerRow");

  if (mode === "parent") {
    studentEls.forEach(el => el.style.display = "none");
    staffEls.forEach(el => el.style.display = "none");
    if (header) header.style.display = "none";
    if (staffMount) staffMount.style.display = "none";
    if (parentViewer) parentViewer.style.display = "";
    updateParentProgressState({ hasSelection: false });
    return;
  }

  studentEls.forEach(el => el.style.display = "");
  staffEls.forEach(el => el.style.display = isStaffUser ? "" : "none");
  if (header) header.style.display = "";
  if (staffMount) staffMount.style.display = isStaffUser ? "" : "none";
  if (parentViewer) parentViewer.style.display = "none";
}

async function refreshHomeForUser(profile) {
  const levelRow = await loadLevel(profile.level || 1);
  if (!levelRow) return;
  renderIdentity(profile, levelRow);
}

async function switchUser(user) {
  if (!user || !user.id) return;
  if (currentProfile?.id === user.id) {
    closeAvatarMenu();
    return;
  }

  const ctx = await safeViewerContext();
  const targetId = String(user.id);

  if (ctx?.mode === "parent") {
    persistParentSelection(ctx.viewerUserId, ctx.studioId, user.id);
  } else if (ctx?.viewerUserId) {
    persistLastActiveStudent(ctx.viewerUserId, ctx.studioId, user.id);
  }

  const switchingToStudent = hasRole(user, "student");
  if (switchingToStudent) {
    setAdminModeEnabled(false);
  }

  setActiveProfileId(targetId);
  localStorage.setItem("loggedInUser", JSON.stringify(user));
  persistActiveStudentId(targetId);
  console.debug(`[SwitchUser] switched to ${targetId} and reloading`);
  closeAvatarMenu();
  location.href = "index.html";
}

function initAvatarSwitcher(users) {
  const menu = qs("avatarMenu");
  if (!menu) return;
  renderAccountProfileList(menu, users, {
    activeProfileId: currentProfile?.id,
    onSelect: async (profile) => {
      await switchUser(profile);
    },
    variant: "menu",
    emptyState: "No account profiles found."
  });
  menu.hidden = true;
}

async function init() {
  document.body.classList.add(HOME_INIT_LOADING_CLASS);
  if (typeof window.ingestSupabaseSessionFromHash === "function") {
    await window.ingestSupabaseSessionFromHash(supabase);
  }

  // 🔒 Hard auth gate
  const { data: sessionData } = await supabase.auth.getSession();
  if (!sessionData?.session) {
    window.location.href = loginRedirectUrl();
    return;
  }
  const sessionExists = Boolean(sessionData?.session);
  const urlToken = new URLSearchParams(window.location.search).get("token");
  const pendingInviteToken = urlToken || localStorage.getItem("pendingInviteToken");
  const tokenExists = Boolean(pendingInviteToken);
  const needsFinishSetup = localStorage.getItem("needsFinishSetup") === "1";

  let viewerContext = await getViewerContext();
  homeIdentityContext = {
    authUserId: viewerContext?.viewerUserId || null,
    viewerUserId: viewerContext?.viewerUserId || null,
    activeStudentId: getActiveStudentIdForContext(viewerContext),
    accountIsStaff: false,
    activeIsStudent: false,
    adminMode: getAdminModeEnabled(),
    showStaffUI: false,
    mode: viewerContext?.mode || "unknown",
    studioId: viewerContext?.studioId || null
  };
  console.log("[Home][IdentityContext] page-load", homeIdentityContext);
  console.log("[Identity] viewer context", viewerContext);
  console.log("[AuthGuard] session exists:", sessionExists, "token exists:", tokenExists, "needsFinishSetup:", needsFinishSetup, "userRow exists:", Boolean(viewerContext?.userRow), "studio membership exists:", Boolean(viewerContext?.studioId));
  const isOnIndexPage = /(^|[\\/])index\.html$/i.test(window.location.pathname || "");
  if (!viewerContext?.viewerUserId) {
    window.location.href = loginRedirectUrl();
    return;
  }

  const viewerContextProfile = normalizeUserRow(viewerContext.userRow);
  if (!viewerContext?.userRow) {
    revealHomeAfterIdentity();
    alert("We couldn't load your profile. Try refreshing or contact support.");
    if (!isOnIndexPage) window.location.href = "index.html";
    return;
  }
  if (viewerContext.mode === "unknown") {
    if (tokenExists || needsFinishSetup) {
      alert("Please finish setup before continuing.");
      const target = tokenExists
        ? `finish-setup.html?token=${encodeURIComponent(pendingInviteToken)}`
        : "finish-setup.html";
      window.location.href = target;
      return;
    }
    revealHomeAfterIdentity();
    alert("We couldn't load your profile. Try refreshing or contact support.");
    if (!isOnIndexPage) window.location.href = "index.html";
    return;
  }

  if (typeof window.AA_applyViewMode === "function") {
    const viewResult = window.AA_applyViewMode();
    updateViewModeToggle(viewResult);
  }
  initViewModeToggle();

  maybeShowSwitchStudentTip(viewerContext.mode, false);

  const authUserId = viewerContext.viewerUserId;

  if (viewerContext.mode === "parent") {
    isParentReadOnly = true;
    setHomeMode("parent");
    const linkedStudents = await loadLinkedStudentsForParent(authUserId, viewerContext.studioId);
    const savedStudentId = getLastActiveStudent(authUserId, viewerContext.studioId);
    const savedStudent = savedStudentId && linkedStudents.find(s => String(s.id) === String(savedStudentId));
    if (savedStudent && String(getActiveProfileId() || "") !== String(savedStudent.id)) {
      persistParentSelection(authUserId, viewerContext.studioId, savedStudent.id);
      setActiveProfileId(savedStudent.id);
      window.location.href = "index.html";
      return;
    }
    if (savedStudentId && !savedStudent) {
      clearLastActiveStudent(authUserId, viewerContext.studioId);
    }
    if (linkedStudents.length === 1) {
      const studentId = linkedStudents[0].id;
      if (studentId && String(getActiveProfileId() || "") !== String(studentId)) {
        persistParentSelection(authUserId, viewerContext.studioId, studentId);
        setActiveProfileId(studentId);
        window.location.href = "index.html";
        return;
      }
    } else if (linkedStudents.length === 0) {
      setParentNotice("No students yet. Go to Family to add one.");
      initParentViewerSelector([], null, authUserId, viewerContext.studioId);
      revealHomeAfterIdentity();
      return;
    } else {
      const storedKey = getParentSelectionKey(viewerContext.studioId, authUserId);
      const stored = storedKey ? localStorage.getItem(storedKey) : null;
      const storedExists = stored && linkedStudents.some(s => String(s.id) === String(stored));
      if (storedExists && String(getActiveProfileId() || "") !== String(stored)) {
        persistActiveStudentId(stored);
        setActiveProfileId(stored);
        window.location.href = "index.html";
        return;
      }
      if (stored && !storedExists && storedKey) {
        localStorage.removeItem(storedKey);
      }
      persistActiveStudentId(null);
      setParentNotice("Select a student to continue.");
      initParentViewerSelector(linkedStudents, null, authUserId, viewerContext.studioId);
      revealHomeAfterIdentity();
      return;
    }
  }

  if (viewerContext.mode === "student") {
    await renderActiveStudentHeader({
      useHomeHeader: true,
      nameTemplate: (student) => `Welcome, ${student?.firstName || "Student"}!`,
      skipMenu: true
    });
  }

  const activeProfileId = getStoredActiveStudentId() || viewerContext.activeProfileId || null;
  let authProfile = null;
  if (activeProfileId) {
    const { data, error: authErr } = await supabase
      .from('users')
      .select('*')
      .eq('id', activeProfileId)
      .single();
    if (!authErr && data) {
      authProfile = data;
      console.log('[Identity] loaded profile id', authProfile.id);
      const name = authProfile.firstName || 'Student';
      qs('welcomeText').textContent = `Welcome, ${name}!`;
    }
  }

  const activeStudioId = viewerContext.studioId || localStorage.getItem("activeStudioId");
  console.log('[Home] activeStudioId', activeStudioId);
  if (authUserId && activeStudioId) {
    const { data: membershipRow } = await supabase
      .from("studio_members")
      .select("roles")
      .eq("user_id", authUserId)
      .eq("studio_id", activeStudioId)
      .maybeSingle();
    const accountRolesRaw = Array.isArray(membershipRow?.roles) ? membershipRow.roles : [membershipRow?.roles].filter(Boolean);
    const accountRoles = accountRolesRaw.map((role) => String(role || "").toLowerCase());
    const accountIsStaff = accountRoles.includes("admin") || accountRoles.includes("teacher");
    const activeUserRow = currentProfile || authProfile || null;
    const activeRoleList = Array.isArray(activeUserRow?.roles)
      ? activeUserRow.roles.map((role) => String(role || "").toLowerCase())
      : [];
    const activeIsStudent = Boolean(activeUserRow?.parent_uuid) || activeRoleList.includes("student");
    const adminMode = getAdminModeEnabled();
    const showStaffUI = accountIsStaff && (!activeIsStudent || adminMode);
    homeShowStaffUI = showStaffUI;
    homeIdentityContext = {
      authUserId,
      viewerUserId: viewerContext?.viewerUserId || authUserId,
      activeStudentId: String(activeUserRow?.id || getActiveStudentIdForContext(viewerContext) || "").trim() || null,
      accountIsStaff,
      activeIsStudent,
      adminMode,
      showStaffUI,
      mode: viewerContext?.mode || "unknown",
      studioId: activeStudioId
    };
    console.log("[UI] accountIsStaff", accountIsStaff, "activeIsStudent", activeIsStudent, "adminMode", adminMode, "showStaffUI", showStaffUI);
    console.log("[Home][IdentityContext] resolved", resolveHomeIdentityContext(viewerContext));
    document.body.classList.remove("view-teacher", "view-student");
    document.body.classList.add(showStaffUI ? "view-teacher" : "view-student");

    const { data: studioRow } = await supabase
      .from('studios')
      .select('name')
      .eq('id', activeStudioId)
      .maybeSingle();

    const isStaff = showStaffUI;
    const isOwner = !!viewerContext.isOwner;
    const isAdmin = !!viewerContext.isAdmin;
    const isTeacher = !!viewerContext.isTeacher;
    const showStaffRoleBadges = showStaffUI;
    const showAdminBadge = showStaffRoleBadges && (isOwner || isAdmin);
    const showTeacherBadge = showStaffRoleBadges && !showAdminBadge && isTeacher;
    isStaffUser = showStaffUI;
    isParentReadOnly = viewerContext.mode === "parent";
    if (isParentReadOnly) document.body.classList.add('is-parent');
    if (showStaffUI) document.body.classList.add('is-staff');
    else document.body.classList.remove('is-staff');
    if (isOwner) document.body.classList.add('is-owner');
    else document.body.classList.remove('is-owner');
    if (showAdminBadge) document.body.classList.add('is-admin');
    else document.body.classList.remove('is-admin');
    console.log('[Home] active profile roles', viewerContext.viewerRoles, 'account roles', viewerContext.accountRoles);
    console.log('[Home] isStaff', isStaff);

    const studioNameLine = document.getElementById('studioNameLine');
    if (studioNameLine) {
      studioNameLine.textContent = `Studio: ${studioRow?.name || '—'}`;
    }

    document.querySelectorAll('.student-only').forEach(el => {
      el.style.display = showStaffUI ? 'none' : '';
    });
    document.querySelectorAll('.staff-only').forEach(el => {
      el.style.display = showStaffUI ? '' : 'none';
    });
    document.querySelectorAll('.admin-only').forEach(el => {
      el.style.display = showAdminBadge ? '' : 'none';
    });

    const roleBadge = document.getElementById('roleBadge');
    if (roleBadge) {
      if (isOwner) {
        roleBadge.textContent = "OWNER";
        roleBadge.style.display = "";
      } else if (showAdminBadge) {
        roleBadge.textContent = "ADMIN";
        roleBadge.style.display = "";
      } else if (showTeacherBadge) {
        roleBadge.textContent = "TEACHER";
        roleBadge.style.display = "";
      } else {
        roleBadge.textContent = "";
        roleBadge.style.display = "none";
      }
    }

    const hideMyPoints = showStaffUI;
    console.log('[UI] hideMyPoints', hideMyPoints);
    const myPointsLink = document.getElementById('myPointsLink');
    if (myPointsLink) {
      myPointsLink.style.display = hideMyPoints ? 'none' : '';
    }

    if (showStaffUI) {
      renderStaffChallengesRibbon();
      renderStaffQuickLogShell();
      const studioId = activeStudioId;
      const staffContext = {
        ...viewerContext,
        isAdmin,
        isTeacher
      };
      console.log("[ChallengesUI] init studioId =", studioId);
      await initStaffChallengesUI({
        studioId,
        user: {
          id: authUserId,
          isAdmin,
          isTeacher
        },
        roles: viewerContext.viewerRoles || [],
        showToast
      });
      await initStaffQuickLog({
        authUserId,
        studioId: activeStudioId,
        roles: viewerContext.viewerRoles
      });
      await updateStaffApprovalNotice(staffContext);
    }
    applyParentReadOnlyUI();
  }

  const ensuredProfile = await ensureUserRow();
  if (ensuredProfile) {
    localStorage.setItem("loggedInUser", JSON.stringify(ensuredProfile));
  }
  const activeProfileIdCurrent = getStoredActiveStudentId() || getActiveProfileId() || null;

  const routeResult = await ensureStudioContextAndRoute({ redirectHome: false });
  if (routeResult?.redirected) return;


  // 🔁 Active student must already be selected
  const raw = localStorage.getItem("loggedInUser");
  if (!raw && !ensuredProfile && !viewerContextProfile) {
    // Logged in parent, but no student selected yet
    window.location.href = "settings.html";
    return;
  }

  const storedProfile = raw ? safeParse(raw) : null;
  const profile = storedProfile || ensuredProfile || viewerContextProfile;
  currentProfile = profile;

  if (viewerContext?.mode === "student") {
    clearParentSelection(viewerContext.viewerUserId, viewerContext.studioId);
    if (activeProfileIdCurrent && String(profile?.id) !== String(activeProfileIdCurrent)) {
      const { data: viewerProfile } = await supabase
        .from("users")
        .select("*")
        .eq("id", activeProfileIdCurrent)
        .single();
      if (viewerProfile) {
        currentProfile = viewerProfile;
        localStorage.setItem("loggedInUser", JSON.stringify(viewerProfile));
      }
    }
    persistActiveStudentId(currentProfile?.id || activeProfileIdCurrent || null);
  }

  availableUsers = await getAccountProfiles(viewerContext, {
    includeInactive: true,
    fallbackProfile: ensuredProfile || profile
  });
  const studioQuickLogSettings = await loadStudioQuickLogTypeSettings();
  renderStudentQuickLogTypeChips(getEnabledStudentQuickLogTypes(studioQuickLogSettings));
  initAvatarSwitcher(availableUsers);
  setAvailableProfilesContext(availableUsers);
  const resolvedActiveId = currentProfile?.id || activeProfileIdCurrent || null;
  persistActiveStudentId(resolvedActiveId);
  setActiveProfileIdContext(resolvedActiveId);
  renderHomeAvatarFromActiveProfile();
  bindAvatarMenu();
  maybeShowSwitchStudentTip(viewerContext.mode, availableUsers.length > 1);

  if (viewerContext?.mode === "parent") {
    const studentProfiles = availableUsers.filter(user => hasRole(user, "student"));
    const storedKey = getParentSelectionKey(viewerContext.studioId, viewerContext.viewerUserId);
    const stored = storedKey ? localStorage.getItem(storedKey) : null;
    if (!stored && studentProfiles.length === 1) {
      persistParentSelection(viewerContext.viewerUserId, viewerContext.studioId, studentProfiles[0].id);
    }
  }

initParentViewerSelector(availableUsers, profile, viewerContext.viewerUserId, viewerContext.studioId);

  if (viewerContext?.mode === "parent") {
    const storedKey = getParentSelectionKey(viewerContext.studioId, viewerContext.viewerUserId);
    if (storedKey && !localStorage.getItem(storedKey) && !getStoredActiveStudentId()) {
      console.log("[Identity] parent requires student selection", {
        viewerUserId: viewerContext.viewerUserId,
        studioId: viewerContext.studioId
      });
      updateParentProgressState({ hasSelection: false });
      revealHomeAfterIdentity();
      return;
    }
  }

  await refreshActiveStudentData({ fallbackProfile: profile });
  if (!isStaffUser && !isParentReadOnly) {
    const studentId = String(getStoredActiveStudentId() || currentProfile?.id || activeProfileIdCurrent || "").trim();
    console.log("[StudentChallengesUI] init studentId =", studentId);
    await initStudentChallengesUI({
      studioId: viewerContext.studioId || localStorage.getItem("activeStudioId"),
      studentId,
      roles: viewerContext.viewerRoles || [],
      showToast
    });
    await initStudentLogActions();
  }
  revealHomeAfterIdentity();
  scheduleStudentTutorial({
    viewerContext,
    profileId: resolvedActiveId,
    profile: currentProfile || profile
  });
  scheduleTeacherAdminTutorial({ viewerContext });
}

document.addEventListener('DOMContentLoaded', () => {
  init().catch((error) => {
    console.error("[Home] init failed", error);
    revealHomeAfterIdentity();
  });
});

function getTodayString() {
  return getLocalDateString(new Date());
}

function getLocalDateString(date) {
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, "0");
  const day = String(date.getDate()).padStart(2, "0");
  return `${year}-${month}-${day}`;
}

function getBadgeImagePath(slug) {
  const raw = String(slug || "").trim();
  if (!raw) return "/images/badges/demo.png";
  const BADGE_FILE_ALIASES = {
    comeback_kid: "comeback-kid",
    book_finisher: "book finisher",
    memory_master: "memory_master_",
    participation_regular: "participation_community",
    member_first: "longevity_member",
    member_musician: "longevity_musician",
    member_veteran: "longevity_veteran",
    member_legacy: "longevity_legacy"
  };
  const fileSlug = BADGE_FILE_ALIASES[raw] || raw;
  return `/images/badges/${fileSlug}.png`;
}

function buildSingleDateSelectorMarkup({ inputId, prefix }) {
  return `
    <input id="${inputId}" type="hidden" value="${getTodayString()}">
    <button id="${prefix}DateToggle" class="blue-button staff-calendar-toggle" type="button">Select date</button>
    <div id="${prefix}DatePanel" class="staff-calendar-panel" hidden>
      <div class="calendar">
        <div class="calendar-header">
          <button id="${prefix}CalPrev" class="calendar-nav" type="button">‹</button>
          <div id="${prefix}CalMonthLabel" class="calendar-title"></div>
          <button id="${prefix}CalNext" class="calendar-nav" type="button">›</button>
        </div>
        <div class="calendar-weekdays">
          <span>Sun</span><span>Mon</span><span>Tue</span><span>Wed</span><span>Thu</span><span>Fri</span><span>Sat</span>
        </div>
        <div id="${prefix}Calendar" class="calendar-grid"></div>
      </div>
    </div>
  `;
}

function initSingleDateSelector({ inputId, prefix }) {
  const hiddenInput = qs(inputId);
  const toggleBtn = qs(`${prefix}DateToggle`);
  const panel = qs(`${prefix}DatePanel`);
  const calendarEl = qs(`${prefix}Calendar`);
  const monthLabel = qs(`${prefix}CalMonthLabel`);
  const prevBtn = qs(`${prefix}CalPrev`);
  const nextBtn = qs(`${prefix}CalNext`);
  if (!hiddenInput || !toggleBtn || !panel || !calendarEl || !monthLabel || !prevBtn || !nextBtn) return;

  const today = new Date();
  const todayEnd = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);
  const todayStart = new Date(today.getFullYear(), today.getMonth(), today.getDate());
  const monthNames = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];

  let selectedDate = String(hiddenInput.value || "").trim() || getTodayString();
  let view = { year: today.getFullYear(), month: today.getMonth() };

  const updateToggleLabel = () => {
    const selected = selectedDate ? new Date(`${selectedDate}T00:00:00`) : null;
    const formatted = selected && !Number.isNaN(selected.getTime())
      ? selected.toLocaleDateString(undefined, { month: "short", day: "numeric", year: "numeric" })
      : "Select date";
    toggleBtn.textContent = `Date: ${formatted}`;
  };

  const renderCalendar = () => {
    calendarEl.innerHTML = "";
    const firstDay = new Date(view.year, view.month, 1);
    const startDay = firstDay.getDay();
    const gridStart = new Date(view.year, view.month, 1 - startDay);
    monthLabel.textContent = `${monthNames[view.month]} ${view.year}`;

    const monthEnd = new Date(view.year, view.month + 1, 0);
    prevBtn.disabled = false;
    nextBtn.disabled = monthEnd >= todayEnd;

    for (let i = 0; i < 42; i++) {
      const cellDate = new Date(gridStart);
      cellDate.setDate(gridStart.getDate() + i);
      const dateStr = getLocalDateString(cellDate);
      const inMonth = cellDate.getMonth() === view.month;
      const inRange = cellDate <= todayEnd;

      const cell = document.createElement("button");
      cell.type = "button";
      cell.className = "calendar-day";
      cell.dataset.date = dateStr;
      cell.textContent = String(cellDate.getDate());

      if (!inMonth) cell.classList.add("outside");
      if (!inRange) {
        cell.classList.add("disabled");
        cell.disabled = true;
      } else {
        if (dateStr === selectedDate) cell.classList.add("selected");
        cell.addEventListener("click", () => {
          selectedDate = dateStr;
          hiddenInput.value = dateStr;
          updateToggleLabel();
          renderCalendar();
          panel.setAttribute("hidden", "");
        });
      }

      calendarEl.appendChild(cell);
    }
  };

  const selectedAsDate = new Date(`${selectedDate}T00:00:00`);
  if (!Number.isNaN(selectedAsDate.getTime()) && selectedAsDate <= todayEnd) {
    view = { year: selectedAsDate.getFullYear(), month: selectedAsDate.getMonth() };
  } else {
    selectedDate = getTodayString();
    hiddenInput.value = selectedDate;
  }

  toggleBtn.addEventListener("click", () => {
    const isOpen = !panel.hasAttribute("hidden");
    if (isOpen) panel.setAttribute("hidden", "");
    else panel.removeAttribute("hidden");
  });

  prevBtn.addEventListener("click", () => {
    const prevMonth = new Date(view.year, view.month - 1, 1);
    view = { year: prevMonth.getFullYear(), month: prevMonth.getMonth() };
    renderCalendar();
  });

  nextBtn.addEventListener("click", () => {
    const nextMonth = new Date(view.year, view.month + 1, 1);
    if (nextMonth <= todayStart) {
      view = { year: nextMonth.getFullYear(), month: nextMonth.getMonth() };
      renderCalendar();
    }
  });

  updateToggleLabel();
  renderCalendar();
}

function showToast(message) {
  const toast = qs("toast");
  if (!toast) return;
  toast.textContent = message;
  toast.classList.add("show");
  clearTimeout(toast._hideTimer);
  toast._hideTimer = setTimeout(() => {
    toast.classList.remove("show");
  }, 2200);
}

function closeStudentModal({ completed = false } = {}) {
  const overlay = qs("studentLogModalOverlay");
  if (overlay) overlay.style.display = "none";
  if (!completed) dispatchTutorialAction("aa:tutorial-student-modal-dismissed");
}

function resetModalScroll(modalEl) {
  if (!modalEl) return;
  const scroller =
    modalEl.querySelector(".modal-content, .modal-body, .dialog-body, .sheet-body") ||
    modalEl;
  scroller.scrollTop = 0;
}

function openStudentModal({ title, bodyHtml, submitLabel, onSubmit }) {
  const overlay = qs("studentLogModalOverlay");
  const titleEl = qs("studentLogModalTitle");
  const bodyEl = qs("studentLogModalBody");
  const submitBtn = qs("studentLogModalSubmit");
  const cancelBtn = qs("studentLogModalCancel");
  const closeBtn = qs("studentLogModalClose");
  if (!overlay || !titleEl || !bodyEl || !submitBtn || !cancelBtn || !closeBtn) return;

  titleEl.textContent = title;
  bodyEl.innerHTML = bodyHtml;
  submitBtn.textContent = submitLabel || "Submit";
  submitBtn.onclick = async (e) => {
    e.preventDefault();
    await onSubmit();
  };
  cancelBtn.onclick = (e) => {
    e.preventDefault();
    closeStudentModal();
  };
  closeBtn.onclick = (e) => {
    e.preventDefault();
    closeStudentModal();
  };
  overlay.onclick = (e) => {
    if (e.target === overlay) closeStudentModal();
  };
  overlay.style.display = "flex";
  resetModalScroll(overlay.querySelector(".modal"));
}

function setPracticeButtonState(button, logged) {
  if (!button) return;
  if (logged) {
    button.disabled = true;
    button.innerHTML = "✅ <span>You already logged today&apos;s practice</span>";
  } else {
    button.disabled = false;
    button.innerHTML = "✅ <span>Log Today&apos;s Practice</span> <span class=\"muted\">(+5 XP)</span>";
  }
}

async function checkPracticeLoggedToday(userId) {
  const today = getTodayString();
  const { data, error } = await supabase
    .from("logs")
    .select("id")
    .eq("userId", userId)
    .eq("category", "practice")
    .eq("date", today)
    .limit(1);
  if (error) {
    console.error("Failed to check practice logs", error);
    return false;
  }
  return Array.isArray(data) && data.length > 0;
}

async function applyApprovedRecalc(userId, recalculatedResult = null) {
  const result = recalculatedResult || await calculateXpAndLevel(userId);
  if (!result || !currentProfile || currentProfile.id !== userId) return;
  const nextLevel = result.currentLevel?.name
    ? result.currentLevel
    : await loadLevel(result.currentLevel?.id || currentProfile.level || 1);
  currentProfile = {
    ...currentProfile,
    points: result.totalPoints,
    level: nextLevel?.id || currentProfile.level
  };
  currentLevelRow = nextLevel;
  if (currentLevelRow) renderIdentity(currentProfile, currentLevelRow);
  updatePendingProgressFill();
}

async function loadPendingPoints(userId) {
  if (!userId) return;
  console.log("[Home] fetching pending logs for userId", userId);
  const { data, error } = await supabase
    .from("logs")
    .select("points")
    .eq("userId", userId)
    .eq("status", "pending");
  if (error) {
    console.error("Failed to load pending logs", error);
    pendingPointsTotal = 0;
    return;
  }
  if (!data || data.length === 0) {
    logEmptyFetch("pending logs", userId);
  }
  pendingPointsTotal = (data || []).reduce((sum, log) => sum + (log.points || 0), 0);
}

async function loadPendingReviewCount(viewerContext) {
  if (!viewerContext?.studioId) return 0;
  const isAdmin = !!viewerContext?.isAdmin;
  const isTeacher = !!viewerContext?.isTeacher;
  if (!isAdmin && !isTeacher) return 0;

  if (isAdmin) {
    const { data, error } = await supabase
      .from("logs")
      .select("id")
      .eq("studio_id", viewerContext.studioId)
      .eq("status", "pending");
    if (error) {
      console.error("[Home] Failed to load pending review logs for admin", error);
      return 0;
    }
    return Array.isArray(data) ? data.length : 0;
  }

  const { data: usersData, error: usersError } = await supabase
    .from("users")
    .select("id, teacherIds")
    .eq("studio_id", viewerContext.studioId);
  if (usersError) {
    console.error("[Home] Failed to load teacher students for review count", usersError);
    return 0;
  }

  const myStudentIds = (usersData || [])
    .filter(u => Array.isArray(u.teacherIds) && u.teacherIds.map(String).includes(String(viewerContext.viewerUserId)))
    .map(u => String(u.id));
  if (!myStudentIds.length) return 0;

  const { data: logsData, error: logsError } = await supabase
    .from("logs")
    .select("id")
    .eq("studio_id", viewerContext.studioId)
    .eq("status", "pending")
    .in("userId", myStudentIds);
  if (logsError) {
    console.error("[Home] Failed to load teacher pending review logs", logsError);
    return 0;
  }
  return Array.isArray(logsData) ? logsData.length : 0;
}

async function updateStaffApprovalNotice(viewerContext) {
  const notice = qs("staffApprovalNotice");
  if (!notice) return;

  if (!viewerContext?.isAdmin && !viewerContext?.isTeacher) {
    notice.style.display = "none";
    return;
  }

  const pendingCount = await loadPendingReviewCount(viewerContext);
  if (!pendingCount) {
    notice.style.display = "none";
    return;
  }

  notice.textContent = pendingCount === 1
    ? "1 log needs approval. Tap to review."
    : `${pendingCount} logs need approval. Tap to review.`;
  notice.onclick = () => {
    window.location.href = "review-logs.html?filter=pending";
  };
  notice.style.display = "";
}

function updatePendingProgressFill() {
  const pendingEl = qs("progressFillPending");
  if (!pendingEl || !currentProfile || !currentLevelRow) return;
  const range = Math.max(1, currentLevelRow.maxPoints - currentLevelRow.minPoints);
  const approvedPct = Math.min(100, Math.round(((currentProfile.points - currentLevelRow.minPoints) / range) * 100));
  const pendingPct = Math.max(0, Math.round((pendingPointsTotal / range) * 100));
  const combined = Math.min(100, approvedPct + pendingPct);
  pendingEl.style.width = `${combined}%`;
}

async function loadEarnedBadges(userId, studioId) {
  if (!userId || !studioId) return [];

  const joined = await supabase
    .from("user_badges")
    .select("badge_slug,earned_at,stars,badge_definitions(name,family,tier,sort_order)")
    .eq("user_id", userId)
    .eq("studio_id", studioId);

  if (!joined.error) {
    const rows = joined.data || [];
    rows.sort((a, b) => {
      const af = a?.badge_definitions?.family || "";
      const bf = b?.badge_definitions?.family || "";
      if (af !== bf) return af.localeCompare(bf);
      const at = Number(a?.badge_definitions?.tier || 0);
      const bt = Number(b?.badge_definitions?.tier || 0);
      if (at !== bt) return at - bt;
      return String(a.badge_slug || "").localeCompare(String(b.badge_slug || ""));
    });
    return rows;
  }

  const base = await supabase
    .from("user_badges")
    .select("badge_slug,earned_at,stars")
    .eq("user_id", userId)
    .eq("studio_id", studioId);
  if (base.error) {
    console.error("[Home] Failed loading user badges", base.error);
    return [];
  }

  const slugs = Array.from(new Set((base.data || []).map((r) => String(r.badge_slug || "")).filter(Boolean)));
  if (!slugs.length) return [];
  const defs = await supabase
    .from("badge_definitions")
    .select("slug,name,family,tier,sort_order")
    .in("slug", slugs);
  if (defs.error) {
    console.error("[Home] Failed loading badge definitions", defs.error);
    return (base.data || []).map((row) => ({ ...row, badge_definitions: null }));
  }

  const defMap = new Map((defs.data || []).map((d) => [String(d.slug), d]));
  return (base.data || [])
    .map((row) => ({ ...row, badge_definitions: defMap.get(String(row.badge_slug || "")) || null }))
    .sort((a, b) => {
      const af = a?.badge_definitions?.family || "";
      const bf = b?.badge_definitions?.family || "";
      if (af !== bf) return af.localeCompare(bf);
      const at = Number(a?.badge_definitions?.tier || 0);
      const bt = Number(b?.badge_definitions?.tier || 0);
      if (at !== bt) return at - bt;
      return String(a.badge_slug || "").localeCompare(String(b.badge_slug || ""));
    });
}

async function recomputeBadgesForStudent(userId, studioId) {
  const targetUserId = String(userId || "").trim();
  const targetStudioId = String(studioId || "").trim();
  if (!targetUserId || !targetStudioId) {
    throw new Error("Missing user or studio id for badge recompute.");
  }

  const { data, error } = await supabase.rpc("recompute_badges_for_student", {
    p_studio_id: targetStudioId,
    p_user_id: targetUserId
  });
  if (error) throw error;
  console.log("[Badges][Recompute] student", data);
  return data;
}

async function recomputeBadgesForStudio(studioId) {
  const targetStudioId = String(studioId || "").trim();
  if (!targetStudioId) {
    throw new Error("Missing studio id for badge recompute.");
  }

  const { data, error } = await supabase.rpc("recompute_badges_for_studio", {
    p_studio_id: targetStudioId
  });
  if (error) throw error;
  console.log("[Badges][Recompute] studio", data);
  return data;
}

function exposeBadgeRecomputeDebugTools({ userId, studioId } = {}) {
  const targetUserId = String(userId || "").trim();
  const targetStudioId = String(studioId || "").trim();
  window.AA_recalculateBadgesForActiveStudent = async () => {
    const result = await recomputeBadgesForStudent(targetUserId, targetStudioId);
    showToast("Badge recompute complete. Refreshing...");
    window.location.reload();
    return result;
  };
  window.AA_recalculateBadgesForStudio = async () => {
    const result = await recomputeBadgesForStudio(targetStudioId);
    showToast("Studio badge recompute complete. Refreshing...");
    window.location.reload();
    return result;
  };
}

async function maybeRunBadgeRecomputeDebug({ userId, studioId } = {}) {
  const params = new URLSearchParams(window.location.search || "");
  const mode = String(params.get("recalculateBadges") || params.get("recomputeBadges") || "").toLowerCase();
  if (!mode) return null;

  if (mode === "studio" || mode === "all") {
    showToast("Recalculating badges for studio...");
    return recomputeBadgesForStudio(studioId);
  }

  showToast("Recalculating badges...");
  return recomputeBadgesForStudent(userId, studioId);
}

function normalizeBadgeMetricValue(metrics, type) {
  const key = String(type || "").trim();
  const byType = {
    practice_logs: "practiceLogs",
    practice_streak_days: "maxPracticeStreak",
    goals_completed: "goalsCompleted",
    participation_logs: "participationLogs",
    technique_completed: "techniqueCompleted",
    theory_completed: "theoryCompleted",
    festival_participation: "festivalParticipations",
    performance_participation: "performanceParticipations",
    competition_participation: "competitionParticipations",
    teacher_challenges_completed: "teacherChallengesCompleted",
    memorization_points: "memorizationPoints",
    lesson_books_completed: "lessonBooksCompleted",
    total_logs: "totalLogs",
    streak_repairs_used: "streakRepairUsed",
    streak_repair_tokens_unused: "streakRepairTokensUnused"
  };
  if (key === "technique_or_theory_completed") {
    return Number(metrics?.techniqueCompleted || 0) + Number(metrics?.theoryCompleted || 0);
  }
  if (key === "streak_repair_tokens_unused") {
    return Math.max(0, Number(metrics?.streakRepairEarned || 0) - Number(metrics?.streakRepairUsed || 0));
  }
  return Number(metrics?.[byType[key]] || 0);
}

function getClientBadgeProgress(definition, metrics) {
  const criteria = definition?.criteria && typeof definition.criteria === "object" ? definition.criteria : {};
  const type = String(criteria.type || "").trim().toLowerCase();
  if (!type) return { current: 0, required: 1, percent: 0 };

  const booleanProgress = (value) => ({
    current: value ? 1 : 0,
    required: 1,
    percent: value ? 100 : 0
  });

  if (type === "practice_log_before_hour") return booleanProgress(Boolean(metrics?.hasEarlyBird));
  if (type === "practice_log_after_hour") return booleanProgress(Boolean(metrics?.hasNightOwl));
  if (type === "practice_gap_return") return booleanProgress(Boolean(metrics?.hasComebackKid));
  if (type === "distinct_categories_rolling_days") return booleanProgress(Boolean(metrics?.hasMultiTasker));
  if (type === "streak_restart_after_rhythm") return booleanProgress(Boolean(metrics?.hasPowerWeek));
  if (type === "repair_preserved_combo") {
    return booleanProgress(
      Number(metrics?.streakRepairUsed || 0) >= 1 &&
      Number(metrics?.maxPracticeStreak || 0) >= 30 &&
      Boolean(metrics?.repairProtectionSeen)
    );
  }

  if (type === "combined") {
    const requires = Array.isArray(criteria.requires) ? criteria.requires : [];
    const totals = requires.reduce((acc, req) => {
      const required = Math.max(1, Number(req?.min || 1));
      const value = normalizeBadgeMetricValue(metrics, req?.metric);
      acc.current += Math.min(value, required);
      acc.required += required;
      return acc;
    }, { current: 0, required: 0 });
    const required = Math.max(1, totals.required);
    return {
      current: totals.current,
      required,
      percent: Math.max(0, Math.min(100, Math.round((totals.current / required) * 100)))
    };
  }

  if (type === "seasonal_weeks") {
    return { current: 0, required: Math.max(1, Number(criteria.min_weeks || 1)), percent: 0 };
  }

  const required = Math.max(1, Number(criteria.min || 1));
  const value = normalizeBadgeMetricValue(metrics, type);
  return {
    current: Math.min(value, required),
    required,
    percent: Math.max(0, Math.min(100, Math.round((value / required) * 100)))
  };
}

async function loadNextUpBadgeFromClient({ userId, studioId, recentBadge = null, onOpenModal = null, onDebug = null, reason = "api-fallback" } = {}) {
  const recomputeResult = await recomputeBadgesForStudent(userId, studioId);
  const metrics = recomputeResult?.metrics || {};
  const [defsResult, earnedResult] = await Promise.all([
    supabase
      .from("badge_definitions")
      .select("slug,name,family,tier,criteria,is_active")
      .eq("is_active", true),
    supabase
      .from("user_badges")
      .select("badge_slug")
      .eq("studio_id", studioId)
      .eq("user_id", userId)
  ]);
  if (defsResult.error) throw defsResult.error;
  if (earnedResult.error) throw earnedResult.error;

  const earnedSlugs = new Set((earnedResult.data || []).map((row) => String(row.badge_slug || "")));
  const candidates = (defsResult.data || [])
    .filter((definition) => !earnedSlugs.has(String(definition.slug || "")))
    .map((definition) => {
      const progress = getClientBadgeProgress(definition, metrics);
      return {
        slug: String(definition.slug || ""),
        name: String(definition.name || definition.slug || "Badge"),
        family: String(definition.family || ""),
        tier: Number(definition.tier || 0),
        current: progress.current,
        required: progress.required,
        percent: progress.percent
      };
    });

  const nextUp = candidates.sort((a, b) =>
    (b.percent - a.percent) ||
    (b.required - a.required) ||
    a.slug.localeCompare(b.slug)
  )[0] || null;
  const allEarned = candidates.length === 0;
  lastNextUpDebugState = {
    ok: true,
    reason,
    userId,
    studioId,
    nextUp,
    allEarned,
    source: "client-rpc"
  };
  renderNextUpBadge(nextUp, { allEarned, recentBadge, onOpenModal, onDebug });
  return nextUp;
}

function renderNextUpBadge(nextUp, options = {}) {
  const allEarned = Boolean(options?.allEarned);
  const recentBadge = options?.recentBadge || null;
  const normalized = nextUp
    ? normalizeHeroBadge({
        ...nextUp,
        id: nextUp.id || nextUp.slug || "",
        progress_current: nextUp.current,
        progress_required: nextUp.required,
        progressPct: Number(nextUp.percent || 0) / 100,
        image_url: nextUp.image_url || getBadgeImagePath(nextUp.slug)
      })
    : null;

  const heroBadge = selectHeroBadge(normalized ? [normalized] : []);
  renderBadgeHeroHalfPanel({
    badge: heroBadge,
    recentBadge,
    allEarned,
    onOpenModal: typeof options?.onOpenModal === "function" ? options.onOpenModal : null,
    onDebug: typeof options?.onDebug === "function" ? options.onDebug : null
  });
}

function toRecentHeroBadge(row) {
  const slug = String(row?.badge_slug || "").trim();
  const name = String(row?.badge_definitions?.name || slug || "Badge");
  return {
    id: slug,
    slug,
    name,
    image_url: getBadgeImagePath(slug)
  };
}

function getMostRecentBadgeRow(rows) {
  if (!Array.isArray(rows) || !rows.length) return null;
  return rows
    .slice()
    .sort((a, b) => {
      const at = Date.parse(String(a?.earned_at || ""));
      const bt = Date.parse(String(b?.earned_at || ""));
      return (Number.isFinite(bt) ? bt : 0) - (Number.isFinite(at) ? at : 0);
    })[0] || null;
}

async function loadNextUpBadge({ userId, studioId, recentBadge = null, onOpenModal = null, onDebug = null } = {}) {
  if (!userId || !studioId) {
    lastNextUpDebugState = { ok: false, reason: "missing-user-or-studio", userId, studioId, nextUp: null, allEarned: false };
    renderNextUpBadge(null, { recentBadge, onOpenModal, onDebug });
    return null;
  }

  try {
    const authUserId = await getAuthUserId();
    const activeProfileId = getActiveProfileId();
    const nextUpRequestMethod = "POST";
    const nextUpRequestUrl = "/api/badges/next-up";
    const nextUpRequestBody = { studioId, userId };
    console.log("[Home][NextUp Request]", {
      authUserId,
      activeProfileId,
      method: nextUpRequestMethod,
      url: nextUpRequestUrl,
      body: nextUpRequestBody
    });

    const { data: sessionData } = await supabase.auth.getSession();
    const token = sessionData?.session?.access_token || "";
    const response = await fetch(nextUpRequestUrl, {
      method: nextUpRequestMethod,
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {})
      },
      credentials: "include",
      body: JSON.stringify(nextUpRequestBody)
    });
    const responseText = await response.text();
    let payload = {};
    try {
      payload = responseText ? JSON.parse(responseText) : {};
    } catch {
      payload = {};
    }
    if (!response.ok || !payload?.ok) {
      console.error("[Home] Failed loading next-up badge", {
        method: nextUpRequestMethod,
        url: nextUpRequestUrl,
        status: response.status,
        responseBody: responseText,
        error: payload?.error || null
      });
      lastNextUpDebugState = {
        ok: false,
        reason: "api-non-ok",
        status: response.status,
        error: payload?.error || null,
        userId,
        studioId,
        nextUp: null,
        allEarned: false
      };
      try {
        return await loadNextUpBadgeFromClient({
          userId,
          studioId,
          recentBadge,
          onOpenModal,
          onDebug,
          reason: "api-non-ok-fallback"
        });
      } catch (fallbackError) {
        console.error("[Home] Client next-up fallback failed", fallbackError);
        renderNextUpBadge(null, { allEarned: false, recentBadge, onOpenModal, onDebug });
        return null;
      }
    }
    lastNextUpDebugState = {
      ok: true,
      reason: "api-ok",
      status: response.status,
      userId,
      studioId,
      nextUp: payload.nextUp || null,
      allEarned: Boolean(payload?.allEarned)
    };
    renderNextUpBadge(payload.nextUp || null, { allEarned: !!payload?.allEarned, recentBadge, onOpenModal, onDebug });
    return payload.nextUp || null;
  } catch (error) {
    console.error("[Home] Failed loading next-up badge", error);
    lastNextUpDebugState = {
      ok: false,
      reason: "fetch-throw",
      error: String(error?.message || error || "unknown"),
      userId,
      studioId,
      nextUp: null,
      allEarned: false
    };
    try {
      return await loadNextUpBadgeFromClient({
        userId,
        studioId,
        recentBadge,
        onOpenModal,
        onDebug,
        reason: "fetch-throw-fallback"
      });
    } catch (fallbackError) {
      console.error("[Home] Client next-up fallback failed", fallbackError);
      renderNextUpBadge(null, { allEarned: false, recentBadge, onOpenModal, onDebug });
      return null;
    }
  }
}

async function loadStudentCelebrations({ studioId, studentId, viewerContext } = {}) {
  if (!studioId || !studentId || !viewerContext) return;
  if (viewerContext.mode !== "student") return;
  if (String(studentId) !== String(viewerContext.viewerUserId || "")) return;

  const loadKey = `${studioId}:${studentId}`;
  if (studentCelebrationsLoadedFor === loadKey) return;
  studentCelebrationsLoadedFor = loadKey;

  try {
    const { data: sessionData } = await supabase.auth.getSession();
    const token = sessionData?.session?.access_token || "";
    const response = await fetch("/api/badges/student-celebrations", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(token ? { Authorization: `Bearer ${token}` } : {})
      },
      credentials: "include",
      body: JSON.stringify({ studioId })
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok || !payload?.ok) {
      console.error("[Home] Failed loading student celebrations", payload?.error || response.status);
      return;
    }

    const items = [];
    const awardedBadges = Array.isArray(payload.awardedBadges) ? payload.awardedBadges : [];
    for (const badge of awardedBadges) {
      const slug = String(badge?.slug || "").trim();
      if (!slug) continue;
      items.push({
        type: "badge",
        data: {
          name: String(badge?.name || slug),
          image_url: getBadgeImagePath(slug)
        }
      });
    }

    if (payload?.leveledUp && typeof payload.leveledUp === "object") {
      const level = Number(payload.leveledUp.level || 0) || 1;
      items.push({
        type: "level",
        data: {
          level,
          image_url: `/images/levelBadges/level${level}.png`
        }
      });
    }

    queueCelebrations(items);
  } catch (error) {
    console.error("[Home] Failed loading student celebrations", error);
  }
}

async function refreshActiveStudentData({ userId, fallbackProfile } = {}) {
  const ctx = await safeViewerContext();
  const activeStudentId = userId || getActiveStudentIdForContext(ctx);
  if (!activeStudentId) {
    updateParentProgressState({ hasSelection: false });
    return;
  }
  persistActiveStudentId(activeStudentId);

  const { data: profileRow, error } = await supabase
    .from("users")
    .select("*")
    .eq("id", activeStudentId)
    .single();

  if (error) {
    console.error("[Home] Failed to refresh profile", error);
  }

  const profile = profileRow || fallbackProfile || currentProfile;
  if (profile) {
    persistActiveStudentId(profile.id || activeStudentId);
    localStorage.setItem("loggedInUser", JSON.stringify(profile));
    const totalPoints = Number(profile.points || 0);
    const resolvedLevel = await loadLevel(profile.level || 1);
    currentProfile = {
      ...profile,
      points: totalPoints,
      level: resolvedLevel?.id || profile.level
    };
    currentLevelRow = resolvedLevel;
    if (currentLevelRow) {
      const showWelcome = ctx.mode === "student";
      renderIdentity(currentProfile, currentLevelRow, showWelcome);
    }
  }

  updateParentProgressState({ hasSelection: true });
  await loadPendingPoints(activeStudentId);
  updatePendingProgressFill();
  practiceLoggedToday = await checkPracticeLoggedToday(activeStudentId);
  setPracticeButtonState(qs("quickPracticeBtn"), practiceLoggedToday);

  const studioId = ctx?.studioId || localStorage.getItem("activeStudioId");
  if (!homeShowStaffUI) {
    initBadgeHeroModal();
    exposeBadgeRecomputeDebugTools({ userId: activeStudentId, studioId });
    try {
      await maybeRunBadgeRecomputeDebug({ userId: activeStudentId, studioId });
    } catch (error) {
      console.error("[Badges][Recompute] debug run failed", error);
      showToast("Badge recompute failed. Check console.");
    }
    const earnedRows = await loadEarnedBadges(activeStudentId, studioId);
    const recentBadge = toRecentHeroBadge(getMostRecentBadgeRow(earnedRows));
    const openMyPointsBadges = (badge) => {
      if (document.body.classList.contains("student-tutorial-open") && badge) {
        openBadgeHeroModal(badge);
        return;
      }
      localStorage.setItem("aa.openBadgeCatalog", "1");
      window.location.href = "my-points.html?openBadges=1";
    };
    const debugNextUp = () => {
      console.log("[Home][NextUp Debug]", {
        recentBadge,
        ...lastNextUpDebugState
      });
      showToast("Next-up debug printed to console.");
    };
    await loadNextUpBadge({ userId: activeStudentId, studioId, recentBadge, onOpenModal: openMyPointsBadges, onDebug: debugNextUp });
    if (ctx?.mode === "student") {
      await loadStudentCelebrations({ studioId, studentId: activeStudentId, viewerContext: ctx });
    }
  } else {
    exposeBadgeRecomputeDebugTools({ userId: activeStudentId, studioId });
    try {
      await maybeRunBadgeRecomputeDebug({ userId: activeStudentId, studioId });
    } catch (error) {
      console.error("[Badges][Recompute] debug run failed", error);
      showToast("Badge recompute failed. Check console.");
    }
    renderNextUpBadge(null);
  }
}

function buildCategoryHeader(category, label) {
  const safeCategory = String(category || "").toLowerCase();
  const imgSrc = safeCategory ? `images/categories/${safeCategory}.png` : "images/categories/allCategories.png";
  return `
    <div class="modal-category">
      <img src="${imgSrc}" alt="${label}">
      <div class="modal-category-text">
        <div class="modal-category-label">${label}</div>
      </div>
    </div>
  `;
}

async function insertLogs(rows, { approved }) {
  const ctx = await getViewerContext();
  const identityContext = resolveHomeIdentityContext(ctx);
  if (identityContext.mode === "parent") {
    showToast("Parents are read-only.");
    return false;
  }
  const { data: sessionData, error: sessionError } = await supabase.auth.getSession();
  const authUserId = sessionData?.session?.user?.id || identityContext.authUserId || null;
  const viewerUserId = identityContext.viewerUserId || authUserId;
  const activeStudentId = identityContext.activeStudentId;
  if (!authUserId) {
    if (sessionError) console.error("[Home] session check failed", sessionError);
    window.location.href = loginRedirectUrl();
    return false;
  }

  const studioId = identityContext.studioId || localStorage.getItem("activeStudioId") || null;
  const payload = rows.map(row => ({
    ...row,
    created_by: row.created_by || authUserId,
    ...(studioId ? { studio_id: studioId } : {})
  }));

  const normalizeDate = (value) => {
    if (!value) return null;
    if (value instanceof Date) return getLocalDateString(value);
    if (typeof value === "string") return value.slice(0, 10);
    return String(value);
  };

  const rpcPayloads = payload.map((row, index) => {
    const actorUserId = String(authUserId || "").trim() || null;
    const targetUserId = String(row.userId || activeStudentId || "").trim() || null;
    const normalizedCategory = String(row.category || "").trim().toLowerCase() || null;
    const normalizedPoints = Number(row.points);
    const requestPayload = {
      p_user_id: targetUserId,
      p_studio_id: row.studio_id || studioId || null,
      p_category: normalizedCategory,
      p_points: Number.isFinite(normalizedPoints) ? normalizedPoints : null,
      p_notes: row.notes ?? "",
      p_status: String(row.status || (approved ? "approved" : "pending")).toLowerCase(),
      p_source: "home",
      p_date: normalizeDate(row.date)
    };
    console.log("[Home][insert_log payload]", {
      index,
      actorUserId,
      targetUserId,
      authUserId,
      viewerUserId,
      activeStudentId,
      mode: identityContext.mode,
      staffLoggingMode: identityContext.staffLoggingMode,
      payload: requestPayload
    });
    return requestPayload;
  });

  const invalidPayload = rpcPayloads.find((requestPayload) =>
    !requestPayload.p_user_id ||
    !requestPayload.p_studio_id ||
    !requestPayload.p_category ||
    !Number.isFinite(Number(requestPayload.p_points)) ||
    !requestPayload.p_date
  );
  if (invalidPayload) {
    console.error("[Home] Invalid insert_log payload; aborting RPC call", {
      mode: identityContext.mode,
      staffLoggingMode: identityContext.staffLoggingMode,
      authUserId,
      viewerUserId,
      activeStudentId,
      studioId,
      invalidPayload,
      rpcPayloads
    });
    showToast("Couldn't save log. Missing required fields.");
    return false;
  }

  const approvedUserIds = approved
    ? Array.from(new Set(payload.map((row) => String(row.userId || "").trim()).filter(Boolean)))
    : [];
  const levelSnapshotsBefore = approvedUserIds.length
    ? await fetchStudentLevelSnapshots(approvedUserIds)
    : new Map();

  const results = await Promise.all(rpcPayloads.map((requestPayload) => supabase.rpc("insert_log", requestPayload)));

  const firstError = results.find(r => r.error)?.error || null;
  if (firstError) {
    console.error("Failed to save log:", firstError, {
      mode: identityContext.mode,
      staffLoggingMode: identityContext.staffLoggingMode,
      authUserId,
      viewerUserId,
      activeStudentId,
      studioId,
      rpcPayloads
    });
    const message = String(firstError.message || "");
    if (message.toLowerCase().includes("row-level security")) {
      showToast("Please log in again.");
    } else {
      showToast("Couldn't save log. Try again.");
    }
    return false;
  }
  if (approved && approvedUserIds.length > 0) {
    const recalcResults = await recalculateUsersAfterApprovedBatch(approvedUserIds, levelSnapshotsBefore, {
      studioId
    });
    const activeResult = currentProfile?.id ? recalcResults.get(String(currentProfile.id)) : null;
    if (activeResult) {
      await applyApprovedRecalc(currentProfile.id, activeResult);
    }
  }
  return true;
}

async function initStudentLogActions() {
  const practiceBtn = qs("quickPracticeBtn");
  if (practiceBtn) {
    practiceBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      const ctx = await getViewerContext();
      const identityContext = resolveHomeIdentityContext(ctx);
      console.log("[Home] log action context", { viewerContext: ctx, identityContext });
      if (identityContext.mode === "parent") {
        showToast("Parents are read-only.");
        return;
      }
      const activeStudentId = getActiveStudentIdForContext(ctx);
      if (!activeStudentId) return;
      console.log("[Home] logging as", identityContext.mode, "actorUserId", identityContext.authUserId, "targetUserId", activeStudentId);
      practiceLoggedToday = await checkPracticeLoggedToday(activeStudentId);
      setPracticeButtonState(practiceBtn, practiceLoggedToday);
      if (practiceLoggedToday) {
        showToast("You already logged today's practice.");
        return;
      }
      const today = getTodayString();
      const ok = await insertLogs([{
        userId: activeStudentId,
        category: "practice",
        notes: "",
        date: today,
        points: 5,
        status: "approved"
      }], { approved: true });
      if (ok) {
        await refreshActiveStudentData({ userId: activeStudentId });
        showToast("✅ Practice logged (+5)");
        dispatchTutorialAction("aa:tutorial-student-log-today-complete");
      }
    });
  }

  const pastPracticeBtn = qs("logPastPracticeBtn");
  if (pastPracticeBtn) {
    pastPracticeBtn.addEventListener("click", async (e) => {
      e.preventDefault();
      const ctx = await getViewerContext();
      const identityContext = resolveHomeIdentityContext(ctx);
      console.log("[Home] log action context", { viewerContext: ctx, identityContext });
      if (identityContext.mode === "parent") {
        showToast("Parents are read-only.");
        return;
      }
      const activeStudentId = getActiveStudentIdForContext(ctx);
      if (!activeStudentId) return;
      await openPastPracticeModal(activeStudentId);
    });
  }

  document.querySelectorAll(".action-grid .chip").forEach((btn) => {
    btn.addEventListener("click", async (e) => {
      e.preventDefault();
      const ctx = await getViewerContext();
      const identityContext = resolveHomeIdentityContext(ctx);
      console.log("[Home] log action context", { viewerContext: ctx, identityContext });
      if (identityContext.mode === "parent") {
        showToast("Parents are read-only.");
        return;
      }
      const activeStudentId = getActiveStudentIdForContext(ctx);
      if (!activeStudentId) return;
      await openChipModal(btn, activeStudentId);
    });
  });
}

async function openPastPracticeModal(userId) {
  const today = new Date();
  const start = new Date();
  start.setDate(today.getDate() - 29);
  const studioId = localStorage.getItem("activeStudioId") || null;
  const practiceDatesByUser = await fetchExistingPracticeLogDates({ studioId, userIds: [userId] });
  const existingPracticeDates = practiceDatesByUser.get(String(userId)) || new Set();

  const selectedDates = new Set();
  openStudentModal({
    title: "Log Past Practice",
    submitLabel: "Log Practice",
    bodyHtml: `
      ${buildCategoryHeader("practice", "Practice")}
      <div class="modal-field">
        <label>Select dates (last 30 days)</label>
        <div class="calendar">
          <div class="calendar-header">
            <button id="calPrev" class="calendar-nav" type="button">‹</button>
            <div id="calMonthLabel" class="calendar-title"></div>
            <button id="calNext" class="calendar-nav" type="button">›</button>
          </div>
          <div class="calendar-weekdays">
            <span>Sun</span><span>Mon</span><span>Tue</span><span>Wed</span><span>Thu</span><span>Fri</span><span>Sat</span>
          </div>
          <div id="practiceCalendar" class="calendar-grid"></div>
        </div>
      </div>
      <div class="modal-field">
        <label>Points</label>
        <input class="points-readonly" type="text" value="5 points per day" disabled>
      </div>
      <div class="modal-field">
        <label>Notes (optional)</label>
        <textarea id="practiceNotes" placeholder="Optional note"></textarea>
      </div>
      <div class="status-note">Approved</div>
    `,
    onSubmit: async () => {
      const notes = qs("practiceNotes")?.value?.trim() || "";
      const selected = Array.from(selectedDates);
      if (!selected.length) {
        showToast("Select at least one date.");
        return;
      }
      const duplicateDates = selected.filter((date) => existingPracticeDates.has(date));
      const datesToInsert = selected.filter((date) => !existingPracticeDates.has(date));
      if (!datesToInsert.length) {
        showToast("Practice logs already exist for all selected dates. No new logs were added.");
        return;
      }
      const rows = datesToInsert.map(date => ({
        userId,
        category: "practice",
        notes,
        date,
        points: 5,
        status: "approved"
      }));
      const ok = await insertLogs(rows, { approved: true });
      if (ok) {
        datesToInsert.forEach((date) => existingPracticeDates.add(date));
        renderCalendar();
        if (duplicateDates.length) {
          showToast("Some selected practice dates already had logs and were skipped. The remaining new dates were saved.");
        } else {
          showToast(`✅ Logged ${datesToInsert.length} practice day(s)`);
        }
        await refreshActiveStudentData({ userId });
        closeStudentModal({ completed: true });
        dispatchTutorialAction("aa:tutorial-student-log-past-complete");
      }
    }
  });

  const calendarEl = qs("practiceCalendar");
  const monthLabel = qs("calMonthLabel");
  const prevBtn = qs("calPrev");
  const nextBtn = qs("calNext");
  if (!calendarEl || !monthLabel || !prevBtn || !nextBtn) return;

  const monthNames = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];

  const view = {
    year: today.getFullYear(),
    month: today.getMonth()
  };

  const startRange = new Date(start.getFullYear(), start.getMonth(), start.getDate(), 0, 0, 0, 0);
  const endRange = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);

  const clampToRange = (date) => date >= startRange && date <= endRange;

  const renderCalendar = () => {
    calendarEl.innerHTML = "";
    const firstDay = new Date(view.year, view.month, 1);
    const startDay = firstDay.getDay();
    const gridStart = new Date(view.year, view.month, 1 - startDay);
    monthLabel.textContent = `${monthNames[view.month]} ${view.year}`;

    const monthStart = new Date(view.year, view.month, 1);
    const monthEnd = new Date(view.year, view.month + 1, 0);
    const prevMonthEnd = new Date(view.year, view.month, 0, 23, 59, 59, 999);
    const nextMonthStart = new Date(view.year, view.month + 1, 1, 0, 0, 0, 0);
    prevBtn.disabled = prevMonthEnd < startRange;
    nextBtn.disabled = nextMonthStart > endRange;

    for (let i = 0; i < 42; i++) {
      const cellDate = new Date(gridStart);
      cellDate.setDate(gridStart.getDate() + i);
      const dateStr = getLocalDateString(cellDate);
      const inMonth = cellDate.getMonth() === view.month;
      const inRange = clampToRange(cellDate);

      const cell = document.createElement("button");
      cell.type = "button";
      cell.className = "calendar-day";
      cell.dataset.date = dateStr;
      cell.textContent = String(cellDate.getDate());

      if (!inMonth) cell.classList.add("outside");
      const alreadyLoggedPractice = existingPracticeDates.has(dateStr);
      if (!inRange || alreadyLoggedPractice) {
        cell.classList.add("disabled");
        cell.disabled = true;
        if (alreadyLoggedPractice) cell.title = "Practice already logged for this date";
        if (alreadyLoggedPractice && selectedDates.has(dateStr)) selectedDates.delete(dateStr);
      } else {
        cell.addEventListener("click", () => {
          if (selectedDates.has(dateStr)) {
            selectedDates.delete(dateStr);
            cell.classList.remove("selected");
          } else {
            selectedDates.add(dateStr);
            cell.classList.add("selected");
          }
        });
      }

      if (selectedDates.has(dateStr)) {
        cell.classList.add("selected");
      }

      calendarEl.appendChild(cell);
    }
  };

  prevBtn.addEventListener("click", () => {
    const prevMonth = new Date(view.year, view.month - 1, 1);
    const prevMonthEnd = new Date(view.year, view.month, 0, 23, 59, 59, 999);
    if (prevMonthEnd >= startRange) {
      view.year = prevMonth.getFullYear();
      view.month = prevMonth.getMonth();
      renderCalendar();
    }
  });

  nextBtn.addEventListener("click", () => {
    const nextMonth = new Date(view.year, view.month + 1, 1);
    if (nextMonth <= endRange) {
      view.year = nextMonth.getFullYear();
      view.month = nextMonth.getMonth();
      renderCalendar();
    }
  });

  renderCalendar();
}

async function openChipModal(button, userId) {
  const logType = button.dataset.logType || "fixed";
  const category = button.dataset.category || "";
  const label = button.dataset.hint || button.textContent.trim();
  const points = Number(button.dataset.points || 0);
  const notesPrompt = button.dataset.notesPrompt || "";
  const notesRequired = logType === "festival"
    ? false
    : button.dataset.notesRequired !== "false";

  if (logType === "outside-performance") {
    const existingPoints = await getOutsidePerformancePointsThisMonth(userId);
    if (existingPoints >= 100 || existingPoints + points > 100) {
      showToast("You’ve reached the 100-point outside performance limit for this month.");
      return;
    }
  }

  if (logType === "festival") {
    openFestivalModal({ userId, category, label });
    return;
  }

  if (logType === "memorization") {
    openMemorizationModal({ userId, category, label, notesPrompt, notesRequired });
    return;
  }

  const isDiscretionary = logType === "discretionary";
  openFixedModal({
    userId,
    category,
    label,
    points,
    notesRequired,
    notesPrompt,
    notePrefix: logType === "outside-performance" ? "[Outside Performance] " : "",
    statusText: "Pending approval",
    submitLabel: "Submit",
    statusValue: "pending",
    pointsHint: isDiscretionary ? "5 (teacher may adjust)" : null
  });
}

async function getOutsidePerformancePointsThisMonth(userId) {
  const now = new Date();
  const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
  const monthEnd = new Date(now.getFullYear(), now.getMonth() + 1, 0);
  const startStr = getLocalDateString(monthStart);
  const endStr = getLocalDateString(monthEnd);
  const { data, error } = await supabase
    .from("logs")
    .select("points")
    .eq("userId", userId)
    .eq("category", "performance")
    .in("status", ["pending", "approved"])
    .gte("date", startStr)
    .lte("date", endStr)
    .ilike("notes", "[Outside Performance]%");
  if (error) {
    console.error("Failed outside performance check", error);
    return 0;
  }
  return (data || []).reduce((sum, row) => sum + (row.points || 0), 0);
}

function openFixedModal({ userId, category, label, points, notesRequired, notesPrompt, notePrefix, statusText, submitLabel, statusValue, pointsHint }) {
  const safePoints = Number.isFinite(points) && points > 0 ? points : 5;
  const pointsLabel = pointsHint || safePoints;
  const noteLabel = notesPrompt || `Notes${notesRequired ? " (required)" : " (optional)"}`;
  const notePlaceholder = notesPrompt || "Add a note";
  openStudentModal({
    title: label,
    submitLabel,
    bodyHtml: `
      ${buildCategoryHeader(category, label)}
      <div class="modal-field">
        <label>Date</label>
        ${buildSingleDateSelectorMarkup({ inputId: "logDateInput", prefix: "studentLog" })}
      </div>
      <div class="modal-field">
        <label>Points</label>
        <input id="logPointsInput" class="points-readonly" type="text" value="${pointsLabel}" disabled>
      </div>
      <div class="modal-field">
        <label>${noteLabel}</label>
        <textarea id="logNotesInput" placeholder="${notePlaceholder}" ${notesRequired ? "required" : ""}></textarea>
      </div>
      <div class="status-note">${statusText}</div>
    `,
    onSubmit: async () => {
      const date = qs("logDateInput")?.value;
      const notesValue = qs("logNotesInput")?.value?.trim() || "";
      if (!date) {
        showToast("Select a date.");
        return;
      }
      if (notesRequired && !notesValue) {
        showToast("Please add a note");
        return;
      }
      const notes = `${notePrefix || ""}${notesValue}`.trim();
      const ok = await insertLogs([{
        userId,
        category,
        notes,
        date,
        points: safePoints,
        status: statusValue
      }], { approved: statusValue === "approved" });
      if (ok) {
        showToast("Log submitted.");
        await refreshActiveStudentData({ userId });
        closeStudentModal({ completed: true });
        dispatchTutorialAction("aa:tutorial-student-special-log-complete");
      }
    }
  });

  initSingleDateSelector({ inputId: "logDateInput", prefix: "studentLog" });
}

function openFestivalModal({ userId, category, label }) {
  openStudentModal({
    title: label,
    submitLabel: "Submit",
    bodyHtml: `
      ${buildCategoryHeader(category, label)}
      <div class="modal-field">
        <label>Date</label>
        ${buildSingleDateSelectorMarkup({ inputId: "festivalDateInput", prefix: "festivalLog" })}
      </div>
      <div class="modal-field">
        <label>Rating</label>
        <select id="festivalRating">
          <option value="superior">Superior (200)</option>
          <option value="excellent">Excellent (150)</option>
          <option value="other">Other (100)</option>
        </select>
      </div>
      <div class="modal-field">
        <label>Points</label>
        <input id="festivalPoints" class="points-readonly" type="text" value="200" disabled>
      </div>
      <div class="modal-field">
        <label>Notes (optional)</label>
        <textarea id="festivalNotes" placeholder="Add a note"></textarea>
      </div>
      <div class="status-note">Pending approval</div>
    `,
    onSubmit: async () => {
      const date = qs("festivalDateInput")?.value;
      const rating = qs("festivalRating")?.value;
      const notes = qs("festivalNotes")?.value?.trim() || "";
      if (!date) {
        showToast("Select a date.");
        return;
      }
      const pointsMap = { superior: 200, excellent: 150, other: 100 };
      const points = pointsMap[rating] || 100;
      const ok = await insertLogs([{
        userId,
        category,
        notes,
        date,
        points,
        status: "pending"
      }], { approved: false });
      if (ok) {
        showToast("Log submitted.");
        await refreshActiveStudentData({ userId });
        closeStudentModal({ completed: true });
        dispatchTutorialAction("aa:tutorial-student-special-log-complete");
      }
    }
  });

  initSingleDateSelector({ inputId: "festivalDateInput", prefix: "festivalLog" });

  const ratingSelect = qs("festivalRating");
  const pointsInput = qs("festivalPoints");
  if (ratingSelect && pointsInput) {
    ratingSelect.addEventListener("change", () => {
      const pointsMap = { superior: 200, excellent: 150, other: 100 };
      pointsInput.value = pointsMap[ratingSelect.value] || 100;
    });
  }
}

function openMemorizationModal({ userId, category, label, notesRequired, notesPrompt }) {
  const noteLabel = notesPrompt || "Notes";
  const notePlaceholder = notesPrompt || "Add a note";
  openStudentModal({
    title: label,
    submitLabel: "Submit",
    bodyHtml: `
      ${buildCategoryHeader(category, label)}
      <div class="modal-field">
        <label>Date</label>
        ${buildSingleDateSelectorMarkup({ inputId: "memorizationDateInput", prefix: "memorizationLog" })}
      </div>
      <div class="modal-field">
        <label>Measures</label>
        <input id="memorizationMeasures" type="number" min="1" value="1">
      </div>
      <div class="modal-field">
        <label>Points</label>
        <input id="memorizationPoints" class="points-readonly" type="text" value="2" disabled>
      </div>
      <div class="modal-field">
        <label>${noteLabel}</label>
        <textarea id="memorizationNotes" placeholder="${notePlaceholder}" ${notesRequired ? "required" : ""}></textarea>
      </div>
      <div class="status-note">Pending approval</div>
    `,
    onSubmit: async () => {
      const date = qs("memorizationDateInput")?.value;
      const measures = parseInt(qs("memorizationMeasures")?.value, 10);
      const notes = qs("memorizationNotes")?.value?.trim() || "";
      if (!date || !Number.isFinite(measures) || measures < 1) {
        showToast("Enter a measures count.");
        return;
      }
      if (notesRequired && !notes) {
        showToast("Please add a note");
        return;
      }
      const points = measures * 2;
      const ok = await insertLogs([{
        userId,
        category,
        notes,
        date,
        points,
        status: "pending"
      }], { approved: false });
      if (ok) {
        showToast("Log submitted.");
        await refreshActiveStudentData({ userId });
        closeStudentModal({ completed: true });
        dispatchTutorialAction("aa:tutorial-student-special-log-complete");
      }
    }
  });

  initSingleDateSelector({ inputId: "memorizationDateInput", prefix: "memorizationLog" });

  const measuresInput = qs("memorizationMeasures");
  const pointsInput = qs("memorizationPoints");
  if (measuresInput && pointsInput) {
    measuresInput.addEventListener("input", () => {
      const value = parseInt(measuresInput.value, 10);
      pointsInput.value = Number.isFinite(value) && value > 0 ? String(value * 2) : "0";
    });
  }
}

function renderStaffChallengesRibbon() {
  const mount = document.getElementById('staffChallengesRibbon');
  if (!mount) return;
  mount.innerHTML = `
    <div id="staffChallengesRibbonStrip" class="staff-challenges-ribbon no-challenges" aria-label="Staff challenges ribbon">
      <div class="ribbon-left">
        <div class="ribbon-title">Challenges</div>
        <div class="ribbon-tabs">
          <button id="challengeActiveBtn" class="ribbon-tab is-active" data-tab="active" type="button">Active (0)</button>
          <button id="challengeEndedBtn" class="ribbon-tab" data-tab="ended" type="button">Ended (0)</button>
        </div>
      </div>
      <button class="ribbon-cta" id="btnNewChallenge" type="button">+ New</button>
    </div>
  `;
}

function renderStaffQuickLogShell() {
  const mount = document.getElementById('staffQuickLogMount');
  if (!mount) return;
  mount.innerHTML = `
    <section class="home-staff staff-only" aria-label="Staff quick log">
      <form id="staffQuickLogForm" class="staff-card">
        <div class="quicklog-header-row">
          <div class="quicklog-title">Quick Log</div>
        </div>
        <label for="staffStudentsSearch">Students</label>
        <div id="staffStudentPicker" class="staff-student-picker">
          <input
            id="staffStudentsSearch"
            type="text"
            placeholder="Type a student name..."
            autocomplete="off"
            disabled
          />
          <div id="staffStudentsDropdown" class="staff-student-dropdown" hidden></div>
          <div id="staffStudentsSelected" class="staff-student-selected" aria-live="polite"></div>
          <select id="staffStudents" multiple disabled hidden aria-hidden="true"></select>
        </div>

        <label>Quick prompts</label>
        <div id="staffPromptGrid" class="staff-prompt-grid"></div>

        <button id="staffMoreCategoriesToggle" class="staff-more-categories-toggle" type="button" aria-expanded="false">
          + More categories
        </button>
        <div id="staffMoreCategoriesPanel" class="ql-category-pop staff-more-categories-panel" hidden>
          <label for="staffCategory">Other category</label>
          <select id="staffCategory" disabled>
            <option value="">Loading...</option>
          </select>
        </div>

        <label for="staffPoints">Points</label>
        <input id="staffPoints" type="number" min="0" required />
        <p id="staffPracticePointsNote" class="staff-msg" style="display:none;">Practice category will automatically award 5 points per day.</p>

        <label>Dates</label>
        <button id="staffCalendarToggle" class="blue-button staff-calendar-toggle" type="button">
          Select dates
        </button>
        <div id="staffCalendarPanel" class="staff-calendar-panel" hidden>
          <div class="calendar">
            <div class="calendar-header">
              <button id="staffCalPrev" class="calendar-nav" type="button">‹</button>
              <div id="staffCalMonthLabel" class="calendar-title"></div>
              <button id="staffCalNext" class="calendar-nav" type="button">›</button>
            </div>
            <div class="calendar-weekdays">
              <span>Sun</span><span>Mon</span><span>Tue</span><span>Wed</span><span>Thu</span><span>Fri</span><span>Sat</span>
            </div>
            <div id="staffCalendar" class="calendar-grid"></div>
          </div>
        </div>

        <label for="staffNotes">Notes</label>
        <input id="staffNotes" type="text" />

        <div id="staffQuickLogError" class="staff-msg" style="display:none;"></div>
        <p id="staffQuickLogMsg" class="staff-msg" style="display:none;"></p>

        <div class="button-row" style="margin-top:10px;">
          <button id="staffQuickLogSubmit" type="submit" class="blue-button staff-submit" disabled>Submit Points</button>
        </div>
      </form>
    </section>
  `;
}

async function loadCategoriesForStudio(studioId) {
  if (!studioId) return { data: [], error: new Error('Missing studio id') };
  const { data, error } = await supabase
    .from('categories')
    .select('*')
    .order('id', { ascending: true });
  if (error || !Array.isArray(data)) return { data: [], error };
  const blockedCategoryNames = new Set(["batch_practice"]);
  const filtered = data.filter(category => !blockedCategoryNames.has(String(category?.name || "").toLowerCase()));
  return { data: filtered, error: null };
}

async function loadStudentsForStudio(studioId) {
  if (!studioId) return { data: [], error: new Error('Missing studio id') };
  const { data, error } = await supabase
    .from('users')
    .select('id, firstName, lastName, email')
    .eq('studio_id', studioId)
    .eq('active', true)
    .is('deactivated_at', null)
    .contains('roles', ['student']);
  if (error || !Array.isArray(data)) return { data: [], error };
  return { data, error: null };
}

function addDateChip(container, dateValue) {
  const existing = Array.from(container.querySelectorAll('[data-date]'))
    .some(el => el.dataset.date === dateValue);
  if (existing) return;

  const chip = document.createElement('span');
  chip.className = 'date-chip';
  chip.dataset.date = dateValue;
  chip.textContent = dateValue;

  const removeBtn = document.createElement('button');
  removeBtn.type = 'button';
  removeBtn.textContent = 'x';
  removeBtn.addEventListener('click', () => chip.remove());

  chip.appendChild(removeBtn);
  container.appendChild(chip);
}

function getSelectedDates(container) {
  return Array.from(container.querySelectorAll('[data-date]'))
    .map(el => el.dataset.date)
    .filter(Boolean);
}

async function fetchExistingPracticeLogDates({ studioId, userIds } = {}) {
  const normalizedStudioId = String(studioId || "").trim();
  const ids = Array.isArray(userIds)
    ? userIds.map((id) => String(id || "").trim()).filter(Boolean)
    : [];
  const map = new Map();
  ids.forEach((id) => map.set(id, new Set()));
  if (!normalizedStudioId || !ids.length) return map;

  const { data, error } = await supabase
    .from("logs")
    .select("userId,date,status")
    .eq("studio_id", normalizedStudioId)
    .eq("category", "practice")
    .in("userId", ids)
    .or("status.is.null,status.neq.rejected");

  if (error) {
    console.error("[Home] failed loading existing practice dates", error, { studioId: normalizedStudioId, userIds: ids });
    return map;
  }

  for (const row of Array.isArray(data) ? data : []) {
    const userId = String(row?.userId || "").trim();
    const date = String(row?.date || "").slice(0, 10);
    if (!userId || !date) continue;
    if (!map.has(userId)) map.set(userId, new Set());
    map.get(userId).add(date);
  }

  return map;
}

async function insertLogsWithApproval(rows, includeApprovalFields) {
  const activeStudioId = localStorage.getItem("activeStudioId") || null;
  if (!activeStudioId) {
    return { ok: false, error: new Error("Missing active studio id") };
  }
  const rowsWithStudio = rows.map((r) => ({
    ...r,
    studio_id: r.studio_id || activeStudioId
  }));

  const payload = includeApprovalFields
    ? rowsWithStudio.map(r => ({
        ...r,
        approved_by: r.created_by,
        approved_at: new Date().toISOString()
      }))
    : rowsWithStudio;

  const { error } = await supabase.from('logs').insert(payload);
  if (!error) return { ok: true };

  const msg = String(error.message || '');
  if (includeApprovalFields && (msg.includes('approved_by') || msg.includes('approved_at'))) {
    const { error: retryErr } = await supabase.from('logs').insert(rowsWithStudio);
    if (retryErr) return { ok: false, error: retryErr };
    return { ok: true };
  }
  return { ok: false, error };
}

async function initStaffQuickLog({ authUserId, studioId, roles }) {
  const form = document.getElementById('staffQuickLogForm');
  if (!form) return;

  const categorySelect = document.getElementById('staffCategory');
  const promptGrid = document.getElementById('staffPromptGrid');
  const moreCategoriesToggle = document.getElementById('staffMoreCategoriesToggle');
  const moreCategoriesPanel = document.getElementById('staffMoreCategoriesPanel');
  const studentSelect = document.getElementById('staffStudents');
  const studentPicker = document.getElementById('staffStudentPicker');
  const studentSearchInput = document.getElementById('staffStudentsSearch');
  const studentDropdown = document.getElementById('staffStudentsDropdown');
  const studentSelected = document.getElementById('staffStudentsSelected');
  const calendarEl = document.getElementById('staffCalendar');
  const monthLabel = document.getElementById('staffCalMonthLabel');
  const prevBtn = document.getElementById('staffCalPrev');
  const nextBtn = document.getElementById('staffCalNext');
  const calendarToggle = document.getElementById('staffCalendarToggle');
  const calendarPanel = document.getElementById('staffCalendarPanel');
  const pointsInput = document.getElementById('staffPoints');
  const practicePointsNote = document.getElementById('staffPracticePointsNote');
  const notesInput = document.getElementById('staffNotes');
  const msgEl = document.getElementById('staffQuickLogMsg');
  const errorEl = document.getElementById('staffQuickLogError');
  const submitBtn = document.getElementById('staffQuickLogSubmit');
  const selectedDates = new Set();
  const selectedStudentIds = new Set();
  const categoryRowsByName = new Map();
  let staffPointsManuallyEdited = false;
  let selectedPromptKey = "";
  let selectedPromptCategory = "";
  let practiceDatesByStudentId = new Map();
  let practiceDatesRequestToken = 0;
  let staffStudentRows = [];

  const syncPracticePoints = ({ force = false } = {}) => {
    if (!pointsInput) return;
    const categoryName = String(selectedPromptCategory || categorySelect?.value || "").trim().toLowerCase();
    const isPractice = categoryName === "practice";
    const defaultPoints = getCategoryDefaultPoints(categoryName, categoryRowsByName.get(categoryName) || null);
    pointsInput.disabled = false;
    if (isPractice) {
      if (force || !staffPointsManuallyEdited) pointsInput.value = "5";
      if (practicePointsNote) practicePointsNote.style.display = "block";
      return;
    }
    if (practicePointsNote) practicePointsNote.style.display = "none";
    if (defaultPoints !== null && (force || !staffPointsManuallyEdited)) {
      pointsInput.value = String(defaultPoints);
    } else if (!categoryName && (force || !staffPointsManuallyEdited)) {
      pointsInput.value = "";
    }
  };

  const setStaffPromptActive = (key) => {
    selectedPromptKey = String(key || "");
    promptGrid?.querySelectorAll("[data-staff-prompt]").forEach((button) => {
      const active = String(button.getAttribute("data-staff-prompt") || "") === selectedPromptKey;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
  };

  const applyStaffPrompt = (prompt) => {
    if (!prompt || !pointsInput) return;
    const category = String(prompt.category || "").trim().toLowerCase();
    selectedPromptCategory = category;
    if (categorySelect) categorySelect.value = category;
    pointsInput.disabled = false;
    if (Number.isFinite(Number(prompt.points))) pointsInput.value = String(prompt.points);
    if (notesInput && !String(notesInput.value || "").trim() && prompt.notesPrompt) {
      notesInput.placeholder = prompt.notesPrompt;
    }
    staffPointsManuallyEdited = false;
    setStaffPromptActive(prompt.key);
    if (practicePointsNote) practicePointsNote.style.display = category === "practice" ? "block" : "none";
    void refreshPracticeDateCache();
  };

  const renderStaffPromptGrid = () => {
    if (!promptGrid) return;
    promptGrid.innerHTML = getTeacherLogPrompts().map(prompt => `
      <button
        type="button"
        class="staff-prompt-button"
        data-staff-prompt="${prompt.key}"
        aria-pressed="false"
      >
        <span class="staff-prompt-icon" aria-hidden="true">${prompt.icon || ""}</span>
        <span class="staff-prompt-label">${prompt.label}</span>
      </button>
    `).join("");
    promptGrid.querySelectorAll("[data-staff-prompt]").forEach(button => {
      button.addEventListener("click", () => {
        const key = String(button.getAttribute("data-staff-prompt") || "");
        const prompt = getTeacherLogPrompts().find(item => item.key === key);
        applyStaffPrompt(prompt);
      });
    });
  };

  const getStudentName = (student) => {
    const first = student?.firstName || '';
    const last = student?.lastName || '';
    return last && first ? `${last}, ${first}` : (last || first || student?.email || 'Student');
  };

  const syncStudentSelect = () => {
    if (!studentSelect) return;
    Array.from(studentSelect.options).forEach((option) => {
      option.selected = selectedStudentIds.has(String(option.value));
    });
  };

  const isPracticeCategorySelected = () => String(selectedPromptCategory || categorySelect?.value || "").trim().toLowerCase() === "practice";

  const refreshPracticeDateCache = async () => {
    const requestToken = ++practiceDatesRequestToken;
    if (!isPracticeCategorySelected() || !selectedStudentIds.size) {
      practiceDatesByStudentId = new Map();
      renderStaffCalendar();
      updateStaffCalendarToggle();
      return;
    }
    const ids = Array.from(selectedStudentIds).map((id) => String(id || "").trim()).filter(Boolean);
    const map = await fetchExistingPracticeLogDates({ studioId, userIds: ids });
    if (requestToken !== practiceDatesRequestToken) return;
    practiceDatesByStudentId = map;
    renderStaffCalendar();
    updateStaffCalendarToggle();
  };

  const findExistingNonPracticeDuplicates = async ({ rows, category, points }) => {
    const studioIdNormalized = String(studioId || "").trim();
    const candidateRows = Array.isArray(rows) ? rows : [];
    const userIds = Array.from(new Set(
      candidateRows.map((row) => String(row?.userId || "").trim()).filter(Boolean)
    ));
    const dates = Array.from(new Set(
      candidateRows.map((row) => String(row?.date || "").slice(0, 10)).filter(Boolean)
    ));
    if (!studioIdNormalized || !userIds.length || !dates.length) return [];

    const normalizedCategory = String(category || "").trim().toLowerCase();
    const normalizedPoints = Number(points);
    const { data, error } = await supabase
      .from("logs")
      .select("id,userId,date,category,points,status")
      .eq("studio_id", studioIdNormalized)
      .in("userId", userIds)
      .in("date", dates)
      .or("status.is.null,status.neq.rejected");
    if (error) throw error;

    const existingKeys = new Set();
    for (const row of Array.isArray(data) ? data : []) {
      const rowCategory = String(row?.category || "").trim().toLowerCase();
      const rowPoints = Number(row?.points);
      const rowUserId = String(row?.userId || "").trim();
      const rowDate = String(row?.date || "").slice(0, 10);
      if (!rowUserId || !rowDate) continue;
      if (rowCategory !== normalizedCategory) continue;
      if (!Number.isFinite(rowPoints) || rowPoints !== normalizedPoints) continue;
      existingKeys.add(`${rowUserId}|${rowDate}`);
    }

    return candidateRows.filter((row) => {
      const rowUserId = String(row?.userId || "").trim();
      const rowDate = String(row?.date || "").slice(0, 10);
      if (!rowUserId || !rowDate) return false;
      return existingKeys.has(`${rowUserId}|${rowDate}`);
    });
  };

  const renderSelectedStudents = (students) => {
    if (!studentSelected) return;
    studentSelected.innerHTML = '';
    if (!selectedStudentIds.size) {
      const empty = document.createElement('span');
      empty.className = 'staff-student-empty';
      empty.textContent = 'No students selected';
      studentSelected.appendChild(empty);
      return;
    }

    students
      .filter((student) => selectedStudentIds.has(String(student.id)))
      .forEach((student) => {
        const chip = document.createElement('button');
        chip.type = 'button';
        chip.className = 'staff-student-chip';
        chip.dataset.studentId = String(student.id);
        chip.textContent = `${getStudentName(student)} x`;
        chip.addEventListener('click', () => {
          selectedStudentIds.delete(String(student.id));
          syncStudentSelect();
          renderSelectedStudents(students);
          renderStudentDropdown(students);
          void refreshPracticeDateCache();
        });
        studentSelected.appendChild(chip);
      });
  };

  const renderStudentDropdown = (students) => {
    if (!studentDropdown || !studentSearchInput) return;
    const query = (studentSearchInput.value || '').trim().toLowerCase();
    studentDropdown.innerHTML = '';

    if (!query) {
      studentDropdown.setAttribute('hidden', '');
      return;
    }

    const matches = students.filter((student) =>
      getStudentName(student).toLowerCase().includes(query)
    );

    if (!matches.length) {
      const empty = document.createElement('div');
      empty.className = 'staff-student-no-match';
      empty.textContent = 'No matching students';
      studentDropdown.appendChild(empty);
      studentDropdown.removeAttribute('hidden');
      return;
    }

    matches.forEach((student) => {
      const id = String(student.id);
      const item = document.createElement('button');
      item.type = 'button';
      item.className = 'staff-student-option';
      item.dataset.studentId = id;
      const isSelected = selectedStudentIds.has(id);
      item.textContent = isSelected ? `Selected: ${getStudentName(student)}` : getStudentName(student);
      if (isSelected) item.classList.add('is-selected');
      item.addEventListener('click', () => {
        if (selectedStudentIds.has(id)) {
          selectedStudentIds.delete(id);
        } else {
          selectedStudentIds.add(id);
        }
        syncStudentSelect();
        renderSelectedStudents(students);
        renderStudentDropdown(students);
        studentSearchInput.focus();
        void refreshPracticeDateCache();
      });
      studentDropdown.appendChild(item);
    });

    studentDropdown.removeAttribute('hidden');
  };

  const setError = (message) => {
    if (!errorEl) return;
    errorEl.textContent = message;
    errorEl.style.display = 'block';
    errorEl.style.color = '#c62828';
  };

  const today = new Date();
  const monthNames = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December"
  ];
  const view = { year: today.getFullYear(), month: today.getMonth() };

  const updateStaffCalendarToggle = () => {
    if (!calendarToggle) return;
    const count = selectedDates.size;
    calendarToggle.textContent = count ? `Dates (${count} selected)` : "Select dates";
  };

  const renderStaffCalendar = () => {
    if (!calendarEl || !monthLabel || !prevBtn || !nextBtn) return;
    calendarEl.innerHTML = "";
    const isPracticeMode = isPracticeCategorySelected();
    const selectedIds = Array.from(selectedStudentIds).map((id) => String(id || "").trim()).filter(Boolean);

    const firstDay = new Date(view.year, view.month, 1);
    const startDay = firstDay.getDay();
    const gridStart = new Date(view.year, view.month, 1 - startDay);
    monthLabel.textContent = `${monthNames[view.month]} ${view.year}`;

    const endRange = new Date(today.getFullYear(), today.getMonth(), today.getDate(), 23, 59, 59, 999);

    const monthStart = new Date(view.year, view.month, 1);
    const monthEnd = new Date(view.year, view.month + 1, 0);
    prevBtn.disabled = false;
    nextBtn.disabled = monthEnd >= endRange;

    for (let i = 0; i < 42; i++) {
      const cellDate = new Date(gridStart);
      cellDate.setDate(gridStart.getDate() + i);
      const dateStr = getLocalDateString(cellDate);
      const inMonth = cellDate.getMonth() === view.month;
      const inRange = cellDate <= endRange;

      const cell = document.createElement("button");
      cell.type = "button";
      cell.className = "calendar-day";
      cell.dataset.date = dateStr;
      cell.textContent = String(cellDate.getDate());

      if (!inMonth) cell.classList.add("outside");
      const alreadyLoggedForAllSelected = isPracticeMode
        && selectedIds.length > 0
        && selectedIds.every((id) => (practiceDatesByStudentId.get(id) || new Set()).has(dateStr));
      if (!inRange || alreadyLoggedForAllSelected) {
        cell.classList.add("disabled");
        cell.disabled = true;
        if (alreadyLoggedForAllSelected) cell.title = "Practice already logged for all selected students on this date";
        if (alreadyLoggedForAllSelected && selectedDates.has(dateStr)) selectedDates.delete(dateStr);
      } else {
        cell.addEventListener("click", () => {
          if (selectedDates.has(dateStr)) {
            selectedDates.delete(dateStr);
            cell.classList.remove("selected");
          } else {
            selectedDates.add(dateStr);
            cell.classList.add("selected");
          }
          updateStaffCalendarToggle();
        });
      }

      if (selectedDates.has(dateStr)) {
        cell.classList.add("selected");
      }

      calendarEl.appendChild(cell);
    }
  };

  const clearStaffQuickLogForm = () => {
    selectedStudentIds.clear();
    selectedDates.clear();
    practiceDatesByStudentId = new Map();
    if (studentSearchInput) studentSearchInput.value = "";
    if (studentDropdown) {
      studentDropdown.innerHTML = "";
      studentDropdown.setAttribute("hidden", "");
    }
    if (categorySelect) categorySelect.value = "";
    if (notesInput) notesInput.value = "";
    if (notesInput) notesInput.placeholder = "";
    staffPointsManuallyEdited = false;
    setStaffPromptActive("");
    syncStudentSelect();
    renderSelectedStudents(staffStudentRows);
    syncPracticePoints();
    if (calendarPanel) calendarPanel.setAttribute("hidden", "");
    renderStaffCalendar();
    updateStaffCalendarToggle();
  };

  if (prevBtn && nextBtn) {
    prevBtn.addEventListener("click", () => {
      const prevMonth = new Date(view.year, view.month - 1, 1);
      view.year = prevMonth.getFullYear();
      view.month = prevMonth.getMonth();
      renderStaffCalendar();
    });

    nextBtn.addEventListener("click", () => {
      const nextMonth = new Date(view.year, view.month + 1, 1);
      if (nextMonth <= new Date(today.getFullYear(), today.getMonth(), today.getDate())) {
        view.year = nextMonth.getFullYear();
        view.month = nextMonth.getMonth();
        renderStaffCalendar();
      }
    });
  }

  if (calendarToggle && calendarPanel) {
    calendarToggle.addEventListener("click", () => {
      const isOpen = !calendarPanel.hasAttribute("hidden");
      if (isOpen) {
        calendarPanel.setAttribute("hidden", "");
      } else {
        calendarPanel.removeAttribute("hidden");
      }
    });
  }

  if (moreCategoriesToggle && moreCategoriesPanel) {
    moreCategoriesToggle.addEventListener("click", () => {
      const isOpen = !moreCategoriesPanel.hasAttribute("hidden");
      if (isOpen) {
        moreCategoriesPanel.setAttribute("hidden", "");
        moreCategoriesToggle.setAttribute("aria-expanded", "false");
        moreCategoriesToggle.textContent = "+ More categories";
      } else {
        moreCategoriesPanel.removeAttribute("hidden");
        moreCategoriesToggle.setAttribute("aria-expanded", "true");
        moreCategoriesToggle.textContent = "Hide categories";
      }
    });
  }

  const { data: categories, error: catErr } = await loadCategoriesForStudio(studioId);
  if (catErr?.message) {
    setError(catErr.message);
  }

  if (categorySelect) {
    categorySelect.disabled = false;
    categorySelect.innerHTML = '<option value="">Select category</option>';
    categoryRowsByName.clear();
    categories.forEach(c => {
      const normalizedName = String(c.name || "").trim().toLowerCase();
      if (normalizedName) categoryRowsByName.set(normalizedName, c);
    });
    getTeacherPointCategories().forEach(c => {
      const opt = document.createElement('option');
      opt.value = c.value;
      opt.textContent = c.label;
      categorySelect.appendChild(opt);
    });
    categorySelect.addEventListener('change', () => {
      selectedPromptCategory = "";
      setStaffPromptActive("");
      syncPracticePoints();
      void refreshPracticeDateCache();
    });
    syncPracticePoints();
  }

  pointsInput?.addEventListener("input", () => {
    staffPointsManuallyEdited = true;
  });

  renderStaffPromptGrid();

  const { data: students, error: studentErr } = await loadStudentsForStudio(studioId);
  if (studentErr?.message) {
    setError(studentErr.message);
  }

  if (studentSelect) {
    if (!students.length) {
      studentSelect.innerHTML = '';
      studentSelect.disabled = true;
      if (studentSearchInput) {
        studentSearchInput.value = '';
        studentSearchInput.placeholder = 'No students found';
        studentSearchInput.disabled = true;
      }
      if (studentDropdown) studentDropdown.setAttribute('hidden', '');
      renderSelectedStudents([]);
    } else {
      const sortedStudents = [...students].sort((a, b) => {
        return getStudentName(a).localeCompare(getStudentName(b), undefined, { sensitivity: 'base' });
      });
      staffStudentRows = sortedStudents;

      studentSelect.disabled = false;
      studentSelect.innerHTML = '';
      sortedStudents.forEach((s) => {
        const opt = document.createElement('option');
        opt.value = s.id;
        opt.textContent = getStudentName(s);
        studentSelect.appendChild(opt);
      });

      if (studentSearchInput) {
        studentSearchInput.disabled = false;
        studentSearchInput.placeholder = 'Type a student name...';
        studentSearchInput.addEventListener('input', () => renderStudentDropdown(sortedStudents));
        studentSearchInput.addEventListener('focus', () => renderStudentDropdown(sortedStudents));
      }

      document.addEventListener('click', (event) => {
        if (!studentPicker || !studentDropdown) return;
        if (!studentPicker.contains(event.target)) {
          studentDropdown.setAttribute('hidden', '');
        }
      });

      document.addEventListener('keydown', (event) => {
        if (event.key === 'Escape' && studentDropdown) {
          studentDropdown.setAttribute('hidden', '');
        }
      });

      renderSelectedStudents(sortedStudents);
      void refreshPracticeDateCache();
    }
  }

  const canSubmit = !catErr && !studentErr && getTeacherPointCategories().length > 0 && students.length > 0;
  if (submitBtn) submitBtn.disabled = !canSubmit;

  renderStaffCalendar();
  updateStaffCalendarToggle();

  form.addEventListener('submit', async (e) => {
    e.preventDefault();
    if (!categorySelect || !studentSelect || !pointsInput) return;

    const category = String(selectedPromptCategory || categorySelect.value || "").trim();
    const points = Number(pointsInput.value);
    const notes = notesInput?.value?.trim() || '';
    const dates = Array.from(selectedDates);
    const studentIds = Array.from(studentSelect.selectedOptions).map(o => o.value);

    if (!category || !studentIds.length || !dates.length || !Number.isFinite(points)) {
      if (msgEl) {
        msgEl.textContent = "Please choose a category, at least one student, at least one date, and valid points.";
        msgEl.style.display = "block";
        msgEl.style.color = "#c62828";
      }
      return;
    }

    const isStaff = roles.includes('admin') || roles.includes('teacher');
    const status = isStaff ? 'approved' : 'pending';
    const baseRows = [];
    studentIds.forEach(studentId => {
      dates.forEach(date => {
        baseRows.push({
          userId: studentId,
          date,
          category,
          points,
          notes,
          status,
          created_by: authUserId,
          studio_id: studioId
        });
      });
    });

    let rowsToInsert = baseRows;
    let duplicateRows = [];
    const isPracticeCategory = String(category || "").trim().toLowerCase() === "practice";
    if (isPracticeCategory) {
      duplicateRows = [];
      rowsToInsert = [];
      for (const row of baseRows) {
        const userId = String(row.userId || "").trim();
        const date = String(row.date || "").slice(0, 10);
        const existingDates = practiceDatesByStudentId.get(userId) || new Set();
        if (date && existingDates.has(date)) {
          duplicateRows.push(row);
          continue;
        }
        rowsToInsert.push(row);
      }
      if (!rowsToInsert.length) {
        const allDupMsg = "Practice logs already exist for all selected dates. No new logs were added.";
        if (msgEl) {
          msgEl.textContent = allDupMsg;
          msgEl.style.display = 'block';
          msgEl.style.color = '#c62828';
        }
        return;
      }
    } else {
      try {
        const duplicateNonPracticeRows = await findExistingNonPracticeDuplicates({
          rows: baseRows,
          category,
          points
        });
        if (duplicateNonPracticeRows.length) {
          const duplicateDates = Array.from(new Set(
            duplicateNonPracticeRows.map((row) => String(row?.date || "").slice(0, 10)).filter(Boolean)
          ));
          const datePreview = duplicateDates.slice(0, 3).join(", ");
          const extraDateCount = Math.max(duplicateDates.length - 3, 0);
          const duplicateMsg = `It looks like this log was already submitted for ${duplicateNonPracticeRows.length} entr${duplicateNonPracticeRows.length === 1 ? "y" : "ies"} (same category, points, and date).${datePreview ? ` Dates: ${datePreview}${extraDateCount ? ` (+${extraDateCount} more)` : ""}.` : ""} Are you sure you want to submit another one?`;
          const proceed = window.confirm(duplicateMsg);
          if (!proceed) {
            if (msgEl) {
              msgEl.textContent = "Submission canceled. Duplicate logs were detected.";
              msgEl.style.display = "block";
              msgEl.style.color = "#c62828";
            }
            return;
          }
        }
      } catch (duplicateCheckError) {
        console.error("[QuickLog] failed duplicate check", duplicateCheckError);
        if (msgEl) {
          msgEl.textContent = "Unable to verify duplicates. No logs were submitted.";
          msgEl.style.display = "block";
          msgEl.style.color = "#c62828";
        }
        return;
      }
    }

    const includeApproval = isStaff;
    const approvedUserIds = includeApproval
      ? Array.from(new Set(rowsToInsert.map((row) => String(row.userId || "").trim()).filter(Boolean)))
      : [];
    const levelSnapshotsBefore = approvedUserIds.length
      ? await fetchStudentLevelSnapshots(approvedUserIds)
      : new Map();
    const result = await insertLogsWithApproval(rowsToInsert, includeApproval);
    if (!result.ok) {
      console.error('[QuickLog] insert failed', result.error);
      if (msgEl) {
        msgEl.textContent = 'Failed to submit logs.';
        msgEl.style.display = 'block';
        msgEl.style.color = '#c62828';
      }
      return;
    }

    if (approvedUserIds.length) {
      await recalculateUsersAfterApprovedBatch(approvedUserIds, levelSnapshotsBefore, {
        studioId
      });
    }

    if (isPracticeCategory) {
      rowsToInsert.forEach((row) => {
        const userId = String(row.userId || "").trim();
        const date = String(row.date || "").slice(0, 10);
        if (!userId || !date) return;
        if (!practiceDatesByStudentId.has(userId)) practiceDatesByStudentId.set(userId, new Set());
        practiceDatesByStudentId.get(userId).add(date);
      });
      renderStaffCalendar();
      updateStaffCalendarToggle();
    }

    if (msgEl) {
      if (duplicateRows.length) {
        msgEl.textContent = "Some selected practice dates already had logs and were skipped. The remaining new dates were saved.";
      } else {
        msgEl.textContent = `Logged ${rowsToInsert.length} entries`;
      }
      msgEl.style.display = 'block';
      msgEl.style.color = '#0b7a3a';
    }
    clearStaffQuickLogForm();
    // Success feedback is shown inline via `msgEl` for quick log submissions.
    dispatchTutorialAction("aa:tutorial-staff-quick-log-complete");
  });
}

