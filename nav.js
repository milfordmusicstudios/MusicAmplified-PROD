// Viewer context is lazily loaded so nav can fall back when optional helpers
// cannot load. Nav mode is always resolved from showStaffUI semantics.
import { NAV_CONFIG } from "./nav-config.js";
import { getViewerContextSafely } from "./nav-helpers.js";
import { supabase } from "./supabaseClient.js";

const ADMIN_MODE_KEY = "aa.adminModeEnabled";
const AUTH_PAGES = new Set(["login.html", "signup.html", "forgot-password.html", "info.html", "info"]);
const STAFF_ONLY_PAGES = new Set([
  "review-logs.html",
  "log-points.html",
  "manage-users.html",
  "invite-staff.html",
  "invite-student.html",
  "studio-settings-hub.html",
  "studio-settings.html"
]);

function getPathName() {
  const raw = window.location.pathname || "";
  const file = raw.split("/").pop();
  return file || "index.html";
}

function getAdminModeEnabled() {
  if (typeof window.adminModeEnabled === "boolean") return window.adminModeEnabled;
  return localStorage.getItem(ADMIN_MODE_KEY) === "1";
}

async function resolveShowStaffUI() {
  const ctx = await getViewerContextSafely();
  if (!ctx?.viewerUserId) return false;

  const accountRoles = Array.isArray(ctx.accountRoles)
    ? ctx.accountRoles.map((role) => String(role || "").toLowerCase())
    : Array.isArray(ctx.viewerRoles)
      ? ctx.viewerRoles.map((role) => String(role || "").toLowerCase())
    : [];

  const activeProfileId = ctx.activeProfileId || ctx.viewerUserId;
  let activeIsStudent = false;

  try {
    const { data: activeProfile } = await supabase
      .from("users")
      .select("parent_uuid, roles")
      .eq("id", activeProfileId)
      .maybeSingle();

    const roleList = Array.isArray(activeProfile?.roles)
      ? activeProfile.roles.map((role) => String(role || "").toLowerCase())
      : [];
    activeIsStudent = Boolean(activeProfile?.parent_uuid) || roleList.includes("student");
  } catch (err) {
    console.warn("[nav] active profile lookup failed; defaulting to student nav", err);
    return false;
  }

  const adminMode = getAdminModeEnabled();
  return (accountRoles.includes("admin") || accountRoles.includes("teacher")) && (!activeIsStudent || adminMode);
}

function getNavItems(showStaffUI) {
  return showStaffUI ? NAV_CONFIG.teacher : NAV_CONFIG.student;
}

function renderNav(showStaffUI) {
  const mount = document.getElementById("app-bottom-nav");
  if (!mount) return;

  const items = getNavItems(showStaffUI);
  const current = getPathName();

  const nav = document.createElement("nav");
  nav.className = "bottom-nav";
  nav.setAttribute("aria-label", "Primary");

  items.forEach((item) => {
    const link = document.createElement("a");
    link.href = item.href;
    link.className = "bottom-nav-link";
    link.dataset.navHref = item.href;
    link.textContent = item.label;
    if (item.matchPaths.includes(current)) {
      link.classList.add("is-active");
      link.setAttribute("aria-current", "page");
    }
    nav.appendChild(link);
  });

  mount.innerHTML = "";
  mount.appendChild(nav);
}

function setNavNotificationDot(href, enabled) {
  const selector = `.bottom-nav-link[data-nav-href="${href}"]`;
  const link = document.querySelector(selector);
  if (!(link instanceof HTMLElement)) return;
  link.classList.toggle("has-notification", Boolean(enabled));
  let dot = link.querySelector(".bottom-nav-dot");
  if (enabled) {
    if (!(dot instanceof HTMLElement)) {
      dot = document.createElement("span");
      dot.className = "bottom-nav-dot";
      dot.setAttribute("aria-hidden", "true");
      link.appendChild(dot);
    }
  } else if (dot instanceof HTMLElement) {
    dot.remove();
  }
}

async function hasStudentNeedsInfoLogs(ctx) {
  const activeStudentId = String(
    localStorage.getItem("aa.activeStudentId")
    || ctx?.activeProfileId
    || ctx?.viewerUserId
    || ""
  ).trim();
  if (!activeStudentId) return false;
  let query = supabase
    .from("logs")
    .select("id", { count: "exact", head: true })
    .eq("userId", activeStudentId)
    .eq("status", "needs info");
  if (ctx?.studioId) query = query.eq("studio_id", ctx.studioId);
  const { count, error } = await query;
  if (error) {
    console.warn("[nav] needs-info badge lookup failed", error);
    return false;
  }
  return Number(count || 0) > 0;
}

async function hasStaffPendingLogs(ctx) {
  const studioId = String(ctx?.studioId || "").trim();
  if (!studioId) return false;
  const isAdmin = Array.isArray(ctx?.viewerRoles) && ctx.viewerRoles.map(r => String(r || "").toLowerCase()).includes("admin");
  const isTeacher = Array.isArray(ctx?.viewerRoles) && ctx.viewerRoles.map(r => String(r || "").toLowerCase()).includes("teacher");
  if (!isAdmin && !isTeacher) return false;

  let query = supabase
    .from("logs")
    .select("id", { count: "exact", head: true })
    .eq("studio_id", studioId)
    .eq("status", "pending");

  if (isTeacher && !isAdmin) {
    const { data: studentRows, error: studentErr } = await supabase
      .from("users")
      .select("id,teacherIds")
      .eq("studio_id", studioId);
    if (studentErr) {
      console.warn("[nav] teacher pending lookup users failed", studentErr);
      return false;
    }
    const teacherId = String(ctx?.viewerUserId || "").trim();
    const studentIds = (Array.isArray(studentRows) ? studentRows : [])
      .filter((row) => {
        const ids = Array.isArray(row?.teacherIds) ? row.teacherIds.map(String) : [];
        return ids.includes(teacherId);
      })
      .map((row) => String(row?.id || "").trim())
      .filter(Boolean);
    if (!studentIds.length) return false;
    query = query.in("userId", studentIds);
  }

  const { count, error } = await query;
  if (error) {
    console.warn("[nav] pending badge lookup failed", error);
    return false;
  }
  return Number(count || 0) > 0;
}

function isLevelUpNotification(row) {
  const type = String(row?.type || "").trim().toLowerCase();
  if (type === "level_up" || type === "level_completed") return true;
  const message = String(row?.message || "").toLowerCase();
  return message.includes("reached level") || message.includes("advanced to level") || message.includes("completed level");
}

function shouldIgnoreLevelNotification(row) {
  if (!isLevelUpNotification(row)) return false;
  const raw = `${row?.title || ""} ${row?.message || row?.body || ""}`.toLowerCase();
  if (raw.includes("advanced to") || /\breached\s+level\b/i.test(raw) || raw.includes("level level")) {
    return true;
  }
  if (/^\s*milford\s+music\s+/i.test(String(row?.message || ""))) {
    return true;
  }
  const date = new Date(row?.created_at || "");
  if (Number.isNaN(date.getTime())) return true;
  const year = date.getFullYear();
  return year < 2024 || year > new Date().getFullYear() + 1;
}

function isNotificationRead(row) {
  if (!row) return false;
  return row.read === true;
}

function sortNotificationsNewestFirst(rows) {
  return [...rows].sort((a, b) => {
    const aTime = new Date(a?.created_at || 0).getTime();
    const bTime = new Date(b?.created_at || 0).getTime();
    return bTime - aTime;
  });
}

function mergeNotificationRows(rowSets, limit) {
  const seen = new Set();
  const merged = [];
  rowSets.flat().forEach((row) => {
    if (!row) return;
    const key = String(row?.id || `${row?.created_at || ""}:${row?.message || ""}:${row?.userId || row?.user_id || ""}`);
    if (seen.has(key)) return;
    seen.add(key);
    merged.push(row);
  });
  return sortNotificationsNewestFirst(merged).slice(0, limit);
}

async function hasStaffUnreadLevelUpNotifications(ctx) {
  const viewerUserId = String(ctx?.viewerUserId || "").trim();
  if (!viewerUserId) return false;
  const activeStudioId = String(ctx?.studioId || "").trim();
  const limit = 80;
  const attempts = [
    { label: "user_id+studio_id", userKey: "user_id", includeStudio: Boolean(activeStudioId) },
    { label: "user_id:no_studio_filter", userKey: "user_id", includeStudio: false },
    { label: "legacy_userId+studio_id", userKey: "userId", includeStudio: Boolean(activeStudioId), legacyOnly: true }
  ];
  console.log("[NotifDiag][nav.js][hasStaffUnreadLevelUpNotifications] query plan", {
    source: "nav.js::hasStaffUnreadLevelUpNotifications",
    viewerUserId,
    activeStudioId: activeStudioId || null,
    limit,
    attempts: attempts.map((attempt) => attempt.label)
  });
  const rowSets = [];
  const errors = [];
  for (const attempt of attempts) {
    const filters = {
      [attempt.userKey]: viewerUserId,
      studio_id: attempt.includeStudio ? activeStudioId : "(omitted)",
      orderBy: "created_at desc",
      limit
    };
    console.log("[NotifDiag][nav.js][hasStaffUnreadLevelUpNotifications] query start", {
      source: "nav.js::hasStaffUnreadLevelUpNotifications",
      attempt: attempt.label,
      filters
    });
    let query = supabase
      .from("notifications")
      .select("*")
      .eq(attempt.userKey, viewerUserId)
      .order("created_at", { ascending: false })
      .limit(limit);
    if (attempt.includeStudio) {
      query = query.eq("studio_id", activeStudioId);
    }
    const { data, error } = await query;
    const count = Array.isArray(data) ? data.length : 0;
    console.log("[NotifDiag][nav.js][hasStaffUnreadLevelUpNotifications] query result", {
      source: "nav.js::hasStaffUnreadLevelUpNotifications",
      attempt: attempt.label,
      filters,
      count,
      error: error ?? null
    });
    if (error) {
      errors.push({ attempt: attempt.label, error });
      continue;
    }
    if (attempt.legacyOnly && rowSets.length > 0) continue;
    if (count > 0) rowSets.push(data);
  }
  if (errors.length === attempts.length) {
    console.warn("[nav] level-up notification lookup failed", errors[0]?.error || errors);
    return false;
  }
  const rows = mergeNotificationRows(rowSets, limit);
  const unreadLevelUpCount = rows.filter((row) =>
    isLevelUpNotification(row) &&
    !shouldIgnoreLevelNotification(row) &&
    !isNotificationRead(row)
  ).length;
  console.log("[NotifDiag][nav.js][hasStaffUnreadLevelUpNotifications] query result", {
    source: "nav.js::hasStaffUnreadLevelUpNotifications",
    queriedUserId: viewerUserId,
    queriedStudioId: activeStudioId || null,
    unreadReadFilterLogic: "visible level-completion notification && row.read !== true",
    totalCount: rows.length,
    unreadLevelUpCount,
    errorCount: errors.length,
    errors
  });
  return unreadLevelUpCount > 0;
}

async function applyNavNotificationDots(showStaffUI) {
  try {
    const ctx = await getViewerContextSafely();
    if (!ctx?.viewerUserId) return;
    if (showStaffUI) {
      const hasPending = await hasStaffPendingLogs(ctx);
      const hasUnreadLevelUp = await hasStaffUnreadLevelUpNotifications(ctx);
      setNavNotificationDot("review-logs.html", hasPending || hasUnreadLevelUp);
      setNavNotificationDot("my-points.html", false);
    } else {
      const hasNeedsInfo = await hasStudentNeedsInfoLogs(ctx);
      setNavNotificationDot("my-points.html", hasNeedsInfo);
      setNavNotificationDot("review-logs.html", false);
    }
  } catch (error) {
    console.warn("[nav] notification dot update failed", error);
  }
}

async function refreshNavNotificationDots() {
  const current = getPathName();
  if (AUTH_PAGES.has(current)) return;
  const showStaffUI = await resolveShowStaffUI();
  if (!document.querySelector(".bottom-nav-link")) {
    renderNav(showStaffUI);
  }
  await applyNavNotificationDots(showStaffUI);
}

document.addEventListener("DOMContentLoaded", async () => {
  const current = getPathName();

  if (AUTH_PAGES.has(current)) {
    renderNav(false);
    return;
  }

  const showStaffUI = await resolveShowStaffUI();

  if (!showStaffUI && STAFF_ONLY_PAGES.has(current)) {
    window.location.replace("home.html");
    return;
  }

  renderNav(showStaffUI);
  await applyNavNotificationDots(showStaffUI);
});

window.refreshNavNotificationDots = refreshNavNotificationDots;
window.addEventListener("focus", () => {
  void refreshNavNotificationDots();
});
window.addEventListener("aa:notification-state-changed", () => {
  void refreshNavNotificationDots();
});
