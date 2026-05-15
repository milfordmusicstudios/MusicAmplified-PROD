import { supabase } from "./supabaseClient.js";
import { ensureStudioContextAndRoute } from "./studio-routing.js";

const OFFSETS = [0, 14, -14, 28, -28];
const PAD_X = 28;
const brokenAvatarUrls = new Set();

document.addEventListener("DOMContentLoaded", async () => {
  const routeResult = await ensureStudioContextAndRoute({ redirectHome: false });
  if (routeResult?.redirected) return;

  const popup = document.getElementById("loadingPopup");
  const loadingText = document.getElementById("loadingMessage");
  const countEl = document.getElementById("leaderboardCount");
  const container = document.getElementById("leaderboardBars") || document.getElementById("leaderboardContainer");

  const messages = [
    "🎶 Loading the rhythm of success…",
    "🎸 Tuning up the strings of greatness…",
    "🥁 Drumming up some excitement…",
    "🎹 Hitting all the right keys…",
    "🎤 Mic check... 1, 2, 3, almost there!"
  ];
  if (loadingText) loadingText.textContent = messages[Math.floor(Math.random() * messages.length)];
  if (popup) popup.style.display = "flex";

  const activeStudioId = localStorage.getItem("activeStudioId");
  if (!activeStudioId) {
    if (container) container.innerHTML = "<p class=\"empty-state\">No studio selected.</p>";
    if (popup) popup.style.display = "none";
    return;
  }

  try {
    const { data: levels, error: levelsErr } = await supabase
      .from("levels")
      .select("*")
      .order("minPoints", { ascending: true });
    if (levelsErr || !levels?.length) throw levelsErr || new Error("No levels found.");

    window.__LEVELS_ASC__ = [...levels];
    const levelsDesc = [...levels].sort((a, b) => (b.minPoints ?? 0) - (a.minPoints ?? 0));

    renderLevelBars(container, levelsDesc);

    const studentIds = await fetchStudentIds(activeStudioId);
    if (!studentIds.length) {
      if (container) container.innerHTML = "<p class=\"empty-state\">No students found for this studio.</p>";
      if (countEl) countEl.textContent = "Showing 0 students";
      if (popup) popup.style.display = "none";
      return;
    }

    const students = await fetchStudentsByIds(studentIds, activeStudioId);
    if (!students.length) {
      if (container) container.innerHTML = "<p class=\"empty-state\">No students found for this studio.</p>";
      if (countEl) countEl.textContent = "Showing 0 students";
      if (popup) popup.style.display = "none";
      return;
    }

    const totals = buildTotalsFromUsers(students);

    const activeStudentId = localStorage.getItem("aa.activeStudentId") || null;
    const placements = buildPlacements(students, totals, levels, activeStudentId);
    if (countEl) countEl.textContent = `Showing ${placements.length} students`;
    console.log("[Leaderboard] render summary", {
      totalStudentsFetched: studentIds.length,
      inactiveStudentsRemoved: Math.max(0, studentIds.length - students.length),
      finalLeaderboardStudentsRendered: placements.length,
      activeStatusFieldsUsed: ["users.active = true", "users.deactivated_at is null", "users.showonleaderboard is not false"],
      dataSource: "studio_members user ids + users table filtered query"
    });
    renderAvatars(placements);
    initLeaderboardZoom(placements);

    const reposition = debounce(() => positionAvatars(placements), 150);
    window.addEventListener("resize", reposition);
  } catch (err) {
    console.error("[Leaderboard] load failed", err);
    if (container) container.innerHTML = "<p class=\"empty-state\">Failed to load leaderboard.</p>";
  } finally {
    if (popup) popup.style.display = "none";
  }
});

function renderLevelBars(container, levelsDesc) {
  if (!container) return;
  container.innerHTML = "";
  const levelsAsc = Array.isArray(window.__LEVELS_ASC__) ? window.__LEVELS_ASC__ : [];

  levelsDesc.forEach(level => {
    const row = document.createElement("div");
    row.className = "level-row";
    row.dataset.level = String(level.id);
    const rangeLabel = formatLevelRange(level, levelsAsc);

    const badge = document.createElement("img");
    badge.className = "level-badge";
    badge.src = level.badge || `images/levelBadges/level${level.id}.png`;
    badge.alt = `Level ${level.id}`;
    badge.title = `Level ${level.id}: ${rangeLabel}`;
    badge.tabIndex = 0;
    badge.setAttribute("role", "button");
    badge.setAttribute("aria-label", `Level ${level.id}: ${rangeLabel}`);
    badge.dataset.levelId = String(level.id);
    badge.dataset.levelRange = rangeLabel;
    badge.addEventListener("mouseenter", () => {
      const rect = badge.getBoundingClientRect();
      showOverlay({
        x: rect.right,
        y: rect.top,
        levelNumber: badge.dataset.levelId || "",
        levelRange: badge.dataset.levelRange || ""
      });
    });
    badge.addEventListener("mouseleave", () => {
      document.getElementById("lb-overlay-card")?.remove();
    });
    badge.addEventListener("focus", () => {
      const rect = badge.getBoundingClientRect();
      showOverlay({
        x: rect.right,
        y: rect.top,
        levelNumber: badge.dataset.levelId || "",
        levelRange: badge.dataset.levelRange || ""
      });
    });
    badge.addEventListener("blur", () => {
      document.getElementById("lb-overlay-card")?.remove();
    });

    const bar = document.createElement("div");
    bar.className = "level-bar";
    bar.id = `levelBar-${level.id}`;
    if (level.color) {
      bar.style.background = level.color;
      bar.style.borderColor = darkenColor(level.color);
    }

    row.appendChild(badge);
    row.appendChild(bar);
    container.appendChild(row);
  });
}

async function fetchStudentIds(studioId) {
  try {
    const { data, error } = await supabase
      .from("studio_members")
      .select("user_id, roles")
      .eq("studio_id", studioId)
      .contains("roles", ["student"]);
    if (error) throw error;
    const ids = (data || []).map(r => r.user_id).filter(Boolean);
    if (ids.length) return ids;
  } catch (err) {
    console.warn("[Leaderboard] studio_members fallback", err);
  }

  const { data: logs, error } = await supabase
    .from("logs")
    .select("userId")
    .eq("studio_id", studioId)
    .eq("status", "approved");
  if (error) {
    console.error("[Leaderboard] logs fallback failed", error);
    return [];
  }
  return Array.from(new Set((logs || []).map(l => l.userId).filter(Boolean)));
}

async function fetchStudentsByIds(ids, studioId) {
  if (!ids.length) return [];
  const { data, error } = await supabase
    .from("users")
    .select("id, firstName, lastName, avatarUrl, roles, points, level, active, deactivated_at, showonleaderboard")
    .in("id", ids)
    .eq("studio_id", studioId)
    .eq("active", true)
    .is("deactivated_at", null)
    .not("avatarUrl", "is", null)
    .or("showonleaderboard.is.true,showonleaderboard.is.null");
  if (error) {
    console.error("[Leaderboard] users fetch failed", error);
    return [];
  }
  const students = (data || []).filter(u => {
    const roles = Array.isArray(u.roles) ? u.roles : [u.roles].filter(Boolean);
    return roles.includes("student") && isActiveLeaderboardStudent(u) && hasProfileAvatar(u);
  });
  console.log("[Leaderboard] active student filter", {
    totalStudentsFetched: ids.length,
    activeStudentsReturnedFromUsersQuery: Array.isArray(data) ? data.length : 0,
    inactiveStudentsRemoved: Math.max(0, ids.length - students.length),
    finalLeaderboardStudentsRendered: students.length,
    activeStatusFieldsUsed: ["active", "deactivated_at", "showonleaderboard"]
  });
  return students;
}

function isActiveLeaderboardStudent(student) {
  if (!student) return false;
  if (student.deactivated_at) return false;
  if (student.active !== true) return false;
  if (student.showonleaderboard === false) return false;
  return true;
}

function hasProfileAvatar(student) {
  return Boolean(getValidAvatarUrl(student?.avatarUrl));
}

function buildTotalsFromUsers(students) {
  const totals = {};
  (students || []).filter(student => isActiveLeaderboardStudent(student) && hasProfileAvatar(student)).forEach(student => {
    const id = student.id;
    totals[id] = Number(student.points || 0);
  });
  return totals;
}

function buildPlacements(students, totals, levels, activeStudentId) {
  const placements = [];
  const perLevelCount = {};

  students.filter(student => isActiveLeaderboardStudent(student) && hasProfileAvatar(student)).forEach(student => {
    const total = totals[student.id] || 0;
    const level = getLevelForPoints(total, levels);
    if (!level) return;

    const range = getLevelRange(level, levels);
    const pointsInto = Math.max(0, total - range.minPoints);
    const ratio = range.span > 0 ? Math.min(1, pointsInto / range.span) : 1;

    const index = perLevelCount[level.id] || 0;
    perLevelCount[level.id] = index + 1;
    const offset = OFFSETS[index % OFFSETS.length];

    placements.push({
      student,
      total,
      levelId: level.id,
      ratio,
      offset,
      isSelf: activeStudentId && String(student.id) === String(activeStudentId)
    });
  });

  return placements;
}

function renderAvatars(placements) {
  placements.forEach(p => {
    const bar = document.getElementById(`levelBar-${p.levelId}`);
    if (!bar) return;

    const avatar = document.createElement("img");
    avatar.className = `lb-avatar leaderboard-avatar${p.isSelf ? " is-self" : ""}`;
    const avatarUrl = getValidAvatarUrl(p.student.avatarUrl);
    if (!avatarUrl) return;
    avatar.src = avatarUrl;
    const first = String(p.student.firstName ?? "").trim();
    const last = String(p.student.lastName ?? "").trim();
    const fullName = last && first ? `${last}, ${first}` : (last || first || "Student");
    avatar.alt = fullName;
    avatar.title = `${fullName} — ${p.total} pts`;
    avatar.dataset.levelId = String(p.levelId);
    avatar.dataset.ratio = String(p.ratio);
    avatar.dataset.offset = String(p.offset);
    avatar.dataset.points = String(p.total);
    avatar.dataset.name = fullName;
    avatar.dataset.avatarUrl = avatarUrl || "";
    avatar.addEventListener("error", () => handleBrokenAvatar(avatar, p.student, fullName), { once: true });

    avatar.style.left = `${PAD_X}px`;
    avatar.style.top = `calc(50% + ${p.offset}px)`;

    bar.appendChild(avatar);
    p.el = avatar;
    p.bar = bar;
  });

  requestAnimationFrame(() => positionAvatars(placements));
}

function getValidAvatarUrl(value) {
  const raw = String(value || "").trim();
  if (!raw || raw === "null" || raw === "undefined") return "";
  try {
    const parsed = new URL(raw, window.location.href);
    if (!["http:", "https:", "data:", "blob:"].includes(parsed.protocol)) {
      console.warn("[Leaderboard] blocked avatar URL protocol", { avatarUrl: raw });
      return "";
    }
    return parsed.href;
  } catch (error) {
    console.warn("[Leaderboard] invalid avatar URL", { avatarUrl: raw, error });
    return "";
  }
}

function handleBrokenAvatar(avatar, student, fullName) {
  const brokenUrl = avatar.dataset.avatarUrl || avatar.currentSrc || avatar.src || "";
  if (brokenUrl && !brokenAvatarUrls.has(brokenUrl)) {
    brokenAvatarUrls.add(brokenUrl);
    console.warn("[Leaderboard] broken avatar URL", {
      studentId: student?.id || null,
      name: fullName,
      avatarUrl: brokenUrl
    });
  }
  avatar.remove();
}

function initLeaderboardZoom(placements) {
  const slider = document.getElementById("leaderboardZoom");
  const output = document.getElementById("leaderboardZoomValue");
  const bars = document.getElementById("leaderboardBars");
  const wrapper = document.querySelector(".leaderboard-scroll-wrapper");
  if (!bars) return;

  let zoom = slider ? Number(slider.value || 1) : 1;
  let pinchStartDistance = 0;
  let pinchStartZoom = zoom;

  const applyZoom = (nextZoom) => {
    zoom = Math.max(1, Math.min(4, Number(nextZoom) || 1));
    bars.style.setProperty("--leaderboard-width", `${Math.round(zoom * 100)}%`);
    bars.style.setProperty("--leaderboard-avatar-size", `${Math.round(44 + (zoom - 1) * 4)}px`);
    if (slider) slider.value = String(zoom);
    if (output) output.textContent = `${zoom.toFixed(zoom % 1 ? 2 : 0).replace(/\.00$/, "")}x`;
    console.log("[Leaderboard] zoom changed", { zoom, mode: "layout-width" });
    requestAnimationFrame(() => positionAvatars(placements));
  };

  if (slider) {
    slider.addEventListener("input", () => applyZoom(slider.value));
  }

  if (wrapper) {
    wrapper.addEventListener("wheel", (event) => {
      if (!event.ctrlKey && !event.metaKey) return;
      event.preventDefault();
      applyZoom(zoom + (event.deltaY < 0 ? 0.25 : -0.25));
    }, { passive: false });

    wrapper.addEventListener("touchstart", (event) => {
      if (event.touches.length !== 2) return;
      pinchStartDistance = getTouchDistance(event.touches);
      pinchStartZoom = zoom;
    }, { passive: true });

    wrapper.addEventListener("touchmove", (event) => {
      if (event.touches.length !== 2 || !pinchStartDistance) return;
      event.preventDefault();
      const ratio = getTouchDistance(event.touches) / pinchStartDistance;
      applyZoom(pinchStartZoom * ratio);
    }, { passive: false });
  }

  applyZoom(zoom);
}

function getTouchDistance(touches) {
  const [a, b] = touches;
  if (!a || !b) return 0;
  return Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY);
}

function positionAvatars(placements) {
  placements.forEach(p => {
    const bar = p.bar;
    const avatar = p.el;
    if (!bar || !avatar) return;

    const barWidth = bar.clientWidth || 0;
    const pad = Math.min(PAD_X, Math.max(12, barWidth * 0.08));
    const x = pad + p.ratio * Math.max(1, barWidth - pad * 2);

    avatar.style.left = `${x}px`;
  });
}

function getLevelForPoints(points, levels) {
  const list = Array.isArray(levels) ? levels : [];
  let current = list[0] || null;
  list.forEach(level => {
    if (points >= (level.minPoints ?? 0)) current = level;
  });
  return current;
}

function getLevelRange(level, levels) {
  const list = Array.isArray(levels) ? levels : [];
  const idx = list.findIndex(l => l.id === level.id);
  const next = idx >= 0 ? list[idx + 1] : null;
  const minPoints = level.minPoints ?? 0;
  const maxPoints = level.maxPoints ?? next?.minPoints ?? (minPoints + 1);
  const span = Math.max(1, maxPoints - minPoints);
  return { minPoints, maxPoints, span };
}

function formatLevelRange(level, levels) {
  const range = getLevelRange(level, levels);
  const min = Number(range.minPoints || 0);
  const explicitMax = Number(level?.maxPoints);
  const hasExplicitMax = Number.isFinite(explicitMax) && explicitMax >= min;
  const idx = Array.isArray(levels) ? levels.findIndex(l => l.id === level.id) : -1;
  const next = idx >= 0 ? levels[idx + 1] : null;
  if (!next && !hasExplicitMax) return `${min}+ points`;
  const max = hasExplicitMax ? explicitMax : Math.max(min, Number(next?.minPoints || min + 1) - 1);
  return `${min}-${max} points`;
}

function darkenColor(hex, amt = -30) {
  try {
    let col = hex.replace("#", "");
    if (col.length === 3) col = col.split("").map(c => c + c).join("");
    const num = parseInt(col, 16);
    let r = (num >> 16) + amt, g = ((num >> 8) & 0x00FF) + amt, b = (num & 0x0000FF) + amt;
    r = Math.max(0, Math.min(255, r));
    g = Math.max(0, Math.min(255, g));
    b = Math.max(0, Math.min(255, b));
    return `rgb(${r},${g},${b})`;
  } catch {
    return "#222";
  }
}

function debounce(fn, wait) {
  let t;
  return (...args) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...args), wait);
  };
}

/* ====== Gated popup overlay (append-only logic) ====== */
function clamp(v, min, max) { return Math.max(min, Math.min(max, v)); }
function computeLevelAndPercent(points) {
  const levelsAsc = Array.isArray(window.__LEVELS_ASC__) ? window.__LEVELS_ASC__ : [];
  if (!levelsAsc.length) return { levelNumber: 1, percentWithin: 0 };

  let idx = 0;
  for (let i = 0; i < levelsAsc.length; i++) {
    if (points >= (levelsAsc[i].minPoints ?? 0)) idx = i; else break;
  }
  const current = levelsAsc[idx];
  const next = levelsAsc[idx + 1];

  const levelNumber = (current?.id != null)
    ? Number(current.id)
    : (idx + 1);

  if (!next) return { levelNumber, percentWithin: 1 };

  const span = Math.max(1, (next.minPoints - current.minPoints));
  const pct = clamp((points - current.minPoints) / span, 0, 1);
  return { levelNumber, percentWithin: pct };
}
function squareFromPercent(p01) {
  return clamp(Math.ceil(p01 * 12), 1, 12);
}
function viewerCanSeeNames() {
  const role = (localStorage.getItem('activeRole') || '').toLowerCase();
  return role === 'admin' || role === 'teacher';
}
function viewerIsAdmin() {
  const role = (localStorage.getItem('activeRole') || '').toLowerCase();
  return role === 'admin';
}
function ensureOverlay() {
  let card = document.getElementById('lb-overlay-card');
  if (!card) {
    card = document.createElement('div');
    card.id = 'lb-overlay-card';
    Object.assign(card.style, {
      position: 'fixed',
      zIndex: '9999',
      background: 'white',
      border: '1px solid rgba(0,0,0,.12)',
      borderRadius: '10px',
      boxShadow: '0 8px 24px rgba(0,0,0,.18)',
      padding: '10px 12px',
      fontFamily: 'system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif',
      fontSize: '14px',
      color: '#222'
    });
    card.addEventListener('click', () => card.remove());
    document.addEventListener('keydown', (e) => { if (e.key === 'Escape') card.remove(); });
    document.body.appendChild(card);
  }
  return card;
}
function showOverlay({ x, y, name, points, levelNumber, square, levelRange }) {
  const card = ensureOverlay();
  const nameRow = (name && viewerCanSeeNames())
    ? `<div style="font-weight:700; color:#00477d; margin-bottom:4px;">${name}</div>`
    : '';

  const squarePart = viewerIsAdmin() ? ` • square <b>${square}</b>/12` : '';

  card.innerHTML = `
    ${nameRow}
    ${points === undefined ? "" : `<div style="margin-bottom:2px;">Points: <b>${Number(points) || 0}</b></div>`}
    <div>Level <b>${levelNumber}</b>${squarePart}</div>
    ${levelRange ? `<div>Range: <b>${levelRange}</b></div>` : ""}
  `;

  const pad = 8;
  const vw = Math.max(document.documentElement.clientWidth, window.innerWidth || 0);
  const vh = Math.max(document.documentElement.clientHeight, window.innerHeight || 0);
  const rect = card.getBoundingClientRect();
  let left = x + pad;
  let top = y + pad;
  if (left + rect.width > vw - 8) left = vw - rect.width - 8;
  if (top + rect.height > vh - 8) top = vh - rect.height - 8;

  card.style.left = Math.max(8, left) + 'px';
  card.style.top  = Math.max(8, top)  + 'px';
}

document.addEventListener('click', (e) => {
  const levelBadge = e.target.closest('.level-badge');
  if (levelBadge) {
    showOverlay({
      x: e.clientX,
      y: e.clientY,
      levelNumber: levelBadge.dataset.levelId || "",
      levelRange: levelBadge.dataset.levelRange || ""
    });
    return;
  }

  const el = e.target.closest('.leaderboard-avatar');
  if (!el) {
    const card = document.getElementById('lb-overlay-card');
    if (card && !card.contains(e.target)) card.remove();
    return;
  }

  const points = Number(el.dataset.points || '0');
  const { levelNumber, percentWithin } = computeLevelAndPercent(points);
  const square = squareFromPercent(percentWithin);

  const name = el.dataset.name || null;

  showOverlay({
    x: e.clientX,
    y: e.clientY,
    name,
    points,
    levelNumber,
    square
  });
});

document.addEventListener("keydown", (e) => {
  const levelBadge = e.target?.closest?.(".level-badge");
  if (!levelBadge || (e.key !== "Enter" && e.key !== " ")) return;
  e.preventDefault();
  const rect = levelBadge.getBoundingClientRect();
  showOverlay({
    x: rect.right,
    y: rect.top,
    levelNumber: levelBadge.dataset.levelId || "",
    levelRange: levelBadge.dataset.levelRange || ""
  });
});
