import { supabase } from "./supabaseClient.js";
import { createLevelCompletedNotification, getViewerContext } from './utils.js';
import { ensureStudioContextAndRoute } from "./studio-routing.js";

const BADGE_DEMO_SRC = "images/badges/demo.png";
const ACTIVE_STUDENT_STORAGE_KEY = "aa.activeStudentId";

function getBadgeImagePath(slug) {
  const raw = String(slug || "").trim();
  if (!raw) return BADGE_DEMO_SRC;
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
  return `images/badges/${fileSlug}.png`;
}

function humanizeToken(value) {
  return String(value || "")
    .replace(/_/g, " ")
    .replace(/\b\w/g, (m) => m.toUpperCase())
    .trim();
}

function formatBadgeCriteria(criteria) {
  if (!criteria || typeof criteria !== "object") return "Complete the required activity to unlock this badge.";
  const type = String(criteria.type || "").toLowerCase();

  const byType = {
    practice_logs: `Log at least ${Number(criteria.min || 0)} practice entries.`,
    practice_streak_days: `Reach a ${Number(criteria.min || 0)}-day practice streak.`,
    goals_completed: `Complete at least ${Number(criteria.min || 0)} goals.`,
    participation_logs: `Log at least ${Number(criteria.min || 0)} participation entries.`,
    technique_or_theory_completed: `Complete at least ${Number(criteria.min || 0)} technique or theory test(s).`,
    technique_completed: `Complete at least ${Number(criteria.min || 0)} technique tests.`,
    theory_completed: `Complete at least ${Number(criteria.min || 0)} theory tests.`,
    festival_participation: `Participate in at least ${Number(criteria.min || 0)} festival event(s).`,
    performance_participation: `Participate in at least ${Number(criteria.min || 0)} performance event(s).`,
    competition_participation: `Participate in at least ${Number(criteria.min || 0)} competition event(s).`,
    teacher_challenges_completed: `Complete at least ${Number(criteria.min || 0)} teacher challenge(s).`,
    memorization_points: `Earn at least ${Number(criteria.min || 0)} memorization points.`,
    lesson_books_completed: `Complete at least ${Number(criteria.min || 0)} lesson books.`,
    total_logs: `Submit at least ${Number(criteria.min || 0)} total logs.`,
    streak_repairs_used: `Use streak repair at least ${Number(criteria.min || 0)} time(s).`,
    streak_repair_tokens_unused: `Keep at least ${Number(criteria.min || 0)} streak repair token(s) unused.`,
    consecutive_membership_months: `Maintain membership for ${Number(criteria.min || 0)} consecutive month(s).`,
    practice_log_before_hour: `Log practice before ${Number(criteria.hour || 0)}:00.`,
    practice_log_after_hour: `Log practice after ${Number(criteria.hour || 0)}:00.`,
    practice_gap_return: `Return to practice after a ${Number(criteria.min_gap_days || 0)}+ day gap.`,
    distinct_categories_rolling_days: `Log ${Number(criteria.categories_min || 0)}+ categories within ${Number(criteria.window_days || 0)} days.`,
    seasonal_weeks: `During ${humanizeToken(criteria.season)}, complete ${Number(criteria.min_weeks || 0)} weeks with ${Number(criteria.min_days_per_week || 0)}+ practice days each week.`,
    repair_preserved_combo: "Use streak repair and preserve your long streak progress.",
    streak_restart_after_rhythm: "Restart and rebuild your streak after reaching rhythm-level consistency."
  };

  if (byType[type]) return byType[type];

  if (Array.isArray(criteria.requires) && criteria.requires.length) {
    const pieces = criteria.requires.map((req) => `${humanizeToken(req.metric)}: ${Number(req.min || 0)}+`);
    return `Meet all requirements: ${pieces.join(", ")}.`;
  }

  const parts = Object.entries(criteria).map(([key, val]) => `${humanizeToken(key)}: ${String(val)}`);
  return parts.length ? parts.join(" | ") : "Complete the required activity to unlock this badge.";
}

function buildFamilyEvolutionRows(catalog) {
  const defs = Array.isArray(catalog?.definitions) ? catalog.definitions : [];
  const unlockedSet = catalog?.unlockedSet instanceof Set ? catalog.unlockedSet : new Set();
  const byFamily = new Map();

  const resolveFamilyKey = (def) => {
    const familyKey = String(def?.family || "general").toLowerCase();
    const slug = String(def?.slug || "").toLowerCase();
    const criteria = def?.criteria && typeof def.criteria === "object" ? def.criteria : {};

    // "fun" badges are standalone, not an evolution chain.
    if (familyKey === "fun") {
      return `fun:${slug || "badge"}`;
    }

    // Split seasonal into separate evolution tracks by season.
    if (familyKey === "seasonal") {
      const season = String(criteria.season || "").toLowerCase();
      if (season === "winter" || slug.includes("winter")) return "seasonal:winter";
      if (season === "summer" || slug.includes("summer")) return "seasonal:summer";
      return "seasonal:other";
    }

    return familyKey;
  };

  const toFamilyLabel = (key) => {
    if (key.startsWith("fun:")) return "Fun";
    if (key === "seasonal:winter") return "Seasonal Winter";
    if (key === "seasonal:summer") return "Seasonal Summer";
    if (key === "seasonal:other") return "Seasonal";
    return key;
  };

  const isMysteryFamily = (key) => {
    if (key.startsWith("fun:")) return true;
    if (key.startsWith("seasonal:")) return true;
    if (key === "streak_repair") return true;
    return false;
  };

  defs.forEach((def) => {
    const familyKey = resolveFamilyKey(def);
    if (!byFamily.has(familyKey)) byFamily.set(familyKey, []);
    byFamily.get(familyKey).push(def);
  });

  const familyRows = [];
  Array.from(byFamily.keys()).sort((a, b) => a.localeCompare(b)).forEach((familyKey) => {
    const familyDefs = byFamily.get(familyKey).slice().sort((a, b) => {
      const ao = Number(a.sort_order || 0);
      const bo = Number(b.sort_order || 0);
      if (ao !== bo) return ao - bo;
      return Number(a.tier || 0) - Number(b.tier || 0);
    });

    const unlockedDefs = familyDefs.filter((def) => unlockedSet.has(String(def.slug || "")));
    const highestUnlocked = unlockedDefs.length ? unlockedDefs[unlockedDefs.length - 1] : null;
    const nextDef = familyDefs.find((def) => !unlockedSet.has(String(def.slug || ""))) || null;
    const displayDef = highestUnlocked || familyDefs[0] || null;
    if (!displayDef) return;

    familyRows.push({
      family: familyKey,
      familyLabel: toFamilyLabel(familyKey),
      familyDefs,
      displayDef,
      highestUnlocked,
      nextDef,
      unlocked: Boolean(highestUnlocked),
      mysteryWhenLocked: isMysteryFamily(familyKey),
      unlockedTierCount: unlockedDefs.length,
      tierTotal: familyDefs.length
    });
  });

  return familyRows;
}

async function loadBadgeCatalog({ userId, studioId }) {
  try {
    await supabase.rpc("recompute_badges_for_student", {
      p_studio_id: studioId,
      p_user_id: userId
    });
  } catch (err) {
    console.warn("[My Points] badge recompute before catalog failed", err);
  }

  const { data, error } = await supabase.rpc("get_student_badge_catalog", {
    p_studio_id: studioId,
    p_user_id: userId
  });
  if (error) throw error;

  const rows = Array.isArray(data) ? data : [];
  const earnedRows = rows
    .filter((row) => row.unlocked)
    .sort((a, b) => new Date(b.earned_at || 0) - new Date(a.earned_at || 0));
  const unlockedSet = new Set(earnedRows.map((row) => String(row.slug || "")));
  const latest = earnedRows[0] || null;

  return {
    definitions: rows.map((row) => ({
      slug: row.slug,
      name: row.name,
      family: row.family,
      tier: row.tier,
      sort_order: row.sort_order,
      criteria: row.criteria,
      is_active: row.is_active
    })),
    unlockedSet,
    latestSlug: latest ? String(latest.slug || "") : "",
    unlockedCount: unlockedSet.size
  };
}

function getLastToastLevel(userId) {
  const key = `aa:lastLevelToast:${String(userId)}`;
  const raw = localStorage.getItem(key);
  const value = parseInt(raw, 10);
  return Number.isFinite(value) ? value : null;
}

function setLastToastLevel(userId, level) {
  if (!Number.isFinite(level)) return;
  const key = `aa:lastLevelToast:${String(userId)}`;
  localStorage.setItem(key, String(level));
}

function showToast(message) {
  const toast = document.getElementById("toast");
  if (!toast) return;
  toast.textContent = message;
  toast.classList.add("show");
  clearTimeout(toast._hideTimer);
  toast._hideTimer = setTimeout(() => {
    toast.classList.remove("show");
  }, 2200);
}

async function createLevelUpNotifications({ studioId, studentUserId, studentName, level }) {
  if (!studentUserId || !level) return;
  try {
    const enteredLevel = Number(level || 0);
    const completedLevel = enteredLevel - 1;
    if (!Number.isFinite(completedLevel) || completedLevel <= 0) return;
    const result = await createLevelCompletedNotification({
      studioId,
      studentUserId,
      studentName,
      completedLevelStart: completedLevel,
      completedLevelEnd: completedLevel
    });
    if (result?.ok === false) {
      console.warn("[My Points] level-completed notification insert failed", result.error || result);
    }
  } catch (err) {
    console.warn("[My Points] level-completed notifications error", err);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  console.log("[DEBUG] My Points: Script loaded");

  const routeResult = await ensureStudioContextAndRoute({ redirectHome: false });
  if (routeResult?.redirected) return;

  const { data: sessionData } = await supabase.auth.getSession();
  if (!sessionData?.session) {
    window.location.href = "login.html";
    return;
  }

  const viewerContext = await getViewerContext();
  console.log("[Identity] viewer context", viewerContext);
  let activeStudentId = localStorage.getItem(ACTIVE_STUDENT_STORAGE_KEY);
  if (!activeStudentId && viewerContext?.activeProfileId) {
    activeStudentId = String(viewerContext.activeProfileId);
    localStorage.setItem(ACTIVE_STUDENT_STORAGE_KEY, activeStudentId);
  }
  if (!activeStudentId) {
    alert("Select a student on Home to view points.");
    window.location.href = "index.html";
    return;
  }

  const userId = activeStudentId;
  console.log("[DEBUG] Fetching logs for user ID:", userId);

  const logsTableBody = document.getElementById("logsTableBody");
  const categorySummary = document.getElementById("categorySummary");
  const pointsTitle = document.getElementById("pointsTitle");
  const levelBadge = document.getElementById("levelBadge");
  const searchInput = document.getElementById("searchInput");
  const statusFilter = document.getElementById("statusFilter");
  const showAllCategoriesBtn = document.getElementById("showAllCategoriesBtn");
  const badgeCatalogModal = document.getElementById("badgeCatalogModal");
  const badgeCatalogBody = document.getElementById("badgeCatalogBody");
  const badgeCatalogCloseBtn = document.getElementById("badgeCatalogCloseBtn");
  const needsInfoEditOverlay = document.getElementById("needsInfoEditOverlay");
  const needsInfoEditClose = document.getElementById("needsInfoEditClose");
  const needsInfoEditCancel = document.getElementById("needsInfoEditCancel");
  const needsInfoEditSubmit = document.getElementById("needsInfoEditSubmit");
  const needsInfoEditCategory = document.getElementById("needsInfoEditCategory");
  const needsInfoEditDate = document.getElementById("needsInfoEditDate");
  const needsInfoEditPoints = document.getElementById("needsInfoEditPoints");
  const needsInfoEditStatus = document.getElementById("needsInfoEditStatus");
  const needsInfoEditNotes = document.getElementById("needsInfoEditNotes");
  const needsInfoEditMsg = document.getElementById("needsInfoEditMsg");

  let allLogs = [];
  let filteredLogs = [];
  let needsInfoBlinkPlayed = false;
  let badgeCatalog = { definitions: [], unlockedSet: new Set(), latestSlug: "", unlockedCount: 0 };
  let activeCategoryFilter = "all";
  let selectedNeedsInfoLogId = "";
  const openBadgesIntent = new URLSearchParams(window.location.search).get("openBadges") === "1"
    || localStorage.getItem("aa.openBadgeCatalog") === "1";

  try {
    // ✅ Fetch logs
    console.log("[My Points] fetching logs for userId", userId);
    const { data: logs, error: logsError } = await supabase
      .from("logs")
      .select("*")
      .eq("userId", userId)
      .order("date", { ascending: false });

    if (logsError) throw logsError;
    console.log("[DEBUG] Logs fetched:", logs);
    if (!logs || logs.length === 0) {
      console.log("[My Points] empty logs", {
        activeStudentId: userId,
        roles: viewerContext.roles,
        studioId: viewerContext.studioId
      });
    }

    const { data: approvedLogs, error: approvedErr } = await supabase
      .from("logs")
      .select("points")
      .eq("userId", userId)
      .eq("status", "approved");
    if (approvedErr) throw approvedErr;

    const totalPoints = (approvedLogs || []).reduce((sum, log) => sum + (log.points || 0), 0);
    console.log("[DEBUG] Total approved points:", totalPoints);

    const { data: levels, error: levelsErr } = await supabase
      .from("levels")
      .select("*")
      .order("minPoints", { ascending: true });
    if (levelsErr) throw levelsErr;

    const currentLevel =
      (levels || []).find(l => totalPoints >= l.minPoints && totalPoints <= l.maxPoints)
      || (levels || [])[levels?.length - 1]
      || null;

    if (levelBadge) {
      levelBadge.src = currentLevel?.badge || "images/levelBadges/level1.png";
    }

    const { data: profileRow } = await supabase
      .from("users")
      .select("firstName, lastName")
      .eq("id", userId)
      .single();
    const firstName = profileRow?.firstName || "";
    const studentName = `${profileRow?.firstName || ""} ${profileRow?.lastName || ""}`.trim() || "Student";
    if (pointsTitle) {
      pointsTitle.textContent = firstName ? `${firstName}'s Points` : "My Points";
    }

    const currentLevelNumber = Number(currentLevel?.id || currentLevel?.name);
    const storedLevel = getLastToastLevel(userId);
    if (Number.isFinite(currentLevelNumber)) {
      if (storedLevel === null) {
        setLastToastLevel(userId, currentLevelNumber);
      } else if (currentLevelNumber > storedLevel) {
        showToast(`🎉 You reached Level ${currentLevelNumber}!`);
        setLastToastLevel(userId, currentLevelNumber);
      }
    }

    allLogs = (logs || []).slice().sort((a, b) => new Date(b.date) - new Date(a.date));
    badgeCatalog = await loadBadgeCatalog({
      userId,
      studioId: viewerContext.studioId || localStorage.getItem("activeStudioId")
    });
    renderCategorySummary(allLogs, badgeCatalog);
    applyFilters();
    if (openBadgesIntent) {
      localStorage.removeItem("aa.openBadgeCatalog");
      openBadgeCatalogModal();
    }

  } catch (err) {
    console.error("[ERROR] My Points:", err);
  }

  // ---- FUNCTIONS ----
  function renderBadgeCatalogModal(catalog) {
    if (!badgeCatalogBody) return;
    const familyRows = buildFamilyEvolutionRows(catalog).sort((a, b) =>
      humanizeToken(a.familyLabel || a.family).localeCompare(humanizeToken(b.familyLabel || b.family))
    );
    const unlockedRows = familyRows.filter((row) => row.unlocked);
    const lockedRows = familyRows.filter((row) => !row.unlocked);
    const lockedRegularRows = lockedRows.filter((row) => !row.mysteryWhenLocked);
    const lockedMysteryRows = lockedRows.filter((row) => row.mysteryWhenLocked);

    const renderTile = (row) => {
      const slug = String(row.displayDef.slug || "");
      const src = row.unlocked ? getBadgeImagePath(slug) : BADGE_DEMO_SRC;
      const mysteryLocked = !row.unlocked && row.mysteryWhenLocked;
      const tierTotal = Math.max(1, Number(row.tierTotal || 1));
      const unlockedTierCount = Math.max(0, Math.min(tierTotal, Number(row.unlockedTierCount || 0)));
      const progressText = tierTotal > 1 ? `${unlockedTierCount} of ${tierTotal}` : "";
      const title = mysteryLocked
        ? "???"
        : row.unlocked
          ? (row.highestUnlocked?.name || humanizeToken(slug))
          : humanizeToken(row.familyLabel || row.family);
      const nextText = mysteryLocked
        ? "???"
        : row.unlocked && row.nextDef
          ? `Next: ${row.nextDef.name || humanizeToken(row.nextDef.slug)}`
          : (row.unlocked ? (tierTotal > 1 ? "All evolution tiers earned." : "") : "");
      const criteriaDef = row.unlocked ? (row.highestUnlocked || row.displayDef) : row.displayDef;
      const howText = mysteryLocked
        ? "???"
        : row.unlocked
          ? `${formatBadgeCriteria(criteriaDef?.criteria)}`
          : `${formatBadgeCriteria(criteriaDef?.criteria)}`;

      return `
        <div class="badge-evo-item">
          <img class="badge-evo-image" src="${src}" alt="${title}" onerror="this.onerror=null;this.src='images/badges/demo.png'">
          <div class="badge-evo-title">${title}</div>
          ${progressText ? `<div class="badge-evo-progress">${progressText}</div>` : ""}
          ${nextText ? `<div class="badge-evo-next">${nextText}</div>` : ""}
          <div class="badge-evo-sub">${howText}</div>
        </div>
      `;
    };

    const sections = [];
    if (unlockedRows.length) {
      sections.push(`<div class="badge-evo-grid">${unlockedRows.map(renderTile).join("")}</div>`);
    }
    if (lockedRegularRows.length) {
      sections.push(`<div class="badge-evo-divider">keep logging points to unlock further badges</div>`);
      sections.push(`<div class="badge-evo-grid">${lockedRegularRows.map(renderTile).join("")}</div>`);
    }
    if (lockedMysteryRows.length) {
      sections.push(`<div class="badge-evo-grid">${lockedMysteryRows.map(renderTile).join("")}</div>`);
    }

    badgeCatalogBody.innerHTML = sections.length
      ? sections.join("")
      : `<div class="badge-catalog-empty">No badge definitions available yet.</div>`;
  }

  function openBadgeCatalogModal() {
    if (!badgeCatalogModal) return;
    renderBadgeCatalogModal(badgeCatalog);
    badgeCatalogModal.style.display = "flex";
    badgeCatalogModal.setAttribute("aria-hidden", "false");
  }

  function closeBadgeCatalogModal() {
    if (!badgeCatalogModal) return;
    badgeCatalogModal.style.display = "none";
    badgeCatalogModal.setAttribute("aria-hidden", "true");
  }

  function setNeedsInfoEditMessage(message, isError = false) {
    if (!needsInfoEditMsg) return;
    if (!message) {
      needsInfoEditMsg.textContent = "";
      needsInfoEditMsg.style.display = "none";
      return;
    }
    needsInfoEditMsg.textContent = String(message);
    needsInfoEditMsg.style.display = "block";
    needsInfoEditMsg.style.color = isError ? "#c62828" : "#0b7a3a";
  }

  function closeNeedsInfoEditor() {
    selectedNeedsInfoLogId = "";
    if (needsInfoEditOverlay) {
      needsInfoEditOverlay.style.display = "none";
      needsInfoEditOverlay.setAttribute("aria-hidden", "true");
    }
    setNeedsInfoEditMessage("");
  }

  function openNeedsInfoEditor(log) {
    if (!log || String(log.status || "").toLowerCase() !== "needs info") return;
    selectedNeedsInfoLogId = String(log.id || "");
    if (needsInfoEditCategory) needsInfoEditCategory.value = String(log.category || "");
    if (needsInfoEditDate) {
      needsInfoEditDate.value = log.date ? new Date(log.date).toLocaleDateString() : "";
    }
    if (needsInfoEditPoints) needsInfoEditPoints.value = String(log.points ?? "");
    if (needsInfoEditStatus) needsInfoEditStatus.value = "Needs Info";
    if (needsInfoEditNotes) {
      needsInfoEditNotes.value = String(log.notes || "");
      needsInfoEditNotes.focus();
      needsInfoEditNotes.setSelectionRange(needsInfoEditNotes.value.length, needsInfoEditNotes.value.length);
    }
    setNeedsInfoEditMessage("");
    if (needsInfoEditOverlay) {
      needsInfoEditOverlay.style.display = "flex";
      needsInfoEditOverlay.setAttribute("aria-hidden", "false");
    }
  }

  async function submitNeedsInfoUpdate() {
    const logId = String(selectedNeedsInfoLogId || "").trim();
    if (!logId) return;
    const notes = String(needsInfoEditNotes?.value || "").trim();
    if (!notes) {
      setNeedsInfoEditMessage("Please add notes before submitting.", true);
      return;
    }
    if (needsInfoEditSubmit) needsInfoEditSubmit.disabled = true;
    setNeedsInfoEditMessage("");
    const payload = { notes, status: "pending" };
    const { error } = await supabase
      .from("logs")
      .update(payload)
      .eq("id", logId)
      .eq("userId", userId);
    if (needsInfoEditSubmit) needsInfoEditSubmit.disabled = false;
    if (error) {
      console.error("[My Points] needs-info update failed", error);
      setNeedsInfoEditMessage(error?.message || "Couldn't update log.", true);
      return;
    }

    allLogs = allLogs.map((log) => (
      String(log?.id || "") === logId
        ? { ...log, notes, status: "pending" }
        : log
    ));
    applyFilters();
    window.dispatchEvent(new Event("aa:notification-state-changed"));
    closeNeedsInfoEditor();
    showToast("Log updated and resubmitted for review.");
  }

  function updateCategoryFilterUI() {
    const categoryCards = categorySummary.querySelectorAll("[data-category-card]");
    categoryCards.forEach((card) => {
      const category = String(card.dataset.categoryCard || "").toLowerCase();
      const isActive = activeCategoryFilter !== "all" && category === activeCategoryFilter;
      card.classList.toggle("is-active", isActive);
    });
    if (showAllCategoriesBtn) {
      showAllCategoriesBtn.hidden = activeCategoryFilter === "all";
    }
  }

  function renderCategorySummary(logs, catalog) {
    categorySummary.innerHTML = "";
    const categories = [
      { key: "practice", label: "Practice" },
      { key: "participation", label: "Participation" },
      { key: "performance", label: "Performance" },
      { key: "personal", label: "Personal" },
      { key: "proficiency", label: "Proficiency" },
      { key: "badges", label: "Badges" }
    ];

    const categoryIconMap = {
      practice: "images/categories/practice.png",
      participation: "images/categories/participation.png",
      performance: "images/categories/performance.png",
      personal: "images/categories/personal.png",
      proficiency: "images/categories/proficiency.png"
    };

    const summary = {};
    logs.forEach(l => {
      const cat = String(l.category || "").toLowerCase();
      if (!summary[cat]) summary[cat] = { approvedPoints: 0, approvedCount: 0, pendingPoints: 0, pendingCount: 0 };
      const isApproved = String(l.status || "").toLowerCase() === "approved";
      if (isApproved) {
        summary[cat].approvedPoints += l.points || 0;
        summary[cat].approvedCount += 1;
      } else {
        summary[cat].pendingPoints += l.points || 0;
        summary[cat].pendingCount += 1;
      }
    });

    categorySummary.innerHTML = categories.map(cat => {
      if (cat.key === "badges") {
        const latestSlug = String(catalog?.latestSlug || "").trim();
        const hasUnlocked = Boolean(latestSlug);
        const icon = hasUnlocked ? getBadgeImagePath(latestSlug) : BADGE_DEMO_SRC;
        const badgeSub = hasUnlocked
          ? `${Number(catalog?.unlockedCount || 0)} unlocked • Tap to view all`
          : "No badges unlocked yet";
        return `
          <button type="button" class="summary-card category-card badge-summary-card" data-badge-card="true" aria-label="Open badge catalog">
            <img class="category-icon" src="${icon}" alt="Badges" onerror="this.onerror=null;this.src='${BADGE_DEMO_SRC}'">
            <div class="summary-label">Badges</div>
            <div class="summary-value">${Number(catalog?.unlockedCount || 0)}</div>
            <div class="summary-sub">${badgeSub}</div>
          </button>
        `;
      }

      const data = summary[cat.key] || { approvedPoints: 0, approvedCount: 0, pendingPoints: 0, pendingCount: 0 };
      const icon = categoryIconMap[cat.key] || "images/categories/allCategories.png";
      return `
        <button type="button" class="summary-card category-card" data-category-card="${cat.key}" aria-label="Filter logs by ${cat.label}">
          <img class="category-icon" src="${icon}" alt="${cat.label}">
          <div class="summary-label">${cat.label}</div>
          <div class="summary-value">${data.approvedPoints} pts</div>
          <div class="summary-sub">
            ${data.approvedCount} logs • Pending ${data.pendingCount}
          </div>
        </button>
      `;
    }).join("");

    const badgeCard = categorySummary.querySelector("[data-badge-card='true']");
    if (badgeCard) {
      badgeCard.addEventListener("click", openBadgeCatalogModal);
    }

    const categoryCards = categorySummary.querySelectorAll("[data-category-card]");
    categoryCards.forEach((card) => {
      card.addEventListener("click", () => {
        const category = String(card.dataset.categoryCard || "").toLowerCase();
        activeCategoryFilter = activeCategoryFilter === category ? "all" : category;
        updateCategoryFilterUI();
        applyFilters();
      });
    });

    updateCategoryFilterUI();
  }

  function renderLogs(logs) {
    logsTableBody.innerHTML = "";
    let highlightedNeedsInfoRow = false;
    logs.forEach((log, index) => {
      const icon = `images/categories/${(log.category || "allCategories").toLowerCase()}.png`;
      const status = String(log.status || "pending");
      const isNeedsInfo = status.toLowerCase() === "needs info";
      const shouldBlinkNeedsInfo = isNeedsInfo && !needsInfoBlinkPlayed && !highlightedNeedsInfoRow;
      if (shouldBlinkNeedsInfo) highlightedNeedsInfoRow = true;
      const rowClass = `${index % 2 === 0 ? 'log-row-even' : 'log-row-odd'} ${status.toLowerCase() === "pending" ? "row-pending" : ""} ${isNeedsInfo ? "row-needs-info" : ""} ${shouldBlinkNeedsInfo ? "attention-blink-3" : ""}`;
      logsTableBody.innerHTML += `
        <tr class="${rowClass}" data-log-id="${String(log.id || "")}" ${isNeedsInfo ? 'data-needs-info-editable="true"' : ""}>
          <td data-label="Category"><span class="mobile-log-category"><img src="${icon}" style="width:30px;height:30px" alt="${log.category || "Category"}"><span>${log.category || "Log"}</span></span></td>
          <td data-label="Date">${log.date ? new Date(log.date).toLocaleDateString() : ""}</td>
          <td data-label="Points">${log.points ?? ""}</td>
          <td data-label="Notes">${log.notes || ""}</td>
          <td data-label="Status"><span class="status-pill status-${status.toLowerCase().replace(" ", "-")}">${status}</span></td>
        </tr>`;
    });
    if (highlightedNeedsInfoRow) needsInfoBlinkPlayed = true;
  }

  function syncHeaderWidths() {
    const headerCells = document.querySelectorAll("#pointsHeaderTable th");
    const firstRowCells = document.querySelectorAll("#logsTable tr:first-child td");
    if (!firstRowCells.length) return;
    headerCells.forEach((th, i) => {
      if (firstRowCells[i]) {
        th.style.width = firstRowCells[i].offsetWidth + "px";
      }
    });
  }

  function applyFilters() {
    const query = (searchInput?.value || "").trim().toLowerCase();
    const status = statusFilter?.value || "all";

    filteredLogs = allLogs.filter(log => {
      const matchesStatus = status === "all"
        ? true
        : String(log.status || "").toLowerCase() === status;
      const matchesCategory = activeCategoryFilter === "all"
        ? true
        : String(log.category || "").toLowerCase() === activeCategoryFilter;
      const haystack = [
        log.category,
        log.notes,
        log.status,
        log.date ? new Date(log.date).toLocaleDateString() : ""
      ].join(" ").toLowerCase();
      const matchesQuery = !query || haystack.includes(query);
      return matchesStatus && matchesCategory && matchesQuery;
    });
    filteredLogs.sort((a, b) => {
      const aNeedsInfo = String(a?.status || "").toLowerCase() === "needs info";
      const bNeedsInfo = String(b?.status || "").toLowerCase() === "needs info";
      if (aNeedsInfo !== bNeedsInfo) return aNeedsInfo ? -1 : 1;
      return new Date(b?.date || 0) - new Date(a?.date || 0);
    });

    renderLogs(filteredLogs);
  }

  if (searchInput) searchInput.addEventListener("input", applyFilters);
  if (statusFilter) statusFilter.addEventListener("change", applyFilters);
  if (logsTableBody) {
    logsTableBody.addEventListener("click", (event) => {
      const row = event.target instanceof HTMLElement ? event.target.closest("tr[data-needs-info-editable='true']") : null;
      if (!(row instanceof HTMLElement)) return;
      const logId = String(row.dataset.logId || "").trim();
      if (!logId) return;
      const log = allLogs.find((entry) => String(entry?.id || "") === logId);
      if (!log) return;
      openNeedsInfoEditor(log);
    });
  }
  if (showAllCategoriesBtn) {
    showAllCategoriesBtn.addEventListener("click", () => {
      activeCategoryFilter = "all";
      updateCategoryFilterUI();
      applyFilters();
    });
  }
  if (badgeCatalogCloseBtn) badgeCatalogCloseBtn.addEventListener("click", closeBadgeCatalogModal);
  if (badgeCatalogModal) {
    badgeCatalogModal.addEventListener("click", (event) => {
      if (event.target === badgeCatalogModal) closeBadgeCatalogModal();
    });
  }
  if (needsInfoEditClose) needsInfoEditClose.addEventListener("click", closeNeedsInfoEditor);
  if (needsInfoEditCancel) needsInfoEditCancel.addEventListener("click", closeNeedsInfoEditor);
  if (needsInfoEditOverlay) {
    needsInfoEditOverlay.addEventListener("click", (event) => {
      if (event.target === needsInfoEditOverlay) closeNeedsInfoEditor();
    });
  }
  if (needsInfoEditSubmit) {
    needsInfoEditSubmit.addEventListener("click", () => {
      void submitNeedsInfoUpdate();
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") {
      closeBadgeCatalogModal();
      closeNeedsInfoEditor();
    }
  });

  new ResizeObserver(syncHeaderWidths).observe(document.querySelector("#logsTable"));
});
