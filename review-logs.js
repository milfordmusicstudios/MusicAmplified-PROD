import { supabase } from "./supabaseClient.js";
import { createLevelCompletedNotification, fetchStudentLevelSnapshots, getCategoryDefaultPoints, getViewerContext, recalculateUserPoints, recalculateUsersAfterApprovedBatch } from './utils.js';
import { ensureStudioContextAndRoute } from "./studio-routing.js";
import { createTeacherAdminTutorial } from "./student-tutorial.js";
import { getTeacherLogPrompts, getTeacherPointCategories } from "./log-prompts.js";
const dispatchTutorialAction = (action) => {
  if (!action) return;
  window.dispatchEvent(new CustomEvent(String(action)));
};

const DEBUG_REVIEW_LOGS = false;

const categoryOptions = ["practice", "participation", "performance", "personal", "proficiency"];
const categoryColors = {
  practice: "#8dcb3d",
  participation: "#58c1c7",
  performance: "#c05df0",
  personal: "#f3ab40",
  proficiency: "#ff7099"
};

document.addEventListener("DOMContentLoaded", async () => {
  console.log("[DEBUG] Review Logs: Script loaded");

  const routeResult = await ensureStudioContextAndRoute({ redirectHome: false });
  if (routeResult?.redirected) return;

  const user = JSON.parse(localStorage.getItem("loggedInUser") || "null");
  if (!user?.id) {
    window.location.href = "login.html";
    return;
  }

  const viewerContext = await getViewerContext();
  const reviewLogsErrorPanel = document.getElementById("reviewLogsErrorPanel");
  const hideReviewLogsError = () => {
    if (!reviewLogsErrorPanel) return;
    reviewLogsErrorPanel.innerHTML = "";
    reviewLogsErrorPanel.style.display = "none";
  };
  const showReviewLogsError = (message, error = null) => {
    if (!reviewLogsErrorPanel) return;
    const mainMessage = error?.message || message || "An error occurred while loading logs.";
    const code = error?.code ?? error?.status ?? error?.statusCode ?? "N/A";
    const userId = viewerContext?.viewerUserId || "Unknown";
    const studioId = viewerContext?.studioId || "Unknown";
    const roles = Array.isArray(viewerContext?.viewerRoles) && viewerContext.viewerRoles.length
      ? viewerContext.viewerRoles.join(", ")
      : "None";

    reviewLogsErrorPanel.innerHTML = "";
    const titleLine = document.createElement("div");
    const titleStrong = document.createElement("strong");
    titleStrong.textContent = mainMessage;
    titleLine.appendChild(titleStrong);
    reviewLogsErrorPanel.appendChild(titleLine);

    const appendLine = (label, value) => {
      const line = document.createElement("div");
      const labelEl = document.createElement("strong");
      labelEl.textContent = `${label}:`;
      line.appendChild(labelEl);
      const displayValue = value ?? "N/A";
      line.appendChild(document.createTextNode(` ${displayValue}`));
      reviewLogsErrorPanel.appendChild(line);
    };

    appendLine("Code", code);
    appendLine("User ID", userId);
    appendLine("Studio ID", studioId);
    appendLine("Roles", roles);

    if (DEBUG_REVIEW_LOGS && error) {
      const detailText = error.details || error.hint || "";
      if (detailText) {
        const detailLine = document.createElement("div");
        detailLine.style.fontSize = "12px";
        detailLine.style.color = "#3b3b3b";
        detailLine.textContent = `Details: ${detailText}`;
        reviewLogsErrorPanel.appendChild(detailLine);
      }
    }

    reviewLogsErrorPanel.style.display = "flex";
  };

  console.log("[AuthZ]", { page: "review-logs", roles: viewerContext.viewerRoles, studioId: viewerContext.studioId });
  if (!viewerContext.isAdmin && !viewerContext.isTeacher) {
    alert("Access denied.");
    window.location.href = "index.html";
    return;
  }
  if (!viewerContext.viewerUserId) {
    showReviewLogsError("No active session detected. Please sign in again.");
    return;
  }
  if (!viewerContext.studioId) {
    showReviewLogsError("No studio selected for this account.");
    return;
  }
  const activeRole = viewerContext.isAdmin ? "admin" : "teacher";
  const teacherAdminTutorial = createTeacherAdminTutorial({
    profileId: viewerContext?.viewerUserId || viewerContext?.activeProfileId || null
  });
  void teacherAdminTutorial.maybeStart();
  if (document.body) {
    document.body.classList.remove("is-staff", "is-admin");
    document.body.classList.add(activeRole === "admin" ? "is-admin" : "is-staff");
  }
  const awardBadgesForApprovedUsers = (userIds) => {
    const uniqueIds = Array.from(new Set((userIds || []).map(id => String(id || "").trim()).filter(Boolean)));
    if (!uniqueIds.length) return;

    if (typeof window.showToast === "function") {
      window.showToast("Awarding badges...");
    }

    Promise.allSettled(uniqueIds.map(async (uid) => {
      const { data: sessionData } = await supabase.auth.getSession();
      const token = sessionData?.session?.access_token || "";
      const headers = { "Content-Type": "application/json" };
      if (token) headers.Authorization = `Bearer ${token}`;

      const response = await fetch("/api/badges/evaluate-on-approve", {
        method: "POST",
        credentials: "include",
        headers,
        body: JSON.stringify({
          studioId: viewerContext.studioId,
          userId: uid
        })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.error || `HTTP ${response.status}`);
      }
      return null;
    })).then((results) => {
      const failures = results.filter(r => r.status === "rejected");
      if (failures.length) {
        console.warn("[Badges] evaluate-on-approve failed", failures.map(f => f.reason?.message || f.reason));
      }
    });
  };

  const backfillLevelCompletedNotificationsForStudio = async () => {
    const activeStudioId = String(viewerContext?.studioId || "").trim();
    if (!activeStudioId) {
      throw new Error("No active studio id available for notification backfill.");
    }
    console.log("[NotifDiag][review-logs.js][backfillLevelCompletedNotificationsForStudio] rpc start", {
      source: "review-logs.js::backfillLevelCompletedNotificationsForStudio",
      rpc: "backfill_level_notifications_for_studio",
      studio_id: activeStudioId,
      user_id: viewerContext?.viewerUserId || null
    });
    const { data, error } = await supabase.rpc("backfill_level_notifications_for_studio", {
      p_studio_id: activeStudioId
    });
    console.log("[NotifDiag][review-logs.js][backfillLevelCompletedNotificationsForStudio] rpc result", {
      source: "review-logs.js::backfillLevelCompletedNotificationsForStudio",
      data: data ?? null,
      error: error ?? null
    });
    if (error) throw error;
    window.dispatchEvent(new Event("aa:notification-state-changed"));
    return data;
  };

  window.AA_backfillLevelCompletedNotificationsForStudio = backfillLevelCompletedNotificationsForStudio;

  if (new URLSearchParams(window.location.search).get("backfillNotifications") === "studio") {
    backfillLevelCompletedNotificationsForStudio().catch((error) => {
      console.warn("[ReviewLogs] notification backfill failed", error);
      showReviewLogsError("Notification backfill failed.", error);
    });
  }

  const roleBadge = document.getElementById("reviewRoleBadge");
  if (roleBadge instanceof HTMLImageElement) {
    if (activeRole === "admin") {
      roleBadge.src = "images/levelBadges/admin.png";
      roleBadge.alt = "Admin";
      roleBadge.style.display = "";
    } else if (activeRole === "teacher") {
      roleBadge.src = "images/levelBadges/teacher.png";
      roleBadge.alt = "Teacher";
      roleBadge.style.display = "";
    } else {
      roleBadge.style.display = "none";
    }
  }

  const logsTableBody = document.getElementById("logsTableBody");
  const categorySummary = document.getElementById("categorySummary");
  const searchInput = document.getElementById("searchInput");
  const filterLogsBtn = document.getElementById("filterLogsBtn");
  const filterLogsModal = document.getElementById("filterLogsModal");
  const filterLogsClose = document.getElementById("filterLogsClose");
  const filterStudentSearch = document.getElementById("filterStudentSearch");
  const filterStudentsDropdown = document.getElementById("filterStudentsDropdown");
  const filterStudentsSelected = document.getElementById("filterStudentsSelected");
  const filterDateFrom = document.getElementById("filterDateFrom");
  const filterDateTo = document.getElementById("filterDateTo");
  const filterKeyword = document.getElementById("filterKeyword");
  const filterCategory = document.getElementById("filterCategory");
  const filterStatus = document.getElementById("filterStatus");
  const applyLogFiltersBtn = document.getElementById("applyLogFiltersBtn");
  const resetLogFiltersBtn = document.getElementById("resetLogFiltersBtn");
  const activeFiltersSummary = document.getElementById("activeFiltersSummary");
  const bulkActionBar = document.getElementById("bulkActionBar");

  let allLogs = [];
  let users = [];
  let filteredLogs = [];
  let currentSort = { field: "date", order: "desc" };
  let currentPage = 1;
  let logsPerPage = 25;
  let activeCardFilter = "all";
  let serverLogFilters = {
    studentIds: [],
    dateFrom: "",
    dateTo: "",
    keyword: "",
    category: "",
    status: ""
  };
  let filterStudentRoster = [];
  const filterSelectedStudentIds = new Set();
  let pendingCardFlashPlayed = false;
  const LOG_FETCH_PAGE_SIZE = 1000;
  const LOG_FETCH_ADMIN_CAP = 20000;
  const requestedFilter = new URLSearchParams(window.location.search).get("filter");
  const normalizedRequestedFilter = requestedFilter === "needs-approval" ? "pending" : requestedFilter;
  if (normalizedRequestedFilter === "pending" || normalizedRequestedFilter === "approved-today" || normalizedRequestedFilter === "needs info" || normalizedRequestedFilter === "all") {
    activeCardFilter = normalizedRequestedFilter;
  }

  const DATE_ONLY_RE = /^\d{4}-\d{2}-\d{2}$/;
  const padDatePart = (value) => String(value).padStart(2, "0");
  const todayString = () => {
    const today = new Date();
    return `${today.getFullYear()}-${padDatePart(today.getMonth() + 1)}-${padDatePart(today.getDate())}`;
  };
  const getDateOnlyString = (value) => {
    if (!value) return "";
    const text = String(value);
    if (DATE_ONLY_RE.test(text)) return text;
    if (DATE_ONLY_RE.test(text.slice(0, 10))) return text.slice(0, 10);
    return "";
  };
  const formatDateOnlyForDisplay = (value, options = { month: "short", day: "2-digit" }) => {
    const dateOnly = getDateOnlyString(value);
    if (!dateOnly) return "";
    const [year, month, day] = dateOnly.split("-").map(Number);
    return new Intl.DateTimeFormat("en-US", options).format(new Date(year, month - 1, day));
  };
  const isApprovedStatus = (value) => String(value || "").toLowerCase() === "approved";
  const isNeedsInfoStatus = (value) => String(value || "").toLowerCase() === "needs info";
  const isSameDay = (value, today) => getDateOnlyString(value) === today;
  const getApprovedTimestamp = (log) => log._approvedAtLocal || log.approved_at || log.updated_at || "";
  const isApprovedToday = (log, today) => {
    if (!isApprovedStatus(log.status)) return false;
    const approvedStamp = getApprovedTimestamp(log);
    return approvedStamp ? isSameDay(approvedStamp, today) : false;
  };
  const maybeFlashPendingCard = (pendingCount) => {
    if (pendingCardFlashPlayed || Number(pendingCount || 0) <= 0) return;
    const pendingCard = categorySummary?.querySelector(".summary-card.pending");
    if (!(pendingCard instanceof HTMLElement)) return;
    pendingCard.classList.add("attention-blink-3");
    pendingCard.addEventListener("animationend", () => {
      pendingCard.classList.remove("attention-blink-3");
    }, { once: true });
    pendingCardFlashPlayed = true;
  };

  const createNeedsInfoNotification = async (logRow) => {
    const targetUserId = String(logRow?.userId || "").trim();
    if (!targetUserId) return;
    const category = String(logRow?.category || "log").trim();
    const dateText = logRow?.date
      ? formatDateOnlyForDisplay(logRow.date, { month: "numeric", day: "numeric", year: "numeric" })
      : "selected date";
    const message = `Your ${category} log from ${dateText} was marked Needs Info. Please update details.`;
    const basePayload = {
      userId: targetUserId,
      message,
      type: "needs_info",
      read: false,
      related_log_id: String(logRow?.id || "").trim() || null,
      studio_id: viewerContext.studioId || null,
      created_by: viewerContext.viewerUserId || null
    };
    try {
      console.log("[NotifDiag][review-logs.js][createNeedsInfoNotification] before insert", {
        source: "review-logs.js::createNeedsInfoNotification",
        payload: [basePayload],
        resolved_userId: targetUserId,
        resolved_studio_id: basePayload.studio_id,
        resolved_created_by: basePayload.created_by,
        resolved_related_log_id: basePayload.related_log_id
      });
      const { data: insertData, error } = await supabase.from("notifications").insert([basePayload]);
      console.log("[NotifDiag][review-logs.js][createNeedsInfoNotification] after insert", {
        source: "review-logs.js::createNeedsInfoNotification",
        data: insertData ?? null,
        error: error ?? null,
        summary: error ? "insert_error" : "insert_ok"
      });
      if (!error) return;
      const msg = String(error.message || "").toLowerCase();
      if (msg.includes("column") || msg.includes("does not exist")) {
        const fallbackPayload = [{ userId: targetUserId, message }];
        console.log("[NotifDiag][review-logs.js][createNeedsInfoNotification] before fallback insert", {
          source: "review-logs.js::createNeedsInfoNotification:fallback",
          payload: fallbackPayload,
          resolved_userId: targetUserId,
          resolved_studio_id: null,
          resolved_created_by: null,
          resolved_related_log_id: null
        });
        const { data: fallbackData, error: fallbackErr } = await supabase.from("notifications").insert(fallbackPayload);
        console.log("[NotifDiag][review-logs.js][createNeedsInfoNotification] after fallback insert", {
          source: "review-logs.js::createNeedsInfoNotification:fallback",
          data: fallbackData ?? null,
          error: fallbackErr ?? null,
          summary: fallbackErr ? "fallback_insert_error" : "fallback_insert_ok"
        });
      } else {
        console.warn("[ReviewLogs] needs-info notification insert failed", error);
      }
    } catch (err) {
      console.warn("[ReviewLogs] needs-info notification error", err);
    }
  };

  const canonicalStatus = (value) => String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[\s\-\.]+/g, "_");

  const isTeacherChallengeLog = (logRow) => {
    const category = String(logRow?.category || "").trim().toLowerCase();
    const notes = String(logRow?.notes || "").trim().toLowerCase();
    return category === "teacher challenge" || notes.startsWith("teacher challenge:");
  };

  const extractChallengeTitleFromLogNotes = (notesValue) => {
    const raw = String(notesValue || "").trim();
    if (!raw) return "";
    const lowered = raw.toLowerCase();
    const prefix = "teacher challenge:";
    if (!lowered.startsWith(prefix)) return "";
    const body = raw.slice(prefix.length).trim();
    if (!body) return "";
    const splitIndex = body.indexOf(" - ");
    if (splitIndex > -1) {
      return body.slice(0, splitIndex).trim();
    }
    return body.trim();
  };

  const maybeApproveTeacherChallengeFromLog = async (logRow) => {
    if (!logRow || !isTeacherChallengeLog(logRow)) return false;
    const logId = String(logRow?.id || "").trim();
    const studentId = String(logRow?.userId || "").trim();
    const studioId = String(viewerContext?.studioId || "").trim();
    if (!logId || !studentId || !studioId) return false;

    const currentStatus = canonicalStatus(logRow?.status);
    if (currentStatus && currentStatus !== "pending" && currentStatus !== "pending_review" && currentStatus !== "completed_pending") {
      return false;
    }

    const challengeTitle = extractChallengeTitleFromLogNotes(logRow?.notes);
    const { data: pendingAssignments, error: assignmentErr } = await supabase
      .from("teacher_challenge_assignments")
      .select(`
        id,
        status,
        challenge_id,
        teacher_challenges:challenge_id (
          title
        )
      `)
      .eq("studio_id", studioId)
      .eq("student_id", studentId)
      .in("status", ["pending_review", "pending"]);
    if (assignmentErr) {
      console.warn("[ReviewLogs] challenge assignment lookup failed", assignmentErr);
      return false;
    }

    const candidates = Array.isArray(pendingAssignments) ? pendingAssignments : [];
    if (!candidates.length) return false;

    const normalizedTitle = challengeTitle.toLowerCase();
    const matched = normalizedTitle
      ? candidates.find((row) => String(row?.teacher_challenges?.title || "").trim().toLowerCase() === normalizedTitle)
        || candidates.find((row) => String(row?.teacher_challenges?.title || "").trim().toLowerCase().includes(normalizedTitle))
      : null;
    const assignment = matched || candidates[0];
    const assignmentId = String(assignment?.id || "").trim();
    if (!assignmentId) return false;

    const { error: approveErr } = await supabase.rpc("approve_teacher_challenge_completion", {
      p_assignment_id: assignmentId,
      p_log_id: logId
    });
    if (approveErr) {
      console.warn("[ReviewLogs] challenge approval RPC failed", approveErr, { assignmentId, logId, challengeTitle });
      return false;
    }
    return true;
  };

  const normalizeFilterValue = (value) => String(value || "").trim();
  const hasServerFilters = () => Boolean(
    serverLogFilters.studentIds?.length ||
    serverLogFilters.dateFrom ||
    serverLogFilters.dateTo ||
    serverLogFilters.keyword ||
    serverLogFilters.category ||
    serverLogFilters.status
  );
  const getVisibleStudents = () => {
    if (!viewerContext.isTeacher || viewerContext.isAdmin) return users;
    return users.filter(u => Array.isArray(u.teacherIds) && u.teacherIds.map(String).includes(String(viewerContext.viewerUserId)));
  };
  const getFilterStudentName = (student) => getQuickAddStudentName(student);
  const renderFilterSelectedStudents = () => {
    if (!filterStudentsSelected) return;
    filterStudentsSelected.innerHTML = "";
    if (!filterSelectedStudentIds.size) {
      const empty = document.createElement("span");
      empty.className = "staff-student-empty";
      empty.textContent = "No students selected";
      filterStudentsSelected.appendChild(empty);
      return;
    }
    filterStudentRoster
      .filter((student) => filterSelectedStudentIds.has(String(student.id)))
      .forEach((student) => {
        const chip = document.createElement("button");
        chip.type = "button";
        chip.className = "staff-student-chip";
        chip.dataset.studentId = String(student.id);
        chip.textContent = `${getFilterStudentName(student)} x`;
        chip.addEventListener("click", () => {
          filterSelectedStudentIds.delete(String(student.id));
          renderFilterSelectedStudents();
          renderFilterStudentDropdown();
        });
        filterStudentsSelected.appendChild(chip);
      });
  };
  const renderFilterStudentDropdown = () => {
    if (!filterStudentSearch || !filterStudentsDropdown) return;
    const query = String(filterStudentSearch.value || "").trim().toLowerCase();
    filterStudentsDropdown.innerHTML = "";
    if (!query) {
      filterStudentsDropdown.setAttribute("hidden", "");
      return;
    }
    const matches = filterStudentRoster.filter((student) =>
      getFilterStudentName(student).toLowerCase().includes(query)
    );
    if (!matches.length) {
      const empty = document.createElement("div");
      empty.className = "staff-student-no-match";
      empty.textContent = "No matching students";
      filterStudentsDropdown.appendChild(empty);
      filterStudentsDropdown.removeAttribute("hidden");
      return;
    }
    matches.forEach((student) => {
      const id = String(student.id);
      const item = document.createElement("button");
      item.type = "button";
      item.className = "staff-student-option";
      item.dataset.studentId = id;
      const isSelected = filterSelectedStudentIds.has(id);
      item.textContent = isSelected ? `Selected: ${getFilterStudentName(student)}` : getFilterStudentName(student);
      if (isSelected) item.classList.add("is-selected");
      item.addEventListener("click", () => {
        if (filterSelectedStudentIds.has(id)) filterSelectedStudentIds.delete(id);
        else filterSelectedStudentIds.add(id);
        renderFilterSelectedStudents();
        renderFilterStudentDropdown();
        filterStudentSearch.focus();
      });
      filterStudentsDropdown.appendChild(item);
    });
    filterStudentsDropdown.removeAttribute("hidden");
  };
  const loadFilterStudents = async () => {
    if (!filterStudentSearch) return;
    const { data, error } = await supabase
      .from("users")
      .select("id, firstName, lastName, email, roles, teacherIds")
      .eq("studio_id", viewerContext.studioId)
      .eq("active", true)
      .is("deactivated_at", null);
    if (error) {
      console.error("[ReviewLogs] filter students failed", error);
      filterStudentRoster = [];
      filterStudentSearch.value = "";
      filterStudentSearch.placeholder = "Error loading students";
      filterStudentSearch.disabled = true;
      renderFilterSelectedStudents();
      return;
    }
    filterStudentRoster = (data || [])
      .filter((student) => {
        const displayName = getFilterStudentName(student);
        if (!displayName || displayName === "Student") return false;
        const roles = Array.isArray(student.roles) ? student.roles : [student.roles];
        const isStudent = roles.map((role) => String(role || "").toLowerCase()).includes("student");
        if (!isStudent) return false;
        if (viewerContext.isAdmin) return true;
        if (!viewerContext.isTeacher) return false;
        const teacherIds = Array.isArray(student.teacherIds) ? student.teacherIds.map(String) : [];
        return teacherIds.includes(String(viewerContext.viewerUserId));
      })
      .sort((a, b) => getFilterStudentName(a).localeCompare(getFilterStudentName(b), undefined, { sensitivity: "base" }));
    const validIds = new Set(filterStudentRoster.map((student) => String(student.id)));
    Array.from(filterSelectedStudentIds).forEach((id) => {
      if (!validIds.has(id)) filterSelectedStudentIds.delete(id);
    });
    filterStudentSearch.disabled = false;
    filterStudentSearch.placeholder = filterStudentRoster.length ? "Type a student name..." : "No students found";
    renderFilterSelectedStudents();
    renderFilterStudentDropdown();
  };
  const syncFilterModalFields = () => {
    filterSelectedStudentIds.clear();
    (serverLogFilters.studentIds || []).forEach((id) => {
      const normalized = String(id || "").trim();
      if (normalized) filterSelectedStudentIds.add(normalized);
    });
    if (filterStudentSearch instanceof HTMLInputElement) filterStudentSearch.value = "";
    renderFilterSelectedStudents();
    renderFilterStudentDropdown();
    if (filterDateFrom instanceof HTMLInputElement) filterDateFrom.value = serverLogFilters.dateFrom;
    if (filterDateTo instanceof HTMLInputElement) filterDateTo.value = serverLogFilters.dateTo;
    if (filterKeyword instanceof HTMLInputElement) filterKeyword.value = serverLogFilters.keyword;
    if (filterCategory instanceof HTMLSelectElement) filterCategory.value = serverLogFilters.category;
    if (filterStatus instanceof HTMLSelectElement) filterStatus.value = serverLogFilters.status;
  };
  const collectFilterModalFields = () => ({
    studentIds: Array.from(filterSelectedStudentIds),
    dateFrom: getDateOnlyString(filterDateFrom?.value),
    dateTo: getDateOnlyString(filterDateTo?.value),
    keyword: normalizeFilterValue(filterKeyword?.value),
    category: normalizeFilterValue(filterCategory?.value).toLowerCase(),
    status: normalizeFilterValue(filterStatus?.value).toLowerCase()
  });
  const getStudentNameById = (studentId) => {
    const row = filterStudentRoster.find(u => String(u.id) === String(studentId))
      || users.find(u => String(u.id) === String(studentId));
    return row ? getFilterStudentName(row) : "";
  };
  const renderActiveFiltersSummary = () => {
    if (!activeFiltersSummary) return;
    const parts = [];
    if (serverLogFilters.studentIds?.length) {
      parts.push(serverLogFilters.studentIds.map((id) => getStudentNameById(id)).filter(Boolean).join(", ") || "Selected students");
    }
    if (serverLogFilters.category) parts.push(serverLogFilters.category.replace(/\b\w/g, c => c.toUpperCase()));
    if (serverLogFilters.status) parts.push(serverLogFilters.status.replace(/\b\w/g, c => c.toUpperCase()));
    if (serverLogFilters.dateFrom || serverLogFilters.dateTo) {
      const fromLabel = serverLogFilters.dateFrom ? formatDateOnlyForDisplay(serverLogFilters.dateFrom, { month: "short", day: "numeric" }) : "Start";
      const toLabel = serverLogFilters.dateTo ? formatDateOnlyForDisplay(serverLogFilters.dateTo, { month: "short", day: "numeric" }) : "Today";
      parts.push(`${fromLabel}-${toLabel}`);
    }
    if (serverLogFilters.keyword) parts.push(`"${serverLogFilters.keyword}"`);
    const quickKeyword = normalizeFilterValue(searchInput?.value);
    if (quickKeyword) parts.push(`Quick: "${quickKeyword}"`);
    if (!parts.length) {
      activeFiltersSummary.textContent = "";
      activeFiltersSummary.style.display = "none";
      return;
    }
    activeFiltersSummary.textContent = `Filtered by: ${parts.join(", ")}`;
    activeFiltersSummary.style.display = "block";
  };
  const fetchLogsPage = async ({ from, to, teacherStudentIds }) => {
    console.log("[ReviewLogs] requesting logs range", {
      from,
      to,
      filtersActive: hasServerFilters(),
      filters: serverLogFilters
    });
    let query = supabase
      .from("logs")
      .select("*")
      .eq("studio_id", viewerContext.studioId)
      .order("date", { ascending: false, nulls: "last" })
      .range(from, to);
    if (serverLogFilters.dateFrom) query = query.gte("date", serverLogFilters.dateFrom);
    if (serverLogFilters.dateTo) query = query.lte("date", serverLogFilters.dateTo);
    if (serverLogFilters.studentIds?.length) {
      query = query.in("userId", serverLogFilters.studentIds);
    } else if (Array.isArray(teacherStudentIds)) {
      if (!teacherStudentIds.length) return { data: [], error: null };
      query = query.in("userId", teacherStudentIds);
    }
    if (serverLogFilters.category) query = query.eq("category", serverLogFilters.category);
    if (serverLogFilters.status) query = query.eq("status", serverLogFilters.status);
    return query;
  };
  const fetchLogsPaginated = async () => {
    const teacherStudentIds = viewerContext.isTeacher && !viewerContext.isAdmin
      ? getVisibleStudents().map(s => String(s.id)).filter(Boolean)
      : null;
    const rows = [];
    for (let from = 0; from < LOG_FETCH_ADMIN_CAP; from += LOG_FETCH_PAGE_SIZE) {
      const to = Math.min(from + LOG_FETCH_PAGE_SIZE - 1, LOG_FETCH_ADMIN_CAP - 1);
      const { data, error } = await fetchLogsPage({ from, to, teacherStudentIds });
      if (error) throw error;
      const batch = Array.isArray(data) ? data : [];
      rows.push(...batch);
      if (batch.length < LOG_FETCH_PAGE_SIZE) break;
    }
    console.log("[ReviewLogs] fetched logs", {
      fetchedCount: rows.length,
      filtersActive: hasServerFilters(),
      cap: LOG_FETCH_ADMIN_CAP,
      hitCap: rows.length >= LOG_FETCH_ADMIN_CAP
    });
    return rows;
  };
  const loadReviewLogs = async () => {
    hideReviewLogsError();
    logsTableBody.innerHTML = `<tr><td colspan="7" class="logs-empty-state">Loading logs...</td></tr>`;
    const { data: usersData, error: usersError } = await supabase
      .from("users")
      .select("id, firstName, lastName, teacherIds")
      .eq("studio_id", viewerContext.studioId);
    if (usersError) throw usersError;

    users = usersData || [];
    const logsData = await fetchLogsPaginated();
    allLogs = (logsData || []).map(l => ({
      ...l,
      fullName:
        (users.find(u => String(u.id) === String(l.userId))?.firstName || "Unknown") +
        " " +
        (users.find(u => String(u.id) === String(l.userId))?.lastName || "")
    }));

    hideReviewLogsError();
    applyFilters();
  };

  try {
    await loadReviewLogs();
  } catch (err) {
    console.error("[ERROR] Review Logs:", err);
    showReviewLogsError("Failed to load logs.", err);
  }

  // Search + Card Filter
  searchInput.addEventListener("input", applyFilters);
  const openFilterLogsModal = async () => {
    syncFilterModalFields();
    await loadFilterStudents();
    if (filterLogsModal) filterLogsModal.style.display = "flex";
  };
  const closeFilterLogsModal = () => {
    if (filterLogsModal) filterLogsModal.style.display = "none";
  };
  filterLogsBtn?.addEventListener("click", () => {
    void openFilterLogsModal();
  });
  filterLogsClose?.addEventListener("click", closeFilterLogsModal);
  filterLogsModal?.addEventListener("click", (event) => {
    if (event.target === filterLogsModal) closeFilterLogsModal();
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && filterLogsModal?.style.display !== "none") closeFilterLogsModal();
  });
  applyLogFiltersBtn?.addEventListener("click", async () => {
    serverLogFilters = collectFilterModalFields();
    closeFilterLogsModal();
    currentPage = 1;
    try {
      await loadReviewLogs();
    } catch (err) {
      console.error("[ReviewLogs] filtered load failed", err);
      showReviewLogsError("Failed to load filtered logs.", err);
    }
  });
  resetLogFiltersBtn?.addEventListener("click", async () => {
    serverLogFilters = {
      studentIds: [],
      dateFrom: "",
      dateTo: "",
      keyword: "",
      category: "",
      status: ""
    };
    filterSelectedStudentIds.clear();
    syncFilterModalFields();
    currentPage = 1;
    try {
      await loadReviewLogs();
    } catch (err) {
      console.error("[ReviewLogs] reset filtered load failed", err);
      showReviewLogsError("Failed to reload logs.", err);
    }
  });
  filterStudentSearch?.addEventListener("input", renderFilterStudentDropdown);
  filterStudentSearch?.addEventListener("focus", renderFilterStudentDropdown);
  document.addEventListener("click", (event) => {
    if (!filterStudentSearch || !filterStudentsDropdown) return;
    const picker = filterStudentSearch.closest(".staff-student-picker");
    if (!picker) return;
    if (!picker.contains(event.target)) filterStudentsDropdown.setAttribute("hidden", "");
  });
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape" && filterStudentsDropdown) {
      filterStudentsDropdown.setAttribute("hidden", "");
    }
  });

  function applyFilters() {
    const quickKeyword = normalizeFilterValue(searchInput.value).toLowerCase();
    const modalKeyword = normalizeFilterValue(serverLogFilters.keyword).toLowerCase();
    const todayStr = todayString();

    filteredLogs = allLogs.filter(l => {
      const notesText = String(l.notes || "").toLowerCase();
      const matchesQuickSearch = !quickKeyword || notesText.includes(quickKeyword);
      const matchesModalKeyword = !modalKeyword || notesText.includes(modalKeyword);
      const logDate = getDateOnlyString(l.date);
      const matchesStudent = !serverLogFilters.studentIds?.length || serverLogFilters.studentIds.map(String).includes(String(l.userId));
      const matchesDateFrom = !serverLogFilters.dateFrom || (logDate && logDate >= serverLogFilters.dateFrom);
      const matchesDateTo = !serverLogFilters.dateTo || (logDate && logDate <= serverLogFilters.dateTo);
      const matchesCategory = !serverLogFilters.category || String(l.category || "").toLowerCase() === serverLogFilters.category;
      const matchesStatusFilter = !serverLogFilters.status || String(l.status || "").toLowerCase() === serverLogFilters.status;
      let matchesCard = true;
      if (activeCardFilter === "pending") {
        matchesCard = String(l.status || "").toLowerCase() === "pending";
      } else if (activeCardFilter === "approved-today") {
        matchesCard = isApprovedToday(l, todayStr);
      } else if (activeCardFilter === "needs info") {
        matchesCard = String(l.status || "").toLowerCase() === "needs info";
      }
      return matchesQuickSearch &&
        matchesModalKeyword &&
        matchesStudent &&
        matchesDateFrom &&
        matchesDateTo &&
        matchesCategory &&
        matchesStatusFilter &&
        matchesCard;
    });

    currentPage = 1;
    sortLogs();
    renderCategorySummary(allLogs);
    renderActiveFiltersSummary();
    console.log("[ReviewLogs] filters applied", {
      loadedCount: allLogs.length,
      filteredCount: filteredLogs.length,
      filtersActive: hasServerFilters() || Boolean(quickKeyword) || activeCardFilter !== "all",
      serverFilters: serverLogFilters,
      quickKeywordActive: Boolean(quickKeyword),
      cardFilter: activeCardFilter
    });
    renderLogsTable(filteredLogs);
  }

  // Column Sorting
  document.querySelectorAll("#logsTable th[data-sort]").forEach(th => {
    th.style.cursor = "pointer";
    th.addEventListener("click", () => {
      const field = th.dataset.sort;
      if (currentSort.field === field) {
        currentSort.order = currentSort.order === "asc" ? "desc" : "asc";
      } else {
        currentSort.field = field;
        currentSort.order = "asc";
      }
      sortLogs();
      renderLogsTable(filteredLogs);
    });
  });

  function sortLogs() {
    filteredLogs.sort((a, b) => {
      let aVal = a[currentSort.field] || "";
      let bVal = b[currentSort.field] || "";

      if (currentSort.field === "date") {
        aVal = getDateOnlyString(aVal);
        bVal = getDateOnlyString(bVal);
      }
      if (currentSort.field === "points") {
        aVal = parseInt(aVal) || 0;
        bVal = parseInt(bVal) || 0;
      }

      return currentSort.order === "asc" ? (aVal > bVal ? 1 : -1) : (aVal < bVal ? 1 : -1);
    });
  }

  // Pagination
  document.getElementById("prevPageBtn").addEventListener("click", () => {
    if (currentPage > 1) {
      currentPage--;
      renderLogsTable(filteredLogs);
    }
  });

  document.getElementById("nextPageBtn").addEventListener("click", () => {
    const totalPages = Math.ceil(filteredLogs.length / logsPerPage);
    if (currentPage < totalPages) {
      currentPage++;
      renderLogsTable(filteredLogs);
    }
  });

  document.getElementById("logsPerPage").addEventListener("change", e => {
    logsPerPage = parseInt(e.target.value);
    currentPage = 1;
    renderLogsTable(filteredLogs);
  });

  function renderCategorySummary(list) {
    if (!categorySummary) return;
    const pendingCount = list.filter(l => String(l.status || "").toLowerCase() === "pending").length;
    const todayStr = todayString();
    const approvedTodayCount = list.filter(l => isApprovedToday(l, todayStr)).length;
    const needsInfoCount = list.filter(l => String(l.status || "").toLowerCase() === "needs info").length;

    const cards = [
      { label: "Pending Logs", value: pendingCount },
      { label: "Approved Today", value: approvedTodayCount },
      { label: "Needs Info", value: needsInfoCount },
      { label: "Total Logs", value: list.length }
    ];

    categorySummary.innerHTML = cards.map(card => {
      const key = card.label.toLowerCase();
      let extraClass = "";
      let filterTag = "all";
      if (key.includes("pending")) {
        extraClass = "pending";
        filterTag = "pending";
      } else if (key.includes("approved")) {
        extraClass = "approved";
        filterTag = "approved-today";
      } else if (key.includes("total")) {
        extraClass = "total";
        filterTag = "all";
      } else if (key.includes("needs info")) {
        extraClass = "review";
        filterTag = "needs info";
      } else {
        extraClass = "review";
      }
      return `
      <div class="summary-card ${extraClass} ${activeCardFilter === filterTag ? "is-active" : ""}" data-filter="${filterTag}">
        <div class="summary-label">${card.label}</div>
        <div class="summary-value">${card.value}</div>
      </div>
    `;
    }).join("");

    categorySummary.querySelectorAll(".summary-card").forEach(card => {
      card.addEventListener("click", () => {
        const filter = card.dataset.filter || "all";
        activeCardFilter = filter;
        applyFilters();
      });
    });
    maybeFlashPendingCard(pendingCount);
  }

  function formatShortDate(value) {
    return formatDateOnlyForDisplay(value, { month: "short", day: "2-digit" });
  }

  function renderLogsTable(list) {
    logsTableBody.innerHTML = "";
    if (!allLogs.length) {
      const hasAnyFilter = hasServerFilters() || Boolean(normalizeFilterValue(searchInput?.value)) || activeCardFilter !== "all";
      logsTableBody.innerHTML = `<tr><td colspan="7" class="logs-empty-state">${hasAnyFilter ? "No logs match the active filters." : "No logs found yet."}</td></tr>`;
      document.getElementById("selectAll").checked = false;
      updateBulkActionBarVisibility();
      return;
    }
    if (!list.length) {
      logsTableBody.innerHTML = `<tr><td colspan="7" class="logs-empty-state">No logs match the active filters.</td></tr>`;
      document.getElementById("selectAll").checked = false;
      updateBulkActionBarVisibility();
      return;
    }
    const start = (currentPage - 1) * logsPerPage;
    const end = start + logsPerPage;
    const pageLogs = list.slice(start, end);

    pageLogs.forEach((log, index) => {
      const row = document.createElement("tr");
      row.className = index % 2 === 0 ? "log-row-even" : "log-row-odd";
      const categoryKey = String(log.category || "").toLowerCase();
      row.innerHTML = `
        <td class="checkbox-cell"><input type="checkbox" class="select-log" data-id="${log.id}"></td>
        <td>${log.fullName}</td>
        <td class="category-cell" style="--cat-color:${categoryColors[categoryKey] || '#ccc'};">
          <select class="edit-input" data-id="${log.id}" data-field="category">
            ${categoryOptions.map(c =>
              `<option value="${c}" ${log.category?.toLowerCase() === c ? "selected" : ""}>${c}</option>`
            ).join("")}
          </select>
        </td>
        <td class="date-cell">
          <div class="date-wrapper">
            <input type="date" class="edit-input date-picker" data-id="${log.id}" data-field="date" value="${getDateOnlyString(log.date)}">
            <span class="date-label">${formatShortDate(log.date)}</span>
          </div>
        </td>
        <td><input type="number" class="edit-input" data-id="${log.id}" data-field="points" value="${log.points ?? 0}"></td>
        <td><textarea class="edit-input" data-id="${log.id}" data-field="notes">${log.notes || ""}</textarea></td>
        <td>
          <select class="edit-input status-select status-pill" data-id="${log.id}" data-field="status" data-status="${String(log.status || "pending").toLowerCase()}">
            <option value="pending" ${log.status === "pending" ? "selected" : ""}>Pending</option>
            <option value="approved" ${log.status === "approved" ? "selected" : ""}>Approved</option>
            <option value="rejected" ${log.status === "rejected" ? "selected" : ""}>Rejected</option>
            <option value="needs info" ${log.status === "needs info" ? "selected" : ""}>Needs Info</option>
          </select>
        </td>`;
      logsTableBody.appendChild(row);
    });

    document.getElementById("selectAll").checked = false;
    applyEditListeners();
    updateBulkActionBarVisibility();
  }

  function updateBulkActionBarVisibility() {
    if (!bulkActionBar) return;
    const selectedCount = document.querySelectorAll("#logsTableBody .select-log:checked").length;
    bulkActionBar.style.display = selectedCount > 0 ? "flex" : "none";
  }

  function applyEditListeners() {
    document.querySelectorAll(".edit-input").forEach(el => {
      if (el.tagName.toLowerCase() === "textarea") {
        el.addEventListener("input", () => {
          el.style.height = "auto";
          el.style.height = el.scrollHeight + "px";
        });
      }

      el.addEventListener("change", async e => {
        const logId = e.target.dataset.id;
        const field = e.target.dataset.field;
        let value = e.target.value;

        // Normalize values
        if (field === "points") value = parseInt(value) || 0;
        if (field === "category") value = String(value).toLowerCase();
        if (field === "date") value = getDateOnlyString(value);
        if (field === "date") {
          const wrapper = e.target.closest(".date-wrapper");
          const label = wrapper?.querySelector(".date-label");
          if (label) {
            label.textContent = formatShortDate(value);
          }
        }

        const updated = allLogs.find(l => String(l.id) === String(logId));
        if (!updated) return;
        const previousStatus = String(updated.status || "").toLowerCase();
        const nowApproved = field === "status" && String(value).toLowerCase() === "approved";
        const pointsChangedWhileApproved = field === "points" && String(updated.status).toLowerCase() === "approved";
        const levelSnapshotsBefore = (nowApproved || pointsChangedWhileApproved)
          ? await fetchStudentLevelSnapshots([updated.userId])
          : new Map();

        let handledByChallengeApprovalRpc = false;
        if (nowApproved) {
          handledByChallengeApprovalRpc = await maybeApproveTeacherChallengeFromLog(updated);
        }
        if (!handledByChallengeApprovalRpc) {
          const { error } = await supabase.from("logs").update({ [field]: value }).eq("id", logId);
          if (error) {
            alert("Failed to update log.");
            console.error(error);
            return;
          }
        }

        if (field === "status" && e.target instanceof HTMLSelectElement) {
          const statusValue = String(value || "").toLowerCase();
          e.target.dataset.status = statusValue;
        }
        console.log(`[DEBUG] Updated log ${logId}: ${field} = ${value}`);

        // Keep our local copy in sync
        updated[field] = value;

        // If the log is now approved, or points changed while approved, recalc that student.

        if (nowApproved) {
          updated._approvedAtLocal = new Date().toISOString();
          updated.updated_at = updated._approvedAtLocal;
        }

        if (nowApproved || pointsChangedWhileApproved) {
          try {
            const before = levelSnapshotsBefore.get(String(updated.userId)) || null;
            await recalculateUserPoints(String(updated.userId), {
              levelSnapshotBefore: before,
              previousLevelOverride: before?.level ?? null,
              studioIdOverride: viewerContext.studioId
            });
          } catch (recalcErr) {
            console.error("[ERROR] recalculateUserPoints:", recalcErr);
          }
        }
        if (nowApproved) {
          awardBadgesForApprovedUsers([updated.userId]);
        }
        const nowNeedsInfo = field === "status" && isNeedsInfoStatus(value) && previousStatus !== "needs info";
        if (nowNeedsInfo) {
          await createNeedsInfoNotification(updated);
        }
        applyFilters();
        window.dispatchEvent(new Event("aa:notification-state-changed"));
      });
    });
  }

  // Delete selected logs
  document.getElementById("deleteSelectedBtn").addEventListener("click", async () => {
    const selectedIds = Array.from(document.querySelectorAll(".select-log:checked"))
      .map(cb => String(cb.dataset.id).trim())
      .filter(Boolean);

    if (selectedIds.length === 0) {
      alert("No logs selected.");
      return;
    }

    if (!confirm(`Are you sure you want to permanently delete ${selectedIds.length} logs? This action cannot be undone.`)) {
      return;
    }

    try {
      const selectedRows = allLogs.filter(l => selectedIds.includes(String(l.id)));
      const affectedUserIds = Array.from(new Set(
        selectedRows.map(l => String(l.userId || "").trim()).filter(Boolean)
      ));
      const levelSnapshotsBefore = affectedUserIds.length
        ? await fetchStudentLevelSnapshots(affectedUserIds)
        : new Map();

      const numericSelectedIds = selectedIds
        .map(id => Number(id))
        .filter(id => Number.isSafeInteger(id));
      if (numericSelectedIds.length !== selectedIds.length) {
        console.error("[DELETE ERROR] Invalid log ids", { selectedIds, numericSelectedIds });
        alert("Some selected logs had invalid IDs. Please refresh and try again.");
        return;
      }

      const { data: deletedRows, error } = await supabase.rpc("delete_logs_for_studio", {
        p_studio_id: viewerContext.studioId,
        p_log_ids: numericSelectedIds
      });
      console.log("[DELETE RESULT]", { selectedIds, numericSelectedIds, deletedRows, error });
      if (error) {
        console.error("[DELETE ERROR]", error, { selectedIds, numericSelectedIds, deletedRows });
        alert("Failed to delete logs: " + error.message);
        return;
      }

      const deletedIds = new Set((deletedRows || []).map(row => String(row.id)));
      if (deletedIds.size === 0) {
        console.warn("[DELETE WARNING] Delete returned no rows", { selectedIds, numericSelectedIds, deletedRows, error });
        alert("No logs were deleted. Please refresh and try again.");
        return;
      }

      const deletedUserIds = Array.from(new Set(
        (deletedRows || [])
          .flatMap(row => [
            row?.userId,
            row?.user_id,
            ...(Array.isArray(row?.affected_user_ids) ? row.affected_user_ids : [])
          ])
          .map(userId => String(userId || "").trim())
          .filter(Boolean)
      ));
      if (deletedUserIds.length) {
        const recalcResults = await recalculateUsersAfterApprovedBatch(deletedUserIds, levelSnapshotsBefore, {
          studioId: viewerContext.studioId
        });
        const failedRecalcIds = Array.from(recalcResults.entries())
          .filter(([, result]) => !result)
          .map(([studentId]) => studentId);
        if (failedRecalcIds.length) {
          throw new Error(`Deleted logs, but recalculation failed for ${failedRecalcIds.length} student(s).`);
        }
      }

      allLogs = allLogs.filter(l => !deletedIds.has(String(l.id)));
      applyFilters();
      updateBulkActionBarVisibility();
      await updateNotificationsButtonState();
      window.dispatchEvent(new Event("aa:notification-state-changed"));
      window.dispatchEvent(new Event("aa:points-state-changed"));
      alert("Selected logs deleted successfully.");
      return;
    } catch (err) {
      console.error("Delete logs failed:", err);
      alert("Failed to delete logs: " + (err?.message || "Unknown error"));
      return;
    }
    });

  document.getElementById("bulkApproveBtn").addEventListener("click", async () => {
    await bulkUpdateSelectedStatuses("approved");
  });

  document.getElementById("bulkRejectBtn")?.addEventListener("click", async () => {
    await bulkUpdateSelectedStatuses("rejected");
  });

  async function bulkUpdateSelectedStatuses(nextStatus) {
    const normalizedStatus = String(nextStatus || "").trim().toLowerCase();
    if (!["approved", "rejected"].includes(normalizedStatus)) return;
    const selectedIds = Array.from(document.querySelectorAll(".select-log:checked"))
      .map(cb => String(cb.dataset.id).trim())
      .filter(Boolean);
    if (selectedIds.length === 0) {
      alert("No logs selected.");
      return;
    }

    const selectedRows = allLogs.filter(l => selectedIds.includes(String(l.id)));
    const affectedUserIds = Array.from(new Set(
      selectedRows.map(l => String(l.userId || "").trim()).filter(Boolean)
    ));

    try {
      const levelSnapshotsBefore = normalizedStatus === "approved" && affectedUserIds.length
        ? await fetchStudentLevelSnapshots(affectedUserIds)
        : new Map();
      let selectedIdsToDirectUpdate = [...selectedIds];
      if (normalizedStatus === "approved") {
        const rpcHandledIds = new Set();
        for (const row of selectedRows) {
          const handled = await maybeApproveTeacherChallengeFromLog(row);
          if (handled) rpcHandledIds.add(String(row.id));
        }
        selectedIdsToDirectUpdate = selectedIds.filter((id) => !rpcHandledIds.has(String(id)));
      }

      if (selectedIdsToDirectUpdate.length) {
        const updatePayload = normalizedStatus === "approved"
          ? { status: "approved" }
          : { status: "rejected" };
        const { data, error } = await supabase
          .from("logs")
          .update(updatePayload)
          .in("id", selectedIdsToDirectUpdate);
        if (error) {
          console.error(`[${normalizedStatus.toUpperCase()} ERROR]`, error);
          alert(`Failed to ${normalizedStatus} logs: ${error.message}`);
          return;
        }
        console.log(`[Bulk ${normalizedStatus}] updated`, selectedIdsToDirectUpdate.length, data);
      }

      const approvedStamp = new Date().toISOString();
      allLogs = allLogs.map(l => {
        if (!selectedIds.includes(String(l.id))) return l;
        if (normalizedStatus === "approved") {
          return { ...l, status: "approved", _approvedAtLocal: approvedStamp, updated_at: approvedStamp };
        }
        return { ...l, status: "rejected" };
      });

      if (normalizedStatus === "approved") {
        await recalculateUsersAfterApprovedBatch(affectedUserIds, levelSnapshotsBefore, {
          studioId: viewerContext.studioId
        });
      }

      if (normalizedStatus === "approved") {
        awardBadgesForApprovedUsers(affectedUserIds);
      }

      applyFilters();
      updateBulkActionBarVisibility();
      window.dispatchEvent(new Event("aa:notification-state-changed"));
    } catch (err) {
      console.error(`${normalizedStatus} logs failed:`, err);
      alert(`Failed to ${normalizedStatus} logs.`);
    }
  }

// ✅ Select All Checkbox
document.getElementById("selectAll").addEventListener("change", (e) => {
  const isChecked = e.target.checked;
  document.querySelectorAll("#logsTableBody .select-log").forEach(cb => {
    cb.checked = isChecked;
  });
  updateBulkActionBarVisibility();
});

logsTableBody.addEventListener("change", (e) => {
  if (e.target.classList.contains("select-log")) {
    updateBulkActionBarVisibility();
  }
});

// === Notifications Tab Integration ===
const showLogsBtn = document.getElementById("showLogsBtn");
const showNotificationsBtn = document.getElementById("showNotificationsBtn");
const logsWrapper = document.getElementById("logsWrapper");
const paginationControls = document.getElementById("paginationControls");
const notificationsSection = document.getElementById("notificationsSection");
const NOTIFICATION_FETCH_PAGE_SIZE = 1000;
const NOTIFICATION_FETCH_ADMIN_CAP = 20000;
const DEFAULT_NOTIFICATION_PAGE_SIZE = 100;
const notificationFilterElements = {
  modal: document.getElementById("filterNotificationsModal"),
  close: document.getElementById("filterNotificationsClose"),
  studentSearch: document.getElementById("notificationFilterStudentSearch"),
  studentsDropdown: document.getElementById("notificationFilterStudentsDropdown"),
  studentsSelected: document.getElementById("notificationFilterStudentsSelected"),
  dateFrom: document.getElementById("notificationFilterDateFrom"),
  dateTo: document.getElementById("notificationFilterDateTo"),
  keyword: document.getElementById("notificationFilterKeyword"),
  type: document.getElementById("notificationFilterType"),
  recognition: document.getElementById("notificationFilterRecognition"),
  sort: document.getElementById("notificationSort"),
  apply: document.getElementById("applyNotificationFiltersBtn"),
  reset: document.getElementById("resetNotificationFiltersBtn")
};
const recognitionExportElements = {
  modal: document.getElementById("recognitionExportModal"),
  close: document.getElementById("recognitionExportClose"),
  cancel: document.getElementById("cancelRecognitionExportBtn"),
  run: document.getElementById("runRecognitionExportBtn"),
  format: document.getElementById("recognitionExportFormat"),
  scope: document.getElementById("recognitionExportScope"),
  dateRange: document.getElementById("recognitionExportDateRange"),
  dateFrom: document.getElementById("recognitionExportDateFrom"),
  dateTo: document.getElementById("recognitionExportDateTo"),
  sort: document.getElementById("recognitionExportSort"),
  includeDates: document.getElementById("recognitionExportIncludeDates"),
  includeTeacher: document.getElementById("recognitionExportIncludeTeacher"),
  includeInstrument: document.getElementById("recognitionExportIncludeInstrument"),
  includePoints: document.getElementById("recognitionExportIncludePoints"),
  onlyUnrecognized: document.getElementById("recognitionExportOnlyUnrecognized"),
  includeInactive: document.getElementById("recognitionExportIncludeInactive")
};
let notificationFilters = {
  studentIds: [],
  dateFrom: "",
  dateTo: "",
  keyword: "",
  type: "",
  recognition: "all",
  sort: "newest"
};
let notificationFilterRoster = [];
const notificationSelectedStudentIds = new Set();
let latestRecognitionNotificationRows = [];
let recognitionExportRoster = [];
let recognitionExportRosterLoaded = false;
let recognitionLevels = [];
let recognitionLevelsLoaded = false;

const isLevelUpNotification = (row) => {
  const type = String(row?.type || "").toLowerCase();
  if (type === "level_up" || type === "level_completed") return true;
  const message = String(row?.message || "").toLowerCase();
  return message.includes("reached level") || message.includes("advanced to level") || message.includes("completed level");
};

const getNotificationRawMessage = (row) => `${row?.title || ""} ${row?.message || row?.body || ""}`.trim();

const hasLegacyLevelNotificationLanguage = (row) => {
  if (!isLevelUpNotification(row)) return false;
  const raw = getNotificationRawMessage(row).toLowerCase();
  return raw.includes("advanced to") ||
    /\breached\s+level\b/i.test(raw) ||
    raw.includes("level level");
};

const hasInvalidLevelNotificationTimestamp = (row) => {
  if (!isLevelUpNotification(row)) return false;
  const date = new Date(row?.created_at || "");
  if (Number.isNaN(date.getTime())) return true;
  const year = date.getFullYear();
  return year < 2024 || year > new Date().getFullYear() + 1;
};

const isNotificationRead = (row) => {
  if (!row) return false;
  return row?.read === true;
};

const isRecognitionGiven = (row) => {
  if (!row) return false;
  return row?.recognition_given === true || row?.recognitionGiven === true;
};

const formatRecognitionRecordedAt = (value) => {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleString();
};

const getRecognitionRecordedAtValue = (row) =>
  row?.recognition_given_at || row?.recognitionGivenAt || "";

const getRecognitionGivenByValue = (row) =>
  String(row?.recognition_given_by || row?.recognitionGivenBy || "").trim();

const getRecognitionNoteValue = (row) =>
  String(row?.recognition_note ?? row?.recognitionNote ?? "");

const getNotificationUserId = (row) => String(row?.user_id || row?.userId || "").trim();
const getNotificationType = (row) => String(row?.type || "").trim().toLowerCase();
const getCanonicalNotificationType = (row) =>
  isLevelUpNotification(row) ? "level_completed" : getNotificationType(row);
const getNotificationDisplayMessage = (row) => {
  const message = String(row?.message || "").trim();
  const legacy = message.match(/^\s*(.*?)\s+(?:reached|advanced to)\s+Level\s+(?:Level\s+)?([0-9]+)\.?\s*$/i);
  if (legacy) {
    const studentName = legacy[1]?.trim() || "Student";
    const enteredLevel = Number(legacy[2]);
    const completedLevel = enteredLevel - 1;
    if (Number.isFinite(completedLevel) && completedLevel > 0) {
      return `${studentName} completed Level ${completedLevel}.`;
    }
  }
  return message;
};
const getNotificationText = (row) => `${row?.title || ""} ${getNotificationDisplayMessage(row)}`.trim();
const getNotificationLevelNumber = (row) => {
  const direct = Number(row?.completed_level_end ?? row?.completedLevelEnd ?? row?.completed_level_start ?? row?.completedLevelStart);
  if (Number.isFinite(direct) && direct > 0) return direct;
  const match = String(row?.message || "").match(/\bLevels?\s+(\d+)/i);
  return match ? Number(match[1]) : Number.POSITIVE_INFINITY;
};
const getNotificationStudentName = (row) => {
  const message = getNotificationDisplayMessage(row);
  const match = message.match(/^(.+?)\s+(?:completed|reached|advanced)/i);
  if (match?.[1]) return match[1].trim();
  const userId = getNotificationUserId(row);
  const rosterMatch = notificationFilterRoster.find((student) => String(student.id) === userId);
  if (rosterMatch) return getQuickAddStudentName(rosterMatch);
  const exportRosterMatch = recognitionExportRoster.find((student) => String(student.id) === userId);
  return exportRosterMatch ? getQuickAddStudentName(exportRosterMatch) : "";
};
const isRecognitionTestAccountNotification = (row) =>
  isLevelUpNotification(row) && getNotificationStudentName(row).toLowerCase() === "milford music";
const shouldHideRecognitionNotification = (row) =>
  isRecognitionTestAccountNotification(row) ||
  hasLegacyLevelNotificationLanguage(row) ||
  hasInvalidLevelNotificationTimestamp(row);

const normalizeRecognitionStudentName = (name) =>
  String(name || "").trim().replace(/\s+/g, " ").toLowerCase();

const formatRecognitionExportDate = (date) => {
  if (!(date instanceof Date) || Number.isNaN(date.getTime())) return "";
  return date.toLocaleDateString("en-US");
};

const getRecognitionExportRosterMatch = (studentName) => {
  const normalized = normalizeRecognitionStudentName(studentName);
  if (!normalized) return null;
  return recognitionExportRoster.find((row) => normalizeRecognitionStudentName(getQuickAddStudentName(row)) === normalized) || null;
};

const getRecognitionExportRosterMatchFor = (studentName, userId = "") => {
  const byName = getRecognitionExportRosterMatch(studentName);
  if (byName) return byName;
  const normalizedUserId = String(userId || "").trim();
  if (normalizedUserId) {
    const byId = recognitionExportRoster.find((row) => String(row?.id || "") === normalizedUserId);
    if (byId) return byId;
  }
  return null;
};

const isRecognitionRosterInactive = (studentName) => {
  const match = getRecognitionExportRosterMatch(studentName);
  if (!match) return false;
  return match.active === false || Boolean(match.deactivated_at);
};

const getRecognitionTeacherText = (studentName) => {
  const match = getRecognitionExportRosterMatch(studentName);
  if (!match || !Array.isArray(match.teacherNames)) return "";
  return match.teacherNames.filter(Boolean).join(", ");
};

const getRecognitionInstrumentText = (studentName) => {
  const match = getRecognitionExportRosterMatch(studentName);
  const value = match?.instrument || match?.instrumentName || match?.primaryInstrument || match?.primary_instrument || "";
  return Array.isArray(value) ? value.filter(Boolean).join(", ") : String(value || "");
};

const getRecognitionPointsText = (studentName) => {
  const match = getRecognitionExportRosterMatch(studentName);
  const value = match?.totalPoints ?? match?.points ?? match?.total_points ?? "";
  return value === null || value === undefined ? "" : String(value);
};

const getLevelMinPoints = (level) => {
  const value = Number(level?.minPoints ?? level?.min_points ?? level?.minpoints ?? 0);
  return Number.isFinite(value) ? value : 0;
};

const getLevelId = (level) => {
  const candidates = [level?.level, level?.level_id, level?.levelNumber, level?.level_number, level?.id];
  for (const candidate of candidates) {
    const value = Number(candidate);
    if (Number.isFinite(value)) return value;
  }
  return null;
};

const getMinimumPointsForLevelId = (levelId) => {
  const target = Number(levelId);
  if (!Number.isFinite(target)) return 0;
  const level = recognitionLevels.find((entry) => getLevelId(entry) === target);
  return level ? getLevelMinPoints(level) : 0;
};

async function loadRecognitionLevels() {
  if (recognitionLevelsLoaded) return recognitionLevels;
  recognitionLevelsLoaded = true;
  try {
    const { data, error } = await supabase
      .from("levels")
      .select("*");
    if (error) throw error;
    recognitionLevels = (Array.isArray(data) ? data : [])
      .sort((a, b) => getLevelMinPoints(a) - getLevelMinPoints(b));
  } catch (error) {
    recognitionLevelsLoaded = false;
    recognitionLevels = [];
    console.warn("[ReviewLogs] recognition levels failed", error);
  }
  return recognitionLevels;
}

const getRecognitionTotalPointsFor = (studentName, userId = "") => {
  const match = getRecognitionExportRosterMatchFor(studentName, userId);
  const value = Number(match?.totalPoints ?? match?.points ?? match?.total_points);
  const currentLevelFloor = getMinimumPointsForLevelId(match?.level);
  if (Number.isFinite(value)) return Math.max(value, currentLevelFloor);
  return currentLevelFloor || "";
};

const getRecognitionPointsNeededForNextLevelFor = (studentName, userId = "") => {
  const totalPoints = getRecognitionTotalPointsFor(studentName, userId);
  if (!Number.isFinite(Number(totalPoints))) return "";
  const next = recognitionLevels.find((level) => Number(totalPoints) < getLevelMinPoints(level));
  if (!next) return "Max Level";
  return Math.max(0, Math.ceil(getLevelMinPoints(next) - Number(totalPoints)));
};

const getValidRecognitionNotificationRows = (notifications = [], options = {}) =>
  (Array.isArray(notifications) ? notifications : [])
    .filter((row) => {
      if (!row || shouldHideRecognitionNotification(row)) return false;
      if (getCanonicalNotificationType(row) !== "level_completed") return false;
      const student = getNotificationStudentName(row).trim();
      if (!student || student.toLowerCase() === "milford music") return false;
      if (!options.includeInactive && isRecognitionRosterInactive(student)) return false;
      if (options.onlyUnrecognized && isRecognitionGiven(row)) return false;
      const createdAt = new Date(row?.created_at || "");
      if (Number.isNaN(createdAt.getTime()) || createdAt.getFullYear() < 2024) return false;
      const dateOnly = getDateOnlyString(row?.created_at);
      if (options.dateFrom && (!dateOnly || dateOnly < options.dateFrom)) return false;
      if (options.dateTo && (!dateOnly || dateOnly > options.dateTo)) return false;
      const level = getNotificationLevelNumber(row);
      return Number.isFinite(level) && level > 0;
    });

const toRecognitionExportRow = (row) => {
  const student = getNotificationStudentName(row).trim();
  const userId = getNotificationUserId(row);
  const createdAt = new Date(row?.created_at || "");
  const totalPoints = getRecognitionTotalPointsFor(student, userId);
  return {
    source: row,
    userId,
    student,
    level: getNotificationLevelNumber(row),
    totalPoints,
    pointsNeededForNextLevel: getRecognitionPointsNeededForNextLevelFor(student, userId),
    createdAt,
    recognitionGiven: isRecognitionGiven(row),
    recognitionDate: getRecognitionRecordedAtValue(row) ? new Date(getRecognitionRecordedAtValue(row)) : null
  };
};

const sortRecognitionRows = (rows, sort) => [...rows].sort((a, b) => {
  if (sort === "student_az") return a.student.localeCompare(b.student, undefined, { sensitivity: "base" });
  if (sort === "date_desc") return b.createdAt.getTime() - a.createdAt.getTime();
  if (sort === "date_asc") return a.createdAt.getTime() - b.createdAt.getTime();
  return (b.level - a.level) ||
    a.student.localeCompare(b.student, undefined, { sensitivity: "base" });
});

const getRecognitionScopeSubtitle = (scope) => {
  if (scope === "recent") return "Most Recent Level Notifications";
  if (scope === "history") return "Entire Level Notification History";
  if (scope === "custom") return "Custom Date Range";
  return "Highest Completed Level Per Student";
};

const getRecognitionCsvFilename = (scope) => {
  if (scope === "history") return "recognition-history.csv";
  if (scope === "custom") return "recognition-custom-date-range.csv";
  if (scope === "recent") return "recognition-recent-levels.csv";
  return "recognition-highest-levels.csv";
};

const getHighestCompletedLevelsForRecognition = (notifications = [], options = {}) => {
  const byStudent = new Map();
  getValidRecognitionNotificationRows(notifications, options).forEach((row) => {
    const exportRow = toRecognitionExportRow(row);
    const key = normalizeRecognitionStudentName(exportRow.student);
    const existing = byStudent.get(key);
    if (
      !existing ||
      exportRow.level > existing.level ||
      (exportRow.level === existing.level && exportRow.createdAt.getTime() < existing.createdAt.getTime())
    ) {
      byStudent.set(key, exportRow);
    }
  });
  return sortRecognitionRows(Array.from(byStudent.values()), options.sort || "level_desc");
};

window.getHighestCompletedLevelsForRecognition = getHighestCompletedLevelsForRecognition;

const csvEscape = (value) => {
  const text = String(value ?? "");
  return /[",\n\r]/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
};

const getCurrentRecognitionNotificationRows = async () => {
  if (latestRecognitionNotificationRows.length) {
    return latestRecognitionNotificationRows;
  }
  const { data, error } = await fetchViewerNotifications(NOTIFICATION_FETCH_ADMIN_CAP);
  if (error) throw error;
  return sortNotificationsNewestFirst(mergeNotificationRows([data || []], NOTIFICATION_FETCH_ADMIN_CAP));
};

const getRecognitionExportRows = async (options = {}) => {
  await loadRecognitionLevels();
  await loadRecognitionExportRoster();
  const notifications = await getCurrentRecognitionNotificationRows();
  const scopedOptions = {
    includeInactive: Boolean(options.includeInactive),
    onlyUnrecognized: Boolean(options.onlyUnrecognized),
    dateFrom: options.scope === "custom" ? options.dateFrom : "",
    dateTo: options.scope === "custom" ? options.dateTo : "",
    sort: options.sort || "level_desc"
  };
  if (options.scope === "highest") {
    return getHighestCompletedLevelsForRecognition(notifications, scopedOptions);
  }
  const rows = getValidRecognitionNotificationRows(notifications, scopedOptions).map(toRecognitionExportRow);
  return sortRecognitionRows(rows, options.sort || (options.scope === "recent" ? "date_desc" : "level_desc"));
};

const getRecognitionExportColumns = (options, rows) => {
  const columns = [
    { key: "student", label: "Student", value: (row) => row.student },
    { key: "level", label: "Level Completed", value: (row) => row.level },
    { key: "pointsNeededForNextLevel", label: "Points Needed For Next Level", value: (row) => row.pointsNeededForNextLevel },
    ...(options.includeDates === false ? [] : [{ key: "completionDate", label: "Completion Date", value: (row) => formatRecognitionExportDate(row.createdAt) }]),
    { key: "recognitionGiven", label: "Recognition Given", value: (row) => row.recognitionGiven ? "Yes" : "No" },
    ...(rows.some((row) => row.recognitionGiven && row.recognitionDate instanceof Date && !Number.isNaN(row.recognitionDate.getTime()))
      ? [{ key: "recognitionDate", label: "Recognition Date", value: (row) => formatRecognitionExportDate(row.recognitionDate) }]
      : [])
  ];
  if (options.includeTeacher) columns.push({ key: "teacher", label: "Teacher", value: (row) => getRecognitionTeacherText(row.student) });
  if (options.includeInstrument) columns.push({ key: "instrument", label: "Instrument", value: (row) => getRecognitionInstrumentText(row.student) });
  if (options.includePoints) columns.push({ key: "totalPoints", label: "Total Points", value: (row) => row.totalPoints });
  return columns;
};

const downloadRecognitionCsv = (rows, columns, filename, statusEl) => {
  const csvRows = [
    columns.map((column) => column.label),
    ...rows.map((row) => columns.map((column) => column.value(row)))
  ];
  const csv = csvRows.map((row) => row.map(csvEscape).join(",")).join("\r\n");
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  link.remove();
  URL.revokeObjectURL(url);
  if (statusEl) statusEl.textContent = `Exported ${rows.length} student${rows.length === 1 ? "" : "s"}.`;
};

const htmlEscape = (value) => String(value ?? "")
  .replace(/&/g, "&amp;")
  .replace(/</g, "&lt;")
  .replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;");

const printRecognitionSummary = (rows, columns, subtitle, statusEl) => {
  const generatedAt = new Date().toLocaleDateString("en-US");
  const rowsHtml = rows.length
    ? rows.map((row) => `
        <tr>
          ${columns.map((column) => `<td>${htmlEscape(column.value(row))}</td>`).join("")}
        </tr>
      `).join("")
    : `<tr><td colspan="${columns.length}">No level-completion recognitions found.</td></tr>`;
  const printWindow = window.open("", "_blank");
  if (!printWindow) {
    if (statusEl) statusEl.textContent = "Print popup was blocked.";
    return;
  }
  printWindow.document.write(`<!doctype html>
      <html>
      <head>
        <meta charset="utf-8">
        <title>Recital Recognition List</title>
        <style>
          body { font-family: Arial, sans-serif; color: #123; margin: 32px; }
          h1 { margin: 0 0 4px; font-size: 28px; }
          h2 { margin: 0 0 8px; font-size: 16px; font-weight: 600; color: #345; }
          .generated { margin: 0 0 24px; font-size: 12px; color: #556; }
          table { width: 100%; border-collapse: collapse; }
          th, td { text-align: left; padding: 10px 8px; border-bottom: 1px solid #ccd8e2; }
          th { font-size: 12px; text-transform: uppercase; letter-spacing: .04em; color: #345; }
          @media print { body { margin: 20mm; } }
        </style>
      </head>
      <body>
        <h1>Recital Recognition List</h1>
        <h2>${htmlEscape(subtitle)}</h2>
        <p class="generated">Generated ${htmlEscape(generatedAt)}</p>
        <table>
          <thead>
            <tr>
              ${columns.map((column) => `<th>${htmlEscape(column.label)}</th>`).join("")}
            </tr>
          </thead>
          <tbody>${rowsHtml}</tbody>
        </table>
        <script>
          window.addEventListener("load", () => {
            window.print();
          });
        <\/script>
      </body>
      </html>`);
  printWindow.document.close();
  if (statusEl) statusEl.textContent = `Prepared ${rows.length} student${rows.length === 1 ? "" : "s"} for print.`;
};
const getNotificationDedupeKey = (row) => {
  const type = getCanonicalNotificationType(row);
  const levelNumber = getNotificationLevelNumber(row);
  const messageKey = String(row?.message || "").trim().toLowerCase();
  const studentName = getNotificationStudentName(row).toLowerCase();
  const levelOrMessage = Number.isFinite(levelNumber)
    ? `level:${levelNumber}`
    : `message:${messageKey}`;
  const personKey = (type === "level_completed" || type === "level_up")
    ? (studentName || messageKey)
    : getNotificationUserId(row);
  return [
    String(row?.studio_id || "").trim(),
    personKey,
    type,
    levelOrMessage
  ].join("|");
};
const dedupeNotificationRows = (rows) => {
  const seen = new Set();
  const deduped = [];
  (Array.isArray(rows) ? rows : []).forEach((row) => {
    if (!row) return;
    const key = getNotificationDedupeKey(row);
    if (seen.has(key)) return;
    seen.add(key);
    deduped.push(row);
  });
  return deduped;
};

const sortNotificationsNewestFirst = (rows) => [...rows].sort((a, b) => {
  const aTime = new Date(a?.created_at || 0).getTime();
  const bTime = new Date(b?.created_at || 0).getTime();
  return bTime - aTime;
});

const mergeNotificationRows = (rowSets, limit) => {
  const seen = new Set();
  const merged = [];
  rowSets.flat().sort((a, b) => {
    const aTime = new Date(a?.created_at || 0).getTime();
    const bTime = new Date(b?.created_at || 0).getTime();
    return aTime - bTime;
  }).forEach((row) => {
    if (!row) return;
    const key = getNotificationDedupeKey(row) || String(row?.id || `${row?.created_at || ""}:${row?.message || ""}:${row?.userId || row?.user_id || ""}`);
    if (seen.has(key)) return;
    seen.add(key);
    merged.push(row);
  });
  return sortNotificationsNewestFirst(merged).slice(0, limit);
};

const markNotificationsRead = async (rows) => {
  const viewerUserId = String(viewerContext?.viewerUserId || "").trim();
  const unreadIds = (Array.isArray(rows) ? rows : [])
    .filter((row) => {
      if (isNotificationRead(row)) return false;
      const rowUserId = String(row?.user_id || row?.userId || "").trim();
      return rowUserId && rowUserId === viewerUserId;
    })
    .map((row) => String(row?.id || "").trim())
    .filter(Boolean);
  if (!unreadIds.length) return;
  const attempts = [
    { label: "id+userId", userKey: "userId" },
    { label: "id+user_id", userKey: "user_id" },
    { label: "id-only", userKey: "" }
  ];
  console.log("[NotifDiag][review-logs.js][markNotificationsRead] before update", {
    source: "review-logs.js::markNotificationsRead",
    unreadIdsCount: unreadIds.length,
    userIdFilter: viewerContext.viewerUserId,
    attempts: attempts.map((attempt) => attempt.label)
  });
  for (const attempt of attempts) {
    let query = supabase
      .from("notifications")
      .update({ read: true })
      .in("id", unreadIds)
      .select("id");
    if (attempt.userKey) {
      query = query.eq(attempt.userKey, viewerContext.viewerUserId);
    }
    const { data, error } = await query;
    const updatedRows = Array.isArray(data) ? data.length : 0;
    console.log("[NotifDiag][review-logs.js][markNotificationsRead] after update", {
      source: "review-logs.js::markNotificationsRead",
      attempt: attempt.label,
      updatedRows,
      data: data ?? null,
      error: error ?? null,
      summary: error ? "update_error" : (updatedRows > 0 ? "update_ok" : "update_no_rows")
    });
    if (!error && updatedRows > 0) return;
  }
};

const updateRecognitionState = async (row, recognitionGiven, recognitionNote) => {
  const id = String(row?.id || "").trim();
  if (!id) return;
  const nowIso = new Date().toISOString();
  try {
    const { data, error } = await supabase.rpc("set_notification_recognition", {
      p_notification_id: Number(id),
      p_recognition_given: Boolean(recognitionGiven),
      p_recognition_note: String(recognitionNote || "").trim() || null
    });
    if (!error) {
      const updatedRow = Array.isArray(data) ? data[0] : data;
      if (updatedRow) {
        Object.assign(row, {
          recognition_given: updatedRow.recognition_given,
          recognition_given_at: updatedRow.recognition_given_at,
          recognition_given_by: updatedRow.recognition_given_by,
          recognition_note: updatedRow.recognition_note
        });
      }
      return;
    }
    console.warn("[ReviewLogs] recognition RPC failed; trying direct update", error);
  } catch (error) {
    console.warn("[ReviewLogs] recognition RPC failed; trying direct update", error);
  }
  const snakePayload = {
    recognition_given: Boolean(recognitionGiven),
    recognition_given_at: recognitionGiven ? nowIso : null,
    recognition_given_by: recognitionGiven ? String(viewerContext?.viewerUserId || "").trim() || null : null,
    recognition_note: String(recognitionNote || "").trim() || null
  };
  const camelPayload = {
    recognitionGiven: Boolean(recognitionGiven),
    recognitionGivenAt: recognitionGiven ? nowIso : null,
    recognitionGivenBy: recognitionGiven ? String(viewerContext?.viewerUserId || "").trim() || null : null,
    recognitionNote: String(recognitionNote || "").trim() || null
  };
  const payloadAttempts = [
    { label: "snake:userId", payload: snakePayload, userKey: "userId" },
    { label: "camel:userId", payload: camelPayload, userKey: "userId" },
    { label: "snake:user_id", payload: snakePayload, userKey: "user_id" },
    { label: "camel:user_id", payload: camelPayload, userKey: "user_id" }
  ];
  console.log("[NotifDiag][review-logs.js][toggleRecognitionGiven] toggled", {
    source: "review-logs.js::toggleRecognitionGiven",
    recognitionGiven: Boolean(recognitionGiven),
    notificationId: id
  });
  console.log("[NotifDiag][review-logs.js][toggleRecognitionGiven] outgoing update payloads", {
    source: "review-logs.js::toggleRecognitionGiven",
    notificationId: id,
    payloadAttempts,
    userIdFilter: viewerContext.viewerUserId
  });
  try {
    let updated = false;
    let lastError = null;
    for (const attempt of payloadAttempts) {
      const { data, error } = await supabase
        .from("notifications")
        .update(attempt.payload)
        .eq("id", id)
        .eq(attempt.userKey, viewerContext.viewerUserId)
        .select("id");
      const updatedRows = Array.isArray(data) ? data.length : 0;
      console.log("[NotifDiag][review-logs.js][toggleRecognitionGiven] update response", {
        source: "review-logs.js::toggleRecognitionGiven",
        attempt: attempt.label,
        updatedRows,
        data: data ?? null,
        error: error ?? null,
        summary: error ? "update_error" : (updatedRows > 0 ? "update_ok" : "update_no_rows")
      });
      if (!error && updatedRows > 0) {
        updated = true;
        Object.assign(row, attempt.payload);
        break;
      }
      if (error) {
        lastError = error;
      }
    }
    if (!updated) {
      const fallback = await supabase
        .from("notifications")
        .update(snakePayload)
        .eq("id", id)
        .select("id");
      const fallbackUpdatedRows = Array.isArray(fallback?.data) ? fallback.data.length : 0;
      console.log("[NotifDiag][review-logs.js][toggleRecognitionGiven] fallback by id response", {
        source: "review-logs.js::toggleRecognitionGiven",
        updatedRows: fallbackUpdatedRows,
        data: fallback?.data ?? null,
        error: fallback?.error ?? null,
        summary: fallback?.error ? "update_error" : (fallbackUpdatedRows > 0 ? "update_ok" : "update_no_rows")
      });
      if (!fallback?.error && fallbackUpdatedRows > 0) {
        updated = true;
        Object.assign(row, snakePayload);
      } else if (fallback?.error) {
        lastError = fallback.error;
      }
    }
    if (!updated && lastError) {
      console.warn("[ReviewLogs] recognition update failed", lastError);
    }
  } catch (error) {
    console.warn("[ReviewLogs] recognition update failed", error);
  }
};

async function fetchNotificationsAttemptPages(attempt, limit) {
  const viewerUserId = String(viewerContext?.viewerUserId || "").trim();
  const activeStudioId = String(viewerContext?.studioId || "").trim();
  const maxRows = Number.isFinite(Number(limit)) ? Number(limit) : NOTIFICATION_FETCH_ADMIN_CAP;
  const rows = [];
  for (let from = 0; from < maxRows; from += NOTIFICATION_FETCH_PAGE_SIZE) {
    const to = Math.min(from + NOTIFICATION_FETCH_PAGE_SIZE - 1, maxRows - 1);
    const filters = {
      ...(attempt.userKey ? { [attempt.userKey]: viewerUserId } : {}),
      studio_id: attempt.includeStudio ? activeStudioId : "(omitted)",
      range: `${from}-${to}`,
      orderBy: "created_at desc"
    };
    console.log("[NotifDiag][review-logs.js][fetchViewerNotifications] query start", {
      source: "review-logs.js::fetchViewerNotifications",
      attempt: attempt.label,
      filters
    });
    let query = supabase
      .from("notifications")
      .select("*")
      .order("created_at", { ascending: false })
      .range(from, to);
    if (attempt.userKey) {
      query = query.eq(attempt.userKey, viewerUserId);
    }
    if (attempt.includeStudio) {
      query = query.eq("studio_id", activeStudioId);
    }
    const { data, error } = await query;
    const count = Array.isArray(data) ? data.length : 0;
    console.log("[NotifDiag][review-logs.js][fetchViewerNotifications] query result", {
      source: "review-logs.js::fetchViewerNotifications",
      attempt: attempt.label,
      filters,
      count,
      error: error ?? null
    });
    if (error) return { data: rows, error };
    rows.push(...(data || []));
    if (count < NOTIFICATION_FETCH_PAGE_SIZE) break;
  }
  return { data: rows, error: null };
}

async function fetchViewerNotifications(limit = NOTIFICATION_FETCH_ADMIN_CAP) {
  const viewerUserId = String(viewerContext?.viewerUserId || "").trim();
  const activeStudioId = String(viewerContext?.studioId || "").trim();
  const maxRows = Number.isFinite(Number(limit)) ? Number(limit) : NOTIFICATION_FETCH_ADMIN_CAP;
  const staffCanViewStudioNotifications = Boolean(activeStudioId && (viewerContext?.isAdmin || viewerContext?.isTeacher));
  const attempts = staffCanViewStudioNotifications
    ? [{ label: "studio_id:all_notifications", userKey: "", includeStudio: true, studioOnly: true }]
    : [
        { label: "user_id+studio_id", userKey: "user_id", includeStudio: Boolean(activeStudioId) },
        { label: "user_id:no_studio_filter", userKey: "user_id", includeStudio: false },
        { label: "legacy_userId+studio_id", userKey: "userId", includeStudio: Boolean(activeStudioId), legacyOnly: true }
      ];
  console.log("[NotifDiag][review-logs.js][fetchViewerNotifications] query plan", {
    source: "review-logs.js::fetchViewerNotifications",
    viewerUserId,
    activeStudioId: activeStudioId || null,
    limit: maxRows,
    pageSize: NOTIFICATION_FETCH_PAGE_SIZE,
    attempts: attempts.map((attempt) => attempt.label),
    activeFilters: staffCanViewStudioNotifications
      ? { studio_id: activeStudioId || null }
      : { viewerUserId, studio_id: activeStudioId || null },
    reason: staffCanViewStudioNotifications
      ? "Staff notification tab fetches every notification in the active studio with pagination."
      : "Fetch canonical user_id rows first; legacy userId is fallback only; page through results."
  });
  const rowSets = [];
  const errors = [];
  let rawFetchedCount = 0;
  for (const attempt of attempts) {
    const { data, error } = await fetchNotificationsAttemptPages(attempt, maxRows);
    const count = Array.isArray(data) ? data.length : 0;
    rawFetchedCount += count;
    if (error) {
      errors.push({ attempt: attempt.label, error });
      continue;
    }
    if (attempt.legacyOnly && rowSets.length > 0) continue;
    if (count > 0) rowSets.push(data);
  }
  const merged = mergeNotificationRows(rowSets, maxRows);
  console.log("[NotifDiag][review-logs.js][fetchViewerNotifications] merged result", {
    source: "review-logs.js::fetchViewerNotifications",
    viewerUserId,
    activeStudioId: activeStudioId || null,
    mergedCount: merged.length,
    totalNotificationsFetched: rawFetchedCount,
    totalNotificationsAfterMerge: merged.length,
    pageSize: NOTIFICATION_FETCH_PAGE_SIZE,
    cap: maxRows,
    activeFilters: staffCanViewStudioNotifications
      ? { studio_id: activeStudioId || null }
      : { viewerUserId, studio_id: activeStudioId || null },
    errorCount: errors.length,
    errors
  });
  if (merged.length > 0 || errors.length < attempts.length) {
    return { data: merged, error: null };
  }
  return { data: [], error: errors[0]?.error || null };
}

window.AA_checkLevelNotificationCleanup = async () => {
  const { data, error } = await fetchViewerNotifications(NOTIFICATION_FETCH_ADMIN_CAP);
  if (error) throw error;
  const badRows = (Array.isArray(data) ? data : []).filter((row) =>
    isLevelUpNotification(row) &&
    (hasLegacyLevelNotificationLanguage(row) || hasInvalidLevelNotificationTimestamp(row))
  );
  const report = badRows.map((row) => ({
    id: row?.id || "",
    created_at: row?.created_at || "",
    type: row?.type || "",
    message: row?.message || "",
    reasons: [
      hasLegacyLevelNotificationLanguage(row) ? "legacy language" : "",
      hasInvalidLevelNotificationTimestamp(row) ? "invalid timestamp" : ""
    ].filter(Boolean).join(", ")
  }));
  console.table(report);
  return report;
};

async function updateNotificationsButtonState() {
  if (!showNotificationsBtn) return;
  const { data, error } = await fetchViewerNotifications(NOTIFICATION_FETCH_ADMIN_CAP);
  if (error) {
    console.warn("[ReviewLogs] notification button state fetch failed", error);
    showNotificationsBtn.classList.remove("has-alert");
    return;
  }
  const viewerUserId = String(viewerContext?.viewerUserId || "").trim();
  const unresolvedLevelUpCount = data.filter((row) => {
    const rowUserId = String(row?.user_id || row?.userId || "").trim();
    return rowUserId === viewerUserId &&
      isLevelUpNotification(row) &&
      !shouldHideRecognitionNotification(row) &&
      !isNotificationRead(row);
  }).length;
  console.log("[NotifDiag][review-logs.js][updateNotificationsButtonState] unread logic", {
    source: "review-logs.js::updateNotificationsButtonState",
    queriedUserId: viewerContext.viewerUserId,
    queriedStudioId: viewerContext?.studioId || null,
    totalNotifications: Array.isArray(data) ? data.length : 0,
    unreadReadFilterLogic: "visible level-completion notification && row.read !== true",
    unresolvedLevelUpCount
  });
  showNotificationsBtn.classList.toggle("has-alert", unresolvedLevelUpCount > 0);
  showNotificationsBtn.setAttribute("aria-label", unresolvedLevelUpCount > 0
    ? `Notifications (${unresolvedLevelUpCount} level completion alerts pending)`
    : "Notifications");
}

function hasActiveNotificationFilters() {
  return Boolean(
    notificationFilters.studentIds.length ||
    notificationFilters.dateFrom ||
    notificationFilters.dateTo ||
    notificationFilters.keyword ||
    notificationFilters.type ||
    notificationFilters.recognition !== "all" ||
    notificationFilters.sort !== "newest"
  );
}

function renderNotificationFilterSummary(container, count) {
  const parts = [];
  if (notificationFilters.studentIds.length) {
    parts.push(notificationFilters.studentIds.map((id) => {
      const student = notificationFilterRoster.find((row) => String(row.id) === String(id));
      return student ? getQuickAddStudentName(student) : "";
    }).filter(Boolean).join(", ") || "Selected students");
  }
  if (notificationFilters.type) parts.push(notificationFilters.type.replace(/_/g, " "));
  if (notificationFilters.recognition !== "all") parts.push(notificationFilters.recognition === "given" ? "Recognition given" : "Recognition not given");
  if (notificationFilters.dateFrom || notificationFilters.dateTo) {
    const from = notificationFilters.dateFrom ? formatDateOnlyForDisplay(notificationFilters.dateFrom, { month: "short", day: "numeric" }) : "Start";
    const to = notificationFilters.dateTo ? formatDateOnlyForDisplay(notificationFilters.dateTo, { month: "short", day: "numeric" }) : "Today";
    parts.push(`${from}-${to}`);
  }
  if (notificationFilters.keyword) parts.push(`"${notificationFilters.keyword}"`);
  if (notificationFilters.sort !== "newest") parts.push(`Sort: ${notificationFilters.sort.replace(/_/g, " ")}`);
  container.textContent = parts.length ? `Filtered by: ${parts.join(", ")} (${count} match${count === 1 ? "" : "es"})` : "";
  container.style.display = parts.length ? "block" : "none";
}

function applyNotificationFiltersAndSort(rows) {
  const selectedIds = notificationFilters.studentIds.map(String);
  const selectedNames = selectedIds.map((id) => {
    const student = notificationFilterRoster.find((row) => String(row.id) === id);
    return student ? getQuickAddStudentName(student).toLowerCase() : "";
  }).filter(Boolean);
  const keyword = notificationFilters.keyword.toLowerCase();
  const filtered = (Array.isArray(rows) ? rows : []).filter((row) => {
    if (shouldHideRecognitionNotification(row)) return false;
    const dateOnly = getDateOnlyString(row?.created_at);
    const text = getNotificationText(row).toLowerCase();
    const userId = getNotificationUserId(row);
    const matchesStudent = !selectedIds.length ||
      selectedIds.includes(userId) ||
      selectedNames.some((name) => name && text.includes(name));
    const matchesFrom = !notificationFilters.dateFrom || (dateOnly && dateOnly >= notificationFilters.dateFrom);
    const matchesTo = !notificationFilters.dateTo || (dateOnly && dateOnly <= notificationFilters.dateTo);
    const matchesKeyword = !keyword || text.includes(keyword);
    const matchesType = !notificationFilters.type || getCanonicalNotificationType(row) === notificationFilters.type;
    const recognized = isRecognitionGiven(row);
    const matchesRecognition = notificationFilters.recognition === "all" ||
      (notificationFilters.recognition === "given" && recognized) ||
      (notificationFilters.recognition === "not_given" && !recognized);
    return matchesStudent && matchesFrom && matchesTo && matchesKeyword && matchesType && matchesRecognition;
  });
  return filtered.sort((a, b) => {
    if (notificationFilters.sort === "oldest") {
      return new Date(a?.created_at || 0).getTime() - new Date(b?.created_at || 0).getTime();
    }
    if (notificationFilters.sort === "student_az") {
      return getNotificationStudentName(a).localeCompare(getNotificationStudentName(b), undefined, { sensitivity: "base" });
    }
    if (notificationFilters.sort === "level_low_high") {
      return getNotificationLevelNumber(a) - getNotificationLevelNumber(b);
    }
    if (notificationFilters.sort === "level_high_low") {
      return getNotificationLevelNumber(b) - getNotificationLevelNumber(a);
    }
    if (notificationFilters.sort === "recognition_not_given") {
      return Number(isRecognitionGiven(a)) - Number(isRecognitionGiven(b)) ||
        (new Date(b?.created_at || 0).getTime() - new Date(a?.created_at || 0).getTime());
    }
    return new Date(b?.created_at || 0).getTime() - new Date(a?.created_at || 0).getTime();
  });
}

function renderNotificationPickerSelected() {
  const target = notificationFilterElements.studentsSelected;
  if (!target) return;
  target.innerHTML = "";
  if (!notificationSelectedStudentIds.size) {
    const empty = document.createElement("span");
    empty.className = "staff-student-empty";
    empty.textContent = "No students selected";
    target.appendChild(empty);
    return;
  }
  notificationFilterRoster
    .filter((student) => notificationSelectedStudentIds.has(String(student.id)))
    .forEach((student) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "staff-student-chip";
      chip.textContent = `${getQuickAddStudentName(student)} x`;
      chip.addEventListener("click", () => {
        notificationSelectedStudentIds.delete(String(student.id));
        renderNotificationPickerSelected();
        renderNotificationPickerDropdown();
      });
      target.appendChild(chip);
    });
}

function renderNotificationPickerDropdown() {
  const input = notificationFilterElements.studentSearch;
  const dropdown = notificationFilterElements.studentsDropdown;
  if (!input || !dropdown) return;
  const query = String(input.value || "").trim().toLowerCase();
  dropdown.innerHTML = "";
  if (!query) {
    dropdown.setAttribute("hidden", "");
    return;
  }
  const matches = notificationFilterRoster.filter((student) => getQuickAddStudentName(student).toLowerCase().includes(query));
  if (!matches.length) {
    const empty = document.createElement("div");
    empty.className = "staff-student-no-match";
    empty.textContent = "No matching students";
    dropdown.appendChild(empty);
    dropdown.removeAttribute("hidden");
    return;
  }
  matches.forEach((student) => {
    const id = String(student.id);
    const item = document.createElement("button");
    item.type = "button";
    item.className = "staff-student-option";
    const selected = notificationSelectedStudentIds.has(id);
    item.textContent = selected ? `Selected: ${getQuickAddStudentName(student)}` : getQuickAddStudentName(student);
    if (selected) item.classList.add("is-selected");
    item.addEventListener("click", () => {
      if (notificationSelectedStudentIds.has(id)) notificationSelectedStudentIds.delete(id);
      else notificationSelectedStudentIds.add(id);
      renderNotificationPickerSelected();
      renderNotificationPickerDropdown();
      input.focus();
    });
    dropdown.appendChild(item);
  });
  dropdown.removeAttribute("hidden");
}

async function loadNotificationFilterStudents() {
  const { data, error } = await supabase
    .from("users")
    .select("id, firstName, lastName, email, roles, teacherIds")
    .eq("studio_id", viewerContext.studioId)
    .eq("active", true)
    .is("deactivated_at", null);
  if (error) {
    console.error("[ReviewLogs] notification filter students failed", error);
    notificationFilterRoster = [];
    renderNotificationPickerSelected();
    return;
  }
  notificationFilterRoster = (data || [])
    .filter((student) => {
      const roles = Array.isArray(student.roles) ? student.roles : [student.roles];
      return roles.map((role) => String(role || "").toLowerCase()).includes("student") && getQuickAddStudentName(student) !== "Student";
    })
    .sort((a, b) => getQuickAddStudentName(a).localeCompare(getQuickAddStudentName(b), undefined, { sensitivity: "base" }));
  renderNotificationPickerSelected();
  renderNotificationPickerDropdown();
}

async function loadRecognitionExportRoster() {
  if (recognitionExportRosterLoaded) return recognitionExportRoster;
  recognitionExportRosterLoaded = true;
  try {
    const { data: students, error: studentsError } = await supabase
      .from("users")
      .select("id, firstName, lastName, email, roles, teacherIds, active, deactivated_at, points, level, instrument, instrumentName, primaryInstrument, primary_instrument")
      .eq("studio_id", viewerContext.studioId);
    if (studentsError) throw studentsError;

    const { data: approvedLogs, error: logsError } = await supabase
      .from("logs")
      .select("userId, points")
      .eq("studio_id", viewerContext.studioId)
      .eq("status", "approved");
    if (logsError) throw logsError;
    const approvedPointsByUserId = new Map();
    (approvedLogs || []).forEach((log) => {
      const userId = String(log?.userId || "").trim();
      const points = Number(log?.points || 0);
      if (!userId || !Number.isFinite(points)) return;
      approvedPointsByUserId.set(userId, Number(approvedPointsByUserId.get(userId) || 0) + points);
    });

    const { data: staffRows } = await supabase
      .from("users")
      .select("id, firstName, lastName, email, roles")
      .eq("studio_id", viewerContext.studioId);
    const staffNameById = new Map((staffRows || []).map((row) => [String(row.id), getQuickAddStudentName(row)]));

    recognitionExportRoster = (students || [])
      .filter((row) => {
        const roles = Array.isArray(row.roles) ? row.roles : [row.roles];
        return roles.map((role) => String(role || "").toLowerCase()).includes("student");
      })
      .map((row) => ({
        ...row,
        approvedTotalPoints: Number(approvedPointsByUserId.get(String(row.id)) || 0),
        totalPoints: Math.max(
          Number.isFinite(Number(row.points)) ? Number(row.points) : 0,
          Number(approvedPointsByUserId.get(String(row.id)) || 0),
          getMinimumPointsForLevelId(row.level)
        ),
        teacherNames: Array.isArray(row.teacherIds)
          ? row.teacherIds.map((id) => staffNameById.get(String(id))).filter(Boolean)
          : []
      }));
  } catch (error) {
    recognitionExportRosterLoaded = false;
    recognitionExportRoster = [];
    console.warn("[ReviewLogs] recognition export roster failed", error);
  }
  return recognitionExportRoster;
}

function syncNotificationFilterModalFields() {
  notificationSelectedStudentIds.clear();
  notificationFilters.studentIds.forEach((id) => notificationSelectedStudentIds.add(String(id)));
  if (notificationFilterElements.studentSearch) notificationFilterElements.studentSearch.value = "";
  if (notificationFilterElements.dateFrom) notificationFilterElements.dateFrom.value = notificationFilters.dateFrom;
  if (notificationFilterElements.dateTo) notificationFilterElements.dateTo.value = notificationFilters.dateTo;
  if (notificationFilterElements.keyword) notificationFilterElements.keyword.value = notificationFilters.keyword;
  if (notificationFilterElements.type) notificationFilterElements.type.value = notificationFilters.type;
  if (notificationFilterElements.recognition) notificationFilterElements.recognition.value = notificationFilters.recognition;
  if (notificationFilterElements.sort) notificationFilterElements.sort.value = notificationFilters.sort;
  renderNotificationPickerSelected();
  renderNotificationPickerDropdown();
}

function collectNotificationFilterModalFields() {
  return {
    studentIds: Array.from(notificationSelectedStudentIds),
    dateFrom: notificationFilterElements.dateFrom?.value || "",
    dateTo: notificationFilterElements.dateTo?.value || "",
    keyword: String(notificationFilterElements.keyword?.value || "").trim(),
    type: notificationFilterElements.type?.value || "",
    recognition: notificationFilterElements.recognition?.value || "all",
    sort: notificationFilterElements.sort?.value || "newest"
  };
}

function closeNotificationFilterModal() {
  if (notificationFilterElements.modal) notificationFilterElements.modal.style.display = "none";
  if (notificationFilterElements.studentsDropdown) notificationFilterElements.studentsDropdown.setAttribute("hidden", "");
}

async function openNotificationFilterModal() {
  await loadNotificationFilterStudents();
  syncNotificationFilterModalFields();
  if (notificationFilterElements.modal) notificationFilterElements.modal.style.display = "flex";
  if (notificationFilterElements.studentSearch) notificationFilterElements.studentSearch.focus();
}

function resetNotificationFilters() {
  notificationFilters = {
    studentIds: [],
    dateFrom: "",
    dateTo: "",
    keyword: "",
    type: "",
    recognition: "all",
    sort: "newest"
  };
  notificationSelectedStudentIds.clear();
  syncNotificationFilterModalFields();
}

function syncRecognitionExportDateRange() {
  const showRange = recognitionExportElements.scope?.value === "custom";
  if (recognitionExportElements.dateRange) {
    recognitionExportElements.dateRange.style.display = showRange ? "" : "none";
  }
}

function resetRecognitionExportOptions() {
  if (recognitionExportElements.format) recognitionExportElements.format.value = "print";
  if (recognitionExportElements.scope) recognitionExportElements.scope.value = "highest";
  if (recognitionExportElements.sort) recognitionExportElements.sort.value = "level_desc";
  if (recognitionExportElements.dateFrom) recognitionExportElements.dateFrom.value = "";
  if (recognitionExportElements.dateTo) recognitionExportElements.dateTo.value = "";
  if (recognitionExportElements.includeDates) recognitionExportElements.includeDates.checked = true;
  if (recognitionExportElements.includeTeacher) recognitionExportElements.includeTeacher.checked = false;
  if (recognitionExportElements.includeInstrument) recognitionExportElements.includeInstrument.checked = false;
  if (recognitionExportElements.includePoints) recognitionExportElements.includePoints.checked = false;
  if (recognitionExportElements.onlyUnrecognized) recognitionExportElements.onlyUnrecognized.checked = false;
  if (recognitionExportElements.includeInactive) recognitionExportElements.includeInactive.checked = false;
  syncRecognitionExportDateRange();
}

function openRecognitionExportModal() {
  resetRecognitionExportOptions();
  if (recognitionExportElements.modal) recognitionExportElements.modal.style.display = "flex";
}

function closeRecognitionExportModal() {
  if (recognitionExportElements.modal) recognitionExportElements.modal.style.display = "none";
}

function collectRecognitionExportOptions() {
  return {
    format: recognitionExportElements.format?.value || "print",
    scope: recognitionExportElements.scope?.value || "highest",
    sort: recognitionExportElements.sort?.value || "level_desc",
    dateFrom: recognitionExportElements.dateFrom?.value || "",
    dateTo: recognitionExportElements.dateTo?.value || "",
    includeDates: recognitionExportElements.includeDates?.checked !== false,
    includeTeacher: Boolean(recognitionExportElements.includeTeacher?.checked),
    includeInstrument: Boolean(recognitionExportElements.includeInstrument?.checked),
    includePoints: Boolean(recognitionExportElements.includePoints?.checked),
    onlyUnrecognized: Boolean(recognitionExportElements.onlyUnrecognized?.checked),
    includeInactive: Boolean(recognitionExportElements.includeInactive?.checked)
  };
}

async function runRecognitionExport() {
  const button = recognitionExportElements.run;
  if (button) button.disabled = true;
  try {
    const options = collectRecognitionExportOptions();
    const rows = await getRecognitionExportRows(options);
    const columns = getRecognitionExportColumns(options, rows);
    const subtitle = getRecognitionScopeSubtitle(options.scope);
    const status = document.querySelector(".notification-admin-status");
    closeRecognitionExportModal();
    if (options.format === "csv") {
      downloadRecognitionCsv(rows, columns, getRecognitionCsvFilename(options.scope), status);
    } else {
      printRecognitionSummary(rows, columns, subtitle, status);
    }
  } catch (error) {
    console.error("[ReviewLogs] recognition export failed", error);
    const status = document.querySelector(".notification-admin-status");
    if (status) status.textContent = "Export failed: " + (error?.message || "Unknown error");
  } finally {
    if (button) button.disabled = false;
  }
}

if (notificationFilterElements.close) {
  notificationFilterElements.close.addEventListener("click", closeNotificationFilterModal);
}
if (notificationFilterElements.modal) {
  notificationFilterElements.modal.addEventListener("click", (event) => {
    if (event.target === notificationFilterElements.modal) closeNotificationFilterModal();
  });
}
if (notificationFilterElements.studentSearch) {
  notificationFilterElements.studentSearch.addEventListener("input", renderNotificationPickerDropdown);
  notificationFilterElements.studentSearch.addEventListener("focus", renderNotificationPickerDropdown);
}
if (notificationFilterElements.apply) {
  notificationFilterElements.apply.addEventListener("click", async () => {
    notificationFilters = collectNotificationFilterModalFields();
    closeNotificationFilterModal();
    await loadNotifications("", { resetPage: true });
  });
}
if (notificationFilterElements.reset) {
  notificationFilterElements.reset.addEventListener("click", async () => {
    resetNotificationFilters();
    closeNotificationFilterModal();
    await loadNotifications("", { resetPage: true });
  });
}
if (recognitionExportElements.close) {
  recognitionExportElements.close.addEventListener("click", closeRecognitionExportModal);
}
if (recognitionExportElements.cancel) {
  recognitionExportElements.cancel.addEventListener("click", closeRecognitionExportModal);
}
if (recognitionExportElements.modal) {
  recognitionExportElements.modal.addEventListener("click", (event) => {
    if (event.target === recognitionExportElements.modal) closeRecognitionExportModal();
  });
}
if (recognitionExportElements.scope) {
  recognitionExportElements.scope.addEventListener("change", syncRecognitionExportDateRange);
}
if (recognitionExportElements.run) {
  recognitionExportElements.run.addEventListener("click", () => {
    void runRecognitionExport();
  });
}
document.addEventListener("click", (event) => {
  const picker = notificationFilterElements.studentSearch?.closest(".staff-student-picker");
  if (picker && !picker.contains(event.target) && notificationFilterElements.studentsDropdown) {
    notificationFilterElements.studentsDropdown.setAttribute("hidden", "");
  }
});

if (showLogsBtn && showNotificationsBtn) {
  showLogsBtn.addEventListener("click", () => {
    logsWrapper.style.display = "block";
    if (paginationControls) paginationControls.style.display = "";
    notificationsSection.style.display = "none";
  });

  showNotificationsBtn.addEventListener("click", async () => {
    logsWrapper.style.display = "none";
    if (paginationControls) paginationControls.style.display = "none";
    notificationsSection.style.display = "block";
    showNotificationsBtn.classList.remove("has-alert");
    showNotificationsBtn.setAttribute("aria-label", "Notifications");
    await loadNotifications();
    window.dispatchEvent(new Event("aa:notification-state-changed"));
  });
}

function createNotificationAdminControls(statusText = "") {
  const controls = document.createElement("div");
  controls.className = "notification-admin-controls";

  const status = document.createElement("span");
  status.className = "notification-admin-status";
  status.textContent = statusText;

  const filterButton = document.createElement("button");
  filterButton.id = "filterNotificationsBtn";
  filterButton.className = "blue-button";
  filterButton.type = "button";
  filterButton.textContent = "Filter Notifications";
  filterButton.addEventListener("click", () => {
    void openNotificationFilterModal();
  });
  controls.appendChild(filterButton);

  const exportButton = document.createElement("button");
  exportButton.id = "openRecognitionExportBtn";
  exportButton.className = "blue-button";
  exportButton.type = "button";
  exportButton.textContent = "Export / Print";
  exportButton.addEventListener("click", () => {
    openRecognitionExportModal();
  });
  controls.appendChild(exportButton);

  if (!viewerContext?.isAdmin) {
    controls.appendChild(status);
    return controls;
  }

  const button = document.createElement("button");
  button.id = "recalculateNotificationsBtn";
  button.className = "blue-button";
  button.type = "button";
  button.textContent = "Recalculate Notifications";

  button.addEventListener("click", async () => {
    if (!confirm("This will rebuild missing level-completion notifications. Continue?")) return;
    button.disabled = true;
    status.textContent = "Recalculating...";
    try {
      const { data, error } = await supabase.rpc("backfill_level_notifications_for_studio", {
        p_studio_id: viewerContext.studioId
      });
      if (error) throw error;
      const created = Number(data?.insertedNotifications ?? data?.inserted_notifications ?? 0);
      const updatedTimestamps = Number(data?.updatedTimestamps ?? data?.updated_timestamps ?? data?.updatedNotificationTimestamps ?? 0);
      const timestampUpdatesAttempted = Number(data?.timestampUpdatesAttempted ?? data?.timestamp_updates_attempted ?? 0);
      const timestampUpdatesCompleted = Number(data?.timestampUpdatesCompleted ?? data?.timestamp_updates_completed ?? updatedTimestamps);
      const attempted = Number(data?.attempted ?? timestampUpdatesAttempted);
      const updated = Number(data?.updated ?? timestampUpdatesCompleted);
      const duplicateGroupsFound = Number(data?.duplicateGroupsFound ?? data?.duplicate_groups_found ?? 0);
      const unmatchedStudent = Number(data?.unmatched_student ?? data?.unmatchedStudent ?? 0);
      const unmatchedLevel = Number(data?.unmatched_level ?? data?.unmatchedLevel ?? 0);
      const noCrossingLog = Number(data?.no_crossing_log ?? data?.noCrossingLog ?? 0);
      const preview = await fetchViewerNotifications(NOTIFICATION_FETCH_ADMIN_CAP);
      if (preview.error) throw preview.error;
      const rawPreviewCount = Array.isArray(preview.data) ? preview.data.length : 0;
      const dedupedPreview = mergeNotificationRows([preview.data || []], NOTIFICATION_FETCH_ADMIN_CAP);
      const hiddenDuplicates = Math.max(0, rawPreviewCount - dedupedPreview.length);
      console.log("[ReviewLogs] notification backfill complete", {
        rpc: "backfill_level_notifications_for_studio",
        studioId: viewerContext.studioId,
        result: data,
        created,
        attempted,
        updated,
        timestampUpdatesAttempted,
        timestampUpdatesCompleted,
        duplicateGroupsFound,
        unmatchedStudent,
        unmatchedLevel,
        noCrossingLog,
        hiddenDuplicates
      });
      await loadNotifications(
        `Created ${created} notification${created === 1 ? "" : "s"}; updated ${updatedTimestamps} timestamp${updatedTimestamps === 1 ? "" : "s"}; hidden ${hiddenDuplicates} duplicate${hiddenDuplicates === 1 ? "" : "s"}. Unmatched: ${unmatchedStudent} student, ${unmatchedLevel} level, ${noCrossingLog} crossing log.`,
        { resetPage: true, prefetchedNotifications: preview.data, prefetchedError: preview.error }
      );
      await updateNotificationsButtonState();
      window.dispatchEvent(new Event("aa:notification-state-changed"));
    } catch (error) {
      console.error("[ReviewLogs] notification backfill failed", error);
      status.textContent = "Recalculation failed: " + (error?.message || "Unknown error");
      button.disabled = false;
    }
  });

  controls.appendChild(button);
  controls.appendChild(status);
  return controls;
}

async function loadNotifications(statusText = "", options = {}) {
  notificationsSection.innerHTML = "<p>Loading notifications...</p>";
  await loadRecognitionLevels();
  await loadRecognitionExportRoster();
  let notificationCurrentPage = options?.resetPage ? 1 : 1;
  let notificationPageSize = Number(document.getElementById("logsPerPage")?.value || DEFAULT_NOTIFICATION_PAGE_SIZE);
  if (!Number.isFinite(notificationPageSize) || notificationPageSize <= 0) notificationPageSize = DEFAULT_NOTIFICATION_PAGE_SIZE;

  const prefetchedNotifications = Array.isArray(options?.prefetchedNotifications)
    ? options.prefetchedNotifications
    : null;
  const fetchResult = prefetchedNotifications
    ? { data: prefetchedNotifications, error: options?.prefetchedError || null }
    : await fetchViewerNotifications(NOTIFICATION_FETCH_ADMIN_CAP);
  const { data: notifications, error } = fetchResult;

  console.log("[NotifDiag][review-logs.js][loadNotifications] render fetch", {
    source: "review-logs.js::loadNotifications",
    queriedUserId: viewerContext.viewerUserId,
    queriedStudioId: viewerContext?.studioId || null,
    unreadReadFilterLogic: "red dot uses read/unread only",
    count: Array.isArray(notifications) ? notifications.length : 0,
    error: error || null
  });

  if (error) {
    notificationsSection.innerHTML = `<p>Error loading notifications: ${error.message}</p>`;
    return;
  }

  if (!notifications || notifications.length === 0) {
    latestRecognitionNotificationRows = [];
    notificationsSection.innerHTML = "";
    notificationsSection.appendChild(createNotificationAdminControls(statusText));
    const empty = document.createElement("p");
    empty.textContent = "No notifications yet.";
    notificationsSection.appendChild(empty);
    return { rawNotificationCount: 0, dedupedNotificationCount: 0, hiddenDuplicateCount: 0 };
  }

  const rawNotificationCount = Array.isArray(notifications) ? notifications.length : 0;
  const dedupedNotifications = sortNotificationsNewestFirst(mergeNotificationRows([notifications], NOTIFICATION_FETCH_ADMIN_CAP));
  await markNotificationsRead(dedupedNotifications);
  const normalizedNotifications = dedupedNotifications.map((row) => ({ ...row, read: true }));
  latestRecognitionNotificationRows = normalizedNotifications;
  const filteredNotifications = applyNotificationFiltersAndSort(normalizedNotifications);
  const dedupedNotificationCount = normalizedNotifications.length;
  const filteredNotificationCount = filteredNotifications.length;
  const hiddenDuplicateCount = Math.max(0, rawNotificationCount - dedupedNotificationCount);

  console.log("[ReviewLogs] notifications fetched for render", {
    rawNotificationCount,
    dedupedNotificationCount,
    filteredNotificationCount,
    duplicateGroupsFound: hiddenDuplicateCount,
    duplicatesHiddenInUi: hiddenDuplicateCount,
    currentPage: notificationCurrentPage,
    pageSize: notificationPageSize,
    activeFilters: notificationFilters
  });
  console.log("[ReviewLogs] notifications loaded", {
    notificationsLoaded: normalizedNotifications.length,
    rawNotificationCount,
    dedupedNotificationCount,
    filteredNotificationCount,
    duplicatesHiddenInUi: hiddenDuplicateCount,
    currentPage: notificationCurrentPage,
    pageSize: notificationPageSize,
    renderedNotificationCount: 0
  });

  const filterSummaryEl = document.createElement("div");
  filterSummaryEl.className = "active-filters-summary notification-active-filters-summary";
  renderNotificationFilterSummary(filterSummaryEl, filteredNotificationCount);

  const countEl = document.createElement("div");
  countEl.className = "notification-count-summary";

  const list = document.createElement("ul");
  list.className = "review-notification-list";
  list.style.listStyle = "none";
  list.style.padding = "0";
  const header = document.createElement("li");
  header.className = "review-notification-header";
  header.setAttribute("aria-hidden", "true");
  header.innerHTML = `
      <div class="review-notification-header-main">Notification</div>
      <div class="review-notification-header-recognition">
        <span>Recognition given</span>
        <button
          type="button"
          class="review-notification-help-trigger"
          title="Mark this when the student's level completion has been recognized by your studio."
          aria-label="Recognition given help"
        >?</button>
      </div>
  `;
  list.appendChild(header);

  const pager = document.createElement("div");
  pager.className = "notification-pagination-row";
  const prevBtn = document.createElement("button");
  prevBtn.type = "button";
  prevBtn.className = "blue-button";
  prevBtn.textContent = "Prev";
  const pageInfo = document.createElement("span");
  pageInfo.className = "notification-page-info";
  const nextBtn = document.createElement("button");
  nextBtn.type = "button";
  nextBtn.className = "blue-button";
  nextBtn.textContent = "Next";
  const pageSizeSelect = document.createElement("select");
  pageSizeSelect.className = "blue-button";
  [25, 50, 100].forEach((size) => {
    const option = document.createElement("option");
    option.value = String(size);
    option.textContent = String(size);
    if (size === notificationPageSize) option.selected = true;
    pageSizeSelect.appendChild(option);
  });
  if (![25, 50, 100].includes(notificationPageSize)) {
    const option = document.createElement("option");
    option.value = String(notificationPageSize);
    option.textContent = String(notificationPageSize);
    option.selected = true;
    pageSizeSelect.appendChild(option);
  }
  pager.appendChild(prevBtn);
  pager.appendChild(pageInfo);
  pager.appendChild(nextBtn);
  pager.appendChild(pageSizeSelect);

  const renderNotificationRows = () => {
    list.querySelectorAll(".review-notification-item").forEach((item) => item.remove());
    const total = filteredNotifications.length;
    const totalPages = Math.max(1, Math.ceil(total / notificationPageSize));
    notificationCurrentPage = Math.max(1, Math.min(notificationCurrentPage, totalPages));
    const startIndex = (notificationCurrentPage - 1) * notificationPageSize;
    const endIndexExclusive = Math.min(startIndex + notificationPageSize, total);
    const pageRows = filteredNotifications.slice(startIndex, endIndexExclusive);
    if (!pageRows.length) {
      const emptyItem = document.createElement("li");
      emptyItem.className = "review-notification-item";
      emptyItem.textContent = hasActiveNotificationFilters()
        ? "No notifications match the active filters."
        : "No notifications yet.";
      list.appendChild(emptyItem);
    }
    pageRows.forEach(n => {
    const li = document.createElement("li");
    const recognized = isRecognitionGiven(n);
    const isLevelUp = isLevelUpNotification(n);
    const recognitionTime = formatRecognitionRecordedAt(getRecognitionRecordedAtValue(n));
    const recognitionBy = getRecognitionGivenByValue(n);
    const existingNote = getRecognitionNoteValue(n);
    const pointsToNextValue = isLevelUp
      ? getRecognitionPointsNeededForNextLevelFor(getNotificationStudentName(n), getNotificationUserId(n))
      : "";
    const pointsToNext = Number.isFinite(Number(pointsToNextValue))
      ? `${pointsToNextValue} point${Number(pointsToNextValue) === 1 ? "" : "s"}`
      : String(pointsToNextValue || "");
    li.className = `review-notification-item${recognized ? " is-recognized" : ""}`;
    li.innerHTML = `
      <div class="review-notification-main">
        <b>${new Date(n.created_at).toLocaleString()}</b><br>
        ${getNotificationDisplayMessage(n) || ""}
        ${isLevelUp && pointsToNext ? `<div class="review-notification-recorded">Points needed for next level: ${pointsToNext}</div>` : ""}
        ${isLevelUp && recognized && recognitionTime ? `<div class="review-notification-recorded">Recognition recorded on ${recognitionTime}${recognitionBy ? ` by ${recognitionBy}` : ""}</div>` : ""}
      </div>
      ${isLevelUp ? `
        <div class="review-notification-recognition">
          <input
            type="checkbox"
            class="review-notification-recognition-toggle"
            data-notification-recognition="true"
            ${recognized ? "checked" : ""}
            aria-label="Recognition given"
          >
          <input
            type="text"
            class="review-notification-note"
            data-recognition-note="true"
            placeholder="Recognition note (optional)"
            value="${existingNote.replace(/"/g, "&quot;")}"
          >
        </div>
      ` : ""}
    `;
    const checkbox = li.querySelector("input[data-notification-recognition='true']");
    const noteInput = li.querySelector("input[data-recognition-note='true']");
    let lastSubmittedRecognition = recognized;
    let lastSubmittedNote = existingNote;
    const saveRecognitionState = async (nextRecognitionGiven, nextNoteValue) => {
      const normalizedRecognition = Boolean(nextRecognitionGiven);
      const normalizedNote = String(nextNoteValue || "");
      if (
        normalizedRecognition === lastSubmittedRecognition &&
        normalizedNote === lastSubmittedNote
      ) {
        return;
      }
      await updateRecognitionState(n, normalizedRecognition, normalizedNote);
      lastSubmittedRecognition = normalizedRecognition;
      lastSubmittedNote = normalizedNote;
    };
    if (checkbox instanceof HTMLInputElement) {
      checkbox.addEventListener("change", async () => {
        const nextRecognitionGiven = checkbox.checked;
        const noteValue = noteInput instanceof HTMLInputElement ? noteInput.value : "";
        li.classList.toggle("is-recognized", nextRecognitionGiven);
        await saveRecognitionState(nextRecognitionGiven, noteValue);
        if (nextRecognitionGiven) {
          const stamp = formatRecognitionRecordedAt(new Date().toISOString());
          let recordedEl = li.querySelector(".review-notification-recorded");
          if (!(recordedEl instanceof HTMLElement)) {
            recordedEl = document.createElement("div");
            recordedEl.className = "review-notification-recorded";
            const main = li.querySelector(".review-notification-main");
            if (main instanceof HTMLElement) main.appendChild(recordedEl);
          }
          recordedEl.textContent = `Recognition recorded on ${stamp}`;
        } else {
          const recordedEl = li.querySelector(".review-notification-recorded");
          if (recordedEl instanceof HTMLElement) recordedEl.remove();
        }
        await updateNotificationsButtonState();
        window.dispatchEvent(new Event("aa:notification-state-changed"));
        dispatchTutorialAction("aa:tutorial-staff-recognition-complete");
      });
    }
    if (noteInput instanceof HTMLInputElement) {
      noteInput.addEventListener("blur", async () => {
        await saveRecognitionState(
          checkbox instanceof HTMLInputElement ? checkbox.checked : recognized,
          noteInput.value
        );
        dispatchTutorialAction("aa:tutorial-staff-recognition-complete");
      });
      noteInput.addEventListener("keydown", async (event) => {
        if (event.key !== "Enter" || event.shiftKey || event.isComposing) return;
        event.preventDefault();
        await saveRecognitionState(
          checkbox instanceof HTMLInputElement ? checkbox.checked : recognized,
          noteInput.value
        );
        noteInput.blur();
      });
    }
    list.appendChild(li);
    });
    const displayStart = total ? startIndex + 1 : 0;
    const displayEnd = endIndexExclusive;
    countEl.textContent = `Showing ${displayStart}-${displayEnd} of ${total} notifications`;
    pageInfo.textContent = `Page ${notificationCurrentPage} of ${totalPages}`;
    prevBtn.disabled = notificationCurrentPage <= 1;
    nextBtn.disabled = notificationCurrentPage >= totalPages;
    console.log("[ReviewLogs] notifications rendered", {
      rawNotificationCount,
      dedupedNotificationCount,
      filteredNotificationCount,
      duplicatesHiddenInUi: hiddenDuplicateCount,
      totalNotificationsRendered: pageRows.length,
      totalNotificationsAvailable: filteredNotifications.length,
      currentPage: notificationCurrentPage,
      pageSize: notificationPageSize,
      renderedNotificationCount: pageRows.length,
      activeFilters: notificationFilters
    });
  };

  prevBtn.addEventListener("click", () => {
    if (notificationCurrentPage <= 1) return;
    notificationCurrentPage -= 1;
    renderNotificationRows();
  });
  nextBtn.addEventListener("click", () => {
    const totalPages = Math.max(1, Math.ceil(filteredNotifications.length / notificationPageSize));
    if (notificationCurrentPage >= totalPages) return;
    notificationCurrentPage += 1;
    renderNotificationRows();
  });
  pageSizeSelect.addEventListener("change", () => {
    const nextSize = Number(pageSizeSelect.value);
    notificationPageSize = Number.isFinite(nextSize) && nextSize > 0 ? nextSize : DEFAULT_NOTIFICATION_PAGE_SIZE;
    notificationCurrentPage = 1;
    renderNotificationRows();
  });

  renderNotificationRows();

  notificationsSection.innerHTML = "";
  notificationsSection.appendChild(createNotificationAdminControls(statusText));
  notificationsSection.appendChild(filterSummaryEl);
  notificationsSection.appendChild(countEl);
  notificationsSection.appendChild(pager);
  notificationsSection.appendChild(list);
  await updateNotificationsButtonState();
  return {
    rawNotificationCount,
    dedupedNotificationCount,
    filteredNotificationCount,
    hiddenDuplicateCount
  };
}

await updateNotificationsButtonState();
window.addEventListener("aa:notification-state-changed", () => {
  void updateNotificationsButtonState();
});
// === QUICK ADD MODAL ===
const quickAddBtn = document.getElementById("quickAddBtn");
const quickAddModal = document.getElementById("quickAddModal");
const quickAddCancel = document.getElementById("quickAddCancel");
const quickAddSubmitAnother = document.getElementById("quickAddSubmitAnother");
const quickAddSubmitClose = document.getElementById("quickAddSubmitClose");

const quickAddStudentSearch = document.getElementById("quickAddStudentSearch");
const quickAddStudentsSelect = document.getElementById("quickAddStudents");
const quickAddStudentsDropdown = document.getElementById("quickAddStudentsDropdown");
const quickAddStudentsSelected = document.getElementById("quickAddStudentsSelected");
const quickAddPromptGrid = document.getElementById("quickAddPromptGrid");
const quickAddCategory = document.getElementById("quickAddCategory");
const quickAddMoreCategoriesToggle = document.getElementById("quickAddMoreCategoriesToggle");
const quickAddMoreCategoriesPanel = document.getElementById("quickAddMoreCategoriesPanel");
const quickAddCalendar = document.getElementById("quickAddCalendar");
const quickAddCalMonthLabel = document.getElementById("quickAddCalMonthLabel");
const quickAddCalPrev = document.getElementById("quickAddCalPrev");
const quickAddCalNext = document.getElementById("quickAddCalNext");
const quickAddCalendarToggle = document.getElementById("quickAddCalendarToggle");
const quickAddCalendarPanel = document.getElementById("quickAddCalendarPanel");
const quickAddPoints = document.getElementById("quickAddPoints");
const quickAddPracticePointsNote = document.getElementById("quickAddPracticePointsNote");
const quickAddNotes = document.getElementById("quickAddNotes");
const quickAddStatusMsg = document.getElementById("quickAddStatusMsg");

let quickAddRoster = [];
const quickAddSelectedStudentIds = new Set();
const quickAddSelectedDates = new Set();
const quickAddCategoryDefaults = new Map();
let quickAddPointsManuallyEdited = false;
let quickAddSelectedPromptKey = "";
let quickAddSelectedPromptCategory = "";

const quickAddMonthNames = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December"
];
const quickAddToday = new Date();
const quickAddCalendarView = {
  year: quickAddToday.getFullYear(),
  month: quickAddToday.getMonth()
};

function setQuickAddStatus(message, type = "success") {
  if (!quickAddStatusMsg) return;
  if (!message) {
    quickAddStatusMsg.textContent = "";
    quickAddStatusMsg.style.display = "none";
    return;
  }
  quickAddStatusMsg.textContent = String(message);
  quickAddStatusMsg.style.display = "block";
  quickAddStatusMsg.style.color = type === "error" ? "#c62828" : type === "warning" ? "#9a5b00" : "#0b7a3a";
}

function getQuickAddLocalDateString(dateLike) {
  if (typeof dateLike === "string" && /^\d{4}-\d{2}-\d{2}$/.test(dateLike)) {
    return dateLike;
  }
  const d = new Date(dateLike);
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

function getQuickAddStudentName(student) {
  const first = student?.firstName || "";
  const last = student?.lastName || "";
  return `${first} ${last}`.trim() || student?.email || "Student";
}

function extractQuickAddDefaultPoints(categoryRow, categoryName) {
  return getCategoryDefaultPoints(categoryName, categoryRow);
}

function getQuickAddCategoryDefaultPoints(categoryName) {
  const normalized = String(categoryName || "").trim().toLowerCase();
  if (!normalized) return null;
  const dbDefault = quickAddCategoryDefaults.get(normalized);
  if (Number.isFinite(dbDefault) && dbDefault >= 0) return dbDefault;
  return getCategoryDefaultPoints(normalized, null);
}

function syncQuickAddPoints({ force = false } = {}) {
  if (!quickAddPoints) return;
  const category = String(quickAddSelectedPromptCategory || quickAddCategory?.value || "").trim().toLowerCase();
  const isPractice = category === "practice";
  const defaultPoints = getQuickAddCategoryDefaultPoints(category);

  quickAddPoints.disabled = false;
  if (isPractice) {
    if (force || !quickAddPointsManuallyEdited) quickAddPoints.value = "5";
    if (quickAddPracticePointsNote) quickAddPracticePointsNote.style.display = "block";
    return;
  }

  if (quickAddPracticePointsNote) quickAddPracticePointsNote.style.display = "none";
  if (defaultPoints !== null && (force || !quickAddPointsManuallyEdited)) {
    quickAddPoints.value = String(defaultPoints);
  } else if (!category && (force || !quickAddPointsManuallyEdited)) {
    quickAddPoints.value = "";
  }
}

function setQuickAddPromptActive(key) {
  quickAddSelectedPromptKey = String(key || "");
  quickAddPromptGrid?.querySelectorAll("[data-quickadd-prompt]").forEach((button) => {
    const active = String(button.getAttribute("data-quickadd-prompt") || "") === quickAddSelectedPromptKey;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-pressed", active ? "true" : "false");
  });
}

function applyQuickAddPrompt(prompt) {
  if (!prompt || !quickAddPoints) return;
  const category = String(prompt.category || "").trim().toLowerCase();
  quickAddSelectedPromptCategory = category;
  if (quickAddCategory) quickAddCategory.value = category;
  quickAddPoints.disabled = false;
  if (Number.isFinite(Number(prompt.points))) quickAddPoints.value = String(prompt.points);
  if (quickAddNotes && !String(quickAddNotes.value || "").trim() && prompt.notesPrompt) {
    quickAddNotes.placeholder = prompt.notesPrompt;
  }
  quickAddPointsManuallyEdited = false;
  setQuickAddPromptActive(prompt.key);
  if (quickAddPracticePointsNote) quickAddPracticePointsNote.style.display = category === "practice" ? "block" : "none";
}

function renderQuickAddPromptGrid() {
  if (!quickAddPromptGrid) return;
  quickAddPromptGrid.innerHTML = getTeacherLogPrompts().map(prompt => `
    <button
      type="button"
      class="staff-prompt-button"
      data-quickadd-prompt="${prompt.key}"
      aria-pressed="false"
    >
      <span class="staff-prompt-icon" aria-hidden="true">${prompt.icon || ""}</span>
      <span class="staff-prompt-label">${prompt.label}</span>
    </button>
  `).join("");
  quickAddPromptGrid.querySelectorAll("[data-quickadd-prompt]").forEach((button) => {
    button.addEventListener("click", () => {
      const key = String(button.getAttribute("data-quickadd-prompt") || "");
      const prompt = getTeacherLogPrompts().find(item => item.key === key);
      applyQuickAddPrompt(prompt);
    });
  });
}

function syncQuickAddStudentSelect() {
  if (!quickAddStudentsSelect) return;
  Array.from(quickAddStudentsSelect.options).forEach((option) => {
    option.selected = quickAddSelectedStudentIds.has(String(option.value));
  });
}

function renderQuickAddSelectedStudents() {
  if (!quickAddStudentsSelected) return;
  quickAddStudentsSelected.innerHTML = "";

  if (!quickAddSelectedStudentIds.size) {
    const empty = document.createElement("span");
    empty.className = "staff-student-empty";
    empty.textContent = "No students selected";
    quickAddStudentsSelected.appendChild(empty);
    return;
  }

  quickAddRoster
    .filter((student) => quickAddSelectedStudentIds.has(String(student.id)))
    .forEach((student) => {
      const chip = document.createElement("button");
      chip.type = "button";
      chip.className = "staff-student-chip";
      chip.dataset.studentId = String(student.id);
      chip.textContent = `${getQuickAddStudentName(student)} x`;
      chip.addEventListener("click", () => {
        quickAddSelectedStudentIds.delete(String(student.id));
        syncQuickAddStudentSelect();
        renderQuickAddSelectedStudents();
        renderQuickAddStudentDropdown();
      });
      quickAddStudentsSelected.appendChild(chip);
    });
}

function renderQuickAddStudentDropdown() {
  if (!quickAddStudentSearch || !quickAddStudentsDropdown) return;
  const query = String(quickAddStudentSearch.value || "").trim().toLowerCase();
  quickAddStudentsDropdown.innerHTML = "";

  if (!query) {
    quickAddStudentsDropdown.setAttribute("hidden", "");
    return;
  }

  const matches = quickAddRoster.filter((student) =>
    getQuickAddStudentName(student).toLowerCase().includes(query)
  );

  if (!matches.length) {
    const empty = document.createElement("div");
    empty.className = "staff-student-no-match";
    empty.textContent = "No matching students";
    quickAddStudentsDropdown.appendChild(empty);
    quickAddStudentsDropdown.removeAttribute("hidden");
    return;
  }

  matches.forEach((student) => {
    const id = String(student.id);
    const item = document.createElement("button");
    item.type = "button";
    item.className = "staff-student-option";
    item.dataset.studentId = id;

    const isSelected = quickAddSelectedStudentIds.has(id);
    item.textContent = isSelected ? `Selected: ${getQuickAddStudentName(student)}` : getQuickAddStudentName(student);
    if (isSelected) item.classList.add("is-selected");

    item.addEventListener("click", () => {
      if (quickAddSelectedStudentIds.has(id)) quickAddSelectedStudentIds.delete(id);
      else quickAddSelectedStudentIds.add(id);
      syncQuickAddStudentSelect();
      renderQuickAddSelectedStudents();
      renderQuickAddStudentDropdown();
      quickAddStudentSearch.focus();
    });

    quickAddStudentsDropdown.appendChild(item);
  });

  quickAddStudentsDropdown.removeAttribute("hidden");
}

function updateQuickAddCalendarToggle() {
  if (!quickAddCalendarToggle) return;
  const count = quickAddSelectedDates.size;
  quickAddCalendarToggle.textContent = count ? `Dates (${count} selected)` : "Select dates";
}

function renderQuickAddCalendar() {
  if (!quickAddCalendar || !quickAddCalMonthLabel || !quickAddCalPrev || !quickAddCalNext) return;
  quickAddCalendar.innerHTML = "";

  const firstDay = new Date(quickAddCalendarView.year, quickAddCalendarView.month, 1);
  const startDay = firstDay.getDay();
  const gridStart = new Date(quickAddCalendarView.year, quickAddCalendarView.month, 1 - startDay);
  quickAddCalMonthLabel.textContent = `${quickAddMonthNames[quickAddCalendarView.month]} ${quickAddCalendarView.year}`;

  const todayEnd = new Date();
  todayEnd.setHours(23, 59, 59, 999);
  const monthEnd = new Date(quickAddCalendarView.year, quickAddCalendarView.month + 1, 0);
  quickAddCalNext.disabled = monthEnd >= todayEnd;

  for (let i = 0; i < 42; i++) {
    const cellDate = new Date(gridStart);
    cellDate.setDate(gridStart.getDate() + i);
    const dateStr = getQuickAddLocalDateString(cellDate);
    const inMonth = cellDate.getMonth() === quickAddCalendarView.month;
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
      cell.addEventListener("click", () => {
        if (quickAddSelectedDates.has(dateStr)) {
          quickAddSelectedDates.delete(dateStr);
          cell.classList.remove("selected");
        } else {
          quickAddSelectedDates.add(dateStr);
          cell.classList.add("selected");
        }
        updateQuickAddCalendarToggle();
      });
    }

    if (quickAddSelectedDates.has(dateStr)) cell.classList.add("selected");
    quickAddCalendar.appendChild(cell);
  }
}

function resetQuickAddModalState() {
  quickAddSelectedStudentIds.clear();
  quickAddSelectedDates.clear();
  if (quickAddStudentSearch) {
    quickAddStudentSearch.value = "";
    quickAddStudentSearch.placeholder = "Type a student name...";
    quickAddStudentSearch.disabled = false;
  }
  if (quickAddStudentsDropdown) quickAddStudentsDropdown.setAttribute("hidden", "");
  if (quickAddCategory) quickAddCategory.value = "";
  if (quickAddNotes) quickAddNotes.value = "";
  if (quickAddNotes) quickAddNotes.placeholder = "Optional note";
  if (quickAddPoints) {
    quickAddPoints.value = "";
    quickAddPoints.disabled = false;
  }
  quickAddPointsManuallyEdited = false;
  setQuickAddPromptActive("");
  if (quickAddMoreCategoriesPanel) quickAddMoreCategoriesPanel.setAttribute("hidden", "");
  if (quickAddMoreCategoriesToggle) {
    quickAddMoreCategoriesToggle.setAttribute("aria-expanded", "false");
    quickAddMoreCategoriesToggle.textContent = "+ More categories";
  }
  if (quickAddPracticePointsNote) quickAddPracticePointsNote.style.display = "none";
  quickAddCalendarView.year = quickAddToday.getFullYear();
  quickAddCalendarView.month = quickAddToday.getMonth();
  syncQuickAddStudentSelect();
  renderQuickAddSelectedStudents();
  renderQuickAddStudentDropdown();
  syncQuickAddPoints();
  updateQuickAddCalendarToggle();
  renderQuickAddCalendar();
  setQuickAddStatus("");
}

async function loadQuickAddCategories() {
  if (!quickAddCategory) return;
  const { data: categories, error } = await supabase
    .from("categories")
    .select("*")
    .order("id", { ascending: true });

  if (error) {
    console.error("Quick Add categories failed:", error);
  }

  quickAddCategory.innerHTML = '<option value="">Select category</option>';
  quickAddCategoryDefaults.clear();
  (categories || []).forEach((cat) => {
    const name = String(cat?.name || "").trim();
    if (!name) return;
    const normalized = name.toLowerCase();
    const defaultPoints = extractQuickAddDefaultPoints(cat, normalized);
    if (defaultPoints !== null) quickAddCategoryDefaults.set(normalized, defaultPoints);
  });

  getTeacherPointCategories().forEach((cat) => {
    const option = document.createElement("option");
    option.value = cat.value;
    option.textContent = cat.label;
    quickAddCategory.appendChild(option);
  });
  quickAddCategory.disabled = false;
}

async function loadQuickAddStudents() {
  if (!quickAddStudentsSelect) return;
  quickAddStudentsSelect.innerHTML = "";

  const { data: students, error } = await supabase
    .from("users")
    .select("id, firstName, lastName, email, roles, teacherIds, active, deactivated_at, points, instrument, instrumentName, primaryInstrument, primary_instrument")
    .eq("studio_id", viewerContext.studioId)
    .eq("active", true)
    .is("deactivated_at", null);

  if (error) {
    console.error("Quick Add students failed:", error);
    if (quickAddStudentSearch) {
      quickAddStudentSearch.value = "";
      quickAddStudentSearch.placeholder = "Error loading students";
      quickAddStudentSearch.disabled = true;
    }
    quickAddRoster = [];
    renderQuickAddSelectedStudents();
    return;
  }

  quickAddRoster = (students || [])
    .filter((student) => {
      const roles = Array.isArray(student.roles) ? student.roles : [student.roles];
      const isStudent = roles.map((role) => String(role || "").toLowerCase()).includes("student");
      if (!isStudent) return false;
      if (viewerContext.isAdmin) return true;
      if (!viewerContext.isTeacher) return false;
      const teacherIds = Array.isArray(student.teacherIds) ? student.teacherIds.map(String) : [];
      return teacherIds.includes(String(viewerContext.viewerUserId));
    })
    .sort((a, b) => getQuickAddStudentName(a).localeCompare(getQuickAddStudentName(b), undefined, { sensitivity: "base" }));

  quickAddRoster.forEach((student) => {
    const option = document.createElement("option");
    option.value = student.id;
    option.textContent = getQuickAddStudentName(student);
    quickAddStudentsSelect.appendChild(option);
  });

  if (!quickAddRoster.length && quickAddStudentSearch) {
    quickAddStudentSearch.value = "";
    quickAddStudentSearch.placeholder = "No students found";
    quickAddStudentSearch.disabled = true;
  }

  syncQuickAddStudentSelect();
  renderQuickAddSelectedStudents();
  renderQuickAddStudentDropdown();
}

if (quickAddBtn) {
  quickAddBtn.addEventListener("click", async () => {
    quickAddModal.style.display = "flex";
    resetQuickAddModalState();
    await loadQuickAddCategories();
    await loadQuickAddStudents();
    renderQuickAddPromptGrid();
    syncQuickAddPoints();
  });
}

if (quickAddCancel) {
  quickAddCancel.addEventListener("click", () => {
    setQuickAddStatus("");
    quickAddModal.style.display = "none";
  });
}

if (quickAddModal) {
  quickAddModal.addEventListener("click", (event) => {
    if (event.target === quickAddModal) {
      setQuickAddStatus("");
      quickAddModal.style.display = "none";
    }
  });
}

if (quickAddCategory) {
  quickAddCategory.addEventListener("change", () => {
    quickAddSelectedPromptCategory = "";
    setQuickAddPromptActive("");
    syncQuickAddPoints();
  });
}

if (quickAddMoreCategoriesToggle && quickAddMoreCategoriesPanel) {
  quickAddMoreCategoriesToggle.addEventListener("click", () => {
    const isOpen = !quickAddMoreCategoriesPanel.hasAttribute("hidden");
    if (isOpen) {
      quickAddMoreCategoriesPanel.setAttribute("hidden", "");
      quickAddMoreCategoriesToggle.setAttribute("aria-expanded", "false");
      quickAddMoreCategoriesToggle.textContent = "+ More categories";
    } else {
      quickAddMoreCategoriesPanel.removeAttribute("hidden");
      quickAddMoreCategoriesToggle.setAttribute("aria-expanded", "true");
      quickAddMoreCategoriesToggle.textContent = "Hide categories";
    }
  });
}

if (quickAddPoints) {
  quickAddPoints.addEventListener("input", () => {
    quickAddPointsManuallyEdited = true;
  });
}

if (quickAddStudentSearch) {
  quickAddStudentSearch.addEventListener("input", renderQuickAddStudentDropdown);
  quickAddStudentSearch.addEventListener("focus", renderQuickAddStudentDropdown);
}

document.addEventListener("click", (event) => {
  if (!quickAddStudentSearch || !quickAddStudentsDropdown) return;
  const picker = quickAddStudentSearch.closest(".staff-student-picker");
  if (!picker) return;
  if (!picker.contains(event.target)) quickAddStudentsDropdown.setAttribute("hidden", "");
});

document.addEventListener("keydown", (event) => {
  if (event.key === "Escape" && quickAddStudentsDropdown) {
    quickAddStudentsDropdown.setAttribute("hidden", "");
  }
});

if (quickAddCalPrev && quickAddCalNext) {
  quickAddCalPrev.addEventListener("click", () => {
    const prevMonth = new Date(quickAddCalendarView.year, quickAddCalendarView.month - 1, 1);
    quickAddCalendarView.year = prevMonth.getFullYear();
    quickAddCalendarView.month = prevMonth.getMonth();
    renderQuickAddCalendar();
  });

  quickAddCalNext.addEventListener("click", () => {
    const nextMonth = new Date(quickAddCalendarView.year, quickAddCalendarView.month + 1, 1);
    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    if (nextMonth <= todayStart) {
      quickAddCalendarView.year = nextMonth.getFullYear();
      quickAddCalendarView.month = nextMonth.getMonth();
      renderQuickAddCalendar();
    }
  });
}

if (quickAddCalendarToggle && quickAddCalendarPanel) {
  quickAddCalendarToggle.addEventListener("click", () => {
    const isOpen = !quickAddCalendarPanel.hasAttribute("hidden");
    if (isOpen) quickAddCalendarPanel.setAttribute("hidden", "");
    else quickAddCalendarPanel.removeAttribute("hidden");
  });
}

async function saveQuickAddLogs({ closeAfterSave = false } = {}) {
  setQuickAddStatus("");
  const selectedIds = Array.from(quickAddSelectedStudentIds);
  const activeStudioId = String(viewerContext?.studioId || "").trim();
  const category = String(quickAddSelectedPromptCategory || quickAddCategory?.value || "").trim();
  const categoryKey = category.toLowerCase();
  const selectedDates = Array.from(quickAddSelectedDates);
  const notes = quickAddNotes?.value?.trim() || "";

  if (!activeStudioId) {
    setQuickAddStatus("Missing active studio. Please reload and try again.", "error");
    return;
  }
  if (selectedIds.length === 0) {
    setQuickAddStatus("Select at least one student.", "error");
    return;
  }
  if (!category) {
    setQuickAddStatus("Please select a category.", "error");
    return;
  }
  if (!selectedDates.length) {
    setQuickAddStatus("Please select at least one date.", "error");
    return;
  }

  const resolvedPoints = Number(quickAddPoints?.value);
  if (!Number.isFinite(resolvedPoints) || resolvedPoints < 0) {
    setQuickAddStatus("Enter valid points.", "error");
    return;
  }

  const inserts = [];
  selectedIds.forEach((id) => {
    selectedDates.forEach((date) => {
      inserts.push({
        userId: id,
        studio_id: activeStudioId,
        category,
        notes,
        date,
        points: resolvedPoints,
        status: "approved",
        created_by: viewerContext.viewerUserId
      });
    });
  });

  const buttons = [quickAddSubmitAnother, quickAddSubmitClose].filter(Boolean);
  buttons.forEach((button) => { button.disabled = true; });
  try {
    const levelSnapshotsBefore = await fetchStudentLevelSnapshots(selectedIds);
    const { error } = await supabase.from("logs").insert(inserts);
    if (error) {
      console.error("Quick Add failed:", error);
      setQuickAddStatus(error?.message || "Error adding logs.", "error");
      return;
    }
    console.log("Quick Add inserted logs", {
      count: inserts.length,
      studentIds: selectedIds,
      activeStudioId
    });

    let levelNotificationFailed = false;
    for (const studentId of selectedIds) {
      console.log("Quick Add recalculating student", studentId, activeStudioId);
      const { data: recalcData, error: recalcError } = await supabase.rpc("recalculate_user_points_and_level", {
        p_studio_id: activeStudioId,
        p_user_id: studentId
      });
      if (recalcError) {
        console.error("Quick Add recalculation error", recalcError, { studentId, activeStudioId });
        throw recalcError;
      }
      console.log("Quick Add recalculation result", recalcData, { studentId, activeStudioId });

      const before = levelSnapshotsBefore.get(String(studentId)) || null;
      const oldLevel = Number(before?.level || 0);
      const newLevel = Number(recalcData?.level || recalcData?.level_id || 0);
      const student = quickAddRoster.find((row) => String(row?.id) === String(studentId));
      const notificationResult = await createLevelCompletedNotification({
        studioId: activeStudioId,
        studentUserId: studentId,
        studentName: getQuickAddStudentName(student),
        previousLevel: oldLevel,
        newLevel
      });
      if (notificationResult && notificationResult.ok === false) {
        levelNotificationFailed = true;
      }
    }

    const successMessage = `Logged ${inserts.length} entr${inserts.length === 1 ? "y" : "ies"} across ${selectedIds.length} student(s).`;
    if (closeAfterSave) {
      if (levelNotificationFailed) {
        setQuickAddStatus("Points saved, but level completion notification failed.", "warning");
        alert("Points saved, but level completion notification failed.");
      } else {
        setQuickAddStatus("");
      }
      quickAddModal.style.display = "none";
      return;
    }

    resetQuickAddModalState();
    setQuickAddStatus(
      levelNotificationFailed
        ? `${successMessage} Points saved, but level completion notification failed. Ready for another log.`
        : `${successMessage} Ready for another log.`,
      levelNotificationFailed ? "warning" : "success"
    );
  } catch (error) {
    console.error("Quick Add save failed:", error);
    setQuickAddStatus(error?.message || "Error saving logs.", "error");
  } finally {
    buttons.forEach((button) => { button.disabled = false; });
  }
}

if (quickAddSubmitAnother) {
  quickAddSubmitAnother.addEventListener("click", async () => {
    await saveQuickAddLogs({ closeAfterSave: false });
  });
}

if (quickAddSubmitClose) {
  quickAddSubmitClose.addEventListener("click", async () => {
    await saveQuickAddLogs({ closeAfterSave: true });
  });
}
});
