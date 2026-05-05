import { supabase } from "./supabaseClient.js";
import { createLevelCompletedNotification, fetchStudentLevelSnapshots, getCategoryDefaultPoints, getViewerContext, recalculateUserPoints, recalculateUsersAfterApprovedBatch } from './utils.js';
import { ensureStudioContextAndRoute } from "./studio-routing.js";
import { createTeacherAdminTutorial } from "./student-tutorial.js";
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

  const backfillLevelUpNotificationsForStudio = async () => {
    const activeStudioId = String(viewerContext?.studioId || "").trim();
    if (!activeStudioId) {
      throw new Error("No active studio id available for notification backfill.");
    }
    console.log("[NotifDiag][review-logs.js][backfillLevelUpNotificationsForStudio] rpc start", {
      source: "review-logs.js::backfillLevelUpNotificationsForStudio",
      rpc: "backfill_level_up_notifications_for_studio",
      studio_id: activeStudioId,
      user_id: viewerContext?.viewerUserId || null
    });
    const { data, error } = await supabase.rpc("backfill_level_up_notifications_for_studio", {
      p_studio_id: activeStudioId
    });
    console.log("[NotifDiag][review-logs.js][backfillLevelUpNotificationsForStudio] rpc result", {
      source: "review-logs.js::backfillLevelUpNotificationsForStudio",
      data: data ?? null,
      error: error ?? null
    });
    if (error) throw error;
    window.dispatchEvent(new Event("aa:notification-state-changed"));
    return data;
  };

  window.AA_backfillLevelUpNotificationsForStudio = backfillLevelUpNotificationsForStudio;

  if (new URLSearchParams(window.location.search).get("backfillNotifications") === "studio") {
    backfillLevelUpNotificationsForStudio().catch((error) => {
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
  const bulkActionBar = document.getElementById("bulkActionBar");

  let allLogs = [];
  let users = [];
  let filteredLogs = [];
  let currentSort = { field: "date", order: "desc" };
  let currentPage = 1;
  let logsPerPage = 25;
  let activeCardFilter = "all";
  let pendingCardFlashPlayed = false;
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

  try {
    hideReviewLogsError();
    const { data: logsData, error: logsError } = await supabase
      .from("logs")
      .select("*")
      .eq("studio_id", viewerContext.studioId)
      .order("date", { ascending: false, nulls: "last" });
    if (logsError) throw logsError;

    const { data: usersData, error: usersError } = await supabase
      .from("users")
      .select("id, firstName, lastName, teacherIds")
      .eq("studio_id", viewerContext.studioId);
    if (usersError) throw usersError;

    users = usersData || [];
    allLogs = (logsData || []).map(l => ({
      ...l,
      fullName:
        (users.find(u => String(u.id) === String(l.userId))?.firstName || "Unknown") +
        " " +
        (users.find(u => String(u.id) === String(l.userId))?.lastName || "")
    }));

    if (viewerContext.isTeacher && !viewerContext.isAdmin) {
      const myStudents = users
        .filter(u => Array.isArray(u.teacherIds) && u.teacherIds.map(String).includes(String(viewerContext.viewerUserId)))
        .map(s => String(s.id));
      allLogs = allLogs.filter(l => myStudents.includes(String(l.userId)));
    }

    hideReviewLogsError();
    applyFilters();
  } catch (err) {
    console.error("[ERROR] Review Logs:", err);
    showReviewLogsError("Failed to load logs.", err);
  }

  // Search + Card Filter
  searchInput.addEventListener("input", applyFilters);

  function applyFilters() {
    const searchVal = searchInput.value.toLowerCase();
    const todayStr = todayString();

    filteredLogs = allLogs.filter(l => {
      const matchesSearch =
        l.fullName.toLowerCase().includes(searchVal) ||
        (l.notes || "").toLowerCase().includes(searchVal) ||
        (l.category || "").toLowerCase().includes(searchVal);
      let matchesCard = true;
      if (activeCardFilter === "pending") {
        matchesCard = String(l.status || "").toLowerCase() === "pending";
      } else if (activeCardFilter === "approved-today") {
        matchesCard = isApprovedToday(l, todayStr);
      } else if (activeCardFilter === "needs info") {
        matchesCard = String(l.status || "").toLowerCase() === "needs info";
      }
      return matchesSearch && matchesCard;
    });

    currentPage = 1;
    sortLogs();
    renderCategorySummary(allLogs);
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
      logsTableBody.innerHTML = `<tr><td colspan="7" class="logs-empty-state">No logs found yet.</td></tr>`;
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
          .map(row => String(row.userId || "").trim())
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
const notificationsSection = document.getElementById("notificationsSection");

const isLevelUpNotification = (row) => {
  const type = String(row?.type || "").toLowerCase();
  if (type === "level_up" || type === "level_completed") return true;
  const message = String(row?.message || "").toLowerCase();
  return message.includes("reached level") || message.includes("advanced to level") || message.includes("completed level");
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

const sortNotificationsNewestFirst = (rows) => [...rows].sort((a, b) => {
  const aTime = new Date(a?.created_at || 0).getTime();
  const bTime = new Date(b?.created_at || 0).getTime();
  return bTime - aTime;
});

const mergeNotificationRows = (rowSets, limit) => {
  const seen = new Set();
  const merged = [];
  rowSets.flat().forEach((row) => {
    if (!row) return;
    const type = String(row?.type || "").toLowerCase();
    const eventKey = type === "level_completed"
      ? [
          "level_completed",
          row?.studio_id || "",
          row?.message || "",
          row?.completed_level_start ?? "",
          row?.completed_level_end ?? ""
        ].join("|")
      : "";
    const key = eventKey || String(row?.id || `${row?.created_at || ""}:${row?.message || ""}:${row?.userId || row?.user_id || ""}`);
    if (seen.has(key)) return;
    seen.add(key);
    merged.push(row);
  });
  return sortNotificationsNewestFirst(merged).slice(0, limit);
};

const markNotificationsRead = async (rows) => {
  const unreadIds = (Array.isArray(rows) ? rows : [])
    .filter((row) => !isNotificationRead(row))
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

async function fetchViewerNotifications(limit = 80) {
  const viewerUserId = String(viewerContext?.viewerUserId || "").trim();
  const activeStudioId = String(viewerContext?.studioId || "").trim();
  const attempts = [
    { label: "user_id+studio_id", userKey: "user_id", includeStudio: Boolean(activeStudioId) },
    { label: "user_id:no_studio_filter", userKey: "user_id", includeStudio: false },
    { label: "legacy_userId+studio_id", userKey: "userId", includeStudio: Boolean(activeStudioId), legacyOnly: true }
  ];
  console.log("[NotifDiag][review-logs.js][fetchViewerNotifications] query plan", {
    source: "review-logs.js::fetchViewerNotifications",
    viewerUserId,
    activeStudioId: activeStudioId || null,
    limit,
    attempts: attempts.map((attempt) => attempt.label),
    reason: "Fetch canonical user_id rows first; legacy userId is fallback only."
  });
  const rowSets = [];
  const errors = [];
  for (const attempt of attempts) {
    const filters = {
      [attempt.userKey]: viewerUserId,
      studio_id: attempt.includeStudio ? activeStudioId : "(omitted)",
      limit,
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
      .eq(attempt.userKey, viewerUserId)
      .order("created_at", { ascending: false })
      .limit(limit);
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
    if (error) {
      errors.push({ attempt: attempt.label, error });
      continue;
    }
    if (attempt.legacyOnly && rowSets.length > 0) continue;
    if (count > 0) rowSets.push(data);
  }
  const merged = mergeNotificationRows(rowSets, limit);
  console.log("[NotifDiag][review-logs.js][fetchViewerNotifications] merged result", {
    source: "review-logs.js::fetchViewerNotifications",
    viewerUserId,
    activeStudioId: activeStudioId || null,
    mergedCount: merged.length,
    errorCount: errors.length,
    errors
  });
  if (merged.length > 0 || errors.length < attempts.length) {
    return { data: merged, error: null };
  }
  return { data: [], error: errors[0]?.error || null };
}

async function updateNotificationsButtonState() {
  if (!showNotificationsBtn) return;
  const { data, error } = await fetchViewerNotifications(60);
  if (error) {
    console.warn("[ReviewLogs] notification button state fetch failed", error);
    showNotificationsBtn.classList.remove("has-alert");
    return;
  }
  const unresolvedLevelUpCount = data.filter((row) => isLevelUpNotification(row) && !isNotificationRead(row)).length;
  console.log("[NotifDiag][review-logs.js][updateNotificationsButtonState] unread logic", {
    source: "review-logs.js::updateNotificationsButtonState",
    queriedUserId: viewerContext.viewerUserId,
    queriedStudioId: viewerContext?.studioId || null,
    totalNotifications: Array.isArray(data) ? data.length : 0,
    unreadReadFilterLogic: "isLevelUpNotification(row) && row.read !== true",
    unresolvedLevelUpCount
  });
  showNotificationsBtn.classList.toggle("has-alert", unresolvedLevelUpCount > 0);
  showNotificationsBtn.setAttribute("aria-label", unresolvedLevelUpCount > 0
    ? `Notifications (${unresolvedLevelUpCount} level completion alerts pending)`
    : "Notifications");
}

if (showLogsBtn && showNotificationsBtn) {
  showLogsBtn.addEventListener("click", () => {
    logsWrapper.style.display = "block";
    notificationsSection.style.display = "none";
  });

  showNotificationsBtn.addEventListener("click", async () => {
    logsWrapper.style.display = "none";
    notificationsSection.style.display = "block";
    await loadNotifications();
  });
}

async function loadNotifications() {
  notificationsSection.innerHTML = "<p>Loading notifications...</p>";

  const { data: notifications, error } = await fetchViewerNotifications(120);

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
    notificationsSection.innerHTML = "<p>No notifications yet.</p>";
    return;
  }

  const uniqueNotifications = mergeNotificationRows([notifications], 120);
  await markNotificationsRead(uniqueNotifications);
  const normalizedNotifications = uniqueNotifications.map((row) => ({ ...row, read: true }));

  const list = document.createElement("ul");
  list.className = "review-notification-list";
  list.style.listStyle = "none";
  list.style.padding = "0";
  list.innerHTML = `
    <li class="review-notification-header" aria-hidden="true">
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
    </li>
  `;

  normalizedNotifications.forEach(n => {
    const li = document.createElement("li");
    const recognized = isRecognitionGiven(n);
    const isLevelUp = isLevelUpNotification(n);
    const recognitionTime = formatRecognitionRecordedAt(getRecognitionRecordedAtValue(n));
    const recognitionBy = getRecognitionGivenByValue(n);
    const existingNote = getRecognitionNoteValue(n);
    li.className = `review-notification-item${recognized ? " is-recognized" : ""}`;
    li.innerHTML = `
      <div class="review-notification-main">
        <b>${new Date(n.created_at).toLocaleString()}</b><br>
        ${n.message || ""}
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

  notificationsSection.innerHTML = "";
  notificationsSection.appendChild(list);
  await updateNotificationsButtonState();
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
const quickAddCategory = document.getElementById("quickAddCategory");
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

function syncQuickAddPoints() {
  if (!quickAddCategory || !quickAddPoints) return;
  const category = String(quickAddCategory.value || "").trim().toLowerCase();
  const isPractice = category === "practice";
  const defaultPoints = getQuickAddCategoryDefaultPoints(category);

  if (isPractice) {
    quickAddPoints.value = "5";
    quickAddPoints.disabled = true;
    if (quickAddPracticePointsNote) quickAddPracticePointsNote.style.display = "block";
    return;
  }

  quickAddPoints.disabled = false;
  if (quickAddPracticePointsNote) quickAddPracticePointsNote.style.display = "none";
  if (defaultPoints !== null) {
    quickAddPoints.value = String(defaultPoints);
  } else if (!category) {
    quickAddPoints.value = "";
  }
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
  if (quickAddPoints) {
    quickAddPoints.value = "";
    quickAddPoints.disabled = false;
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
    quickAddCategory.innerHTML = '<option value="">Error loading categories</option>';
    quickAddCategory.disabled = true;
    return;
  }

  const blockedCategoryNames = new Set(["batch_practice"]);
  const visibleCategories = (categories || []).filter(
    (cat) => !blockedCategoryNames.has(String(cat?.name || "").toLowerCase())
  );

  quickAddCategory.innerHTML = '<option value="">Select category</option>';
  quickAddCategoryDefaults.clear();
  visibleCategories.forEach((cat) => {
    const name = String(cat?.name || "").trim();
    if (!name) return;
    const normalized = name.toLowerCase();
    const defaultPoints = extractQuickAddDefaultPoints(cat, normalized);
    if (defaultPoints !== null) quickAddCategoryDefaults.set(normalized, defaultPoints);

    const option = document.createElement("option");
    option.value = name;
    option.textContent = name;
    quickAddCategory.appendChild(option);
  });
  quickAddCategory.disabled = visibleCategories.length === 0;
}

async function loadQuickAddStudents() {
  if (!quickAddStudentsSelect) return;
  quickAddStudentsSelect.innerHTML = "";

  const { data: students, error } = await supabase
    .from("users")
    .select("id, firstName, lastName, email, roles, teacherIds")
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
  quickAddCategory.addEventListener("change", syncQuickAddPoints);
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
  const category = String(quickAddCategory?.value || "").trim();
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

  const resolvedPoints = categoryKey === "practice" ? 5 : Number(quickAddPoints?.value);
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
