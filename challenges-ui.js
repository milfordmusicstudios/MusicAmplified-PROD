import { supabase } from "./supabaseClient.js";
import { createChallenge, deleteChallenge, updateChallenge } from "./challenges-api.js";

let staffChallengesEscBound = false;
const dispatchTutorialAction = (action) => {
  if (!action) return;
  window.dispatchEvent(new CustomEvent(String(action)));
};

function toDateInputValue(dateObj) {
  const y = dateObj.getFullYear();
  const m = String(dateObj.getMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function addDays(dateInput, days) {
  const d = new Date(`${dateInput}T00:00:00`);
  d.setDate(d.getDate() + days);
  return toDateInputValue(d);
}

function parseRoles(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value.map(role => String(role || "").toLowerCase());
  return [String(value || "").toLowerCase()];
}

function formatAssignmentType(value) {
  if (value === "whole_studio") return "Whole Studio";
  if (value === "teacher_students") return "Specific teacher's students only";
  if (value === "selected_students") return "Select students only";
  return String(value || "Unknown");
}

function splitCategoryFromDescription(value) {
  const text = String(value || "");
  const match = text.match(/^Category:\s*(.+?)(?:\n\n([\s\S]*))?$/i);
  if (!match) return { category: "", description: text };
  return {
    category: String(match[1] || "").trim(),
    description: String(match[2] || "").trim()
  };
}

function getUserLabel(user) {
  const first = String(user?.firstName || "").trim();
  const last = String(user?.lastName || "").trim();
  const full = `${first} ${last}`.trim();
  return full || String(user?.email || "User");
}

function escapeHtml(value) {
  return String(value || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function isStaff(roles) {
  const normalized = Array.isArray(roles) ? roles.map(role => String(role || "").toLowerCase()) : [];
  return normalized.includes("admin") || normalized.includes("teacher");
}

function ensureModals() {
  if (!document.getElementById("createChallengeOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "createChallengeOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal staff-challenge-modal">
        <div class="modal-header">
          <div class="modal-title">Create a Challenge</div>
          <button id="createChallengeCloseBtn" class="modal-close" type="button">x</button>
        </div>
        <div class="modal-body">
          <div class="modal-field">
            <label for="challengeTitleInput">Title</label>
            <input id="challengeTitleInput" type="text" maxlength="120" />
          </div>
          <div class="modal-field">
            <label for="challengePointsInput">Points</label>
            <input id="challengePointsInput" type="number" min="0" step="1" />
          </div>
          <div class="modal-field">
            <label for="challengeCategorySelect">Category</label>
            <select id="challengeCategorySelect">
              <option value="">Select category</option>
            </select>
          </div>
          <div class="modal-field">
            <label>Assign to</label>
            <div class="challenge-radio-group">
              <label class="challenge-radio-option"><input type="radio" name="challengeAssignType" value="whole_studio" checked /> <span>Whole Studio</span></label>
              <label class="challenge-radio-option"><input type="radio" name="challengeAssignType" value="teacher_students" /> <span>Specific teacher's students only</span></label>
              <label class="challenge-radio-option"><input type="radio" name="challengeAssignType" value="selected_students" /> <span>Select students only</span></label>
            </div>
          </div>
          <div id="challengeTeacherField" class="modal-field" style="display:none;">
            <label for="challengeTeacherSelect">Teacher</label>
            <select id="challengeTeacherSelect"></select>
            <div id="challengeTeacherLocked" class="challenge-helper" style="display:none;"></div>
          </div>
          <div id="challengeStudentsField" class="modal-field" style="display:none;">
            <label for="challengeStudentSearchInput">Students</label>
            <div class="challenge-student-picker">
              <input id="challengeStudentSearchInput" type="text" placeholder="Type a student name..." autocomplete="off" />
              <div id="challengeStudentDropdown" class="challenge-student-dropdown" hidden></div>
              <div id="challengeStudentChips" class="challenge-student-chips"></div>
            </div>
          </div>
          <div class="challenge-date-row">
            <div class="modal-field">
              <label for="challengeStartDateInput">Start date</label>
              <input id="challengeStartDateInput" type="date" />
            </div>
            <div class="modal-field">
              <label for="challengeEndDateInput">End date</label>
              <input id="challengeEndDateInput" type="date" />
            </div>
          </div>
          <div class="modal-field">
            <label for="challengeDescriptionInput">Description / instructions</label>
            <textarea id="challengeDescriptionInput" rows="3"></textarea>
          </div>
          <div id="challengeCreateError" class="staff-msg" style="display:none;"></div>
        </div>
        <div class="modal-actions">
          <button id="challengeCancelBtn" type="button" class="blue-button">Cancel</button>
          <button id="challengeCreateSubmitBtn" type="button" class="blue-button">Create challenge</button>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
  }

  if (!document.getElementById("staffChallengesOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "staffChallengesOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal staff-challenge-modal">
        <div class="modal-header">
          <div class="modal-title">Challenges</div>
          <button id="staffChallengesCloseBtn" class="modal-close" type="button">x</button>
        </div>
        <div class="modal-body">
          <div class="staff-challenges-modal-tabs">
            <button id="staffChallengesTabActive" type="button" class="student-challenges-tab is-active">Active</button>
            <button id="staffChallengesTabEnded" type="button" class="student-challenges-tab">Ended</button>
          </div>
          <div id="staffChallengesModalList" class="challenge-active-list"></div>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
  }

  if (!document.getElementById("challengeDetailOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "challengeDetailOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal staff-challenge-modal challenge-detail-modal">
        <div class="modal-header challenge-detail-modal-header">
          <button id="challengeDetailHeaderBackBtn" type="button" class="challenge-action-link challenge-action-link-header">&larr; Back to active</button>
          <div class="modal-title">Challenge details</div>
          <div class="challenge-detail-header-actions">
            <button id="challengeDetailMoreBtn" class="challenge-header-menu-trigger" type="button" aria-haspopup="true" aria-expanded="false" aria-label="More actions">...</button>
            <div id="challengeDetailMoreMenu" class="challenge-header-menu" hidden>
              <button id="challengeDetailDeleteMenuBtn" type="button" class="challenge-header-menu-item is-destructive">Delete challenge</button>
            </div>
            <button id="challengeDetailCloseBtn" class="modal-close" type="button" aria-label="Close">x</button>
          </div>
        </div>
        <div id="challengeDetailBody" class="modal-body"></div>
      </div>
    `;
    document.body.appendChild(overlay);
  }

  if (!document.getElementById("challengeConfirmOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "challengeConfirmOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal challenge-confirm-modal" role="dialog" aria-modal="true" aria-labelledby="challengeConfirmTitle">
        <div class="challenge-confirm-head">
          <div id="challengeConfirmTitle" class="challenge-confirm-title">Confirm action</div>
        </div>
        <div id="challengeConfirmBody" class="challenge-confirm-body"></div>
        <div class="challenge-confirm-actions">
          <button id="challengeConfirmCancelBtn" type="button" class="challenge-action-btn is-secondary">Cancel</button>
          <button id="challengeConfirmOkBtn" type="button" class="challenge-action-btn is-destructive">Delete challenge</button>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
  }

  if (!document.getElementById("challengeLogReviewOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "challengeLogReviewOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal challenge-log-review-modal" role="dialog" aria-modal="true" aria-labelledby="challengeLogReviewTitle">
        <div class="modal-header">
          <div id="challengeLogReviewTitle" class="modal-title">Review student log</div>
          <button id="challengeLogReviewCloseBtn" class="modal-close" type="button" aria-label="Close">x</button>
        </div>
        <div class="modal-body">
          <div id="challengeLogReviewBody" class="challenge-log-review-body"></div>
        </div>
        <div class="challenge-log-review-actions">
          <button id="challengeLogReviewCancelBtn" type="button" class="challenge-action-btn is-secondary">Cancel</button>
          <button id="challengeLogReviewApproveBtn" type="button" class="challenge-action-btn is-primary">Approve log</button>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
  }
}

export async function initStaffChallengesUI({ studioId, user, roles, showToast }) {
  const createHeaderBtn = document.getElementById("btnNewChallenge") || document.getElementById("challengeCreateBtn");
  const activeHeaderBtn = document.getElementById("challengeActiveBtn");
  const endedHeaderBtn = document.getElementById("challengeEndedBtn");
  const ribbon = document.getElementById("staffChallengesRibbonStrip");
  if (!createHeaderBtn || !activeHeaderBtn || !endedHeaderBtn) return;

  const resolvedStudioId = String(studioId || "").trim();
  const awardBadgesOnApprove = async (userId) => {
    const uid = String(userId || "").trim();
    if (!uid || !resolvedStudioId) return;
    try {
      if (typeof showToast === "function") showToast("Awarding badges...");
      const { data: sessionData } = await supabase.auth.getSession();
      const token = sessionData?.session?.access_token || "";
      const headers = { "Content-Type": "application/json" };
      if (token) headers.Authorization = `Bearer ${token}`;

      const response = await fetch("/api/badges/evaluate-on-approve", {
        method: "POST",
        credentials: "include",
        headers,
        body: JSON.stringify({
          studioId: resolvedStudioId,
          userId: uid
        })
      });
      const payload = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(payload?.error || `HTTP ${response.status}`);
      }
    } catch (err) {
      console.warn("[ChallengesUI] badge awarding on approve failed", err?.message || err);
    }
  };
  const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
  const ensureValidStudioId = () => {
    if (uuidPattern.test(resolvedStudioId)) return true;
    console.error("[ChallengesUI] invalid studioId:", resolvedStudioId);
    if (typeof showToast === "function") showToast("Studio not selected; cannot load challenges.");
    return false;
  };

  if (!resolvedStudioId || !user?.id || !isStaff(roles)) {
    createHeaderBtn.hidden = true;
    activeHeaderBtn.hidden = true;
    endedHeaderBtn.hidden = true;
    if (ribbon) ribbon.style.display = "none";
    return;
  }

  ensureModals();
  createHeaderBtn.hidden = false;
  activeHeaderBtn.hidden = false;
  endedHeaderBtn.hidden = false;
  if (ribbon) ribbon.style.display = "";

  const createOverlay = document.getElementById("createChallengeOverlay");
  const staffChallengesOverlay = document.getElementById("staffChallengesOverlay");
  const detailOverlay = document.getElementById("challengeDetailOverlay");

  let users = [];
  let students = [];
  let teachers = [];
  let categories = [];
  const selectedStudentIds = new Set();
  let endTouched = false;

  const titleInput = document.getElementById("challengeTitleInput");
  const pointsInput = document.getElementById("challengePointsInput");
  const teacherField = document.getElementById("challengeTeacherField");
  const teacherSelect = document.getElementById("challengeTeacherSelect");
  const teacherLocked = document.getElementById("challengeTeacherLocked");
  const categorySelect = document.getElementById("challengeCategorySelect");
  const studentsField = document.getElementById("challengeStudentsField");
  const studentSearchInput = document.getElementById("challengeStudentSearchInput");
  const studentDropdown = document.getElementById("challengeStudentDropdown");
  const studentChips = document.getElementById("challengeStudentChips");
  const startInput = document.getElementById("challengeStartDateInput");
  const endInput = document.getElementById("challengeEndDateInput");
  const descriptionInput = document.getElementById("challengeDescriptionInput");
  const createActionBtn = document.getElementById("challengeCreateSubmitBtn");
  const createModalTitle = createOverlay?.querySelector(".modal-title");
  const cancelBtn = document.getElementById("challengeCancelBtn");
  const closeBtn = document.getElementById("createChallengeCloseBtn");
  const errorEl = document.getElementById("challengeCreateError");
  const challengesModalList = document.getElementById("staffChallengesModalList");
  const challengesCloseBtn = document.getElementById("staffChallengesCloseBtn");
  const challengesTabActiveBtn = document.getElementById("staffChallengesTabActive");
  const challengesTabEndedBtn = document.getElementById("staffChallengesTabEnded");
  const detailBody = document.getElementById("challengeDetailBody");
  const detailHeaderBackBtn = document.getElementById("challengeDetailHeaderBackBtn");
  const detailMoreBtn = document.getElementById("challengeDetailMoreBtn");
  const detailMoreMenu = document.getElementById("challengeDetailMoreMenu");
  const detailDeleteMenuBtn = document.getElementById("challengeDetailDeleteMenuBtn");
  const detailCloseBtn = document.getElementById("challengeDetailCloseBtn");
  const confirmOverlay = document.getElementById("challengeConfirmOverlay");
  const confirmTitleEl = document.getElementById("challengeConfirmTitle");
  const confirmBodyEl = document.getElementById("challengeConfirmBody");
  const confirmCancelBtn = document.getElementById("challengeConfirmCancelBtn");
  const confirmOkBtn = document.getElementById("challengeConfirmOkBtn");
  const logReviewOverlay = document.getElementById("challengeLogReviewOverlay");
  const logReviewBody = document.getElementById("challengeLogReviewBody");
  const logReviewCloseBtn = document.getElementById("challengeLogReviewCloseBtn");
  const logReviewCancelBtn = document.getElementById("challengeLogReviewCancelBtn");
  const logReviewApproveBtn = document.getElementById("challengeLogReviewApproveBtn");
  let activeChallengesRows = [];
  let endedChallengesRows = [];
  let editingChallengeId = null;
  let currentChallengesTab = "active";
  let lastModalTrigger = null;
  let confirmResolve = null;
  let selectedPendingLog = null;
  let selectedPendingContext = null;

  const setError = (message = "") => {
    if (!errorEl) return;
    if (!message) {
      errorEl.style.display = "none";
      errorEl.textContent = "";
      return;
    }
    errorEl.style.display = "block";
    errorEl.style.color = "#c62828";
    errorEl.textContent = message;
  };

  const getAssignType = () => {
    const checked = document.querySelector("input[name='challengeAssignType']:checked");
    return checked?.value || "whole_studio";
  };

  const hideCreateModal = ({ completed = false } = {}) => {
    if (createOverlay) createOverlay.style.display = "none";
    if (studentDropdown) studentDropdown.hidden = true;
    setError("");
    if (!completed) {
      dispatchTutorialAction("aa:tutorial-staff-challenge-dismissed");
    }
  };

  const hideChallengesModal = () => {
    if (staffChallengesOverlay) staffChallengesOverlay.style.display = "none";
    if (lastModalTrigger instanceof HTMLElement) {
      lastModalTrigger.focus();
    }
    lastModalTrigger = null;
  };

  const hideDetailModal = () => {
    if (detailOverlay) detailOverlay.style.display = "none";
    if (detailMoreMenu) detailMoreMenu.hidden = true;
    if (detailMoreBtn) detailMoreBtn.setAttribute("aria-expanded", "false");
  };

  const hideConfirmModal = (result = false) => {
    if (confirmOverlay) confirmOverlay.style.display = "none";
    if (typeof confirmResolve === "function") {
      const resolve = confirmResolve;
      confirmResolve = null;
      resolve(Boolean(result));
    }
  };

  const openConfirmModal = ({ title, message, confirmText } = {}) => new Promise(resolve => {
    confirmResolve = resolve;
    if (confirmTitleEl) confirmTitleEl.textContent = String(title || "Confirm action");
    if (confirmBodyEl) confirmBodyEl.textContent = String(message || "");
    if (confirmOkBtn) confirmOkBtn.textContent = String(confirmText || "Confirm");
    if (confirmOverlay) confirmOverlay.style.display = "flex";
    confirmCancelBtn?.focus();
  });

  const getDeleteChallengeFailureMessage = (error) => {
    const fallback = "Unable to delete: this challenge is connected to student logs.";
    const raw = String(error?.message || error?.details || "").toLowerCase();
    if (!raw) return fallback;
    if (raw.includes("completed") || raw.includes("pending_review") || raw.includes("pending review")) {
      return "Unable to delete: this challenge is connected to student logs.";
    }
    if (raw.includes("foreign key") || raw.includes("violates")) {
      return "Unable to delete: this challenge is connected to student logs.";
    }
    return fallback;
  };

  const closeLogReviewModal = () => {
    selectedPendingLog = null;
    selectedPendingContext = null;
    if (logReviewOverlay) logReviewOverlay.style.display = "none";
    if (logReviewBody) logReviewBody.innerHTML = "";
    if (logReviewApproveBtn) {
      logReviewApproveBtn.disabled = true;
      logReviewApproveBtn.textContent = "Approve log";
    }
  };

  const fetchPendingChallengeLogForStudent = async ({ studentId, challengeTitle }) => {
    const student = String(studentId || "").trim();
    const title = String(challengeTitle || "").trim();
    if (!student) return null;

    const { data, error } = await supabase
      .from("logs")
      .select("id, userId, category, status, date, points, notes, created_at")
      .eq("studio_id", resolvedStudioId)
      .eq("userId", student)
      .in("status", ["pending", "pending_review", "pending approval", "completed_pending"])
      .order("date", { ascending: false })
      .order("created_at", { ascending: false });
    if (error) throw error;

    const allRows = Array.isArray(data) ? data : [];
    const rows = allRows.filter(entry => {
      const statusCanonical = String(entry?.status || "")
        .trim()
        .toLowerCase()
        .replace(/[\s\-\.]+/g, "_");
      const isPendingLike = statusCanonical === "pending" || statusCanonical === "pending_review" || statusCanonical === "completed_pending";
      if (!isPendingLike) return false;
      const category = String(entry?.category || "").trim().toLowerCase();
      const notes = String(entry?.notes || "").trim().toLowerCase();
      return (
        category === "teacher challenge" ||
        notes.startsWith("teacher challenge:") ||
        (title && notes.includes(title.toLowerCase()))
      );
    });
    if (window?.APP_ENV === "dev") {
      console.debug("[ChallengesUI] pending log lookup", {
        studentId: student,
        challengeTitle: title,
        fetchedRows: allRows.length,
        matchedRows: rows.length,
        fetchedStatuses: Array.from(new Set(allRows.map(entry => String(entry?.status || ""))))
      });
    }
    if (!rows.length) {
      const fallbackRows = allRows.filter(entry => {
        const statusCanonical = String(entry?.status || "")
          .trim()
          .toLowerCase()
          .replace(/[\s\-\.]+/g, "_");
        return statusCanonical === "pending" || statusCanonical === "pending_review" || statusCanonical === "completed_pending";
      });
      if (window?.APP_ENV === "dev" && fallbackRows.length) {
        console.debug("[ChallengesUI] using fallback pending log without title/category match", {
          studentId: student,
          challengeTitle: title,
          fallbackRows: fallbackRows.length
        });
      }
      if (!fallbackRows.length) return null;
      return fallbackRows[0];
    }
    if (!title) return rows[0];

    const exactPrefix = `teacher challenge: ${title}`.toLowerCase();
    const exact = rows.find(entry => String(entry?.notes || "").trim().toLowerCase() === exactPrefix);
    if (exact) return exact;

    const partial = rows.find(entry => String(entry?.notes || "").toLowerCase().includes(title.toLowerCase()));
    return partial || rows[0];
  };

  const openPendingLogReviewModal = async ({ assignmentId, studentId, studentName, challengeId, mode, challengeTitle }) => {
    if (!logReviewOverlay || !logReviewBody) return;
    selectedPendingLog = null;
    selectedPendingContext = {
      assignmentId: String(assignmentId || ""),
      studentId: String(studentId || ""),
      studentName: String(studentName || ""),
      challengeId: String(challengeId || ""),
      mode: String(mode || "active"),
      challengeTitle: String(challengeTitle || "")
    };
    if (logReviewApproveBtn) {
      logReviewApproveBtn.disabled = true;
      logReviewApproveBtn.textContent = "Approve log";
    }

    logReviewBody.innerHTML = "Loading pending log...";
    logReviewOverlay.style.display = "flex";

    try {
      const logRow = await fetchPendingChallengeLogForStudent({ studentId, challengeTitle });
      if (!logRow) {
        if (logReviewApproveBtn) logReviewApproveBtn.disabled = true;
        logReviewBody.innerHTML = `
          <div class="challenge-detail-meta">No pending log found for this student/challenge.</div>
          <div class="challenge-detail-meta">This is a data sync issue. Challenge completion can only be approved from a pending student log.</div>
        `;
        return;
      }

      selectedPendingLog = logRow;
      if (logReviewApproveBtn) {
        logReviewApproveBtn.disabled = false;
        logReviewApproveBtn.textContent = "Approve log";
      }

      const noteText = String(logRow.notes || "").trim() || "No notes provided.";
      logReviewBody.innerHTML = `
        <div class="challenge-log-review-meta">Student: ${escapeHtml(String(studentName || "Student"))}</div>
        <div class="challenge-log-review-meta">Date: ${escapeHtml(String(logRow.date || ""))}</div>
        <div class="challenge-log-review-meta">Points: ${Number(logRow.points || 0)}</div>
        <div class="challenge-log-review-meta">Status: Pending</div>
        <div class="challenge-log-review-notes">${escapeHtml(noteText)}</div>
      `;
    } catch (error) {
      console.error("[ChallengesUI] failed to load pending log", error);
      logReviewBody.innerHTML = `<div class="challenge-detail-meta">Unable to load student log.</div>`;
    }
  };

  const toggleDetailMoreMenu = (nextOpen) => {
    if (!detailMoreMenu || !detailMoreBtn) return;
    const isOpen = !detailMoreMenu.hidden;
    const shouldOpen = typeof nextOpen === "boolean" ? nextOpen : !isOpen;
    detailMoreMenu.hidden = !shouldOpen;
    detailMoreBtn.setAttribute("aria-expanded", shouldOpen ? "true" : "false");
  };

  const renderStatusChips = ({ newCount = 0, activeCount = 0, pendingCount = 0, completedCount = 0 }, options = {}) => {
    const allowed = Array.isArray(options?.allowedKeys) && options.allowedKeys.length
      ? new Set(options.allowedKeys)
      : null;
    const hideWhenEmpty = options?.hideWhenEmpty === true;
    const chips = [
      { key: "new", label: "Invited", value: Number(newCount || 0) },
      { key: "active", label: "Accepted", value: Number(activeCount || 0) },
      { key: "pending", label: "Pending", value: Number(pendingCount || 0) },
      { key: "completed", label: "Completed", value: Number(completedCount || 0) }
    ].filter(entry => entry.value > 0 && (!allowed || allowed.has(entry.key)));

    if (!chips.length) return hideWhenEmpty ? "" : '<span class="challenge-status-empty">No activity yet</span>';
    return chips
      .map(entry => `<span class="challenge-status-chip is-${entry.key}">${entry.label} ${entry.value}</span>`)
      .join("");
  };

  const resolveChallengeListStatusCounts = (row) => {
    const totalAssigned = Number(row?.total_assigned ?? row?.assignment_count ?? 0);
    const activeCount = Number(row?.active_count || 0);
    const pendingCount = Number(row?.completed_pending_count || 0);
    const completedCount = Number(row?.completed_count || 0);
    const rpcInvitedCount = Number(row?.new_count || 0);
    const derivedInvitedCount = totalAssigned - activeCount - pendingCount - completedCount;
    const invitedCount = derivedInvitedCount >= 0 ? derivedInvitedCount : rpcInvitedCount;
    return {
      newCount: invitedCount,
      activeCount,
      pendingCount,
      completedCount
    };
  };

  const setDefaultDates = () => {
    const today = toDateInputValue(new Date());
    if (startInput) startInput.value = today;
    if (endInput) endInput.value = addDays(today, 30);
    endTouched = false;
  };

  const clearDates = () => {
    setDefaultDates();
  };

  const getTeacherDefaultId = () => {
    if (user?.isTeacher && !user?.isAdmin) return String(user.id);
    return String(teacherSelect?.value || "");
  };

  const renderStudentChips = () => {
    if (!studentChips) return;
    const ids = Array.from(selectedStudentIds);
    if (!ids.length) {
      studentChips.innerHTML = `<span class="staff-student-empty">No students selected</span>`;
      return;
    }
    studentChips.innerHTML = ids.map(id => {
      const student = students.find(entry => String(entry.id) === String(id));
      return `<button type="button" class="staff-student-chip challenge-chip" data-student-id="${id}">${getUserLabel(student)} x</button>`;
    }).join("");
  };

  const renderStudentDropdown = () => {
    if (!studentDropdown || !studentSearchInput) return;
    const query = String(studentSearchInput.value || "").trim().toLowerCase();
    const filtered = students
      .filter(student => !selectedStudentIds.has(String(student.id)))
      .filter(student => !query || getUserLabel(student).toLowerCase().includes(query))
      .sort((a, b) => getUserLabel(a).localeCompare(getUserLabel(b), undefined, { sensitivity: "base" }));

    if (!filtered.length) {
      studentDropdown.innerHTML = `<div class="staff-student-no-match">No students found</div>`;
      studentDropdown.hidden = false;
      return;
    }

    studentDropdown.innerHTML = filtered.map(student => `
      <button type="button" class="staff-student-option challenge-student-option" data-student-id="${student.id}">
        ${getUserLabel(student)}
      </button>
    `).join("");
    studentDropdown.hidden = false;
  };

  const updateAssignFields = () => {
    const assignmentType = getAssignType();
    if (teacherField) teacherField.style.display = assignmentType === "teacher_students" ? "" : "none";
    if (studentsField) studentsField.style.display = assignmentType === "selected_students" ? "" : "none";
    if (assignmentType !== "selected_students" && studentDropdown) studentDropdown.hidden = true;
  };

  const setAssignmentControlsDisabled = (disabled) => {
    document.querySelectorAll("input[name='challengeAssignType']").forEach(input => {
      input.disabled = disabled;
    });
    if (teacherSelect) teacherSelect.disabled = disabled || (user?.isTeacher && !user?.isAdmin);
    if (studentSearchInput) studentSearchInput.disabled = disabled;
  };

  const isFormValid = () => {
    const title = String(titleInput?.value || "").trim();
    const points = Number(pointsInput?.value);
    const category = String(categorySelect?.value || "").trim();
    const startDate = String(startInput?.value || "");
    const endDate = String(endInput?.value || "");
    const assignmentType = getAssignType();
    if (!title) return false;
    if (!Number.isFinite(points) || points < 0) return false;
    if (!category) return false;
    if (!startDate || !endDate || endDate < startDate) return false;
    if (editingChallengeId) return true;
    if (assignmentType === "teacher_students" && !getTeacherDefaultId()) return false;
    if (assignmentType === "selected_students" && selectedStudentIds.size === 0) return false;
    return true;
  };

  const updateCreateEnabled = () => {
    if (createActionBtn) createActionBtn.disabled = !isFormValid();
  };

  const setDefaultChallengeCategory = () => {
    if (!categorySelect) return;
    const preferred = "Personal";
    const hasPreferred = Array.from(categorySelect.options).some(option => String(option.value || "") === preferred);
    if (!hasPreferred) {
      const option = document.createElement("option");
      option.value = preferred;
      option.textContent = preferred;
      categorySelect.appendChild(option);
    }
    categorySelect.value = preferred;
  };

  const resetCreateForm = () => {
    editingChallengeId = null;
    if (titleInput) titleInput.value = "";
    if (pointsInput) pointsInput.value = "5";
    setDefaultChallengeCategory();
    if (descriptionInput) descriptionInput.value = "";
    document.querySelectorAll("input[name='challengeAssignType']").forEach(input => {
      input.checked = input.value === "whole_studio";
    });
    setAssignmentControlsDisabled(false);
    selectedStudentIds.clear();
    renderStudentChips();
    setDefaultDates();
    updateAssignFields();
    if (createActionBtn) createActionBtn.textContent = "Create challenge";
    if (createModalTitle) createModalTitle.textContent = "Create a Challenge";
    updateCreateEnabled();
    setError("");
  };

  const fetchUsers = async () => {
    if (!ensureValidStudioId()) throw new Error("Invalid studio id");
    const { data, error } = await supabase
      .from("users")
      .select("id, firstName, lastName, email, roles, active, deactivated_at, studio_id")
      .eq("studio_id", resolvedStudioId)
      .eq("active", true)
      .is("deactivated_at", null);
    if (error) throw error;
    users = Array.isArray(data) ? data : [];
    students = users.filter(entry => parseRoles(entry.roles).includes("student"));
    teachers = users.filter(entry => {
      const roleSet = parseRoles(entry.roles);
      return roleSet.includes("teacher") || roleSet.includes("admin");
    });
  };

  const fetchCategories = async () => {
    const { data, error } = await supabase
      .from("categories")
      .select("name")
      .order("id", { ascending: true });

    const blockedCategoryNames = new Set(["batch_practice"]);

    if (error) {
      console.warn("[ChallengesUI] categories fetch failed; using fallback options", error);
      categories = ["Technique", "Theory", "Performance", "Creativity", "Practice"];
    } else {
      categories = Array.isArray(data)
        ? data
            .map(row => String(row?.name || "").trim())
            .filter(Boolean)
            .filter(name => !blockedCategoryNames.has(name.toLowerCase()))
        : [];
    }

    if (!categorySelect) return;
    const options = categories
      .sort((a, b) => a.localeCompare(b, undefined, { sensitivity: "base" }))
      .map(name => `<option value="${escapeHtml(name)}">${escapeHtml(name)}</option>`)
      .join("");
    categorySelect.innerHTML = `<option value="">Select category</option>${options}`;
  };

  const populateTeacherField = () => {
    if (!teacherSelect || !teacherLocked) return;
    if (user?.isTeacher && !user?.isAdmin) {
      const self = teachers.find(entry => String(entry.id) === String(user.id));
      teacherSelect.innerHTML = `<option value="${user.id}">${getUserLabel(self || user)}</option>`;
      teacherSelect.value = String(user.id);
      teacherSelect.disabled = true;
      teacherSelect.style.display = "none";
      teacherLocked.style.display = "";
      teacherLocked.textContent = `Teacher: ${getUserLabel(self || user)}`;
      return;
    }
    teacherSelect.style.display = "";
    teacherLocked.style.display = "none";
    teacherSelect.disabled = false;
    teacherSelect.innerHTML = `<option value="">Select teacher</option>` + teachers
      .sort((a, b) => getUserLabel(a).localeCompare(getUserLabel(b), undefined, { sensitivity: "base" }))
      .map(entry => `<option value="${entry.id}">${getUserLabel(entry)}</option>`)
      .join("");
  };

  const applyCreatePrefill = (sourceRow, { clearDateFields = true } = {}) => {
    if (!sourceRow) return;
    const parsed = splitCategoryFromDescription(sourceRow?.description);
    if (titleInput) titleInput.value = String(sourceRow?.title || "");
    if (pointsInput) pointsInput.value = String(Number(sourceRow?.points || 0));
    if (descriptionInput) descriptionInput.value = String(parsed.description || "");
    if (categorySelect) {
      const categoryValue = String(parsed.category || "").trim();
      if (categoryValue) {
        const hasOption = Array.from(categorySelect.options).some(option => String(option.value || "") === categoryValue);
        if (!hasOption) {
          const option = document.createElement("option");
          option.value = categoryValue;
          option.textContent = categoryValue;
          categorySelect.appendChild(option);
        }
        categorySelect.value = categoryValue;
      }
    }

    if (!clearDateFields) {
      if (startInput) startInput.value = String(sourceRow?.start_date || "");
      if (endInput) endInput.value = String(sourceRow?.end_date || "");
      endTouched = true;
    } else {
      // Copying should never auto-submit for past dates.
      clearDates();
    }
    renderStudentChips();
    updateAssignFields();
    updateCreateEnabled();
  };

  const openCreateModal = async (prefillRow = null, options = {}) => {
    try {
      await fetchUsers();
      await fetchCategories();
      populateTeacherField();
      resetCreateForm();
      const mode = String(options?.mode || "create");
      if (mode === "edit" && prefillRow?.id) {
        editingChallengeId = String(prefillRow.id);
        if (createModalTitle) createModalTitle.textContent = "Edit Challenge";
        if (createActionBtn) createActionBtn.textContent = "Save changes";
        setAssignmentControlsDisabled(true);
      }
      if (prefillRow) applyCreatePrefill(prefillRow, { clearDateFields: mode !== "edit" });
      if (createOverlay) createOverlay.style.display = "flex";
      titleInput?.focus();
    } catch (error) {
      setError(error?.message || "Unable to load challenge form");
      if (createOverlay) createOverlay.style.display = "flex";
    }
  };

  const getFieldValue = (sources, keys) => {
    for (const source of sources) {
      if (!source || typeof source !== "object") continue;
      for (const key of keys) {
        if (Object.prototype.hasOwnProperty.call(source, key) && source[key] != null && source[key] !== "") {
          return source[key];
        }
      }
    }
    return null;
  };

  const normalizeAssignmentMode = (value) => {
    const raw = String(value || "").trim().toLowerCase();
    if (!raw) return "";
    if (raw === "teacher_students" || raw === "teacher_only") return "teacher_students";
    if (raw === "selected_students" || raw === "selected") return "selected_students";
    if (raw === "whole_studio" || raw === "all_students" || raw === "all") return "all_students";
    if (raw.includes("teacher")) return "teacher_students";
    if (raw.includes("selected")) return "selected_students";
    if (raw.includes("whole") || raw.includes("all")) return "all_students";
    return "";
  };

  const resolveUserNameById = (userId, usersById) => {
    const id = String(userId || "").trim();
    if (!id) return "";
    const fromMap = usersById?.get(id);
    if (fromMap) return getUserLabel(fromMap);
    const fromUsers = users.find(entry => String(entry?.id || "") === id);
    if (fromUsers) return getUserLabel(fromUsers);
    return "";
  };

  const mapAssignmentToDisplayStatus = (assignment) => {
    const statusCanonical = String(assignment?.status || "")
      .trim()
      .toLowerCase()
      .replace(/[\s\-\.]+/g, "_");
    if (statusCanonical === "completed" || statusCanonical === "done") {
      return { key: "completed", label: "Completed", rank: 4 };
    }
    if (statusCanonical === "pending" || statusCanonical === "pending_review" || statusCanonical === "needs_review" || statusCanonical === "completed_pending") {
      return { key: "pending", label: "Pending", rank: 3 };
    }
    if (statusCanonical === "active" || statusCanonical === "accepted" || statusCanonical === "in_progress") {
      return { key: "accepted", label: "Accepted", rank: 2 };
    }
    if (statusCanonical === "new") {
      return { key: "invited", label: "Invited", rank: 1 };
    }
    return { key: "invited", label: "Invited", rank: 1 };
  };

  const fetchChallengeDetailContext = async (row) => {
    const id = String(row?.id || "").trim();
    if (!id) return { challengeData: row || null, assignments: [], usersById: new Map() };

    let challengeData = row || null;
    try {
      const { data: challengeRow, error: challengeError } = await supabase
        .from("teacher_challenges")
        .select("*")
        .eq("id", id)
        .maybeSingle();
      if (challengeError) throw challengeError;
      if (challengeRow) challengeData = challengeRow;
    } catch (error) {
      console.warn("[ChallengesUI] unable to load full challenge detail row; using RPC row fallback", error);
    }

    const { data: assignmentRows, error: assignmentError } = await supabase
      .from("teacher_challenge_assignments")
      .select("id, student_id, status, accepted_at, completed_at")
      .eq("challenge_id", id)
      .eq("studio_id", resolvedStudioId);
    if (assignmentError) throw assignmentError;

    const assignments = Array.isArray(assignmentRows) ? assignmentRows : [];
    if (window?.APP_ENV === "dev") {
      const canonicalCounts = assignments.reduce((acc, entry) => {
        const canonical = String(entry?.status || "")
          .trim()
          .toLowerCase()
          .replace(/[\s\-\.]+/g, "_") || "(empty)";
        acc[canonical] = Number(acc[canonical] || 0) + 1;
        return acc;
      }, {});
      console.debug("[ChallengesUI] assignment statuses (raw + canonical)", {
        challengeId: id,
        uniqueRawStatuses: Array.from(new Set(assignments.map(entry => String(entry?.status || "")))),
        canonicalCounts
      });
    }
    const creatorId = String(getFieldValue([challengeData, row], ["created_by", "teacher_id"]) || "").trim();
    const assignedTeacherId = String(getFieldValue([challengeData, row], ["assigned_teacher_id", "assignment_teacher_id"]) || "").trim();
    const userIds = new Set(
      assignments
        .map(entry => String(entry?.student_id || "").trim())
        .filter(Boolean)
    );
    if (creatorId) userIds.add(creatorId);
    if (assignedTeacherId) userIds.add(assignedTeacherId);

    const idList = Array.from(userIds);
    if (!idList.length) return { challengeData, assignments, usersById: new Map() };

    const { data: userRows, error: userError } = await supabase
      .from("users")
      .select("id, firstName, lastName, email, roles")
      .in("id", idList);
    if (userError) throw userError;

    const usersById = new Map(
      (Array.isArray(userRows) ? userRows : []).map(userRow => [String(userRow.id), userRow])
    );
    return { challengeData, assignments, usersById };
  };

  const renderChallengeDetail = async (row, mode) => {
    const parsed = splitCategoryFromDescription(row?.description);
    const isActiveMode = mode === "active";
    const descriptionText = String(parsed.description || "").trim();
    const shouldShowDescription = !!descriptionText && !/^no description provided\.?$/i.test(descriptionText);
    if (!detailBody) return;
    const newCount = Number(row?.new_count || 0);
    const activeCount = Number(row?.active_count || 0);
    const pendingCount = Number(row?.completed_pending_count || 0);
    const completedCount = Number(row?.completed_count || 0);

    detailBody.innerHTML = `
      <div class="challenge-detail-block">
        <div class="challenge-detail-title">${escapeHtml(String(row?.title || "Untitled challenge"))}</div>
        <div class="challenge-detail-meta challenge-detail-meta-stack">
          <span id="challengeDetailCreatorLine">Created by: Loading...</span>
          <span id="challengeDetailScopeLine">Assigned to: Loading...</span>
        </div>
        <div class="challenge-meta-card">
          <div class="challenge-meta-grid">
            <div class="challenge-detail-meta-cell"><span class="challenge-detail-meta-label">Start date</span><span class="challenge-detail-meta-value">${escapeHtml(String(row?.start_date || "")) || "N/A"}</span></div>
            <div class="challenge-detail-meta-cell"><span class="challenge-detail-meta-label">End date</span><span class="challenge-detail-meta-value">${escapeHtml(String(row?.end_date || "")) || "N/A"}</span></div>
            <div class="challenge-detail-meta-cell"><span class="challenge-detail-meta-label">Points</span><span class="challenge-detail-meta-value">${Number(row?.points || 0)}</span></div>
            <div class="challenge-detail-meta-cell"><span class="challenge-detail-meta-label">Assigned students</span><span id="challengeDetailAssignedCount" class="challenge-detail-meta-value">${Number(row?.total_assigned ?? row?.assignment_count ?? 0)}</span></div>
            <div class="challenge-detail-meta-cell challenge-meta-grid-full"><span class="challenge-detail-meta-label">Category</span><span class="challenge-detail-meta-value">${escapeHtml(String(parsed.category || "Not set"))}</span></div>
          </div>
        </div>
        ${isActiveMode ? `<div class="challenge-status-chips">${renderStatusChips(
          { newCount, activeCount, pendingCount, completedCount },
          { allowedKeys: ["pending", "completed"], hideWhenEmpty: true }
        )}</div>` : ""}
        ${shouldShowDescription ? `<div class="challenge-detail-desc">${escapeHtml(descriptionText)}</div>` : ""}
        <div id="challengeDetailStudentsBlock" class="challenge-roster-block">Loading student details...</div>
      </div>
      <div class="challenge-detail-actions ${isActiveMode ? "is-active" : "is-ended"}">
        ${isActiveMode ? '<button id="challengeDetailEditBtn" type="button" class="challenge-action-btn is-primary">Edit</button>' : ""}
        ${isActiveMode ? '<button id="challengeDetailDeactivateBtn" type="button" class="challenge-action-btn is-secondary">Deactivate</button>' : ""}
        ${mode === "ended" ? '<button id="challengeDetailCopyBtn" type="button" class="challenge-action-btn is-primary">Start New Round</button>' : ""}
      </div>
    `;
    if (detailHeaderBackBtn) {
      detailHeaderBackBtn.textContent = `\u2190 Back to ${mode === "ended" ? "ended" : "active"}`;
      detailHeaderBackBtn.onclick = () => {
        hideDetailModal();
        currentChallengesTab = mode === "ended" ? "ended" : "active";
        renderChallengesTabButtons();
        openChallengesModalForTab(currentChallengesTab);
      };
    }
    if (detailMoreBtn) detailMoreBtn.hidden = !isActiveMode;
    toggleDetailMoreMenu(false);
    hideChallengesModal();
    if (detailOverlay) detailOverlay.style.display = "flex";
    document.getElementById("challengeDetailCopyBtn")?.addEventListener("click", async () => {
      hideDetailModal();
      await openCreateModal(row);
    });
    document.getElementById("challengeDetailEditBtn")?.addEventListener("click", async () => {
      hideDetailModal();
      await openEditModal(row);
    });
    if (detailDeleteMenuBtn) detailDeleteMenuBtn.onclick = async () => {
      const deleted = await handleDeleteChallenge(row);
      if (!deleted) return;
      hideDetailModal();
      await openChallengesModalForTab(currentChallengesTab);
    };
    document.getElementById("challengeDetailDeactivateBtn")?.addEventListener("click", async () => {
      await handleDeactivateChallenge(row);
    });

    const creatorLine = document.getElementById("challengeDetailCreatorLine");
    const scopeLine = document.getElementById("challengeDetailScopeLine");
    const assignedCountEl = document.getElementById("challengeDetailAssignedCount");
    const studentsBlock = document.getElementById("challengeDetailStudentsBlock");

    const resolveCreatorLabel = ({ challengeData, usersById }) => {
      const creatorId = String(getFieldValue([challengeData, row], ["created_by", "teacher_id"]) || "").trim();
      const creatorName = resolveUserNameById(creatorId, usersById);
      if (creatorName) return creatorName;
      console.warn("[ChallengesUI] creator missing for challenge", { challenge_id: String(row?.id || "") });
      return "Unknown";
    };

    const resolveScopeLabel = ({ challengeData, usersById, assignedCount }) => {
      const modeValue = normalizeAssignmentMode(getFieldValue(
        [challengeData, row],
        ["assigned_to_mode", "assignment_type", "assign_type"]
      ));
      const allStudentsFlag = getFieldValue(
        [challengeData, row],
        ["assigned_to_all", "is_all_students", "all_students", "whole_studio"]
      ) === true;
      const teacherScopeFlag = getFieldValue(
        [challengeData, row],
        ["all_my_students", "assigned_to_teacher_students", "is_teacher_students"]
      ) === true;
      const assignedTeacherId = String(getFieldValue(
        [challengeData, row],
        ["assigned_teacher_id", "assignment_teacher_id", "teacher_id"]
      ) || "").trim();

      if (modeValue === "all_students" || allStudentsFlag) {
        return "All students";
      }
      if (modeValue === "teacher_students" || teacherScopeFlag) {
        const teacherName = resolveUserNameById(
          assignedTeacherId || getFieldValue([challengeData, row], ["teacher_id", "created_by"]),
          usersById
        ) || "Unknown";
        return `All students of ${teacherName}`;
      }
      return `Selected students (${assignedCount})`;
    };

    const buildRoster = ({ assignments, usersById }) => {
      const rosterByStudent = new Map();
      assignments.forEach(assignment => {
        const studentId = String(assignment?.student_id || "").trim();
        if (!studentId) return;
        const userRow = usersById.get(studentId);
        const name = userRow ? getUserLabel(userRow) : "Unknown student";
        const mapped = mapAssignmentToDisplayStatus(assignment);
        const existing = rosterByStudent.get(studentId);
        if (!existing || mapped.rank > existing.rank) {
          rosterByStudent.set(studentId, {
            assignmentId: String(assignment?.id || ""),
            studentId,
            name,
            statusKey: mapped.key,
            statusLabel: mapped.label,
            rank: mapped.rank
          });
        }
      });

      return Array.from(rosterByStudent.values())
        .sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: "base" }));
    };

    const countByStatusKey = (items) => items.reduce((acc, item) => {
      const key = String(item?.statusKey || "").trim();
      if (!key) return acc;
      acc[key] = Number(acc[key] || 0) + 1;
      return acc;
    }, {});

    try {
      const { challengeData, assignments, usersById } = await fetchChallengeDetailContext(row);
      const roster = buildRoster({ assignments, usersById });
      const assignedCount = roster.length;
      const creatorLabel = resolveCreatorLabel({ challengeData, usersById });
      const scopeLabel = resolveScopeLabel({ challengeData, usersById, assignedCount });
      const rosterStatusCounts = countByStatusKey(roster);
      const devMode = window?.APP_ENV === "dev";

      if (devMode) {
        console.debug("[ChallengesUI] detail roster status debug", {
          challengeId: String(row?.id || ""),
          assignmentsFetched: assignments.length,
          rosterStatusCounts
        });
        const listPendingCount = Number(row?.completed_pending_count || 0);
        const rosterPendingCount = Number(rosterStatusCounts.pending || 0);
        if (listPendingCount > 0 && rosterPendingCount === 0) {
          console.warn("[ChallengesUI] pending mismatch between list and detail roster", {
            challengeId: String(row?.id || ""),
            listPendingCount,
            rosterPendingCount
          });
        }
      }

      if (creatorLine) creatorLine.textContent = `Created by: ${creatorLabel}`;
      if (scopeLine) scopeLine.textContent = `Assigned to: ${scopeLabel}`;
      if (assignedCountEl) assignedCountEl.textContent = String(assignedCount);

      if (studentsBlock) {
        if (!roster.length) {
          studentsBlock.innerHTML = `
            <div class="challenge-roster-head">Assigned students (0)</div>
            <div class="challenge-detail-meta">No assigned students.</div>
          `;
        } else {
          studentsBlock.innerHTML = `
            <div class="challenge-roster-head">Assigned students (${roster.length})</div>
            <div class="challenge-roster-list">
              ${roster.map(entry => `
                <div class="challenge-roster-row">
                  <span class="challenge-roster-name">${escapeHtml(entry.name)}</span>
                  ${entry.statusKey === "pending"
                    ? `<button type="button" class="challenge-status-pill status-${entry.statusKey} is-clickable" data-action="review-pending-log" data-assignment-id="${escapeHtml(entry.assignmentId)}" data-student-id="${escapeHtml(entry.studentId)}" data-student-name="${escapeHtml(entry.name)}">${escapeHtml(entry.statusLabel)}</button>`
                    : `<span class="challenge-status-pill status-${entry.statusKey}">${escapeHtml(entry.statusLabel)}</span>`}
                </div>
              `).join("")}
            </div>
          `;
          studentsBlock.querySelectorAll("[data-action='review-pending-log']").forEach(btn => {
            btn.addEventListener("click", async event => {
              event.stopPropagation();
              const assignmentId = String(btn.getAttribute("data-assignment-id") || "");
              const studentId = String(btn.getAttribute("data-student-id") || "");
              const studentName = String(btn.getAttribute("data-student-name") || "");
              await openPendingLogReviewModal({
                assignmentId,
                studentId,
                studentName,
                challengeId: String(row?.id || ""),
                mode,
                challengeTitle: String(row?.title || "")
              });
            });
          });
        }
      }
    } catch (error) {
      console.error("[ChallengesUI] detail assignments fetch failed", error);
      if (creatorLine) creatorLine.textContent = "Created by: Unknown";
      if (scopeLine) {
        scopeLine.textContent = `Assigned to: Selected students (${Number(row?.total_assigned ?? row?.assignment_count ?? 0)})`;
      }
      if (studentsBlock) {
        studentsBlock.textContent = "Unable to load student details.";
      }
    }
  };

  const openEditModal = async (row) => {
    hideChallengesModal();
    await openCreateModal(row, { mode: "edit" });
  };

  const handleDeleteChallenge = async (row) => {
    const challengeId = String(row?.id || "");
    if (!challengeId) return false;
    const confirmed = await openConfirmModal({
      title: "Delete challenge forever?",
      message: "This cannot be undone.",
      confirmText: "Delete forever"
    });
    if (!confirmed) return false;
    try {
      await deleteChallenge(challengeId);
      if (typeof showToast === "function") showToast("Challenge deleted");
      await loadHeaderCounts();
      return true;
    } catch (error) {
      console.error("[ChallengesUI] delete failed", error);
      if (typeof showToast === "function") showToast(getDeleteChallengeFailureMessage(error));
      return false;
    }
  };

  const handleDeactivateChallenge = async (row) => {
    const challengeId = String(row?.id || "");
    if (!challengeId) return;
    if (!window.confirm("does not delete the challenge, but removes it from student's cue")) return;
    try {
      const { error } = await supabase.rpc("deactivate_teacher_challenge", {
        p_challenge_id: challengeId
      });
      if (error) throw error;

      if (typeof showToast === "function") showToast("Challenge deactivated");
      await loadHeaderCounts();
      hideDetailModal();
      await openChallengesModalForTab(currentChallengesTab);
    } catch (error) {
      console.error("[ChallengesUI] deactivate failed", error);
      if (typeof showToast === "function") showToast("Couldn't deactivate challenge.");
    }
  };

  const loadChallengeList = async ({ mode, listEl, render = true }) => {
    if (render && !listEl) return 0;
    if (!ensureValidStudioId()) return 0;
    const previousHtml = render && listEl ? listEl.innerHTML : "";
    if (render && listEl) listEl.innerHTML = "Loading...";
    const today = new Date();
    today.setHours(0, 0, 0, 0);
    const isEndedTab = mode === "ended";

    let rpcRows = [];
    try {
      const { data, error } = await supabase.rpc("list_teacher_challenges_with_counts_for_staff", {
        p_studio_id: resolvedStudioId
      });
      if (error) throw error;
      rpcRows = Array.isArray(data) ? data : [];
    } catch (error) {
      console.error(`[ChallengesUI] failed to load ${mode} challenges`, error);
      if (typeof showToast === "function") showToast("Couldn't load challenges.");
      if (render && listEl) {
        listEl.innerHTML = previousHtml || `<div class="staff-student-no-match">Unable to load ${mode} challenges.</div>`;
      }
      return 0;
    }

    const rows = rpcRows.filter(row => {
      const end = row?.end_date ? new Date(`${row.end_date}T00:00:00`) : null;
      const ended = (row?.is_active === false) || (!!end && end < today);
      return isEndedTab ? ended : !ended;
    });

    if (mode === "active") activeChallengesRows = rows;
    if (mode === "ended") endedChallengesRows = rows;

    if (!rows.length) {
      if (render && listEl) {
        listEl.innerHTML = `<div class="staff-student-no-match">No ${mode} challenges yet.</div>`;
      }
      return 0;
    }

    if (!render || !listEl) return rows.length;

    listEl.innerHTML = rows.map(row => {
      const statusCounts = resolveChallengeListStatusCounts(row);
      return `
      <div class="challenge-active-row challenge-active-row-btn" data-challenge-id="${String(row.id || "")}" data-mode="${mode}">
        <div class="challenge-active-row-top">
          <div class="challenge-active-title-wrap">
            <div class="challenge-active-title">${escapeHtml(String(row.title || "Untitled challenge"))}</div>
            ${!isEndedTab && row?.is_active === false ? '<span class="challenge-inactive-pill">Inactive</span>' : ""}
          </div>
          <div class="challenge-active-date">${isEndedTab ? "Ended" : "Ends"} ${escapeHtml(String(row.end_date || ""))}</div>
        </div>
        <div class="challenge-active-meta is-subtle">Assigned to ${Number(row.total_assigned || 0)} students</div>
        <div class="challenge-status-chips">${renderStatusChips(statusCounts)}</div>
      </div>
    `;
    }).join("");

    listEl.querySelectorAll("[data-challenge-id]").forEach(card => {
      card.addEventListener("click", () => {
        const challengeId = String(card.getAttribute("data-challenge-id") || "");
        const rowMode = String(card.getAttribute("data-mode") || "active");
        const sourceRows = rowMode === "ended" ? endedChallengesRows : activeChallengesRows;
        const row = sourceRows.find(entry => String(entry?.id || "") === challengeId);
        if (!row) return;
        renderChallengeDetail(row, rowMode);
      });
    });

    return rows.length;
  };

  const loadHeaderCounts = async () => {
    const activeCount = await loadChallengeList({ mode: "active", listEl: challengesModalList, render: false });
    activeHeaderBtn.hidden = false;
    activeHeaderBtn.textContent = `Active (${activeCount})`;
    const endedCount = await loadChallengeList({ mode: "ended", listEl: challengesModalList, render: false });
    endedHeaderBtn.hidden = false;
    endedHeaderBtn.textContent = `Ended (${endedCount})`;
    if (ribbon) {
      ribbon.classList.toggle("has-challenges", activeCount > 0);
      ribbon.classList.toggle("no-challenges", activeCount <= 0);
    }
  };

  const renderChallengesTabButtons = () => {
    challengesTabActiveBtn?.classList.toggle("is-active", currentChallengesTab === "active");
    challengesTabEndedBtn?.classList.toggle("is-active", currentChallengesTab === "ended");
    activeHeaderBtn?.classList.toggle("is-active", currentChallengesTab === "active");
    endedHeaderBtn?.classList.toggle("is-active", currentChallengesTab === "ended");
  };

  const openChallengesModalForTab = async (tab) => {
    currentChallengesTab = tab === "ended" ? "ended" : "active";
    renderChallengesTabButtons();
    await loadChallengeList({ mode: currentChallengesTab, listEl: challengesModalList, render: true });
    if (staffChallengesOverlay) {
      staffChallengesOverlay.style.display = "flex";
      const closeTarget = challengesCloseBtn;
      if (closeTarget) closeTarget.focus();
    }
  };

  createHeaderBtn.addEventListener("click", async event => {
    lastModalTrigger = event.currentTarget instanceof HTMLElement ? event.currentTarget : null;
    hideChallengesModal();
    await openCreateModal();
  });
  activeHeaderBtn.addEventListener("click", async event => {
    lastModalTrigger = event.currentTarget instanceof HTMLElement ? event.currentTarget : null;
    await openChallengesModalForTab("active");
  });
  endedHeaderBtn.addEventListener("click", async event => {
    lastModalTrigger = event.currentTarget instanceof HTMLElement ? event.currentTarget : null;
    await openChallengesModalForTab("ended");
  });
  challengesTabActiveBtn?.addEventListener("click", async () => {
    await openChallengesModalForTab("active");
  });
  challengesTabEndedBtn?.addEventListener("click", async () => {
    await openChallengesModalForTab("ended");
  });
  cancelBtn?.addEventListener("click", hideCreateModal);
  closeBtn?.addEventListener("click", hideCreateModal);
  challengesCloseBtn?.addEventListener("click", hideChallengesModal);
  detailCloseBtn?.addEventListener("click", hideDetailModal);
  detailMoreBtn?.addEventListener("click", event => {
    event.stopPropagation();
    toggleDetailMoreMenu();
  });

  createOverlay?.addEventListener("click", event => {
    if (event.target === createOverlay) hideCreateModal();
  });
  staffChallengesOverlay?.addEventListener("click", event => {
    if (event.target === staffChallengesOverlay) hideChallengesModal();
  });
  detailOverlay?.addEventListener("click", event => {
    if (event.target instanceof Element && event.target.closest(".challenge-detail-header-actions")) return;
    toggleDetailMoreMenu(false);
    if (event.target === detailOverlay) hideDetailModal();
  });
  confirmOverlay?.addEventListener("click", event => {
    if (event.target === confirmOverlay) hideConfirmModal(false);
  });
  logReviewOverlay?.addEventListener("click", event => {
    if (event.target === logReviewOverlay) closeLogReviewModal();
  });
  confirmCancelBtn?.addEventListener("click", () => hideConfirmModal(false));
  confirmOkBtn?.addEventListener("click", () => hideConfirmModal(true));
  logReviewCloseBtn?.addEventListener("click", closeLogReviewModal);
  logReviewCancelBtn?.addEventListener("click", closeLogReviewModal);
  logReviewApproveBtn?.addEventListener("click", async () => {
    if (!selectedPendingLog?.id) return;
    if (logReviewApproveBtn) {
      logReviewApproveBtn.disabled = true;
      logReviewApproveBtn.textContent = "Approving...";
    }
    try {
      const assignmentId = String(selectedPendingContext?.assignmentId || "");
      if (!assignmentId) {
        throw new Error("Missing challenge assignment id for approval.");
      }

      const { error: rpcError } = await supabase.rpc("approve_teacher_challenge_completion", {
        p_assignment_id: assignmentId,
        p_log_id: String(selectedPendingLog.id)
      });
      if (rpcError) {
        throw rpcError;
      }

      const reviewedBtn = detailBody?.querySelector(`[data-action='review-pending-log'][data-assignment-id='${assignmentId}']`);
      if (reviewedBtn) {
        const completedSpan = document.createElement("span");
        completedSpan.className = "challenge-status-pill status-completed";
        completedSpan.textContent = "Completed";
        reviewedBtn.replaceWith(completedSpan);
      }

      if (typeof showToast === "function") {
        showToast("Log approved.");
      }
      void awardBadgesOnApprove(selectedPendingLog.userId || selectedPendingContext?.studentId);
      closeLogReviewModal();
    } catch (error) {
      console.error("[ChallengesUI] approve pending log failed", error);
      if (typeof showToast === "function") showToast("Couldn't approve this log.");
      if (logReviewApproveBtn) {
        logReviewApproveBtn.disabled = false;
        logReviewApproveBtn.textContent = "Approve log";
      }
    }
  });
  document.addEventListener("click", event => {
    if (!detailOverlay || detailOverlay.style.display !== "flex") return;
    if (!(event.target instanceof Element)) return;
    if (event.target.closest(".challenge-detail-header-actions")) return;
    toggleDetailMoreMenu(false);
  });
  if (!staffChallengesEscBound) {
    document.addEventListener("keydown", event => {
      if (event.key !== "Escape") return;
      if (detailOverlay?.style.display === "flex") {
        if (detailMoreMenu && !detailMoreMenu.hidden) {
          toggleDetailMoreMenu(false);
          return;
        }
        hideDetailModal();
        return;
      }
      if (createOverlay?.style.display === "flex") {
        hideCreateModal();
        return;
      }
      if (logReviewOverlay?.style.display === "flex") {
        closeLogReviewModal();
        return;
      }
      if (confirmOverlay?.style.display === "flex") {
        hideConfirmModal(false);
        return;
      }
      if (staffChallengesOverlay?.style.display === "flex") {
        hideChallengesModal();
      }
    });
    staffChallengesEscBound = true;
  }

  titleInput?.addEventListener("input", updateCreateEnabled);
  pointsInput?.addEventListener("input", updateCreateEnabled);
  categorySelect?.addEventListener("change", updateCreateEnabled);
  teacherSelect?.addEventListener("change", updateCreateEnabled);
  startInput?.addEventListener("change", () => {
    if (!endTouched && startInput?.value && endInput) endInput.value = addDays(startInput.value, 30);
    updateCreateEnabled();
  });
  endInput?.addEventListener("change", () => {
    endTouched = true;
    updateCreateEnabled();
  });

  document.querySelectorAll("input[name='challengeAssignType']").forEach(input => {
    input.addEventListener("change", () => {
      updateAssignFields();
      updateCreateEnabled();
    });
  });

  studentSearchInput?.addEventListener("input", () => {
    renderStudentDropdown();
    updateCreateEnabled();
  });
  studentSearchInput?.addEventListener("focus", renderStudentDropdown);

  studentDropdown?.addEventListener("click", event => {
    const button = event.target instanceof Element ? event.target.closest(".challenge-student-option") : null;
    if (!button) return;
    const studentId = String(button.getAttribute("data-student-id") || "");
    if (!studentId) return;
    selectedStudentIds.add(studentId);
    if (studentSearchInput) studentSearchInput.value = "";
    renderStudentChips();
    renderStudentDropdown();
    updateCreateEnabled();
  });

  studentChips?.addEventListener("click", event => {
    const button = event.target instanceof Element ? event.target.closest(".challenge-chip") : null;
    if (!button) return;
    const studentId = String(button.getAttribute("data-student-id") || "");
    if (!studentId) return;
    selectedStudentIds.delete(studentId);
    renderStudentChips();
    renderStudentDropdown();
    updateCreateEnabled();
  });

  document.addEventListener("click", event => {
    const target = event.target instanceof Element ? event.target : null;
    if (!target) return;
    if (!target.closest(".challenge-student-picker") && studentDropdown) studentDropdown.hidden = true;
  });

  createActionBtn?.addEventListener("click", async () => {
    setError("");
    if (!isFormValid()) {
      setError("Please complete all required fields.");
      return;
    }

    const assignmentType = getAssignType();
    const selectedCategory = String(categorySelect?.value || "").trim();
    const rawDescription = String(descriptionInput?.value || "").trim();
    const fullDescription = selectedCategory
      ? `Category: ${selectedCategory}${rawDescription ? `\n\n${rawDescription}` : ""}`
      : (rawDescription || null);
    const payload = {
      studioId: resolvedStudioId,
      title: String(titleInput?.value || "").trim(),
      description: fullDescription,
      points: Number(pointsInput?.value),
      assignmentType,
      assignmentTeacherId: assignmentType === "teacher_students" ? getTeacherDefaultId() : null,
      selectedStudentIds: assignmentType === "selected_students" ? Array.from(selectedStudentIds) : null,
      startDate: String(startInput?.value || ""),
      endDate: String(endInput?.value || "")
    };

    createActionBtn.disabled = true;
    try {
      if (!ensureValidStudioId()) return;
      if (editingChallengeId) {
        await updateChallenge({
          challengeId: editingChallengeId,
          title: payload.title,
          description: payload.description,
          points: payload.points,
          startDate: payload.startDate,
          endDate: payload.endDate
        });
      } else {
        await createChallenge(payload);
      }
      hideCreateModal({ completed: true });
      if (typeof showToast === "function") showToast(editingChallengeId ? "Challenge updated" : "Challenge created");
      dispatchTutorialAction("aa:tutorial-staff-challenge-created");
      await loadHeaderCounts();
    } catch (error) {
      setError(error?.message || (editingChallengeId ? "Unable to update challenge" : "Unable to create challenge"));
    } finally {
      createActionBtn.disabled = !isFormValid();
    }
  });

  updateAssignFields();
  renderStudentChips();
  updateCreateEnabled();
  await loadHeaderCounts();
}
