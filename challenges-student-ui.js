import { supabase } from "./supabaseClient.js";
import { completeChallengeAndCreateLog, ensureFirstPracticeChallengeAssignment, fetchMyChallengeAssignments, updateAssignmentStatus } from "./challenges-api.js";
import { completeWeeklyChallenge, fetchWeeklyChallengeCompletion, fetchWeeklyChallengeCompletions, getCurrentChallenge } from "./weekly-challenges-api.js";

const NEW_BANNER_SEEN_KEY = "studentChallengesNewBannerSeen";
const NEW_BANNER_OPENED_KEY = "studentChallengesNewBannerOpened";
const NEW_CHALLENGE_OPENED_PREFIX = "studentChallengeOpened";
const dispatchTutorialAction = (action) => {
  if (!action) return;
  window.dispatchEvent(new CustomEvent(String(action)));
};

function localToday() {
  const now = new Date();
  const y = now.getFullYear();
  const m = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function toDateText(value) {
  return String(value || "");
}

function badgeText(status) {
  if (status === "new" || status === "dismissed") return "Inactive";
  if (status === "active") return "Active";
  if (status === "completed_pending") return "Pending approval";
  if (status === "pending_review" || status === "pending") return "Pending approval";
  if (status === "completed") return "Completed";
  return status || "Unknown";
}

function badgeClass(row) {
  if (row?.expired) return "is-expired";
  if (row?.status === "completed_pending" || row?.status === "pending_review" || row?.status === "pending") return "is-pending";
  if (row?.status === "completed") return "is-completed";
  return "";
}

function weeklyPointText(challenge) {
  const type = String(challenge?.point_type || "").toLowerCase();
  if (type === "memorization" || type === "precision" || type === "performance" || type === "practice") {
    return "Points based on activity completed";
  }
  const points = Number(challenge?.points);
  return Number.isFinite(points) ? `${points} points` : "Points based on activity completed";
}

function weeklyRequiresQuantity(challenge) {
  const type = String(challenge?.point_type || "").toLowerCase();
  return type === "memorization" || type === "precision";
}

function weeklyQuantityLabel(challenge) {
  return String(challenge?.point_type || "").toLowerCase() === "precision" ? "Bars polished" : "Bars memorized";
}

function renderWeeklyLevels(challenge) {
  if (!challenge?.has_levels) return "";
  const rows = [
    ["Beginner", challenge.beginner],
    ["Intermediate", challenge.intermediate],
    ["Advanced", challenge.advanced]
  ].filter(([, text]) => String(text || "").trim());

  return `
    <div class="student-challenge-levels">
      ${rows.map(([label, text]) => `
        <div class="student-challenge-level">
          <div class="student-challenge-level-label">${String(label)}</div>
          <div>${String(text || "")}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function setStudentChallengeToggleVisual(button, isActive) {
  if (!(button instanceof HTMLElement)) return;
  const textEl = button.querySelector(".student-challenge-switch-text");
  if (textEl) textEl.textContent = isActive ? "Active" : "Inactive";
  button.classList.toggle("is-active", isActive);
  button.classList.toggle("is-inactive", !isActive);
  button.setAttribute("aria-pressed", isActive ? "true" : "false");
  button.setAttribute("aria-label", `Set challenge ${isActive ? "inactive" : "active"}`);
  button.setAttribute("data-next-status", isActive ? "dismissed" : "active");
  button.setAttribute("data-state-label", isActive ? "Active" : "Inactive");
}

function ensureStudentChallengeModals() {
  if (!document.getElementById("studentChallengesListOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "studentChallengesListOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal student-challenges-modal">
        <div class="modal-header">
          <div class="modal-title">Teacher Challenges</div>
          <button id="studentChallengesListClose" class="modal-close" type="button">x</button>
        </div>
        <div class="modal-body">
          <div id="studentChallengesTabs" class="student-challenges-tabs"></div>
          <div id="studentChallengesList" class="student-challenges-list"></div>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
  }

  if (!document.getElementById("studentChallengeDetailOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "studentChallengeDetailOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal student-challenges-modal">
        <div class="modal-header">
          <div class="modal-title">Challenge</div>
          <button id="studentChallengeDetailClose" class="modal-close" type="button">x</button>
        </div>
        <div id="studentChallengeDetailBody" class="modal-body"></div>
      </div>
    `;
    document.body.appendChild(overlay);
  }

  if (!document.getElementById("studentChallengeCompleteOverlay")) {
    const overlay = document.createElement("div");
    overlay.id = "studentChallengeCompleteOverlay";
    overlay.className = "modal-overlay";
    overlay.style.display = "none";
    overlay.innerHTML = `
      <div class="modal student-challenges-modal">
        <div class="modal-header">
          <div class="modal-title">Challenge Completed!</div>
          <button id="studentChallengeCompleteClose" class="modal-close" type="button">x</button>
        </div>
        <div class="modal-body">
          <div class="modal-field">
            <label>Challenge</label>
            <input id="studentChallengeCompleteTitle" type="text" readonly />
          </div>
          <div id="studentChallengeCompleteDateRow" class="challenge-date-row">
            <div id="studentChallengeCompleteDateField" class="modal-field">
              <label>Date</label>
              <input id="studentChallengeCompleteDate" type="date" />
            </div>
            <div class="modal-field">
              <label>Points</label>
              <input id="studentChallengeCompletePoints" type="text" readonly />
            </div>
          </div>
          <div id="studentChallengeCompleteLevelField" class="modal-field" style="display:none;">
            <label for="studentChallengeCompleteLevel">Level</label>
            <select id="studentChallengeCompleteLevel">
              <option value="">Select level</option>
              <option value="beginner">Beginner</option>
              <option value="intermediate">Intermediate</option>
              <option value="advanced">Advanced</option>
            </select>
          </div>
          <div id="studentChallengeCompleteQuantityField" class="modal-field" style="display:none;">
            <label id="studentChallengeCompleteQuantityLabel" for="studentChallengeCompleteQuantity">Bars completed</label>
            <input id="studentChallengeCompleteQuantity" type="number" min="1" step="1" value="1" />
          </div>
          <div id="studentChallengeCompleteInstruction" class="student-challenge-instruction" style="display:none;"></div>
          <div class="modal-field">
            <label for="studentChallengeCompleteNote">Note (required)</label>
            <textarea id="studentChallengeCompleteNote" rows="3" placeholder="What did you complete?"></textarea>
          </div>
          <div id="studentChallengeCompleteError" class="staff-msg" style="display:none;"></div>
          <div class="modal-actions">
            <button id="studentChallengeCompleteCancel" type="button" class="blue-button">Cancel</button>
            <button id="studentChallengeCompleteSubmit" type="button" class="blue-button">Submit Log</button>
          </div>
        </div>
      </div>
    `;
    document.body.appendChild(overlay);
  }
}

export async function initStudentChallengesUI({ studioId, studentId, roles, showToast }) {
  const noticeMount = document.getElementById("studentChallengesNoticeMount");
  const subtleMount = document.getElementById("studentChallengesSubtleMount");
  if (!noticeMount || !subtleMount) return;

  const studio = String(studioId || "").trim();
  const targetStudentId = String(studentId || "").trim();
  const isStudent = Array.isArray(roles) && roles.map(r => String(r || "").toLowerCase()).includes("student");
  if (!studio || !isStudent) {
    noticeMount.innerHTML = "";
    subtleMount.innerHTML = "";
    return;
  }
  if (!targetStudentId) {
    console.error("[StudentChallengesUI] missing studentId");
    if (typeof showToast === "function") showToast("No student selected.");
    noticeMount.innerHTML = "";
    subtleMount.innerHTML = "";
    return;
  }

  ensureStudentChallengeModals();

  const listOverlay = document.getElementById("studentChallengesListOverlay");
  const detailOverlay = document.getElementById("studentChallengeDetailOverlay");
  const completeOverlay = document.getElementById("studentChallengeCompleteOverlay");
  const listClose = document.getElementById("studentChallengesListClose");
  const detailClose = document.getElementById("studentChallengeDetailClose");
  const completeClose = document.getElementById("studentChallengeCompleteClose");
  const tabsEl = document.getElementById("studentChallengesTabs");
  const listEl = document.getElementById("studentChallengesList");
  const detailBody = document.getElementById("studentChallengeDetailBody");
  const completeTitleInput = document.getElementById("studentChallengeCompleteTitle");
  const completeDateRow = document.getElementById("studentChallengeCompleteDateRow");
  const completeDateField = document.getElementById("studentChallengeCompleteDateField");
  const completeDateInput = document.getElementById("studentChallengeCompleteDate");
  const completePointsInput = document.getElementById("studentChallengeCompletePoints");
  const completeLevelField = document.getElementById("studentChallengeCompleteLevelField");
  const completeLevelSelect = document.getElementById("studentChallengeCompleteLevel");
  const completeQuantityField = document.getElementById("studentChallengeCompleteQuantityField");
  const completeQuantityLabel = document.getElementById("studentChallengeCompleteQuantityLabel");
  const completeQuantityInput = document.getElementById("studentChallengeCompleteQuantity");
  const completeInstructionEl = document.getElementById("studentChallengeCompleteInstruction");
  const completeNoteInput = document.getElementById("studentChallengeCompleteNote");
  const completeErrorEl = document.getElementById("studentChallengeCompleteError");
  const completeCancelBtn = document.getElementById("studentChallengeCompleteCancel");
  const completeSubmitBtn = document.getElementById("studentChallengeCompleteSubmit");

  let assignments = [];
  let weeklyChallenge = null;
  let weeklyCompletion = null;
  let weeklyCompletions = [];
  let usersById = new Map();
  let activeTab = "current";
  let selectedCompletionAssignmentId = "";
  let selectedWeeklyChallengeId = "";

  const challengeOpenedKey = (key) =>
    `${NEW_CHALLENGE_OPENED_PREFIX}:${studio}:${targetStudentId}:${String(key || "").trim()}`;

  const hasOpenedChallenge = (key) => {
    const normalized = String(key || "").trim();
    return normalized && localStorage.getItem(challengeOpenedKey(normalized)) === "1";
  };

  const markChallengeOpened = (key) => {
    const normalized = String(key || "").trim();
    if (normalized) localStorage.setItem(challengeOpenedKey(normalized), "1");
  };

  const weeklyChallengeKey = () =>
    weeklyChallenge && !weeklyCompletion ? `weekly:${weeklyChallenge.id}` : "";

  const getUnopenedCurrentChallengeKeys = () => {
    const { buckets } = derive();
    const keys = buckets.newVisible
      .map(row => `teacher:${row.id}`)
      .filter(key => !hasOpenedChallenge(key));
    const weeklyKey = weeklyChallengeKey();
    if (weeklyKey && !hasOpenedChallenge(weeklyKey)) keys.unshift(weeklyKey);
    return keys;
  };

  const setListOpen = (open) => {
    if (listOverlay) listOverlay.style.display = open ? "flex" : "none";
    if (!open) dispatchTutorialAction("aa:tutorial-student-challenges-dismissed");
  };

  const setDetailOpen = (open) => {
    if (detailOverlay) detailOverlay.style.display = open ? "flex" : "none";
    if (!open) dispatchTutorialAction("aa:tutorial-student-challenges-dismissed");
  };

  const setCompleteOpen = (open) => {
    if (completeOverlay) completeOverlay.style.display = open ? "flex" : "none";
    if (!open) dispatchTutorialAction("aa:tutorial-student-challenges-dismissed");
  };

  const setCompletionError = (message) => {
    if (!completeErrorEl) return;
    const text = String(message || "").trim();
    completeErrorEl.textContent = text;
    completeErrorEl.style.display = text ? "block" : "none";
  };

  const derive = () => {
    const today = localToday();
    const mapped = assignments.map(row => {
      const challenge = row.teacher_challenges || {};
      const start = String(challenge.start_date || "");
      const end = String(challenge.end_date || "");
      const status = String(row.status || "");
      const startsOnTime = !start || today >= start;
      const endsOnTime = !end || today <= end;
      const inWindow = startsOnTime && endsOnTime;
      const endedByDate = !!end && today > end;
      const activeValue = challenge?.is_active;
      const endedByTeacher = activeValue === false || String(activeValue).toLowerCase() === "false";
      const challengeEnded = endedByDate || endedByTeacher;
      const completedStatus = status === "completed" || status === "completed_pending" || status === "pending_review" || status === "pending";
      const availableStatus = status === "active" || status === "new";
      const current = availableStatus && inWindow && !challengeEnded;
      const expired = !completedStatus && !current && (challengeEnded || status === "dismissed");
      return { ...row, challenge, start, end, status, inWindow, challengeEnded, current, expired, today };
    });

    const completed = mapped.filter(entry => entry.status === "completed" || entry.status === "completed_pending" || entry.status === "pending_review" || entry.status === "pending");
    const current = mapped.filter(entry => entry.current);
    const newVisible = mapped.filter(entry => entry.current && entry.status === "new");
    const activeVisible = current;
    const expired = mapped.filter(entry => entry.expired);
    return { mapped, buckets: { current, completed, expired, newVisible, activeVisible } };
  };

  const markNewBannerOpened = () => {
    sessionStorage.setItem(NEW_BANNER_OPENED_KEY, "1");
    getUnopenedCurrentChallengeKeys().forEach(markChallengeOpened);
  };

  const openListAt = (tabKey) => {
    activeTab = tabKey;
    markNewBannerOpened();
    renderListModal();
    renderHomeSurface();
    setListOpen(true);
  };

  const renderHomeSurface = () => {
    const tabRows = getTabRows();
    const currentCount = tabRows.find(tab => tab.key === "current")?.rows.length || 0;
    const completedCount = tabRows.find(tab => tab.key === "completed")?.rows.length || 0;
    const expiredCount = tabRows.find(tab => tab.key === "expired")?.rows.length || 0;
    const hasAnyChallenge = currentCount > 0 || completedCount > 0 || expiredCount > 0;
    const newCount = getUnopenedCurrentChallengeKeys().length;
    const formatCountLabel = (count, singular, plural) => `${count} ${count === 1 ? singular : plural}`;

    if (newCount > 0) {
      const shouldAnimate = !sessionStorage.getItem(NEW_BANNER_SEEN_KEY);
      sessionStorage.setItem(NEW_BANNER_SEEN_KEY, "1");
      noticeMount.innerHTML = `
        <button id="studentChallengesNoticeBtn" type="button" class="student-challenges-notice-banner${shouldAnimate ? " is-enter" : ""}">
          <span class="student-challenges-notice-title">&#10024; You have ${newCount} NEW Challenge${newCount === 1 ? "" : "s"}!</span>
          <span class="student-challenges-notice-subtext">Tap to view.</span>
        </button>
      `;
      subtleMount.innerHTML = "";
      document.getElementById("studentChallengesNoticeBtn")?.addEventListener("click", () => openListAt("current"));
      return;
    }

    noticeMount.innerHTML = hasAnyChallenge ? "" : `<div class="student-challenges-notice-spacer" aria-hidden="true"></div>`;
    if (hasAnyChallenge) {
      const label = currentCount > 0
        ? formatCountLabel(currentCount, "Current Challenge", "Current Challenges")
        : "Challenges";
      subtleMount.innerHTML = `<button id="studentChallengesSubtleBtn" type="button" class="student-challenges-pill-link">${label}</button>`;
      document.getElementById("studentChallengesSubtleBtn")?.addEventListener("click", () => openListAt("current"));
      return;
    }

    subtleMount.innerHTML = "";
  };

  const getTabRows = () => {
    const { buckets } = derive();
    const currentRows = weeklyChallenge && !weeklyCompletion
      ? [{ kind: "weekly", id: `weekly-${weeklyChallenge.id}`, challenge: weeklyChallenge }].concat(buckets.current)
      : buckets.current;
    const weeklyCompletedRows = weeklyCompletions
      .map(completion => ({
        kind: "weekly",
        id: `weekly-${completion.challenge_id}`,
        challenge: completion.weekly_challenges || (String(completion.challenge_id) === String(weeklyChallenge?.id) ? weeklyChallenge : null),
        completion,
        status: "completed"
      }))
      .filter(row => row.challenge);
    const completedRows = weeklyCompletedRows.concat(buckets.completed);
    return [
      { key: "current", label: "Current", rows: currentRows },
      { key: "completed", label: "Completed", rows: completedRows },
      { key: "expired", label: "Expired", rows: buckets.expired }
    ];
  };

  const renderWeeklyRow = (row, currentTabKey) => {
    const challenge = row?.challenge || {};
    const completion = row?.completion || weeklyCompletion;
    const done = Boolean(completion);
    const isCurrent = currentTabKey === "current";
    return `
      <div class="student-challenge-row student-challenge-row-weekly" data-weekly-challenge-id="${challenge.id}">
        <div class="student-challenge-row-top">
          <div>
            <div class="student-challenge-title">${String(challenge.title || "Studio Wide Challenge")}</div>
            <div class="student-challenge-source">&#9733; Studio Wide Challenge</div>
          </div>
          <span class="student-challenge-badge ${done ? "is-completed" : ""}">${done ? "Completed" : `Week ${Number(challenge.week_number || 0)}`}</span>
        </div>
        <div class="student-challenge-meta">${weeklyPointText(challenge)}</div>
        ${challenge.description ? `<p class="student-challenge-description">${String(challenge.description)}</p>` : ""}
        ${challenge.has_levels
          ? renderWeeklyLevels(challenge)
          : `<div class="student-challenge-task">${String(challenge.challenge || "")}</div>`}
        <div class="student-challenge-instruction">
          <strong>Submission Instructions:</strong> ${String(challenge.notes_instruction || "")}
        </div>
        ${done
          ? `<div class="student-challenge-status-note">Completed ${String(completion.completed_at || "").replace("T", " ").slice(0, 16)} &middot; ${Number(completion.calculated_points || 0)} points submitted</div>`
          : isCurrent
            ? `<div class="student-challenge-row-actions">
                <button type="button" class="blue-button" data-complete-weekly-challenge-id="${challenge.id}">Challenge Completed!</button>
              </div>`
            : ""}
      </div>
    `;
  };

  const renderListModal = () => {
    if (!tabsEl || !listEl) return;
    const tabRows = getTabRows();
    if (!tabRows.some(tab => tab.key === activeTab)) activeTab = "current";

    tabsEl.innerHTML = tabRows.map(tab => `
      <button
        type="button"
        class="student-challenges-tab ${tab.key === activeTab ? "is-active" : ""}"
        data-tab="${tab.key}"
      >
        ${tab.label} (${tab.rows.length})
      </button>
    `).join("");

    const currentTab = tabRows.find(tab => tab.key === activeTab) || tabRows[0];
    if (!currentTab.rows.length) {
      const emptyText = currentTab.key === "completed"
        ? "No completed challenges yet."
        : currentTab.key === "expired"
          ? "No expired challenges."
          : "No current challenges right now.";
      listEl.innerHTML = `<div class="student-challenge-empty">${emptyText}</div>`;
    } else {
      listEl.innerHTML = currentTab.rows.map(row => {
        if (row?.kind === "weekly") return renderWeeklyRow(row, currentTab.key);
        const isCurrent = currentTab.key === "current";
        const isActiveRow = row.status === "active";
        const pendingStatus = row.status === "completed_pending" || row.status === "pending_review" || row.status === "pending";
        const statusLabel = pendingStatus && row.challengeEnded ? "Challenge ended" : badgeText(row.status);
        return `
          <div class="student-challenge-row" data-assignment-id="${row.id}">
            <div class="student-challenge-row-top">
              <div class="student-challenge-title">${String(row.challenge.title || "Untitled challenge")}</div>
              ${isCurrent
                ? `<button
                    type="button"
                    class="student-challenge-switch ${isActiveRow ? "is-active" : "is-inactive"}"
                    data-toggle-assignment-id="${row.id}"
                    data-next-status="${isActiveRow ? "dismissed" : "active"}"
                    aria-label="Set challenge ${isActiveRow ? "inactive" : "active"}"
                    aria-pressed="${isActiveRow ? "true" : "false"}"
                    data-state-label="${isActiveRow ? "Active" : "Inactive"}"
                  ><span class="student-challenge-switch-thumb" aria-hidden="true"></span><span class="student-challenge-switch-text">${isActiveRow ? "Active" : "Inactive"}</span></button>`
                : `<span class="student-challenge-badge ${badgeClass(row)}">${row.expired ? "Expired" : statusLabel}</span>`}
            </div>
            <div class="student-challenge-meta">${Number(row.challenge.points || 0)} points</div>
            <div class="student-challenge-meta">Ends ${toDateText(row.end)}</div>
            ${isCurrent && isActiveRow
              ? `<div class="student-challenge-row-actions">
                  <button
                    type="button"
                    class="blue-button"
                    data-complete-assignment-id="${row.id}"
                  >Challenge Completed!</button>
                </div>`
              : ""}
          </div>
        `;
      }).join("");
    }

    tabsEl.querySelectorAll("[data-tab]").forEach(btn => {
      btn.addEventListener("click", () => {
        activeTab = String(btn.getAttribute("data-tab") || "current");
        renderListModal();
      });
    });

    listEl.querySelectorAll("[data-toggle-assignment-id]").forEach(btn => {
      btn.addEventListener("click", async event => {
        event.stopPropagation();
        const id = String(btn.getAttribute("data-toggle-assignment-id") || "");
        const nextStatus = String(btn.getAttribute("data-next-status") || "");
        const row = assignments.find(entry => String(entry.id) === id);
        if (!row || !nextStatus) return;
        const wasActive = String(row.status || "") === "active";
        setStudentChallengeToggleVisual(btn, !wasActive);
        try {
          if (String(row.status || "") === "dismissed" && nextStatus === "active") {
            await updateAssignmentStatus(id, "new");
          }
          await updateAssignmentStatus(id, nextStatus);
          row.status = nextStatus;
          await refreshAll();
          renderListModal();
          setListOpen(true);
        } catch (error) {
          setStudentChallengeToggleVisual(btn, wasActive);
          console.error("[StudentChallenges] toggle failed", error);
          if (typeof showToast === "function") showToast("Couldn't update challenge status.");
        }
      });
    });

    listEl.querySelectorAll("[data-assignment-id]").forEach(rowEl => {
      rowEl.addEventListener("click", event => {
        const target = event.target instanceof Element ? event.target : null;
        if (target?.closest(".student-challenge-switch")) return;
        if (target?.closest("[data-complete-assignment-id]")) return;
        const id = String(rowEl.getAttribute("data-assignment-id") || "");
        const row = assignments.find(entry => String(entry.id) === id);
        if (!row) return;
        setListOpen(false);
        renderDetailModal(row);
        setDetailOpen(true);
      });
    });

    listEl.querySelectorAll("[data-complete-assignment-id]").forEach(btn => {
      btn.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        const assignmentId = String(btn.getAttribute("data-complete-assignment-id") || "");
        const row = assignments.find(entry => String(entry.id) === assignmentId);
        if (!row) return;
        openCompletionModal(row);
      });
    });

    listEl.querySelectorAll("[data-complete-weekly-challenge-id]").forEach(btn => {
      btn.addEventListener("click", event => {
        event.preventDefault();
        event.stopPropagation();
        openWeeklyCompletionModal();
      });
    });
  };

  const refreshAll = async () => {
    try {
      assignments = await fetchMyChallengeAssignments(studio, targetStudentId);
    } catch (error) {
      console.error("[StudentChallengesUI] failed fetching teacher challenges", error);
      throw error;
    }

    try {
      weeklyChallenge = await getCurrentChallenge();
      weeklyCompletion = weeklyChallenge
        ? await fetchWeeklyChallengeCompletion(studio, targetStudentId, weeklyChallenge.id)
        : null;
      weeklyCompletions = await fetchWeeklyChallengeCompletions(studio, targetStudentId);
    } catch (error) {
      console.error("[StudentChallengesUI] failed fetching weekly challenges", error);
      throw error;
    }

    const { buckets } = derive();
    const tabRows = getTabRows();
    const currentTeacherCount = buckets.current.length;
    const completedCount = tabRows.find(tab => tab.key === "completed")?.rows.length || 0;
    const expiredCount = buckets.expired.length;
    console.log("[StudentChallengesUI] counts", {
      fetchedTeacherChallengesCount: assignments.length,
      currentTeacherCount,
      weeklyChallengeLoaded: Boolean(weeklyChallenge),
      newVisible: buckets.newVisible.length,
      activeVisible: buckets.activeVisible.length,
      current: tabRows.find(tab => tab.key === "current")?.rows.length || 0,
      completed: completedCount,
      expired: expiredCount
    });

    const creatorIds = Array.from(new Set(
      assignments
        .map(row => row?.teacher_challenges?.created_by)
        .filter(Boolean)
        .map(String)
    ));
    if (creatorIds.length) {
      const { data } = await supabase
        .from("users")
        .select("id, firstName, lastName")
        .in("id", creatorIds);
      usersById = new Map((Array.isArray(data) ? data : []).map(u => [String(u.id), u]));
    } else {
      usersById = new Map();
    }

    renderHomeSurface();
    if (listOverlay?.style.display === "flex") renderListModal();
  };

  const ensureAutomaticChallenges = async () => {
    try {
      const assignmentId = await ensureFirstPracticeChallengeAssignment(studio, targetStudentId);
      console.log("[StudentChallengesUI] first practice auto challenge", {
        studioId: studio,
        studentId: targetStudentId,
        assignmentId
      });
    } catch (error) {
      console.error("[StudentChallengesUI] first practice auto challenge failed", error);
    }
  };

  const renderDetailModal = (raw) => {
    if (!detailBody) return;
    const { mapped } = derive();
    const row = mapped.find(entry => String(entry.id) === String(raw.id));
    if (!row) {
      detailBody.innerHTML = `<div class="student-challenge-empty">Challenge not found.</div>`;
      return;
    }

    const challenge = row.challenge;
    const creator = usersById.get(String(challenge.created_by || ""));
    const teacherName = creator ? `${creator.firstName || ""} ${creator.lastName || ""}`.trim() || "Teacher" : "Teacher";
    const isExpired = row.expired;
    const isCompleted = row.status === "completed";
    const isPendingReview = row.status === "completed_pending" || row.status === "pending_review" || row.status === "pending";
    const isActive = row.status === "active" && !isExpired;

    detailBody.innerHTML = `
      <div class="student-challenge-detail">
        <h3>${String(challenge.title || "Untitled challenge")}</h3>
        <div class="student-challenge-meta">${Number(challenge.points || 0)} points</div>
        <div class="student-challenge-meta">${toDateText(row.start)} to ${toDateText(row.end)}</div>
        <div class="student-challenge-meta">Teacher: ${teacherName}</div>
        <p>${String(challenge.description || "No instructions provided.")}</p>
        ${isExpired ? '<div class="student-challenge-status-note">Expired</div>' : ""}
        ${isCompleted ? `<div class="student-challenge-status-note">Completed ${String(row.completed_at || "").replace("T", " ").slice(0, 16)}</div>` : ""}
        ${isPendingReview ? `<div class="student-challenge-status-note">${row.challengeEnded ? "Challenge ended" : "Pending teacher approval"}</div>` : ""}
        <div class="modal-actions">
          ${isActive ? '<button id="challengeDetailCompleteBtn" type="button" class="blue-button">Challenge Completed!</button>' : ""}
          <button id="challengeDetailBackBtn" type="button" class="blue-button">Back</button>
        </div>
      </div>
    `;

    detailBody.querySelector("#challengeDetailCompleteBtn")?.addEventListener("click", () => {
      openCompletionModal(row);
    });
    detailBody.querySelector("#challengeDetailBackBtn")?.addEventListener("click", () => {
      setDetailOpen(false);
      renderListModal();
      setListOpen(true);
    });
  };

  const openCompletionModal = (raw) => {
    const { mapped } = derive();
    const row = mapped.find(entry => String(entry.id) === String(raw?.id || ""));
    if (!row) return;
    if (row.status !== "active" || row.expired) {
      if (typeof showToast === "function") showToast("Only active challenges can be submitted.");
      return;
    }
    selectedWeeklyChallengeId = "";
    selectedCompletionAssignmentId = String(row.id || "");
    if (completeTitleInput) completeTitleInput.value = String(row.challenge?.title || "Challenge");
    if (completeDateRow) completeDateRow.style.display = "";
    if (completeDateField) completeDateField.style.display = "";
    if (completeDateInput) completeDateInput.value = localToday();
    if (completePointsInput) completePointsInput.value = `${Number(row.challenge?.points || 0)} points`;
    if (completeLevelField) completeLevelField.style.display = "none";
    if (completeLevelSelect) completeLevelSelect.value = "";
    if (completeQuantityField) completeQuantityField.style.display = "none";
    if (completeQuantityInput) completeQuantityInput.value = "1";
    if (completeInstructionEl) {
      completeInstructionEl.textContent = "";
      completeInstructionEl.style.display = "none";
    }
    if (completeNoteInput) {
      completeNoteInput.value = "";
      completeNoteInput.placeholder = "What did you complete?";
    }
    setCompletionError("");
    setCompleteOpen(true);
    completeNoteInput?.focus();
  };

  const openWeeklyCompletionModal = () => {
    if (!weeklyChallenge || weeklyCompletion) return;
    selectedCompletionAssignmentId = "";
    selectedWeeklyChallengeId = String(weeklyChallenge.id || "");
    if (completeTitleInput) completeTitleInput.value = String(weeklyChallenge.title || "Studio Wide Challenge");
    if (completeDateRow) completeDateRow.style.display = "";
    if (completeDateField) completeDateField.style.display = "none";
    if (completeDateInput) completeDateInput.value = localToday();
    if (completePointsInput) completePointsInput.value = weeklyPointText(weeklyChallenge);
    if (completeLevelField) completeLevelField.style.display = weeklyChallenge.has_levels ? "" : "none";
    if (completeLevelSelect) completeLevelSelect.value = "";
    if (completeQuantityField) completeQuantityField.style.display = weeklyRequiresQuantity(weeklyChallenge) ? "" : "none";
    if (completeQuantityLabel) completeQuantityLabel.textContent = weeklyQuantityLabel(weeklyChallenge);
    if (completeQuantityInput) completeQuantityInput.value = "1";
    if (completeInstructionEl) {
      completeInstructionEl.textContent = String(weeklyChallenge.notes_instruction || "");
      completeInstructionEl.style.display = completeInstructionEl.textContent ? "block" : "none";
    }
    if (completeNoteInput) {
      completeNoteInput.value = "";
      completeNoteInput.placeholder = String(weeklyChallenge.notes_instruction || "What did you complete?");
    }
    setCompletionError("");
    setCompleteOpen(true);
    completeNoteInput?.focus();
  };

  const submitCompletion = async () => {
    const weeklyId = String(selectedWeeklyChallengeId || "").trim();
    if (weeklyId) {
      const note = String(completeNoteInput?.value || "").trim();
      const selectedLevel = weeklyChallenge?.has_levels ? String(completeLevelSelect?.value || "").trim() : null;
      const quantity = weeklyRequiresQuantity(weeklyChallenge)
        ? parseInt(String(completeQuantityInput?.value || ""), 10)
        : null;
      if (weeklyChallenge?.has_levels && !selectedLevel) {
        setCompletionError("Please select a level.");
        return;
      }
      if (weeklyRequiresQuantity(weeklyChallenge) && (!Number.isFinite(quantity) || quantity < 1)) {
        setCompletionError("Please enter the number of bars.");
        return;
      }
      if (!note) {
        setCompletionError("Please add a note before submitting.");
        return;
      }

      if (completeSubmitBtn) {
        completeSubmitBtn.disabled = true;
        completeSubmitBtn.textContent = "Submitting...";
      }
      setCompletionError("");

      try {
        await completeWeeklyChallenge({
          studioId: studio,
          studentId: targetStudentId,
          challengeId: weeklyId,
          selectedLevel,
          notes: note,
          quantity: Number.isFinite(quantity) ? quantity : null
        });
        await refreshAll();
        renderListModal();
        setCompleteOpen(false);
        setDetailOpen(false);
        setListOpen(true);
        selectedWeeklyChallengeId = "";
        if (typeof showToast === "function") showToast("Weekly challenge completion submitted.");
      } catch (error) {
        console.error("[StudentChallenges] weekly completion submit failed", error);
        const rawMessage = String(error?.message || "");
        if (rawMessage.includes("weekly_challenge_already_completed")) {
          setCompletionError("This weekly challenge has already been completed.");
        } else if (rawMessage.includes("challenge_not_current")) {
          setCompletionError("This is no longer the current weekly challenge.");
        } else {
          setCompletionError(rawMessage || "Couldn't submit weekly challenge.");
        }
      } finally {
        if (completeSubmitBtn) {
          completeSubmitBtn.disabled = false;
          completeSubmitBtn.textContent = "Submit Log";
        }
      }
      return;
    }

    const assignmentId = String(selectedCompletionAssignmentId || "").trim();
    const row = assignments.find(entry => String(entry.id) === assignmentId);
    if (!assignmentId || !row) return;
    const challengeTitle = String(row?.teacher_challenges?.title || "Challenge").trim();
    const note = String(completeNoteInput?.value || "").trim();
    const selectedDate = String(completeDateInput?.value || "").slice(0, 10) || localToday();
    if (!note) {
      setCompletionError("Please add a note before submitting.");
      return;
    }

    if (completeSubmitBtn) {
      completeSubmitBtn.disabled = true;
      completeSubmitBtn.textContent = "Submitting...";
    }
    setCompletionError("");

    try {
      const logId = await completeChallengeAndCreateLog(assignmentId, targetStudentId, selectedDate);
      const logNote = `Teacher Challenge: ${challengeTitle} - ${note}`;
      const { error: updateErr } = await supabase
        .from("logs")
        .update({ notes: logNote })
        .eq("id", logId)
        .eq("userId", targetStudentId);
      if (updateErr) throw updateErr;

      await refreshAll();
      renderListModal();
      setCompleteOpen(false);
      setDetailOpen(false);
      setListOpen(true);
      selectedCompletionAssignmentId = "";
      if (typeof showToast === "function") showToast("Challenge completion submitted.");
    } catch (error) {
      console.error("[StudentChallenges] completion submit failed", error);
      const rawMessage = String(error?.message || "");
      if (rawMessage.toLowerCase().includes("only active assignments")) {
        setCompletionError("Challenge must be active before you can submit it.");
      } else {
        setCompletionError(rawMessage || "Couldn't submit challenge completion.");
      }
    } finally {
      if (completeSubmitBtn) {
        completeSubmitBtn.disabled = false;
        completeSubmitBtn.textContent = "Submit Log";
      }
    }
  };

  listClose?.addEventListener("click", () => setListOpen(false));
  detailClose?.addEventListener("click", () => setDetailOpen(false));
  completeClose?.addEventListener("click", () => setCompleteOpen(false));
  completeCancelBtn?.addEventListener("click", () => setCompleteOpen(false));
  completeSubmitBtn?.addEventListener("click", () => {
    void submitCompletion();
  });
  completeNoteInput?.addEventListener("keydown", event => {
    if (event.key !== "Enter" || event.shiftKey || event.isComposing) return;
    event.preventDefault();
    void submitCompletion();
  });
  listOverlay?.addEventListener("click", event => {
    if (event.target === listOverlay) setListOpen(false);
  });
  detailOverlay?.addEventListener("click", event => {
    if (event.target === detailOverlay) setDetailOpen(false);
  });
  completeOverlay?.addEventListener("click", event => {
    if (event.target === completeOverlay) setCompleteOpen(false);
  });

  try {
    await ensureAutomaticChallenges();
    await refreshAll();
  } catch (error) {
    console.error("[StudentChallenges] failed to initialize", error);
    if (typeof showToast === "function") showToast("Couldn't load challenges.");
  }
}
