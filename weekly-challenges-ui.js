import { completeWeeklyChallenge, fetchWeeklyChallengeCompletion, getCurrentChallenge } from "./weekly-challenges-api.js";

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function pointText(challenge) {
  const type = String(challenge?.point_type || "").toLowerCase();
  if (type === "memorization" || type === "precision" || type === "performance" || type === "practice") {
    return "Points based on activity completed";
  }
  const points = Number(challenge?.points);
  return Number.isFinite(points) ? `${points} points` : "Points based on activity completed";
}

function requiresQuantity(challenge) {
  const type = String(challenge?.point_type || "").toLowerCase();
  return type === "memorization" || type === "precision";
}

function quantityLabel(challenge) {
  const type = String(challenge?.point_type || "").toLowerCase();
  if (type === "precision") return "Bars polished";
  return "Bars memorized";
}

function renderLevels(challenge) {
  if (!challenge?.has_levels) return "";
  const rows = [
    ["beginner", "Beginner", challenge.beginner],
    ["intermediate", "Intermediate", challenge.intermediate],
    ["advanced", "Advanced", challenge.advanced]
  ].filter(([, , text]) => String(text || "").trim());

  return `
    <div class="weekly-challenge-levels">
      ${rows.map(([, label, text]) => `
        <div class="weekly-challenge-level">
          <div class="weekly-challenge-level-label">${escapeHtml(label)}</div>
          <div>${escapeHtml(text)}</div>
        </div>
      `).join("")}
    </div>
  `;
}

function completionText(completion) {
  if (!completion) return "";
  const date = String(completion.completed_at || "").replace("T", " ").slice(0, 16);
  const points = Number(completion.calculated_points || 0);
  return `Completed${date ? ` ${date}` : ""}. ${points} point${points === 1 ? "" : "s"} submitted.`;
}

function ensureWeeklyChallengeModal() {
  if (document.getElementById("weeklyChallengeCompleteOverlay")) return;
  const overlay = document.createElement("div");
  overlay.id = "weeklyChallengeCompleteOverlay";
  overlay.className = "modal-overlay";
  overlay.style.display = "none";
  overlay.innerHTML = `
    <div class="modal weekly-challenge-modal">
      <div class="modal-header">
        <div class="modal-title">Weekly Challenge</div>
        <button id="weeklyChallengeCompleteClose" class="modal-close" type="button">x</button>
      </div>
      <div id="weeklyChallengeCompleteBody" class="modal-body"></div>
    </div>
  `;
  document.body.appendChild(overlay);
}

export async function initWeeklyChallengeUI({ studioId, studentId, roles, showToast } = {}) {
  const mount = document.getElementById("weeklyChallengeMount");
  if (!mount) return;

  const studio = String(studioId || "").trim();
  const targetStudentId = String(studentId || "").trim();
  const isStudent = Array.isArray(roles) && roles.map(role => String(role || "").toLowerCase()).includes("student");
  if (!studio || !targetStudentId || !isStudent) {
    mount.innerHTML = "";
    return;
  }

  ensureWeeklyChallengeModal();

  let challenge = null;
  let completion = null;
  let submitting = false;

  const overlay = document.getElementById("weeklyChallengeCompleteOverlay");
  const body = document.getElementById("weeklyChallengeCompleteBody");
  const closeBtn = document.getElementById("weeklyChallengeCompleteClose");

  const setOpen = (open) => {
    if (overlay) overlay.style.display = open ? "flex" : "none";
  };

  const refresh = async () => {
    challenge = await getCurrentChallenge();
    completion = challenge
      ? await fetchWeeklyChallengeCompletion(studio, targetStudentId, challenge.id)
      : null;
    renderCard();
  };

  const renderCard = () => {
    if (!challenge) {
      mount.innerHTML = "";
      return;
    }

    const done = Boolean(completion);
    mount.innerHTML = `
      <section class="weekly-challenge-card" aria-label="Weekly challenge">
        <div class="weekly-challenge-kicker">Weekly Challenge</div>
        <div class="weekly-challenge-header">
          <h2>${escapeHtml(challenge.title || "Weekly Challenge")}</h2>
          <span class="weekly-challenge-week">Week ${Number(challenge.week_number || 0)}</span>
        </div>
        <div class="weekly-challenge-points">${escapeHtml(pointText(challenge))}</div>
        ${challenge.description ? `<p class="weekly-challenge-description">${escapeHtml(challenge.description)}</p>` : ""}
        ${challenge.has_levels
          ? renderLevels(challenge)
          : `<div class="weekly-challenge-task">${escapeHtml(challenge.challenge || "")}</div>`}
        <div class="weekly-challenge-instructions">
          <div class="weekly-challenge-instructions-label">Submission Instructions</div>
          <div>${escapeHtml(challenge.notes_instruction || "")}</div>
        </div>
        ${done ? `<div class="weekly-challenge-complete-note">${escapeHtml(completionText(completion))}</div>` : ""}
        <button id="weeklyChallengeCompleteBtn" type="button" class="blue-button" ${done ? "disabled" : ""}>
          ${done ? "Challenge Completed" : "Complete Challenge"}
        </button>
      </section>
    `;

    document.getElementById("weeklyChallengeCompleteBtn")?.addEventListener("click", () => {
      if (!done) openCompletionModal();
    });
  };

  const openCompletionModal = () => {
    if (!challenge || !body) return;
    const needsQuantity = requiresQuantity(challenge);
    body.innerHTML = `
      <div class="weekly-challenge-submit">
        <h3>${escapeHtml(challenge.title || "Weekly Challenge")}</h3>
        <div class="student-challenge-meta">${escapeHtml(pointText(challenge))}</div>
        ${challenge.has_levels ? `
          <div class="modal-field">
            <label for="weeklyChallengeLevel">Level</label>
            <select id="weeklyChallengeLevel">
              <option value="">Select level</option>
              <option value="beginner">Beginner</option>
              <option value="intermediate">Intermediate</option>
              <option value="advanced">Advanced</option>
            </select>
          </div>
        ` : ""}
        ${needsQuantity ? `
          <div class="modal-field">
            <label for="weeklyChallengeQuantity">${escapeHtml(quantityLabel(challenge))}</label>
            <input id="weeklyChallengeQuantity" type="number" min="1" step="1" value="1">
          </div>
        ` : ""}
        <div class="modal-field">
          <label for="weeklyChallengeNotes">Notes</label>
          <textarea id="weeklyChallengeNotes" rows="4" placeholder="${escapeHtml(challenge.notes_instruction || "Add your notes.")}"></textarea>
        </div>
        <div id="weeklyChallengeError" class="staff-msg" style="display:none;"></div>
        <div class="modal-actions">
          <button id="weeklyChallengeCancel" type="button" class="blue-button">Cancel</button>
          <button id="weeklyChallengeSubmit" type="button" class="blue-button">Submit</button>
        </div>
      </div>
    `;

    body.querySelector("#weeklyChallengeCancel")?.addEventListener("click", () => setOpen(false));
    body.querySelector("#weeklyChallengeSubmit")?.addEventListener("click", () => {
      void submitCompletion();
    });
    setOpen(true);
    body.querySelector("#weeklyChallengeNotes")?.focus();
  };

  const setError = (message) => {
    const el = document.getElementById("weeklyChallengeError");
    if (!el) return;
    const text = String(message || "").trim();
    el.textContent = text;
    el.style.display = text ? "block" : "none";
  };

  const submitCompletion = async () => {
    if (!challenge || submitting) return;
    const notes = String(document.getElementById("weeklyChallengeNotes")?.value || "").trim();
    const selectedLevel = challenge.has_levels
      ? String(document.getElementById("weeklyChallengeLevel")?.value || "").trim()
      : null;
    const quantity = requiresQuantity(challenge)
      ? parseInt(document.getElementById("weeklyChallengeQuantity")?.value || "", 10)
      : null;

    if (challenge.has_levels && !selectedLevel) {
      setError("Select a level.");
      return;
    }
    if (requiresQuantity(challenge) && (!Number.isFinite(quantity) || quantity < 1)) {
      setError("Enter the number of bars.");
      return;
    }
    if (!notes) {
      setError("Please add notes before submitting.");
      return;
    }

    const submitBtn = document.getElementById("weeklyChallengeSubmit");
    submitting = true;
    if (submitBtn) {
      submitBtn.disabled = true;
      submitBtn.textContent = "Submitting...";
    }
    setError("");

    try {
      await completeWeeklyChallenge({
        studioId: studio,
        studentId: targetStudentId,
        challengeId: challenge.id,
        selectedLevel,
        notes,
        quantity: Number.isFinite(quantity) ? quantity : null
      });
      completion = await fetchWeeklyChallengeCompletion(studio, targetStudentId, challenge.id);
      renderCard();
      setOpen(false);
      if (typeof showToast === "function") showToast("Weekly challenge submitted.");
    } catch (error) {
      console.error("[WeeklyChallenge] submit failed", error);
      const raw = String(error?.message || "");
      if (raw.includes("weekly_challenge_already_completed")) {
        setError("This weekly challenge has already been completed.");
      } else if (raw.includes("challenge_not_current")) {
        setError("This is no longer the current weekly challenge.");
      } else {
        setError(raw || "Couldn't submit weekly challenge.");
      }
    } finally {
      submitting = false;
      if (submitBtn) {
        submitBtn.disabled = false;
        submitBtn.textContent = "Submit";
      }
    }
  };

  closeBtn?.addEventListener("click", () => setOpen(false));
  overlay?.addEventListener("click", event => {
    if (event.target === overlay) setOpen(false);
  });

  try {
    await refresh();
  } catch (error) {
    console.error("[WeeklyChallenge] failed to initialize", error);
    mount.innerHTML = "";
  }
}
