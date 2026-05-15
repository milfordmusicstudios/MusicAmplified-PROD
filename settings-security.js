import { supabase } from "./supabaseClient.js";
import { clearAppSessionCache, ensureUserRow, getAuthUserId, getViewerContext } from "./utils.js";
import { applyTeacherOptionsToSelect, loadTeachersForStudio, parseRoles, refreshTeacherMultiSelect, showToast } from "./settings-shared.js";
import { requestStudentTutorialReplay, requestTeacherAdminTutorialReplay } from "./student-tutorial.js";
import { hasFamilyAccess, isAccountHolder, isSelfManagedStudent } from "./permissions.js";

let authUserId = null;
let activeStudioId = null;
let authProfile = null;
let teacherOptions = [];
let isAccountEditing = false;
let isRecoveryMode = false;
let isStudentLocked = false;
let helpRecipientEmail = "support@milfordmusic.com";
let activeStudioName = "";

function getHighestRole(roles) {
  const priority = { admin: 3, teacher: 2, student: 1, parent: 0 };
  if (!Array.isArray(roles)) return "student";
  return roles.slice().sort((a, b) => (priority[b?.toLowerCase()] ?? -1) - (priority[a?.toLowerCase()] ?? -1))[0];
}

function setTeacherError(message) {
  const errorEl = document.getElementById("accountTeacherError");
  if (!errorEl) return;
  errorEl.textContent = message || "";
  errorEl.style.display = message ? "block" : "none";
}

function setPasswordError(message) {
  const errorEl = document.getElementById("passwordError");
  if (!errorEl) return;
  errorEl.textContent = message || "";
  errorEl.style.display = message ? "block" : "none";
}

function setRecoveryError(message) {
  const errorEl = document.getElementById("passwordRecoveryError");
  if (!errorEl) return;
  errorEl.textContent = message || "";
  errorEl.style.display = message ? "block" : "none";
}

function setHelpRequestError(message) {
  const errorEl = document.getElementById("helpRequestError");
  if (!errorEl) return;
  errorEl.textContent = message || "";
  errorEl.style.display = message ? "block" : "none";
}

function getProfileDisplayName(profile) {
  const first = String(profile?.firstName || "").trim();
  const last = String(profile?.lastName || "").trim();
  return last && first ? `${last}, ${first}` : (last || first || profile?.email || "User");
}

async function loadHelpRecipient(studioId) {
  if (!studioId) return;
  try {
    const { data, error } = await supabase
      .from("studios")
      .select("name, email")
      .eq("id", studioId)
      .maybeSingle();
    if (error) throw error;
    activeStudioName = String(data?.name || "").trim();
    const studioEmail = String(data?.email || "").trim();
    if (studioEmail) helpRecipientEmail = studioEmail;
  } catch (err) {
    console.warn("[Settings] help recipient lookup failed", err);
  }
}

function setHelpRequestModalOpen(open) {
  const modal = document.getElementById("helpRequestModal");
  if (!modal) return;
  modal.classList.toggle("is-open", Boolean(open));
  modal.setAttribute("aria-hidden", open ? "false" : "true");
  if (!open) return;

  setHelpRequestError("");
  const emailInput = document.getElementById("helpRequestEmail");
  const subjectInput = document.getElementById("helpRequestSubject");
  const messageInput = document.getElementById("helpRequestMessage");
  const typeInput = document.getElementById("helpRequestType");
  if (emailInput && !emailInput.value) emailInput.value = authProfile?.email || "";
  if (subjectInput) subjectInput.value = "";
  if (messageInput) messageInput.value = "";
  if (typeInput) typeInput.value = "Help Request";
  setTimeout(() => subjectInput?.focus(), 0);
}

function submitHelpRequestEmail() {
  const type = String(document.getElementById("helpRequestType")?.value || "Help Request").trim();
  const contactEmail = String(document.getElementById("helpRequestEmail")?.value || "").trim();
  const subject = String(document.getElementById("helpRequestSubject")?.value || "").trim();
  const message = String(document.getElementById("helpRequestMessage")?.value || "").trim();

  if (!contactEmail || !subject || !message) {
    setHelpRequestError("Please enter your email, subject, and details.");
    return;
  }

  const fullSubject = `[Music Amplified] ${type}: ${subject}`;
  const body = [
    `Request Type: ${type}`,
    `From: ${getProfileDisplayName(authProfile)}`,
    `Contact Email: ${contactEmail}`,
    `Studio: ${activeStudioName || activeStudioId || "Unknown"}`,
    `User ID: ${authUserId || "Unknown"}`,
    "",
    message
  ].join("\n");

  window.location.href = `mailto:${helpRecipientEmail}?subject=${encodeURIComponent(fullSubject)}&body=${encodeURIComponent(body)}`;
  setHelpRequestModalOpen(false);
}

function detectRecoveryFromUrl() {
  try {
    const query = new URLSearchParams(window.location.search);
    if (query.get("type") === "recovery") return true;

    const hash = (window.location.hash || "").replace(/^#/, "");
    const hashParams = new URLSearchParams(hash);
    if (hashParams.get("type") === "recovery") return true;
  } catch (err) {
    console.debug("[Settings] unable to detect recovery params", err);
  }
  return false;
}

function setRecoveryMode(enabled) {
  if (isRecoveryMode === enabled) {
    return;
  }
  isRecoveryMode = enabled;
  const actionsCard = document.getElementById("actionsCard");
  const recoveryCard = document.getElementById("passwordRecoveryCard");
  if (actionsCard) actionsCard.style.display = enabled ? "none" : "";
  if (recoveryCard) recoveryCard.style.display = enabled ? "" : "none";

  const passwordBody = document.getElementById("passwordBody");
  if (passwordBody && enabled) passwordBody.style.display = "none";

  const recoveryFields = ["recoveryNewPassword", "recoveryConfirmPassword"];
  if (!enabled) {
    recoveryFields.forEach(id => {
      const field = document.getElementById(id);
      if (field) field.value = "";
    });
  }

  setRecoveryError("");
}

function setAccountEditing(editing) {
  if (isStudentLocked) return;
  isAccountEditing = editing;
  const fields = ["accountFirstName", "accountLastName", "accountEmail", "accountTeacherIds"];
  fields.forEach(id => {
    const el = document.getElementById(id);
    if (el) el.disabled = !editing;
  });

  const actions = document.getElementById("accountActions");
  if (actions) actions.style.display = editing ? "flex" : "none";

  const editBtn = document.getElementById("accountEditBtn");
  if (editBtn) editBtn.disabled = editing;
}

function applyStudentLockState(locked) {
  isStudentLocked = Boolean(locked);

  const accountEditBtn = document.getElementById("accountEditBtn");
  const passwordToggleBtn = document.getElementById("passwordToggleBtn");
  const accountHolderOnlyNote = document.getElementById("accountHolderOnlyNote");
  const passwordHolderOnlyNote = document.getElementById("passwordHolderOnlyNote");
  const accountActions = document.getElementById("accountActions");
  const passwordBody = document.getElementById("passwordBody");

  if (accountHolderOnlyNote) accountHolderOnlyNote.style.display = isStudentLocked ? "block" : "none";
  if (passwordHolderOnlyNote) passwordHolderOnlyNote.style.display = isStudentLocked ? "block" : "none";
  if (accountEditBtn) {
    accountEditBtn.disabled = isStudentLocked;
    accountEditBtn.title = isStudentLocked ? "Only the account holder can make changes." : "";
  }
  if (passwordToggleBtn) {
    passwordToggleBtn.disabled = isStudentLocked;
    passwordToggleBtn.title = isStudentLocked ? "Only the account holder can make changes." : "";
  }
  if (accountActions) accountActions.style.display = "none";
  if (passwordBody && isStudentLocked) passwordBody.style.display = "none";

  const accountFields = ["accountFirstName", "accountLastName", "accountEmail", "accountTeacherIds"];
  accountFields.forEach(id => {
    const field = document.getElementById(id);
    if (field) field.disabled = true;
  });

  const passwordFields = [
    "currentPassword",
    "newPassword",
    "confirmNewPassword",
    "passwordSaveBtn",
    "passwordCancelBtn"
  ];
  passwordFields.forEach(id => {
    const field = document.getElementById(id);
    if (field) field.disabled = isStudentLocked;
  });
}

function fillAccountFields(profile) {
  const firstName = document.getElementById("accountFirstName");
  const lastName = document.getElementById("accountLastName");
  const email = document.getElementById("accountEmail");
  if (firstName) firstName.value = profile?.firstName || "";
  if (lastName) lastName.value = profile?.lastName || "";
  if (email) email.value = profile?.email || "";
}

async function saveAccountInfo() {
  if (isStudentLocked) {
    showToast("Only the account holder can make changes.");
    return;
  }
  if (!authProfile) return;
  setTeacherError("");

  const updatedUser = {
    firstName: (document.getElementById("accountFirstName")?.value || "").trim(),
    lastName: (document.getElementById("accountLastName")?.value || "").trim(),
    email: (document.getElementById("accountEmail")?.value || "").trim() || authProfile.email
  };

  const roleList = parseRoles(authProfile.roles || authProfile.role);
  if (roleList.includes("student")) {
    const teacherSelect = document.getElementById("accountTeacherIds");
    const teacherIds = Array.from(teacherSelect?.selectedOptions || []).map(o => o.value);
    if (teacherOptions.length > 0 && teacherIds.length === 0) {
      setTeacherError("Please select your teacher.");
      return;
    }
    updatedUser.teacherIds = teacherIds;
  }

  const { error: dbError } = await supabase.from("users").update(updatedUser).eq("id", authProfile.id);
  if (dbError) {
    console.error("[Settings] failed to update account", dbError);
    showToast("Failed to update account.");
    return;
  }

  if (updatedUser.email && updatedUser.email !== authProfile.email) {
    const { error: emailErr } = await supabase.auth.updateUser({ email: updatedUser.email });
    if (emailErr) {
      console.error("[Settings] failed to update email", emailErr);
      showToast("Failed to update email.");
      return;
    }
    showToast("Check your new email to confirm the change.");
  } else {
    showToast("Account updated.");
  }

  authProfile = { ...authProfile, ...updatedUser };
  localStorage.setItem("loggedInUser", JSON.stringify(authProfile));
  setAccountEditing(false);
}

async function savePasswordChange() {
  if (isStudentLocked) {
    setPasswordError("Only the account holder can make changes.");
    return;
  }
  setPasswordError("");
  const currentPassword = (document.getElementById("currentPassword")?.value || "").trim();
  const newPassword = (document.getElementById("newPassword")?.value || "").trim();
  const confirmPassword = (document.getElementById("confirmNewPassword")?.value || "").trim();

  if (!currentPassword || !newPassword) {
    setPasswordError("Please fill in all password fields.");
    return;
  }
  if (newPassword !== confirmPassword) {
    setPasswordError("New passwords do not match.");
    return;
  }

  const { data: authData } = await supabase.auth.getUser();
  const email = authData?.user?.email || authProfile?.email;
  if (!email) {
    setPasswordError("Missing account email. Please log in again.");
    return;
  }

  const { error: reauthErr } = await supabase.auth.signInWithPassword({ email, password: currentPassword });
  if (reauthErr) {
    setPasswordError("Incorrect current password.");
    return;
  }

  const { error: passErr } = await supabase.auth.updateUser({ password: newPassword });
  if (passErr) {
    console.error("[Settings] password update failed", passErr);
    setPasswordError("Failed to update password.");
    return;
  }

  showToast("Password updated.");
  document.getElementById("currentPassword").value = "";
  document.getElementById("newPassword").value = "";
  document.getElementById("confirmNewPassword").value = "";
  document.getElementById("passwordBody").style.display = "none";
}

async function handleRecoverySubmit() {
  setRecoveryError("");
  const newPassword = (document.getElementById("recoveryNewPassword")?.value || "").trim();
  const confirmPassword = (document.getElementById("recoveryConfirmPassword")?.value || "").trim();

  if (!newPassword || !confirmPassword) {
    setRecoveryError("Please fill in both password fields.");
    return;
  }

  if (newPassword.length < 8) {
    setRecoveryError("Password must be at least 8 characters.");
    return;
  }

  if (newPassword !== confirmPassword) {
    setRecoveryError("New passwords do not match.");
    return;
  }

  const { error } = await supabase.auth.updateUser({ password: newPassword });
  if (error) {
    console.error("[Settings] recovery password update failed", error);
    setRecoveryError("Failed to update password.");
    return;
  }

  showToast("Password updated. You're all set.");
  setRecoveryMode(false);
  setTimeout(() => {
    window.location.href = "index.html";
  }, 1400);
}

function wirePasswordToggles() {
  document.querySelectorAll(".pw-toggle").forEach(btn => {
    btn.addEventListener("click", () => {
      const targetId = btn.getAttribute("data-target");
      const input = document.getElementById(targetId);
      if (!input) return;
      const showing = input.type === "text";
      input.type = showing ? "password" : "text";
      btn.textContent = showing ? "Show" : "Hide";
    });
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  const recoveryDetected = detectRecoveryFromUrl();
  if (recoveryDetected) {
    console.debug("[Settings] recovery mode detected via URL");
    setRecoveryMode(true);
  }

  supabase.auth.onAuthStateChange(event => {
    console.debug("[Settings] auth event:", event);
    if (event === "PASSWORD_RECOVERY") {
      setRecoveryMode(true);
    }
  });

  authUserId = await getAuthUserId();
  if (!authUserId) {
    window.location.replace("./login.html");
    return;
  }

  const viewerContext = await getViewerContext();
  activeStudioId = viewerContext?.studioId || localStorage.getItem("activeStudioId");
  await loadHelpRecipient(activeStudioId);
  const [holder, familyAccess, selfManagedStudent] = await Promise.all([
    isAccountHolder(activeStudioId),
    hasFamilyAccess(activeStudioId),
    isSelfManagedStudent(activeStudioId)
  ]);
  const accountIsParent = Boolean(viewerContext?.accountIsParent || viewerContext?.isParent);
  const studentRequiresHolder = Boolean(
    viewerContext?.isStudent
    && !holder
    && !familyAccess
    && !accountIsParent
    && !selfManagedStudent
  );
  applyStudentLockState(studentRequiresHolder);

  const replayTutorialRow = document.getElementById("replayTutorialRow");
  const replayTutorialBtn = document.getElementById("replayTutorialBtn");
  const replayTutorialLabel = document.getElementById("replayTutorialLabel");
  const replayTutorialCopy = document.getElementById("replayTutorialCopy");
  const prefersTeacherTutorial = Boolean(viewerContext?.isAdmin || viewerContext?.isTeacher);
  const canReplayStudentTutorial = Boolean((viewerContext?.isStudent || viewerContext?.mode === "parent") && !prefersTeacherTutorial);
  const canReplayTeacherTutorial = prefersTeacherTutorial;
  const canReplayTutorial = canReplayStudentTutorial || canReplayTeacherTutorial;
  if (replayTutorialRow) replayTutorialRow.style.display = canReplayTutorial ? "" : "none";
  if (replayTutorialBtn && canReplayTutorial) {
    if (replayTutorialLabel) {
      replayTutorialLabel.textContent = "Tutorial";
    }
    if (replayTutorialCopy) {
      replayTutorialCopy.textContent = "";
    }
    replayTutorialBtn.addEventListener("click", () => {
      if (canReplayTeacherTutorial) {
        requestTeacherAdminTutorialReplay();
        showToast("Opening tutorial...");
      } else {
        requestStudentTutorialReplay();
        showToast("Opening tutorial...");
      }
      window.setTimeout(() => {
        window.location.href = "index.html";
      }, 160);
    });
  }

  const ensured = await ensureUserRow();
  if (ensured && String(ensured.id) === String(authUserId)) {
    authProfile = ensured;
  }

  if (!authProfile) {
    const { data, error } = await supabase
      .from("users")
      .select("*")
      .eq("id", authUserId)
      .single();
    if (error || !data) {
      console.error("[Settings] failed to load auth profile", error);
      showToast("Failed to load account.");
      return;
    }
    authProfile = data;
  }

  fillAccountFields(authProfile);
  setAccountEditing(false);

  const roleList = parseRoles(authProfile.roles || authProfile.role);
  teacherOptions = await loadTeachersForStudio(activeStudioId);
  const teacherWrap = document.getElementById("accountTeacherWrap");
  const teacherSelect = document.getElementById("accountTeacherIds");
  if (roleList.includes("student")) {
    if (teacherWrap) teacherWrap.style.display = "";
    applyTeacherOptionsToSelect(teacherSelect, teacherOptions);
    const selectedIds = Array.isArray(authProfile.teacherIds) ? authProfile.teacherIds.map(String) : [authProfile.teacherIds].filter(Boolean).map(String);
    selectedIds.forEach(id => {
      const opt = Array.from(teacherSelect.options).find(o => o.value === id);
      if (opt) opt.selected = true;
    });
    refreshTeacherMultiSelect(teacherSelect);
    if (teacherSelect) teacherSelect.addEventListener("change", () => setTeacherError(""));
  } else if (teacherWrap) {
    teacherWrap.style.display = "none";
  }
  if (studentRequiresHolder) {
    applyStudentLockState(true);
  } else {
    setAccountEditing(false);
  }

  const editBtn = document.getElementById("accountEditBtn");
  const accountSaveBtn = document.getElementById("accountSaveBtn");
  const accountCancelBtn = document.getElementById("accountCancelBtn");

  if (editBtn) editBtn.addEventListener("click", () => setAccountEditing(true));
  if (accountSaveBtn) accountSaveBtn.addEventListener("click", saveAccountInfo);
  if (accountCancelBtn) {
    accountCancelBtn.addEventListener("click", () => {
      fillAccountFields(authProfile);
      if (teacherSelect && Array.isArray(authProfile.teacherIds)) {
        const selectedIds = authProfile.teacherIds.map(String);
        Array.from(teacherSelect.options).forEach(opt => {
          opt.selected = selectedIds.includes(opt.value);
        });
        refreshTeacherMultiSelect(teacherSelect);
      }
      setAccountEditing(false);
      setTeacherError("");
    });
  }

  const passwordToggleBtn = document.getElementById("passwordToggleBtn");
  const passwordBody = document.getElementById("passwordBody");
  if (passwordToggleBtn && passwordBody) {
    passwordToggleBtn.addEventListener("click", () => {
      if (isStudentLocked) return;
      const showing = passwordBody.style.display === "none";
      passwordBody.style.display = showing ? "block" : "none";
      passwordToggleBtn.textContent = showing ? "Hide Password" : "Change Password";
    });
  }

  const passwordSaveBtn = document.getElementById("passwordSaveBtn");
  const passwordCancelBtn = document.getElementById("passwordCancelBtn");
  if (passwordSaveBtn) passwordSaveBtn.addEventListener("click", savePasswordChange);
  if (passwordCancelBtn) {
    passwordCancelBtn.addEventListener("click", () => {
      document.getElementById("currentPassword").value = "";
      document.getElementById("newPassword").value = "";
      document.getElementById("confirmNewPassword").value = "";
      passwordBody.style.display = "none";
      setPasswordError("");
    });
  }

  const recoverySaveBtn = document.getElementById("recoveryPasswordSaveBtn");
  if (recoverySaveBtn) {
    recoverySaveBtn.addEventListener("click", handleRecoverySubmit);
  }

  const helpRequestBtn = document.getElementById("helpRequestBtn");
  const helpRequestCancel = document.getElementById("helpRequestCancel");
  const helpRequestSubmit = document.getElementById("helpRequestSubmit");
  const helpRequestModal = document.getElementById("helpRequestModal");
  if (helpRequestBtn) helpRequestBtn.addEventListener("click", () => setHelpRequestModalOpen(true));
  if (helpRequestCancel) helpRequestCancel.addEventListener("click", () => setHelpRequestModalOpen(false));
  if (helpRequestSubmit) helpRequestSubmit.addEventListener("click", submitHelpRequestEmail);
  if (helpRequestModal) {
    helpRequestModal.addEventListener("click", (event) => {
      if (event.target === helpRequestModal) setHelpRequestModalOpen(false);
    });
  }
  document.addEventListener("keydown", (event) => {
    if (event.key === "Escape") setHelpRequestModalOpen(false);
  });

  wirePasswordToggles();

  const logoutBtn = document.getElementById("logoutBtn");
  if (logoutBtn) {
    logoutBtn.addEventListener("click", async () => {
      await window.getSupabaseClient()?.auth.signOut();
      await clearAppSessionCache("logout");
      window.location.href = "login.html";
    });
  }
});
