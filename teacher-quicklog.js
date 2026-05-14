(() => {
  const card = document.getElementById("quickLogCard");
  const form = document.getElementById("quickLogForm");
  const studentSelect = document.getElementById("qlStudent");
  const categorySelect = document.getElementById("qlCategory");
  const dateInput = document.getElementById("qlDate");
  const pointsInput = document.getElementById("qlPoints");
  const notesInput = document.getElementById("qlNotes");
  const statusEl = document.getElementById("qlStatus");
  const submitBtn = document.getElementById("qlSubmit");
  let practiceNoteEl = null;
  let promptGrid = null;
  let teacherLogPrompts = [];
  let teacherPointCategories = [];
  let pointsManuallyEdited = false;
  let selectedPromptKey = "";
  let selectedPromptCategory = "";

  let studioId = null;
  let currentUserId = null;
  const supabase = window.supabase;

  function setStatus(message, isError = false) {
    if (!statusEl) return;
    statusEl.textContent = message || "";
    statusEl.style.color = isError ? "#c62828" : "#0b7a3a";
  }

  function parseRoles(profile) {
    const roleSet = new Set();
    if (profile && typeof profile.role === "string") {
      roleSet.add(profile.role.toLowerCase());
    }
    if (Array.isArray(profile?.roles)) {
      profile.roles.forEach(role => roleSet.add(String(role).toLowerCase()));
    } else if (typeof profile?.roles === "string") {
      profile.roles
        .split(",")
        .map(role => role.trim().toLowerCase())
        .filter(Boolean)
        .forEach(role => roleSet.add(role));
    }
    return Array.from(roleSet);
  }

  function isTeacherOrAdmin(profile) {
    const roles = parseRoles(profile);
    return roles.includes("teacher") || roles.includes("admin");
  }

  function isStudent(profile) {
    const roles = parseRoles(profile);
    return roles.includes("student");
  }

  function defaultDate() {
    if (!dateInput) return;
    dateInput.value = new Date().toISOString().split("T")[0];
  }

  function syncPracticePoints() {
    if (!pointsInput) return;
    const isPractice = String(selectedPromptCategory || categorySelect?.value || "").trim().toLowerCase() === "practice";
    pointsInput.disabled = false;
    if (isPractice) {
      if (!pointsManuallyEdited) pointsInput.value = "5";
      if (practiceNoteEl) practiceNoteEl.style.display = "block";
      return;
    }
    if (practiceNoteEl) practiceNoteEl.style.display = "none";
  }

  function setPromptActive(key) {
    selectedPromptKey = String(key || "");
    promptGrid?.querySelectorAll("[data-ql-prompt]").forEach(button => {
      const active = String(button.getAttribute("data-ql-prompt") || "") === selectedPromptKey;
      button.classList.toggle("is-active", active);
      button.setAttribute("aria-pressed", active ? "true" : "false");
    });
  }

  function applyPrompt(prompt) {
    if (!prompt || !categorySelect || !pointsInput) return;
    const category = String(prompt.category || "").trim().toLowerCase();
    selectedPromptCategory = category;
    categorySelect.value = category;
    pointsInput.disabled = false;
    if (Number.isFinite(Number(prompt.points))) pointsInput.value = String(prompt.points);
    if (notesInput && !String(notesInput.value || "").trim() && prompt.notesPrompt) notesInput.placeholder = prompt.notesPrompt;
    pointsManuallyEdited = false;
    setPromptActive(prompt.key);
    if (practiceNoteEl) practiceNoteEl.style.display = category === "practice" ? "block" : "none";
  }

  function ensurePromptGrid() {
    if (!card || promptGrid || !categorySelect) return;
    promptGrid = document.createElement("div");
    promptGrid.id = "qlPromptGrid";
    promptGrid.className = "staff-prompt-grid";
    promptGrid.innerHTML = teacherLogPrompts.map(prompt => `
      <button type="button" class="staff-prompt-button" data-ql-prompt="${prompt.key}" aria-pressed="false">
        <span class="staff-prompt-icon" aria-hidden="true">${prompt.icon || ""}</span>
        <span class="staff-prompt-label">${prompt.label}</span>
      </button>
    `).join("");
    categorySelect.closest(".ql-category-pop, label, div")?.insertAdjacentElement("beforebegin", promptGrid);
    promptGrid.querySelectorAll("[data-ql-prompt]").forEach(button => {
      button.addEventListener("click", () => {
        const key = String(button.getAttribute("data-ql-prompt") || "");
        applyPrompt(teacherLogPrompts.find(prompt => prompt.key === key));
      });
    });
  }

  async function loadCategories() {
    if (!categorySelect) return;
    categorySelect.innerHTML = '<option value="">Select category</option>';

    let query = supabase
      .from("categories")
      .select("id, name")
      .order("name", { ascending: true });
    if (studioId) {
      query = query.eq("studio_id", studioId);
    }

    const { data, error } = await query;
    if (error) {
      setStatus("Could not load categories.", true);
      console.error("Quick log categories error:", error);
      return;
    }

    teacherPointCategories.forEach(cat => {
      const opt = document.createElement("option");
      opt.value = cat.value;
      opt.textContent = cat.label;
      categorySelect.appendChild(opt);
    });
  }

  async function loadStudents() {
    if (!studentSelect) return;
    studentSelect.innerHTML = '<option value="">Select a student</option>';

    let query = supabase
      .from("users")
      .select("id, firstName, lastName, email, role, roles, studio_id");
    if (studioId) {
      query = query.eq("studio_id", studioId);
    }

    const { data, error } = await query;
    if (error) {
      setStatus("Could not load students.", true);
      console.error("Quick log students error:", error);
      return;
    }

    const students = (data || []).filter(isStudent);
    students.sort((a, b) => {
      const aName = `${a.lastName || ""} ${a.firstName || ""}`.trim().toLowerCase();
      const bName = `${b.lastName || ""} ${b.firstName || ""}`.trim().toLowerCase();
      return aName.localeCompare(bName);
    });

    students.forEach(student => {
      const opt = document.createElement("option");
      opt.value = student.id;
      const name = `${student.firstName || ""} ${student.lastName || ""}`.trim();
      opt.textContent = name || student.email || student.id;
      studentSelect.appendChild(opt);
    });
  }

  function resetQuickLogForm() {
    const form = document.getElementById("quickLogForm");
    if (!form) return;

    form.reset();

    const studentEl = document.getElementById("qlStudent");
    const categoryEl = document.getElementById("qlCategory");
    const pointsEl = document.getElementById("qlPoints");
    const notesEl = document.getElementById("qlNotes");
    const dateEl = document.getElementById("qlDate");

    if (studentEl) studentEl.value = "";
    if (categoryEl) categoryEl.value = "";
    selectedPromptCategory = "";
    if (pointsEl) pointsEl.value = "";
    if (notesEl) notesEl.value = "";
    if (notesEl) notesEl.placeholder = "";
    if (dateEl) dateEl.value = "";
    pointsManuallyEdited = false;
    setPromptActive("");

    document.querySelectorAll(".selected, .active").forEach(el =>
      el.classList.remove("selected", "active")
    );
    syncPracticePoints();
  }

  async function handleSubmit(event) {
    event.preventDefault();
    setStatus("");

    const studentId = studentSelect?.value || "";
    const category = selectedPromptCategory || categorySelect?.value || "";
    const date = dateInput?.value || "";
    const points = Number(pointsInput?.value);
    const notes = (notesInput?.value || "").trim();

    if (!studentId || !category || !date) {
      setStatus("Please complete all required fields.", true);
      return;
    }
    if (!Number.isInteger(points) || points < 1) {
      setStatus("Points must be 1 or more.", true);
      return;
    }

    if (submitBtn) submitBtn.disabled = true;

    const activeStudioId = localStorage.getItem("activeStudioId") || null;
    const resolvedStudioId = studioId || activeStudioId;
    if (!resolvedStudioId) {
      if (submitBtn) submitBtn.disabled = false;
      setStatus("No active studio selected.", true);
      return;
    }

    const payload = {
      userId: studentId,
      studio_id: resolvedStudioId,
      date,
      category,
      points,
      notes: notes || null,
      status: "approved",
      created_by: currentUserId
    };

    const { error } = await supabase.from("logs").insert([payload]);
    if (submitBtn) submitBtn.disabled = false;

    if (error) {
      console.error("Quick log insert error:", error);
      setStatus("Couldn't save log. Check console.", true);
      return;
    }

    setStatus("Logged. ✅");
    resetQuickLogForm();
    defaultDate();
  }

  async function init() {
    if (!card || !supabase?.auth) return;
    try {
      const promptsModule = await import("./log-prompts.js");
      teacherLogPrompts = promptsModule.getTeacherLogPrompts();
      teacherPointCategories = promptsModule.getTeacherPointCategories();
    } catch (error) {
      console.warn("Quick log prompts unavailable", error);
      teacherLogPrompts = [];
    }

    const { data: sessionData, error: sessionErr } = await supabase.auth.getSession();
    const session = sessionData?.session || null;
    if (sessionErr || !session) return;

    currentUserId = session.user.id;
    const activeStudioId = localStorage.getItem("activeStudioId") || null;

    let isStaff = false;
    if (activeStudioId) {
      const { data: studioMember } = await supabase
        .from("studio_members")
        .select("roles")
        .eq("user_id", currentUserId)
        .eq("studio_id", activeStudioId)
        .maybeSingle();
      const roles = Array.isArray(studioMember?.roles) ? studioMember.roles : [];
      isStaff = roles.includes("admin") || roles.includes("teacher");
      studioId = activeStudioId;
    }

    if (!isStaff) {
      const { data: profile } = await supabase
        .from("users")
        .select("id, role, roles, studio_id, studioId")
        .eq("id", currentUserId)
        .single();
      if (!profile || !isTeacherOrAdmin(profile)) return;
      studioId = activeStudioId || profile.studio_id || profile.studioId || null;
    }

    card.style.display = "block";
    if (pointsInput && !practiceNoteEl) {
      practiceNoteEl = document.createElement("p");
      practiceNoteEl.id = "qlPracticePointsNote";
      practiceNoteEl.className = "staff-msg";
      practiceNoteEl.style.display = "none";
      practiceNoteEl.textContent = "Practice category will automatically award 5 points per day.";
      pointsInput.insertAdjacentElement("afterend", practiceNoteEl);
    }
    defaultDate();
    await loadCategories();
    await loadStudents();
    ensurePromptGrid();
    categorySelect?.addEventListener("change", () => {
      selectedPromptCategory = "";
      setPromptActive("");
      syncPracticePoints();
    });
    pointsInput?.addEventListener("input", () => {
      pointsManuallyEdited = true;
    });
    syncPracticePoints();

    form?.addEventListener("submit", handleSubmit);
  }

  window.addEventListener("load", init);
})();
