import { supabase } from "./supabaseClient.js";
import { getAccessFlags, getViewerContext } from "./utils.js";

let allUsers = [];
let searchTerm = "";
let searchHandlerBound = false;
let tagPickerOutsideCloseBound = false;
let currentEditingRow = null;
let sortField = "firstName";
let sortDirection = "asc";
const TABS = ["students", "families", "teachers", "all"];
let activeTab = "students";
// 'students' | 'families' | 'teachers' | 'all'

let statusFilter = "active";
// 'active' | 'inactive' | 'all'
let liveSyncChannel = null;
let liveSyncStudioId = "";
let refreshQueuedTimer = null;

const EDITABLE_TEXT_FIELD_TYPES = {
  firstName: "text",
  lastName: "text",
  email: "email"
};

const VALID_TABS = new Set(TABS);
const TAB_COLUMN_KEYS = {
  students: ["firstName", "lastName", "email", "avatar", "teacherIds", "instrument", "points", "level", "active"],
  families: ["firstName", "lastName", "email", "avatar", "roles", "actions"],
  teachers: ["firstName", "lastName", "email", "avatar", "roles", "actions"],
  all: ["firstName", "lastName", "email", "avatar", "roles", "teacherIds", "instrument", "points", "level", "actions"]
};

const COLUMN_DEFS = {
  firstName: { label: "First Name", sort: "firstName" },
  lastName: { label: "Last Name", sort: "lastName" },
  email: { label: "Email", sort: "email" },
  avatar: { label: "Avatar" },
  roles: { label: "Roles", sort: "roles" },
  teacherIds: { label: "Teachers", sort: "teacherIds" },
  instrument: { label: "Instrument", sort: "instrument" },
  points: { label: "Points", sort: "points" },
  level: { label: "Level", sort: "level" },
  active: { label: "Active", sort: "active" },
  actions: { label: "", actions: true }
};

const ROLE_FALLBACKS = ["admin", "teacher"];
const INSTRUMENT_FALLBACKS = ["piano", "voice", "violin", "guitar", "drums"];

let roleOptions = [];
let teacherOptions = [];
let instrumentOptions = [];
let teacherDirectoryUsers = [];

const STATUS_LOADING = "Loading users...";

function normalizeTab(value) {
  const tab = String(value || "").trim().toLowerCase();
  return VALID_TABS.has(tab) ? tab : "students";
}

function normalizeStatusFilter(value) {
  const status = String(value || "").trim().toLowerCase();
  if (status === "active" || status === "inactive" || status === "all") return status;
  return "active";
}

function normalize(value) {
  return String(value ?? "").toLowerCase().trim();
}

function getTabColumns() {
  return TAB_COLUMN_KEYS[activeTab] || TAB_COLUMN_KEYS.students;
}

function ensureSortFieldForActiveTab() {
  const sortableFields = getTabColumns()
    .map(column => COLUMN_DEFS[column]?.sort)
    .filter(Boolean);
  if (!sortableFields.length) return;
  if (!sortableFields.includes(sortField)) {
    sortField = sortableFields[0];
    sortDirection = "asc";
  }
}

function renderTableHeader() {
  const table = document.getElementById("userHeaderTable");
  if (!table) return;

  const thead = table.querySelector("thead");
  if (!thead) return;
  thead.innerHTML = "";

  const tr = document.createElement("tr");
  getTabColumns().forEach(columnKey => {
    const column = COLUMN_DEFS[columnKey];
    if (!column) return;
    const th = document.createElement("th");
    th.textContent = column.label || "";
    if (column.sort) th.dataset.sort = column.sort;
    if (column.actions) {
      th.className = "actions-col";
      th.setAttribute("aria-label", "Actions");
    }
    tr.appendChild(th);
  });
  thead.appendChild(tr);
}

function syncSegmentedUI() {
  document.querySelectorAll("#manageTabs [data-tab]").forEach(button => {
    const tab = normalizeTab(button.dataset.tab);
    const isActive = tab === activeTab;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
  document.querySelectorAll("#statusTabs [data-status]").forEach(button => {
    const status = normalizeStatusFilter(button.dataset.status);
    const isActive = status === statusFilter;
    button.classList.toggle("is-active", isActive);
    button.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

function bindManageUsersTabs() {
  document.querySelectorAll("#manageTabs [data-tab]").forEach(button => {
    if (button.dataset.manageUsersTabBound === "true") return;
    button.dataset.manageUsersTabBound = "true";
    button.addEventListener("click", async () => {
      const nextTab = normalizeTab(button.dataset.tab);
      if (nextTab === activeTab) return;
      activeTab = nextTab;
      statusFilter = "active";
      await refreshManageUsers();
    });
  });
}

function bindStatusToggle() {
  document.querySelectorAll("#statusTabs [data-status]").forEach(button => {
    if (button.dataset.statusBound === "true") return;
    button.dataset.statusBound = "true";
    button.addEventListener("click", async () => {
      const nextStatus = normalizeStatusFilter(button.dataset.status);
      if (nextStatus === statusFilter) return;
      statusFilter = nextStatus;
      await refreshManageUsers();
    });
  });
}

function setLoadedCount(value) {
  renderStatus(value);
}

function renderStatus(message, isError = false) {
  const statusEl = document.getElementById("manageUsersStatus");
  if (!statusEl) return;
  statusEl.textContent = message;
  statusEl.style.color = isError ? "#c62828" : "#0b4f8a";
}

function ensureArray(value) {
  if (!value) return [];
  return Array.isArray(value) ? value : [value];
}

function normalizeArray(value) {
  return ensureArray(value)
    .map(item => (item === undefined || item === null ? "" : String(item).trim()))
    .filter(Boolean);
}

function arraysEqual(a, b) {
  const normA = normalizeArray(a).sort();
  const normB = normalizeArray(b).sort();
  if (normA.length !== normB.length) return false;
  for (let i = 0; i < normA.length; i++) {
    if (normA[i] !== normB[i]) return false;
  }
  return true;
}

function getTeacherLabelById(id) {
  const normalizedId = String(id || "").trim();
  const match = teacherOptions.find(opt => String(opt.value).trim() === normalizedId);
  if (match?.label) return match.label;

  const directoryMatch = teacherDirectoryUsers.find(user => String(user?.id).trim() === normalizedId);
  if (directoryMatch) {
    const fullName = `${directoryMatch.firstName || ""} ${directoryMatch.lastName || ""}`.trim();
    if (fullName) return fullName;
    if (directoryMatch.email) return String(directoryMatch.email);
  }

  const userMatch = allUsers.find(user => String(user?.id).trim() === normalizedId);
  if (userMatch) {
    const fullName = `${userMatch.firstName || ""} ${userMatch.lastName || ""}`.trim();
    if (fullName) return fullName;
    if (userMatch.email) return String(userMatch.email);
  }

  return String(id || "");
}

function formatList(value) {
  if (!value) return "-";
  const list = Array.isArray(value) ? value : [value];
  const normalized = list
    .map(item => (item === undefined || item === null ? "" : String(item).trim()))
    .filter(Boolean);
  return normalized.length ? normalized.join(", ") : "-";
}

function getUserFieldValue(user, field) {
  switch (field) {
    case "firstName":
      return user.firstName || "";
    case "lastName":
      return user.lastName || "";
    case "email":
      return user.email || "";
    case "roles":
      return ensureArray(user.membership_roles);
    case "teacherIds":
      return ensureArray(user.teacherIds);
    case "instrument":
      return ensureArray(user.instrument);
    default:
      return user[field] || "";
  }
}

function getDisplayValue(user, field) {
  if (field === "roles") {
    const roles = ensureArray(getUserFieldValue(user, "roles"))
      .map(role => String(role || "").toLowerCase())
      .map(role => (role === "parent" || role === "guardian" ? "parent/guardian" : role));
    return formatList(roles);
  }
  if (field === "teacherIds") {
    const teacherNames = ensureArray(getUserFieldValue(user, "teacherIds")).map(getTeacherLabelById);
    return formatList(teacherNames);
  }
  if (field === "instrument") return formatList(getUserFieldValue(user, "instrument"));
  if (field === "points" || field === "level") return user[field] ?? "-";
  if (field === "active") return user.deactivated_at ? "Inactive" : "Active";
  return getUserFieldValue(user, field) || "-";
}

function setInputValueFromUser(input, user) {
  const field = input.dataset.field;
  if (!field) return;
  if (input instanceof HTMLElement && input.classList.contains("tag-picker")) {
    const selected = normalizeArray(getUserFieldValue(user, field));
    input.dataset.selected = JSON.stringify(selected);
    input.dataset.open = "false";
    renderTagPicker(input);
    return;
  }
  if (input instanceof HTMLSelectElement && input.multiple) {
    const selected = normalizeArray(getUserFieldValue(user, field));
    Array.from(input.options).forEach(opt => {
      opt.selected = selected.includes(String(opt.value));
    });
    return;
  }
  input.value = String(getUserFieldValue(user, field) || "");
}

function buildOptionLists(users) {
  const roleSet = new Set();
  const teacherMap = new Map();
  const instrumentSet = new Set();
  const assignedTeacherIds = new Set();

  users.forEach(user => {
    ensureArray(user.membership_roles).forEach(role => {
      const raw = String(role || "").trim().toLowerCase();
      const value = raw === "guardian" || raw === "parent/guardian" ? "parent" : raw;
      if (value) roleSet.add(value);
    });
    ensureArray(user.teacherIds).forEach(teacherId => {
      const normalizedId = String(teacherId || "").trim();
      if (normalizedId) assignedTeacherIds.add(normalizedId);
    });
    ensureArray(user.instrument).forEach(inst => {
      const value = String(inst || "").trim();
      if (value) instrumentSet.add(value);
    });
  });

  ROLE_FALLBACKS.forEach(role => roleSet.add(role));
  INSTRUMENT_FALLBACKS.forEach(inst => instrumentSet.add(inst));

  const teacherSourceUsers = teacherDirectoryUsers.length ? teacherDirectoryUsers : users;
  teacherSourceUsers.forEach(user => {
    if (!user.id) return;
    const identityRoles = ensureArray(user.identity_roles).map(role => String(role || "").toLowerCase());
    const membershipRoles = ensureArray(user.membership_roles).map(role => String(role || "").toLowerCase());
    const directRoles = ensureArray(user.roles).map(role => String(role || "").toLowerCase());
    const isStaff = identityRoles.includes("teacher")
      || identityRoles.includes("admin")
      || membershipRoles.includes("teacher")
      || membershipRoles.includes("admin")
      || directRoles.includes("teacher")
      || directRoles.includes("admin");
    const isAssignedTeacher = assignedTeacherIds.has(String(user.id));
    if (!isStaff && !isAssignedTeacher) return;
    const label = `${user.firstName || ""} ${user.lastName || ""}`.trim() || user.email || "Teacher";
    teacherMap.set(String(user.id), { value: String(user.id), label });
  });

  roleOptions = Array.from(roleSet)
    .filter(role => role !== "guardian")
    .map(role => ({
      value: role,
      label: role === "parent" ? "parent/guardian" : role
    }))
    .sort((a, b) => a.label.localeCompare(b.label));

  teacherOptions = Array.from(teacherMap.values()).sort((a, b) => a.label.localeCompare(b.label));

  instrumentOptions = Array.from(instrumentSet)
    .map(instrument => ({ value: instrument, label: instrument }))
    .sort((a, b) => a.label.localeCompare(b.label));
}

async function loadTeacherDirectory(studioId) {
  try {
    const { data, error } = await supabase
      .from("users")
      .select('id, "firstName", "lastName", email, roles')
      .eq("studio_id", studioId);
    if (error) throw error;
    teacherDirectoryUsers = (data ?? []).map(row => ({
      id: row?.id || "",
      firstName: row?.firstName || "",
      lastName: row?.lastName || "",
      email: row?.email || "",
      roles: ensureArray(row?.roles),
      identity_roles: [],
      membership_roles: []
    }));
  } catch (error) {
    teacherDirectoryUsers = [];
    console.warn("[ManageUsers] teacher directory lookup failed", error);
  }
}

function matchesSearch(row) {
  const q = normalize(searchTerm);
  if (!q) return true;
  return (
    normalize(row.firstName).includes(q) ||
    normalize(row.lastName).includes(q) ||
    normalize(row.email).includes(q)
  );
}

function matchesStatus(row) {
  if (statusFilter === "all") return true;
  if (statusFilter === "active") return row.active === true;
  if (statusFilter === "inactive") return row.active === false;
  return true;
}

function matchesTab(row) {
  const roles = row.roles || [];
  if (activeTab === "families") return roles.includes("parent/guardian");
  if (activeTab === "teachers") return roles.includes("teacher") || roles.includes("admin");
  if (activeTab === "all") return true;
  if (activeTab === "students") return roles.includes("student");
  return true;
}

function applyFilters(rows) {
  return (rows ?? []).filter(r => matchesTab(r) && matchesStatus(r) && matchesSearch(r));
}

function isNumericSortField(field) {
  return field === "points" || field === "level";
}

function getSortValue(user, field) {
  if (!user || !field) return "";
  if (field === "roles") return getDisplayValue(user, "roles").toLowerCase();
  if (field === "teacherIds") return getDisplayValue(user, "teacherIds").toLowerCase();
  if (field === "instrument") return getDisplayValue(user, "instrument").toLowerCase();
  if (isNumericSortField(field)) {
    const numeric = Number(user[field]);
    return Number.isFinite(numeric) ? numeric : null;
  }
  return String(user[field] ?? "").toLowerCase();
}

function compareUsers(a, b, field) {
  const left = getSortValue(a, field);
  const right = getSortValue(b, field);

  if (isNumericSortField(field)) {
    if (left === null && right === null) return 0;
    if (left === null) return 1;
    if (right === null) return -1;
    return left - right;
  }

  const cmp = String(left).localeCompare(String(right), undefined, { sensitivity: "base" });
  if (cmp !== 0) return cmp;
  return String(a?.id ?? "").localeCompare(String(b?.id ?? ""), undefined, { sensitivity: "base" });
}

function getSortedUsers(users) {
  if (!Array.isArray(users)) return [];
  const sorted = [...users].sort((a, b) => compareUsers(a, b, sortField));
  if (sortDirection === "desc") sorted.reverse();
  return sorted;
}

function updateSortHeaderState() {
  const headers = document.querySelectorAll("#userHeaderTable th[data-sort]");
  headers.forEach(header => {
    const field = header.dataset.sort;
    const isActive = field === sortField;
    header.style.cursor = "pointer";
    header.tabIndex = 0;
    header.setAttribute("aria-sort", isActive ? (sortDirection === "asc" ? "ascending" : "descending") : "none");
    header.title = isActive
      ? `Sorted ${sortDirection}. Click to sort ${sortDirection === "asc" ? "descending" : "ascending"}.`
      : "Click to sort.";
  });
}

function handleSortHeaderInteraction(field) {
  if (!field) return;
  if (sortField === field) {
    sortDirection = sortDirection === "asc" ? "desc" : "asc";
  } else {
    sortField = field;
    sortDirection = "asc";
  }
  updateSortHeaderState();
  renderUsers();
}

function bindSortHeaders() {
  const headers = document.querySelectorAll("#userHeaderTable th[data-sort]");
  headers.forEach(header => {
    if (header.dataset.sortBound === "true") return;
    const field = header.dataset.sort;
    header.dataset.sortBound = "true";
    header.addEventListener("click", () => handleSortHeaderInteraction(field));
    header.addEventListener("keydown", event => {
      if (event.key !== "Enter" && event.key !== " ") return;
      event.preventDefault();
      handleSortHeaderInteraction(field);
    });
  });
  updateSortHeaderState();
}

function createCell(value) {
  const td = document.createElement("td");
  td.textContent = value || "-";
  return td;
}

function createEditableTextCell(user, field, type = "text") {
  const td = document.createElement("td");

  const span = document.createElement("span");
  span.className = "cell-text";
  span.dataset.field = field;
  span.textContent = getDisplayValue(user, field);

  const input = document.createElement("input");
  input.className = "cell-input";
  input.dataset.field = field;
  input.type = type;
  input.hidden = true;
  setInputValueFromUser(input, user);

  if (field !== "firstName") {
    td.appendChild(span);
    td.appendChild(input);
    return td;
  }

  const wrapper = document.createElement("div");
  wrapper.className = "first-name-wrapper";

  const pencilBtn = document.createElement("button");
  pencilBtn.type = "button";
  pencilBtn.className = "edit-pencil";
  pencilBtn.setAttribute("aria-label", "Edit user");
  pencilBtn.innerHTML = `
    <svg viewBox="0 0 24 24" aria-hidden="true" focusable="false">
      <path d="M5 20h14" />
      <path d="M17.4 4.6a2 2 0 0 1 2.8 2.8l-9.7 9.7H7v-4.5z" />
    </svg>
  `;
  pencilBtn.addEventListener("click", event => {
    event.preventDefault();
    const row = td.closest("tr");
    if (row) enterEditMode(row);
  });

  const actions = document.createElement("div");
  actions.className = "inline-edit-actions";

  const saveBtn = document.createElement("button");
  saveBtn.type = "button";
  saveBtn.className = "mini-edit-btn save-btn row-save-btn";
  saveBtn.textContent = "Save";
  saveBtn.hidden = true;
  saveBtn.addEventListener("click", () => {
    const row = td.closest("tr");
    if (row) saveRow(row);
  });

  const cancelBtn = document.createElement("button");
  cancelBtn.type = "button";
  cancelBtn.className = "mini-edit-btn cancel-btn row-cancel-btn";
  cancelBtn.textContent = "Cancel";
  cancelBtn.hidden = true;
  cancelBtn.addEventListener("click", () => {
    const row = td.closest("tr");
    if (row) cancelEditMode(row);
  });

  actions.appendChild(saveBtn);
  actions.appendChild(cancelBtn);
  wrapper.appendChild(pencilBtn);
  wrapper.appendChild(actions);
  wrapper.appendChild(span);
  wrapper.appendChild(input);
  td.appendChild(wrapper);
  return td;
}

function createMultiSelectCell(user, field, options, cssClass) {
  const td = document.createElement("td");

  const span = document.createElement("span");
  span.className = "cell-text";
  span.dataset.field = field;
  span.textContent = getDisplayValue(user, field);

  const picker = document.createElement("div");
  picker.className = `cell-input tag-picker ${cssClass || ""}`.trim();
  picker.dataset.field = field;
  picker.dataset.valueType = "array";
  picker.dataset.selected = "[]";
  picker.dataset.open = "false";
  picker.dataset.allowCustom = field === "instrument" ? "true" : "false";
  picker.hidden = true;

  picker._options = options.map(option => ({ value: String(option.value), label: String(option.label) }));

  const tags = document.createElement("div");
  tags.className = "tag-picker-tags";

  const list = document.createElement("div");
  list.className = "tag-picker-list";

  picker.appendChild(tags);
  picker.appendChild(list);
  setInputValueFromUser(picker, user);

  picker.addEventListener("click", event => {
    event.stopPropagation();

    const target = event.target instanceof Element
      ? event.target
      : event.target?.parentElement;
    if (!target) return;

    if (target.closest("input.tag-picker-custom-input")) return;

    const addBtn = event.target.closest("button[data-action='add-option']");
    if (addBtn) {
      const selected = getTagPickerSelected(picker);
      const value = String(addBtn.dataset.value || "");
      if (!selected.includes(value)) selected.push(value);
      picker.dataset.selected = JSON.stringify(selected);
      picker.dataset.open = "false";
      renderTagPicker(picker);
      return;
    }

    const removeBtn = event.target.closest("button[data-action='remove-tag']");
    if (removeBtn) {
      const selected = getTagPickerSelected(picker).filter(value => value !== String(removeBtn.dataset.value || ""));
      picker.dataset.selected = JSON.stringify(selected);
      picker.dataset.open = "false";
      renderTagPicker(picker);
      return;
    }

    const addCustomBtn = event.target.closest("button[data-action='add-custom']");
    if (addCustomBtn) {
      const input = picker.querySelector(".tag-picker-custom-input");
      const raw = String(input?.value || "").trim();
      if (!raw) return;

      const optionsList = Array.isArray(picker._options) ? picker._options : [];
      const existing = optionsList.find(option => String(option.label).toLowerCase() === raw.toLowerCase());
      const value = existing ? String(existing.value) : raw;
      if (!existing) {
        optionsList.push({ value, label: raw });
        picker._options = optionsList.sort((a, b) => String(a.label).localeCompare(String(b.label)));
      }

      const selected = getTagPickerSelected(picker);
      if (!selected.includes(value)) selected.push(value);
      picker.dataset.selected = JSON.stringify(selected);
      if (input) input.value = "";
      picker.dataset.open = "false";
      renderTagPicker(picker);
      return;
    }

    const nextOpen = picker.dataset.open !== "true";
    if (nextOpen) closeAllTagPickers(picker);
    picker.dataset.open = nextOpen ? "true" : "false";
    renderTagPicker(picker);
  });

  td.appendChild(span);
  td.appendChild(picker);
  return td;
}

function getTagPickerSelected(picker) {
  try {
    const parsed = JSON.parse(picker.dataset.selected || "[]");
    return Array.isArray(parsed) ? parsed.map(item => String(item)) : [];
  } catch {
    return [];
  }
}

function getTagLabel(picker, value) {
  const options = Array.isArray(picker._options) ? picker._options : [];
  const match = options.find(option => String(option.value) === String(value));
  return match?.label || String(value);
}

function closeAllTagPickers(exceptPicker = null) {
  document.querySelectorAll(".tag-picker[data-open='true']").forEach(picker => {
    if (picker === exceptPicker) return;
    picker.dataset.open = "false";
    renderTagPicker(picker);
  });
}

function renderTagPicker(picker) {
  if (!picker) return;
  const selected = getTagPickerSelected(picker);
  const tagsEl = picker.querySelector(".tag-picker-tags");
  const listEl = picker.querySelector(".tag-picker-list");
  if (!tagsEl || !listEl) return;

  tagsEl.innerHTML = "";
  const isOpen = picker.dataset.open === "true";
  if (!isOpen) {
    const summary = document.createElement("span");
    summary.className = "tag-picker-empty";
    if (!selected.length) {
      summary.textContent = "Select...";
    } else if (selected.length === 1) {
      summary.textContent = getTagLabel(picker, selected[0]);
    } else {
      summary.textContent = `${selected.length} selected`;
    }
    tagsEl.appendChild(summary);
  } else if (!selected.length) {
    const emptyTag = document.createElement("span");
    emptyTag.className = "tag-picker-empty";
    emptyTag.textContent = "No selections yet";
    tagsEl.appendChild(emptyTag);
  } else {
    selected.forEach(value => {
      const tag = document.createElement("span");
      tag.className = "tag-pill";
      tag.textContent = getTagLabel(picker, value);
      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.dataset.action = "remove-tag";
      removeBtn.dataset.value = value;
      removeBtn.setAttribute("aria-label", `Remove ${getTagLabel(picker, value)}`);
      removeBtn.textContent = "x";
      tag.appendChild(removeBtn);
      tagsEl.appendChild(tag);
    });
  }

  const available = (picker._options || []).filter(option => !selected.includes(String(option.value)));
  listEl.innerHTML = "";
  if (picker.dataset.allowCustom === "true") {
    const customWrap = document.createElement("div");
    customWrap.className = "tag-picker-custom";

    const customInput = document.createElement("input");
    customInput.type = "text";
    customInput.placeholder = "Add new instrument";
    customInput.className = "tag-picker-custom-input";
    customInput.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        const addBtn = customWrap.querySelector("button[data-action='add-custom']");
        if (addBtn) addBtn.click();
      }
    });

    const addCustomBtn = document.createElement("button");
    addCustomBtn.type = "button";
    addCustomBtn.className = "tag-picker-custom-btn";
    addCustomBtn.dataset.action = "add-custom";
    addCustomBtn.textContent = "Add";

    customWrap.appendChild(customInput);
    customWrap.appendChild(addCustomBtn);
    listEl.appendChild(customWrap);
  }
  if (!available.length) {
    const empty = document.createElement("div");
    empty.className = "tag-picker-empty";
    empty.textContent = "All options selected";
    listEl.appendChild(empty);
  } else {
    available.forEach(option => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "tag-picker-option";
      button.dataset.action = "add-option";
      button.dataset.value = String(option.value);
      button.textContent = option.label;
      listEl.appendChild(button);
    });
  }

  listEl.hidden = !isOpen;
  listEl.style.display = isOpen ? "flex" : "none";
}

function createAvatarCell(user) {
  const td = document.createElement("td");

  const img = document.createElement("img");
  img.className = "avatar-sm avatar-preview";
  img.src = user.avatarUrl || user.avatar || "images/icons/default.png";
  img.alt = `${user.firstName || "user"} avatar`;
  img.title = "Click to upload avatar";

  const input = document.createElement("input");
  input.type = "file";
  input.accept = "image/*";
  input.className = "avatar-upload-input";
  input.dataset.field = "avatarUrl";
  input.hidden = true;

  const triggerUpload = () => input.click();
  img.addEventListener("click", triggerUpload);
  img.addEventListener("keydown", event => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      triggerUpload();
    }
  });
  img.setAttribute("tabindex", "0");

  input.addEventListener("change", async () => {
    const file = input.files?.[0];
    if (!file) return;
    await uploadAvatarForUser(user, file, img);
    input.value = "";
  });

  td.appendChild(img);
  td.appendChild(input);
  return td;
}

function updateActiveButton(button, isActive) {
  if (!button) return;
  button.textContent = isActive ? "Active" : "Inactive";
  button.classList.toggle("save-btn", isActive);
  button.classList.toggle("cancel-btn", !isActive);
}

function createActiveCell(user) {
  const td = document.createElement("td");
  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "mini-edit-btn";
  updateActiveButton(toggle, !user.deactivated_at);
  toggle.addEventListener("click", async () => {
    await toggleUserActive(user, toggle);
  });
  td.appendChild(toggle);
  return td;
}

function createActionsCell(user) {
  const td = document.createElement("td");
  td.className = "actions-cell";

  const toggle = document.createElement("button");
  toggle.type = "button";
  toggle.className = "mini-edit-btn";
  updateActiveButton(toggle, !user.deactivated_at);
  toggle.addEventListener("click", async () => {
    await toggleUserActive(user, toggle);
  });

  td.appendChild(toggle);
  return td;
}

function createCellForColumn(columnKey, user) {
  if (columnKey === "firstName" || columnKey === "lastName" || columnKey === "email") {
    return createEditableTextCell(user, columnKey, EDITABLE_TEXT_FIELD_TYPES[columnKey] || "text");
  }
  if (columnKey === "avatar") return createAvatarCell(user);
  if (columnKey === "roles") return createMultiSelectCell(user, "roles", roleOptions, "role-select");
  if (columnKey === "teacherIds") return createMultiSelectCell(user, "teacherIds", teacherOptions, "teacher-select");
  if (columnKey === "instrument") return createMultiSelectCell(user, "instrument", instrumentOptions, "instrument-select");
  if (columnKey === "points" || columnKey === "level") return createCell(getDisplayValue(user, columnKey));
  if (columnKey === "active") return createActiveCell(user);
  if (columnKey === "actions") return createActionsCell(user);
  return createCell("-");
}

function setGlobalEditingState(isEditing) {
  const table = document.getElementById("userHeaderTable");
  if (!table) return;
  table.classList.toggle("has-active-edit", Boolean(isEditing));
}

function toggleActionButtons(row, editing) {
  const saveBtn = row.querySelector(".row-save-btn");
  const cancelBtn = row.querySelector(".row-cancel-btn");
  const pencilBtn = row.querySelector(".edit-pencil");
  if (saveBtn) saveBtn.hidden = !editing;
  if (cancelBtn) cancelBtn.hidden = !editing;
  if (pencilBtn) pencilBtn.hidden = editing;
}

function enterEditMode(row) {
  if (!row) return;
  if (currentEditingRow && currentEditingRow !== row) {
    cancelEditMode(currentEditingRow);
  }
  const user = allUsers.find(entry => String(entry.id) === String(row.dataset.userId));
  if (!user) return;

  currentEditingRow = row;
  row.classList.add("is-editing");
  row.querySelectorAll(".cell-input").forEach(input => {
    setInputValueFromUser(input, user);
    if (input.dataset.field !== "avatarUrl") input.hidden = false;
  });
  row.querySelectorAll(".cell-text").forEach(span => (span.hidden = true));
  toggleActionButtons(row, true);
  setGlobalEditingState(true);
}

function cancelEditMode(row) {
  if (!row) return;
  const user = allUsers.find(entry => String(entry.id) === String(row.dataset.userId));
  if (!user) return;

  row.classList.remove("is-editing");
  row.querySelectorAll(".cell-input").forEach(input => {
    setInputValueFromUser(input, user);
    input.hidden = true;
  });
  row.querySelectorAll(".cell-text").forEach(span => {
    span.textContent = getDisplayValue(user, span.dataset.field);
    span.hidden = false;
  });
  toggleActionButtons(row, false);
  if (currentEditingRow === row) currentEditingRow = null;
  setGlobalEditingState(false);
}

function getInputValue(input) {
  if (input instanceof HTMLElement && input.classList.contains("tag-picker")) {
    return getTagPickerSelected(input);
  }
  if (input instanceof HTMLSelectElement && input.multiple) {
    return Array.from(input.selectedOptions).map(option => option.value);
  }
  return String(input.value || "").trim();
}

async function saveRow(row) {
  if (!row) return;
  const userId = row.dataset.userId;
  const user = allUsers.find(entry => String(entry.id) === String(userId));
  if (!user) return;

  const userUpdates = {};
  let roleUpdates = null;

  row.querySelectorAll(".cell-input").forEach(input => {
    const field = input.dataset.field;
    if (!field || field === "avatarUrl") return;

    const newValue = getInputValue(input);
    const oldValue = getUserFieldValue(user, field);

    if (Array.isArray(newValue) || Array.isArray(oldValue)) {
      if (arraysEqual(newValue, oldValue)) return;
      if (field === "roles") {
        roleUpdates = normalizeArray(newValue)
          .map(role => String(role || "").toLowerCase())
          .map(role => (role === "guardian" || role === "parent/guardian" ? "parent" : role))
          .filter((role, index, list) => role && list.indexOf(role) === index);
      } else {
        userUpdates[field] = normalizeArray(newValue);
      }
      return;
    }

    if (String(newValue) === String(oldValue || "")) return;
    userUpdates[field] = newValue;
  });

  if (!Object.keys(userUpdates).length && !roleUpdates) {
    renderStatus("No changes to save.");
    cancelEditMode(row);
    return;
  }

  renderStatus("Saving...");

  if (Object.keys(userUpdates).length) {
    const { error } = await supabase.from("users").update(userUpdates).eq("id", userId);
    if (error) {
      renderStatus("Save failed: " + error.message, true);
      return;
    }
  }

  if (roleUpdates) {
    const { error: memberErr } = await supabase
      .from("studio_members")
      .update({ roles: roleUpdates })
      .eq("studio_id", user.studio_id)
      .eq("user_id", userId);
    if (memberErr) {
      renderStatus("Save failed: " + memberErr.message, true);
      return;
    }
    userUpdates.membership_roles = roleUpdates;
  }

  Object.assign(user, userUpdates);
  buildOptionLists(allUsers);
  renderUsers();
  renderStatus("Saved.");
}

async function toggleUserActive(user, button) {
  if (!user?.id) return;
  const currentlyActive = !user.deactivated_at;
  const nextDeactivatedAt = currentlyActive ? new Date().toISOString() : null;
  const nextActiveFlag = !currentlyActive;

  if (button) button.disabled = true;
  const { data, error } = await supabase.rpc("set_manage_user_active", {
    p_studio_id: user.studio_id,
    p_user_id: user.id,
    p_active: nextActiveFlag
  });
  if (error) {
    renderStatus("Failed to update active status: " + error.message, true);
    if (button) button.disabled = false;
    return;
  }

  const updated = Array.isArray(data) ? data[0] : data;
  user.deactivated_at = updated?.deactivated_at ?? nextDeactivatedAt;
  user.active = typeof updated?.active === "boolean" ? updated.active : nextActiveFlag;

  updateActiveButton(button, user.active);
  if (button) button.disabled = false;
  renderStatus(user.active ? "User activated." : "User deactivated.");
  renderUsers();
}

async function uploadAvatarForUser(user, file, imgEl) {
  if (!user?.id || !file) return;
  renderStatus("Uploading avatar...");

  try {
    const bucket = "avatars";
    const filePath = `${user.id}/avatar.png`;

    const { error: uploadErr } = await supabase
      .storage
      .from(bucket)
      .upload(filePath, file, { upsert: true, contentType: file.type });
    if (uploadErr) throw uploadErr;

    const { data } = supabase.storage.from(bucket).getPublicUrl(filePath);
    const publicUrl = data?.publicUrl;
    if (!publicUrl) throw new Error("Unable to get avatar URL");

    const { error: dbErr } = await supabase
      .from("users")
      .update({ avatarUrl: publicUrl })
      .eq("id", user.id);
    if (dbErr) throw dbErr;

    user.avatarUrl = publicUrl;
    if (imgEl) imgEl.src = `${publicUrl}?v=${Date.now()}`;
    renderStatus("Avatar updated.");
  } catch (error) {
    renderStatus("Avatar upload failed: " + (error.message || "Unknown error"), true);
  }
}

function renderUsers(totalRows = null) {
  const tbody = document.getElementById("userTableBody");
  if (!tbody) return;

  currentEditingRow = null;
  setGlobalEditingState(false);
  tbody.innerHTML = "";
  const visibleColumns = getTabColumns();

  const filteredUsers = applyFilters(allUsers);
  const filtered = getSortedUsers(filteredUsers);
  const total = Number.isFinite(totalRows) ? totalRows : allUsers.length;
  if (!filtered.length) {
    const tr = document.createElement("tr");
    const td = document.createElement("td");
    td.colSpan = visibleColumns.length || 1;
    td.textContent = "No users found.";
    td.style.textAlign = "center";
    tr.appendChild(td);
    tbody.appendChild(tr);
    setLoadedCount(`0 shown • ${total} total`);
    return;
  }

  filtered.forEach(user => {
    const tr = document.createElement("tr");
    tr.dataset.userId = String(user.id || "");
    tr.classList.toggle("is-inactive", Boolean(user.deactivated_at));

    visibleColumns.forEach(columnKey => {
      tr.appendChild(createCellForColumn(columnKey, user));
    });

    tbody.appendChild(tr);
  });

  setLoadedCount(`${filtered.length} shown • ${total} total`);
}

async function resolveStudioId() {
  const access = await getAccessFlags();
  if (access?.studio_id) return access.studio_id;
  const viewerContext = await getViewerContext();
  if (viewerContext?.studioId) return viewerContext.studioId;
  const stored = localStorage.getItem("activeStudioId");
  if (stored) return stored;
  throw new Error("Studio ID not found.");
}

async function fetchManageRows(studioId) {
  const { data, error } = await supabase.rpc("get_manage_users_for_studio", {
    p_studio_id: studioId
  });
  if (error) throw error;
  return data ?? [];
}

function buildUserRow(row) {
  const normalizedMembershipRoles = ensureArray(row?.membership_roles)
    .map(role => String(role || "").toLowerCase())
    .map(role => (role === "guardian" || role === "parent" ? "parent/guardian" : role))
    .filter((role, index, list) => role && list.indexOf(role) === index);
  const normalizedIdentityRoles = ensureArray(row?.identity_roles)
    .map(role => String(role || "").toLowerCase())
    .map(role => (role === "guardian" || role === "parent" ? "parent/guardian" : role))
    .filter((role, index, list) => role && list.indexOf(role) === index);
  const normalizedDirectRoles = ensureArray(row?.roles)
    .map(role => String(role || "").toLowerCase())
    .map(role => (role === "guardian" || role === "parent" ? "parent/guardian" : role))
    .filter((role, index, list) => role && list.indexOf(role) === index);
  const roles = normalizeArray([
    ...normalizedDirectRoles,
    ...normalizedMembershipRoles,
    ...normalizedIdentityRoles
  ]).filter((role, index, list) => role && list.indexOf(role) === index);
  const active = row?.deactivated_at ? false : (typeof row?.active === "boolean" ? row.active : true);
  const deactivatedAt = active ? null : (row?.deactivated_at || new Date(0).toISOString());
  return {
    id: row?.user_id || row?.id || "",
    studio_id: row?.studio_id || "",
    firstName: row?.firstName || "",
    lastName: row?.lastName || "",
    email: row?.email || "",
    avatarUrl: row?.avatarUrl || "",
    membership_roles: normalizedMembershipRoles,
    identity_roles: normalizedIdentityRoles,
    roles,
    teacherIds: ensureArray(row?.teacherIds),
    instrument: ensureArray(row?.instrument),
    points: row?.points ?? null,
    level: row?.level ?? null,
    active,
    deactivated_at: deactivatedAt
  };
}

async function refreshManageUsers() {
  renderStatus(STATUS_LOADING);
  try {
    const studioId = await resolveStudioId();
    const rows = await fetchManageRows(studioId);
    allUsers = rows.map(buildUserRow);
    await loadTeacherDirectory(studioId);
    buildOptionLists(allUsers);
    syncSegmentedUI();
    ensureSortFieldForActiveTab();
    renderTableHeader();
    bindSortHeaders();
    renderUsers(rows.length);
  } catch (error) {
    allUsers = [];
    renderStatus("Failed to load members: " + (error?.message || "Unknown error"), true);
  }
}

function queueManageUsersRefresh(delayMs = 250) {
  if (refreshQueuedTimer) clearTimeout(refreshQueuedTimer);
  refreshQueuedTimer = setTimeout(async () => {
    refreshQueuedTimer = null;
    await refreshManageUsers();
  }, delayMs);
}

function stopManageUsersLiveSync() {
  if (!liveSyncChannel) return;
  try {
    supabase.removeChannel(liveSyncChannel);
  } catch (error) {
    console.warn("[ManageUsers] live sync unsubscribe failed", error);
  }
  liveSyncChannel = null;
  liveSyncStudioId = "";
}

function startManageUsersLiveSync(studioId) {
  const normalizedStudioId = String(studioId || "").trim();
  if (!normalizedStudioId) return;
  if (liveSyncChannel && liveSyncStudioId === normalizedStudioId) return;
  stopManageUsersLiveSync();

  const channelName = `manage-users-live:${normalizedStudioId}`;
  liveSyncChannel = supabase
    .channel(channelName)
    .on(
      "postgres_changes",
      { event: "*", schema: "public", table: "users", filter: `studio_id=eq.${normalizedStudioId}` },
      () => queueManageUsersRefresh(180)
    )
    .on(
      "postgres_changes",
      { event: "*", schema: "public", table: "studio_members", filter: `studio_id=eq.${normalizedStudioId}` },
      () => queueManageUsersRefresh(180)
    )
    .subscribe((status) => {
      if (status === "CHANNEL_ERROR") {
        console.warn("[ManageUsers] live sync channel error; continuing without realtime");
      }
    });
  liveSyncStudioId = normalizedStudioId;
}

async function initManageUsersPanel() {
  const tableBody = document.getElementById("userTableBody");
  const searchInput = document.getElementById("manageUsersSearch");
  const statusEl = document.getElementById("manageUsersStatus");
  if (!tableBody || !statusEl) return;

  activeTab = "students";
  statusFilter = "active";
  ensureSortFieldForActiveTab();
  renderTableHeader();
  bindManageUsersTabs();
  bindStatusToggle();

  const access = await getAccessFlags();
  const viewerContext = await getViewerContext();
  const allowed = Boolean(
    (
      access?.is_owner ||
      access?.can_manage_users ||
      viewerContext?.isOwner ||
      viewerContext?.isAdmin ||
      viewerContext?.isTeacher
    ) && !viewerContext?.isStudent
  );
  if (!allowed) {
    renderStatus("Not authorized.", true);
    console.warn("[ManageUsers] redirecting: insufficient permissions");
    if (window.location.pathname.split("/").pop() === "manage-users.html") {
      window.location.replace("index.html");
    }
    return;
  }

  bindSortHeaders();

  searchTerm = searchInput?.value || "";
  await refreshManageUsers();
  try {
    const studioId = await resolveStudioId();
    startManageUsersLiveSync(studioId);
  } catch (error) {
    console.warn("[ManageUsers] unable to start live sync", error);
  }

  if (searchInput && !searchHandlerBound) {
    searchHandlerBound = true;
    searchInput.addEventListener("input", event => {
      searchTerm = event.target.value || "";
      renderUsers();
    });
  }

  if (!tagPickerOutsideCloseBound) {
    tagPickerOutsideCloseBound = true;
    document.addEventListener("click", event => {
      const target = event.target instanceof Element
        ? event.target
        : event.target?.parentElement;
      if (target?.closest(".tag-picker")) return;
      closeAllTagPickers();
    });
  }

  if (!window.__manageUsersRefreshHooksBound) {
    window.__manageUsersRefreshHooksBound = true;
    window.addEventListener("focus", () => {
      queueManageUsersRefresh(0);
    });
    window.addEventListener("pageshow", () => {
      queueManageUsersRefresh(0);
    });
    document.addEventListener("visibilitychange", () => {
      if (!document.hidden) queueManageUsersRefresh(0);
    });
    window.addEventListener("aa:notification-state-changed", () => {
      queueManageUsersRefresh(120);
    });
  }
}

window.initManageUsersPanel = initManageUsersPanel;
document.addEventListener("DOMContentLoaded", initManageUsersPanel);
window.addEventListener("beforeunload", stopManageUsersLiveSync);
