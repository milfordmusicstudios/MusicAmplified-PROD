import { supabase } from "./supabaseClient.js";

export function parseRoles(roles) {
  if (!roles) return [];
  if (Array.isArray(roles)) return roles.map(r => String(r).toLowerCase());
  if (typeof roles === "string") {
    try {
      const parsed = JSON.parse(roles);
      return Array.isArray(parsed) ? parsed.map(r => String(r).toLowerCase()) : [String(parsed).toLowerCase()];
    } catch {
      return roles.split(",").map(r => r.trim().toLowerCase()).filter(Boolean);
    }
  }
  return [String(roles).toLowerCase()];
}

export function showToast(message) {
  const toast = document.getElementById("toast");
  if (!toast) return;
  toast.textContent = message;
  toast.classList.add("show");
  clearTimeout(toast._hideTimer);
  toast._hideTimer = setTimeout(() => {
    toast.classList.remove("show");
  }, 2200);
}

export function normalizeTextArray(valueOrArray) {
  if (!valueOrArray) return [];
  if (Array.isArray(valueOrArray)) {
    return valueOrArray.map(v => String(v).trim()).filter(Boolean);
  }
  if (typeof valueOrArray === "string") {
    const trimmed = valueOrArray.trim();
    return trimmed ? [trimmed] : [];
  }
  return [String(valueOrArray).trim()].filter(Boolean);
}

function getSelectedTeacherValues(selectEl) {
  return Array.from(selectEl?.selectedOptions || []).map(option => String(option.value));
}

function setSelectedTeacherValues(selectEl, selectedValues) {
  const selected = new Set((selectedValues || []).map(String));
  Array.from(selectEl?.options || []).forEach(option => {
    option.selected = selected.has(String(option.value));
  });
}

function getTeacherPickerLabel(selectEl, value) {
  const option = Array.from(selectEl?.options || []).find(opt => String(opt.value) === String(value));
  return option?.textContent || String(value);
}

function renderTeacherMultiSelect(picker) {
  const selectEl = picker?._selectEl;
  if (!picker || !selectEl) return;

  const selectedValues = getSelectedTeacherValues(selectEl);
  const pillsEl = picker.querySelector(".teacher-pill-picker-pills");
  const menuEl = picker.querySelector(".teacher-pill-picker-menu");
  if (!pillsEl || !menuEl) return;

  const disabled = selectEl.disabled;
  const isOpen = picker.dataset.open === "true" && !disabled;
  picker.classList.toggle("is-disabled", disabled);
  picker.dataset.open = isOpen ? "true" : "false";

  pillsEl.innerHTML = "";
  if (!selectedValues.length) {
    const empty = document.createElement("span");
    empty.className = "teacher-pill-empty";
    empty.textContent = disabled ? "No teachers available" : "Select teacher...";
    pillsEl.appendChild(empty);
  } else {
    selectedValues.forEach(value => {
      const pill = document.createElement("span");
      pill.className = "teacher-pill";
      pill.textContent = getTeacherPickerLabel(selectEl, value);

      const removeBtn = document.createElement("button");
      removeBtn.type = "button";
      removeBtn.dataset.action = "remove-teacher";
      removeBtn.dataset.value = value;
      removeBtn.disabled = disabled;
      removeBtn.setAttribute("aria-label", `Remove ${getTeacherPickerLabel(selectEl, value)}`);
      removeBtn.textContent = "x";

      pill.appendChild(removeBtn);
      pillsEl.appendChild(pill);
    });
  }

  const selected = new Set(selectedValues);
  const available = Array.from(selectEl.options || []).filter(option => option.value && !selected.has(String(option.value)));
  menuEl.innerHTML = "";
  if (!available.length) {
    const empty = document.createElement("div");
    empty.className = "teacher-pill-menu-empty";
    empty.textContent = selectedValues.length ? "All teachers selected" : "No teachers available";
    menuEl.appendChild(empty);
  } else {
    available.forEach(option => {
      const button = document.createElement("button");
      button.type = "button";
      button.className = "teacher-pill-option";
      button.dataset.action = "add-teacher";
      button.dataset.value = option.value;
      button.textContent = option.textContent;
      menuEl.appendChild(button);
    });
  }
  menuEl.hidden = !isOpen;
}

function closeOtherTeacherPickers(exceptPicker) {
  document.querySelectorAll(".teacher-pill-picker[data-open='true']").forEach(picker => {
    if (picker === exceptPicker) return;
    picker.dataset.open = "false";
    renderTeacherMultiSelect(picker);
  });
}

export function enhanceTeacherMultiSelect(selectEl) {
  if (!selectEl || selectEl.dataset.pillPickerEnhanced === "true") {
    if (selectEl?._teacherPillPicker) renderTeacherMultiSelect(selectEl._teacherPillPicker);
    return selectEl?._teacherPillPicker || null;
  }

  selectEl.dataset.pillPickerEnhanced = "true";
  selectEl.classList.add("teacher-select-native");

  const picker = document.createElement("div");
  picker.className = "teacher-pill-picker";
  picker.dataset.open = "false";
  picker._selectEl = selectEl;
  selectEl._teacherPillPicker = picker;

  const pills = document.createElement("div");
  pills.className = "teacher-pill-picker-pills";
  pills.setAttribute("role", "button");
  pills.setAttribute("tabindex", "0");
  pills.setAttribute("aria-label", "Select teachers");

  const menu = document.createElement("div");
  menu.className = "teacher-pill-picker-menu";
  menu.hidden = true;

  picker.appendChild(pills);
  picker.appendChild(menu);
  selectEl.insertAdjacentElement("afterend", picker);

  const syncChange = () => {
    selectEl.dispatchEvent(new Event("change", { bubbles: true }));
    renderTeacherMultiSelect(picker);
  };

  picker.addEventListener("click", event => {
    event.stopPropagation();
    if (selectEl.disabled) return;

    const target = event.target instanceof Element
      ? event.target
      : event.target?.parentElement;
    if (!target) return;

    const addBtn = target.closest("button[data-action='add-teacher']");
    if (addBtn) {
      const selected = getSelectedTeacherValues(selectEl);
      const value = String(addBtn.dataset.value || "");
      if (value && !selected.includes(value)) selected.push(value);
      setSelectedTeacherValues(selectEl, selected);
      picker.dataset.open = "false";
      syncChange();
      return;
    }

    const removeBtn = target.closest("button[data-action='remove-teacher']");
    if (removeBtn) {
      const value = String(removeBtn.dataset.value || "");
      setSelectedTeacherValues(selectEl, getSelectedTeacherValues(selectEl).filter(selectedValue => selectedValue !== value));
      picker.dataset.open = "false";
      syncChange();
      return;
    }

    const nextOpen = picker.dataset.open !== "true";
    if (nextOpen) closeOtherTeacherPickers(picker);
    picker.dataset.open = nextOpen ? "true" : "false";
    renderTeacherMultiSelect(picker);
  });

  pills.addEventListener("keydown", event => {
    if (event.key !== "Enter" && event.key !== " ") return;
    event.preventDefault();
    picker.click();
  });

  const observer = new MutationObserver(() => renderTeacherMultiSelect(picker));
  observer.observe(selectEl, { attributes: true, attributeFilter: ["disabled"] });

  document.addEventListener("click", () => {
    if (picker.dataset.open !== "true") return;
    picker.dataset.open = "false";
    renderTeacherMultiSelect(picker);
  });

  renderTeacherMultiSelect(picker);
  return picker;
}

export function refreshTeacherMultiSelect(selectEl) {
  if (selectEl?._teacherPillPicker) {
    renderTeacherMultiSelect(selectEl._teacherPillPicker);
  }
}

export async function loadTeachersForStudio(studioId) {
  if (!studioId) return [];
  const { data: users, error } = await supabase
    .from("users")
    .select("id, firstName, lastName, roles")
    .eq("studio_id", studioId);

  if (error) {
    console.error("[Settings] teacher load failed", error);
    return [];
  }

  return (users || [])
    .filter(u => {
      const roles = parseRoles(u.roles);
      return roles.includes("teacher") || roles.includes("admin");
    })
    .sort(
      (a, b) =>
        (a.lastName || "").localeCompare(b.lastName || "") ||
        (a.firstName || "").localeCompare(b.firstName || "")
    )
    .map(t => ({
      id: t.id,
      label: (t.lastName && t.firstName
        ? `${t.lastName}, ${t.firstName}`
        : (`${t.lastName ?? ""}${t.firstName ?? ""}`.trim() || "Unnamed Teacher"))
    }));
}

export function applyTeacherOptionsToSelect(selectEl, teacherOptions) {
  if (!selectEl) return;
  const selected = new Set(Array.from(selectEl.selectedOptions || []).map(o => o.value));
  if (!teacherOptions || teacherOptions.length === 0) {
    selectEl.innerHTML = "";
    selectEl.disabled = true;
    const opt = document.createElement("option");
    opt.value = "";
    opt.textContent = "No teachers available";
    selectEl.appendChild(opt);
    enhanceTeacherMultiSelect(selectEl);
    return;
  }
  selectEl.innerHTML = "";
  teacherOptions.forEach(t => {
    const opt = document.createElement("option");
    opt.value = t.id;
    opt.textContent = t.label;
    if (selected.has(t.id)) opt.selected = true;
    selectEl.appendChild(opt);
  });
  selectEl.disabled = false;
  enhanceTeacherMultiSelect(selectEl);
}

export { loadLinkedStudentsForParent as loadLinkedStudents } from "./account-profiles.js";
