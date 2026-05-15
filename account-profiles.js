import { supabase } from "./supabaseClient.js";
import { getViewerContext } from "./utils.js";

const PROFILE_SELECT = "id, firstName, lastName, avatarUrl, roles, deactivated_at";

function normalizeRoles(raw) {
  if (!raw) return [];
  if (Array.isArray(raw)) return raw.map(r => String(r).toLowerCase());
  if (typeof raw === "string") {
    return raw
      .split(",")
      .map(r => r.trim().toLowerCase())
      .filter(Boolean);
  }
  return [String(raw).toLowerCase()];
}

function normalizeProfile(raw) {
  if (!raw) return null;
  const firstName = raw.firstName ?? raw.first_name ?? "";
  const lastName = raw.lastName ?? raw.last_name ?? "";
  const label = lastName && firstName
    ? `${lastName}, ${firstName}`
    : (lastName || firstName || "User");
  const avatarUrl = raw.avatarUrl ?? raw.avatar_url ?? raw.avatar ?? "";
  const roleNames = normalizeRoles(raw.roles);
  return {
    ...raw,
    id: raw.id,
    firstName,
    lastName,
    avatarUrl,
    label,
    roleNames,
    deactivated_at: raw.deactivated_at ?? null
  };
}

function profileComparator(a, b) {
  const rolePriority = (profile) => {
    if (hasRole(profile, "parent")) return 0;
    if (hasRole(profile, "admin")) return 1;
    if (hasRole(profile, "teacher")) return 2;
    return 3;
  };
  const priDiff = rolePriority(a) - rolePriority(b);
  if (priDiff !== 0) return priDiff;
  return (a.label || "").localeCompare(b.label || "");
}

function uniqueProfiles(profiles) {
  const seen = new Map();
  const result = [];
  (profiles || []).forEach(profile => {
    if (!profile || !profile.id) return;
    const key = String(profile.id);
    if (!seen.has(key)) {
      seen.set(key, true);
      result.push(profile);
    }
  });
  return result;
}

function hasRole(profile, role) {
  if (!profile) return false;
  const roleNames = profile.roleNames || normalizeRoles(profile.roles);
  return roleNames.includes(String(role).toLowerCase());
}

async function loadUserById(userId) {
  if (!userId) return null;
  try {
    const { data, error } = await supabase
      .from("users")
      .select(PROFILE_SELECT)
      .eq("id", userId)
      .single();
    if (error) throw error;
    return normalizeProfile(data);
  } catch (err) {
    console.warn("[Accounts] failed to load user", err);
    return null;
  }
}

async function loadLinkedStudentsForParent(parentId, studioId, options = {}) {
  if (!parentId) return [];
  const includeInactive = options.includeInactive !== false;
  try {
    const ids = new Set();
    let linkQuery = supabase
      .from("parent_student_links")
      .select("student_id")
      .eq("parent_id", parentId);
    if (studioId) linkQuery = linkQuery.eq("studio_id", studioId);
    const { data: links, error: linkErr } = await linkQuery;
    if (linkErr) {
      console.warn("[Accounts] parent_student_links fetch failed", linkErr);
    } else {
      (links || []).forEach(link => {
        if (link?.student_id) ids.add(String(link.student_id));
      });
    }

    let childQuery = supabase
      .from("users")
      .select("id")
      .eq("parent_uuid", parentId);
    if (studioId) childQuery = childQuery.eq("studio_id", studioId);
    const { data: directChildren, error: childErr } = await childQuery;
    if (childErr) {
      console.warn("[Accounts] parent_uuid student fetch failed", childErr);
    } else {
      (directChildren || []).forEach(child => {
        if (child?.id) ids.add(String(child.id));
      });
    }

    const studentIds = Array.from(ids);
    if (!studentIds.length) return [];
    const { data: students, error: studentsErr } = await supabase
      .from("users")
      .select(`${PROFILE_SELECT}`)
      .in("id", studentIds)
      .order("lastName", { ascending: true })
      .order("firstName", { ascending: true });
    if (studentsErr) throw studentsErr;
    const list = Array.isArray(students) ? students.map(normalizeProfile) : [];
    if (!includeInactive) {
      return list.filter(profile => !profile.deactivated_at);
    }
    return list;
  } catch (err) {
    console.error("[Accounts] linked student fetch failed", err, JSON.stringify(err, null, 2));
    return [];
  }
}

function defaultMenuItem(profile, ctx) {
  const item = document.createElement("button");
  item.type = "button";
  item.className = "avatar-menu-item";
  if (ctx.isActive) {
    item.classList.add("is-active");
    item.setAttribute("aria-current", "true");
  }
  item.setAttribute("role", "menuitem");
  item.dataset.profileId = profile.id;

  const img = document.createElement("img");
  img.src = profile.avatarUrl || "images/icons/default.png";
  img.alt = "";
  img.width = 28;
  img.height = 28;
  img.addEventListener("error", () => {
    img.onerror = null;
    img.src = "images/icons/default.png";
  });

  const label = document.createElement("span");
  label.textContent = profile.label;

  item.appendChild(img);
  item.appendChild(label);

  item.addEventListener("click", () => {
    if (typeof ctx.onSelect === "function") {
      ctx.onSelect(profile);
    }
  });

  return item;
}

export function renderAccountProfileList(container, profiles, options = {}) {
  if (!container) return;
  container.innerHTML = "";
  if (!Array.isArray(profiles) || profiles.length === 0) {
    if (options.emptyState) {
      container.innerHTML = `<div class="account-profile-empty">${options.emptyState}</div>`;
    }
    return;
  }
  const renderer = typeof options.renderItem === "function"
    ? options.renderItem
    : defaultMenuItem;
  profiles.forEach(profile => {
    const ctx = {
      isActive: options.activeProfileId && String(profile.id) === String(options.activeProfileId),
      onSelect: options.onSelect,
      variant: options.variant || "menu"
    };
    const element = renderer(profile, ctx);
    if (element) {
      container.appendChild(element);
    }
  });
}

export async function getAccountProfiles(viewerContext, options = {}) {
  const ctx = viewerContext || await getViewerContext().catch(err => {
    console.warn("[Accounts] viewer context unavailable", err);
    return null;
  });
  if (!ctx?.viewerUserId) return [];
  const parentId = ctx.viewerUserId;
  const studioId = ctx.studioId;
  const fallbackProfile = options.fallbackProfile || ctx.userRow || null;

  const [parentRow, students] = await Promise.all([
    loadUserById(parentId),
    loadLinkedStudentsForParent(parentId, studioId, { includeInactive: options.includeInactive !== false })
  ]);

  let combined = [];
  if (parentRow) combined.push(parentRow);
  combined = combined.concat(students);
  if (fallbackProfile) {
    const normalized = normalizeProfile(fallbackProfile);
    if (normalized) combined.push(normalized);
  }

  const unique = uniqueProfiles(combined);
  return unique.sort(profileComparator);
}

export { hasRole, loadLinkedStudentsForParent };
