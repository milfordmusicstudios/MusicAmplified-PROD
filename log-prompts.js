export const LOG_PROMPTS = Object.freeze([
  {
    key: "practice",
    label: "Practice",
    icon: "&#10003;",
    category: "practice",
    logType: "fixed",
    points: 5,
    notesPrompt: "What did the student practice?",
    notesRequired: false,
    order: 10,
    studentFacing: false,
    teacherFacing: true
  },
  {
    key: "finishBook",
    label: "Finish a book",
    icon: "&#128214;",
    category: "proficiency",
    logType: "fixed",
    points: 50,
    notesPrompt: "What book did the student complete?",
    notesRequired: true,
    order: 20,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "studioPerformance",
    label: "Studio Performance",
    icon: "&#127908;",
    category: "performance",
    logType: "fixed",
    points: 100,
    notesPrompt: "What performance did the student participate in?",
    notesRequired: true,
    order: 30,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "competition",
    label: "Competition",
    icon: "&#127941;",
    category: "participation",
    logType: "fixed",
    points: 100,
    notesPrompt: "Describe the competition.",
    notesRequired: true,
    order: 40,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "memorization",
    label: "Memorization",
    icon: "&#129504;",
    category: "proficiency",
    logType: "memorization",
    points: 2,
    notesPrompt: "Name of piece.",
    notesRequired: true,
    order: 50,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "personalGoal",
    label: "Personal Goal",
    icon: "&#127919;",
    category: "personal",
    logType: "discretionary",
    points: 5,
    notesPrompt: "Describe the goal.",
    notesRequired: true,
    order: 60,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "groupClass",
    label: "Group Class",
    icon: "&#128101;",
    category: "participation",
    logType: "fixed",
    points: 50,
    notesPrompt: "What class did the student attend?",
    notesRequired: true,
    order: 70,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "outsidePerformance",
    label: "Outside Performance",
    icon: "&#127916;",
    category: "performance",
    logType: "outside-performance",
    points: 25,
    notesPrompt: "What event did the student perform at?",
    notesRequired: true,
    order: 80,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "festival",
    label: "Festival",
    icon: "&#127942;",
    category: "proficiency",
    logType: "festival",
    points: 200,
    notesPrompt: "Festival rating or result.",
    notesRequired: false,
    order: 90,
    studentFacing: true,
    teacherFacing: true
  },
  {
    key: "theoryTechniqueTest",
    label: "Theory / Technique Test",
    icon: "&#128221;",
    category: "proficiency",
    logType: "fixed",
    points: 50,
    notesPrompt: "What level test did the student complete?",
    notesRequired: true,
    order: 100,
    studentFacing: true,
    teacherFacing: true
  }
]);

export const TEACHER_POINT_CATEGORIES = Object.freeze([
  { value: "practice", label: "Practice" },
  { value: "performance", label: "Performance" },
  { value: "participation", label: "Participation" },
  { value: "proficiency", label: "Proficiency" },
  { value: "personal", label: "Personal" }
]);

export function getStudentLogPrompts() {
  return LOG_PROMPTS
    .filter(prompt => prompt.studentFacing && prompt.key !== "practice")
    .sort((a, b) => a.order - b.order);
}

export function getTeacherLogPrompts() {
  return LOG_PROMPTS
    .filter(prompt => prompt.teacherFacing)
    .sort((a, b) => a.order - b.order);
}

export function getLogPromptByKey(key) {
  const normalized = String(key || "").trim();
  return LOG_PROMPTS.find(prompt => prompt.key === normalized) || null;
}

export function getTeacherPromptCategories() {
  return Array.from(new Set(
    getTeacherLogPrompts()
      .map(prompt => String(prompt.category || "").trim().toLowerCase())
      .filter(Boolean)
  ));
}

export function getTeacherPointCategories() {
  return TEACHER_POINT_CATEGORIES.map(category => ({ ...category }));
}
