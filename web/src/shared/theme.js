const THEME_STORAGE_KEY = "maistr0-theme";
const themeToggle = document.getElementById("theme-toggle");
const root = document.documentElement;

function setDashboardTheme(theme, persist = false) {
  const selectedTheme = theme === "light" ? "light" : "dark";
  root.dataset.theme = selectedTheme;
  if (themeToggle) {
    const nextTheme = selectedTheme === "light" ? "dark" : "light";
    themeToggle.textContent = `${nextTheme[0].toUpperCase()}${nextTheme.slice(1)} mode`;
    themeToggle.setAttribute("aria-label", `Switch to ${nextTheme} mode`);
    themeToggle.setAttribute("aria-pressed", String(selectedTheme === "light"));
    themeToggle.title = `Switch to ${nextTheme} mode`;
  }
  if (persist) {
    try {
      localStorage.setItem(THEME_STORAGE_KEY, selectedTheme);
    } catch (error) {
      console.warn("Could not save dashboard theme preference.", error);
    }
  }
}

let savedTheme = "dark";
try {
  if (localStorage.getItem(THEME_STORAGE_KEY) === "light") savedTheme = "light";
} catch (error) {
  console.warn("Could not read dashboard theme preference.", error);
}
setDashboardTheme(savedTheme);

themeToggle?.addEventListener("click", () => {
  setDashboardTheme(root.dataset.theme === "light" ? "dark" : "light", true);
});
