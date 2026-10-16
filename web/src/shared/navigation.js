// Tab Switching
navTabs.forEach((tab) => {
  tab.addEventListener("click", () => {
    navTabs.forEach((t) => t.classList.remove("active"));
    tabContents.forEach((c) => c.classList.remove("active"));
    tab.classList.add("active");
    const target = document.getElementById(tab.dataset.tab);
    if (target) target.classList.add("active");
    if (tab.dataset.tab === "memory-tab") refreshMemory();
    if (tab.dataset.tab === "cluster-tab") void refresh();
  });
});

function wireWorkspaceTabs(tabs, panels) {
  const selectTab = (tab) => {
    const targetID = tab.getAttribute("aria-controls");
    tabs.forEach((item) => {
      const active = item === tab;
      item.classList.toggle("active", active);
      item.setAttribute("aria-selected", String(active));
      item.tabIndex = active ? 0 : -1;
    });
    panels.forEach((panel) => {
      const active = panel.id === targetID;
      panel.classList.toggle("active", active);
      panel.hidden = !active;
    });
  };
  tabs.forEach((tab, index) => {
    tab.tabIndex = tab.getAttribute("aria-selected") === "true" ? 0 : -1;
    tab.addEventListener("click", () => selectTab(tab));
    tab.addEventListener("keydown", (event) => {
      let nextIndex;
      if (event.key === "ArrowRight") nextIndex = (index + 1) % tabs.length;
      else if (event.key === "ArrowLeft") nextIndex = (index - 1 + tabs.length) % tabs.length;
      else if (event.key === "Home") nextIndex = 0;
      else if (event.key === "End") nextIndex = tabs.length - 1;
      else return;
      event.preventDefault();
      selectTab(tabs[nextIndex]);
      tabs[nextIndex].focus();
    });
  });
}

wireWorkspaceTabs(clusterWorkspaceTabs, clusterWindows);
wireWorkspaceTabs(overviewTabs, overviewPanels);
wireWorkspaceTabs(setupTabs, setupPanels);

pairingConnectCancelBtn?.addEventListener("click", () => pairingConnectDialog?.close());
pairingConnectOpenBtn?.addEventListener("click", () => openPairingDialog());

setupCheckBtn?.addEventListener("click", checkSetupReadiness);
catalogNodeSelect?.addEventListener("change", loadCatalogTarget);
catalogEngineSelect?.addEventListener("change", renderModelCatalog);
catalogSearch?.addEventListener("input", renderModelCatalog);
