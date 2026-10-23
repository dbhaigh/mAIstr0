function wirePrimaryTabs(tabs, panels) {
  const tabList = tabs[0]?.closest('[role="tablist"]');
  if (tabList) tabList.setAttribute("role", "tablist");
  const selectTab = (tab) => {
    tabs.forEach((item) => {
      const active = item === tab;
      item.classList.toggle("active", active);
      item.setAttribute("role", "tab");
      item.setAttribute("aria-controls", item.dataset.tab);
      item.setAttribute("aria-selected", String(active));
      item.tabIndex = active ? 0 : -1;
    });
    panels.forEach((panel) => {
      const active = panel.id === tab.dataset.tab;
      panel.classList.toggle("active", active);
      panel.setAttribute("role", "tabpanel");
      panel.setAttribute("aria-labelledby", tab.id);
      panel.hidden = !active;
    });
    activateDashboardTab(tab.dataset.tab);
  };
  tabs.forEach((tab, index) => {
    const target = document.getElementById(tab.dataset.tab);
    if (!tab.id) tab.id = `dashboard-${tab.dataset.tab}-tab`;
    if (target) {
      target.setAttribute("role", "tabpanel");
      target.setAttribute("aria-labelledby", tab.id);
      target.hidden = !tab.classList.contains("active");
    }
    tab.tabIndex = tab.classList.contains("active") ? 0 : -1;
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

wirePrimaryTabs(navTabs, tabContents);

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
    activateWorkspacePanel(targetID);
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
