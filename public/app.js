const state = {
  services: [],
  flows: [],
  activeFlow: null,
  logs: [],
  events: [],
  logFilter: "",
  dbSummary: null,
  dbSummaryFlowId: null,
  subprocessSummary: null,
  config: null,
  servicesPoller: null,
  lastServicesRefreshAt: null,
  flowViewCleared: false,
};

const $ = (selector) => document.querySelector(selector);
const requiredServiceIds = ["preprocessor-api", "preprocessor-worker", "planner-api", "planner-worker", "interviewer"];
const startupGraceMs = 45000;

const flowMap = [
  {
    id: "backoffice",
    title: "Inicio",
    prod: "Backoffice / AWS Lambda",
    local: "Boton Iniciar flujo",
  },
  {
    id: "storage",
    title: "Insumos",
    prod: "Cloud Storage",
    local: "GCS real o fuente local configurada",
  },
  {
    id: "preprocessor",
    title: "Preprocessor",
    prod: "Cloud Run API + Cloud Tasks + Worker",
    local: "API :8010 + Worker :8011 + local_http",
  },
  {
    id: "pubsub-1",
    title: "Evento documental",
    prod: "Pub/Sub -> Eventarc -> Workflows",
    local: "Event bus -> router -> workflow local",
  },
  {
    id: "planner",
    title: "Planner",
    prod: "Cloud Run API + Cloud Tasks + Worker",
    local: "API :8020 + Worker :8021",
  },
  {
    id: "pubsub-2",
    title: "Evento planner",
    prod: "Pub/Sub -> Eventarc -> Workflows",
    local: "Event bus -> workflow completion",
  },
  {
    id: "interviewer",
    title: "Entrevista",
    prod: "Cloud Run Interviewer + Gemini Live",
    local: "Interviewer :8030",
  },
  {
    id: "notification",
    title: "Cierre",
    prod: "Pub/Sub/Workflows -> AWS Notification API",
    local: "stage.notification.requested simulado",
  },
];

const awsCheckpoints = [
  {
    id: "preprocessing_documents",
    title: "Preprocesamiento documental",
    prod: "Workflows recibe preprocessor.run.completed/partial/failed y llama AWS Notification API",
    local: "Se registra stage.notification.requested con payload simulado",
  },
  {
    id: "planning_interview",
    title: "Generacion de plan",
    prod: "Workflows recibe planner.job.started/stage_changed/completed/failed y notifica avance/cierre",
    local: "El simulador crea eventos stage.notification.requested",
  },
  {
    id: "interview_ready",
    title: "Entrevista disponible",
    prod: "Workflows o el backend externo reciben que existe launch_token/URL",
    local: "Se muestra launch_token y payload que se enviaria",
  },
  {
    id: "interview_execution",
    title: "Ejecucion de entrevista",
    prod: "Interviewer publica interview.started/progress/completed/failed y Workflows notifica AWS",
    local: "Se activara cuando la entrevista emita eventos al sink local",
  },
];

const nodeMeta = {
  backoffice: {
    title: "Inicio local",
    prod: "Backoffice / AWS Lambda",
    local: "UI Flow Lab",
  },
  "cloud-storage": {
    title: "Cloud Storage",
    prod: "Cloud Storage",
    local: "GCS real o adapter local si se configura",
  },
  "preprocessor-api": {
    title: "Preprocessor API",
    prod: "Cloud Run: rcsa-doc-preprocessor-api",
    local: "http://127.0.0.1:8010",
  },
  "cloud-tasks-preprocessor": {
    title: "Cloud Tasks Preprocessor",
    prod: "Cloud Tasks queue rcsa-doc-preprocessor",
    local: "TASK_DISPATCH_BACKEND=local_http",
  },
  "preprocessor-worker": {
    title: "Preprocessor Worker",
    prod: "Cloud Run: rcsa-doc-preprocessor-worker",
    local: "http://127.0.0.1:8011",
  },
  "pubsub-local": {
    title: "Pub/Sub",
    prod: "Topic preprocessor.run.completed/partial",
    local: "POST /api/events + events.jsonl",
  },
  "eventarc-local": {
    title: "Eventarc",
    prod: "Trigger Eventarc desde Pub/Sub",
    local: "Router local por event_type",
  },
  "workflow-local": {
    title: "Workflows",
    prod: "wf-planner-coordinator / wf-preprocessor-run-coordinator",
    local: "workflow simulator en server.js",
  },
  "planner-api": {
    title: "Planner API",
    prod: "Cloud Run: rcsa-planner-api",
    local: "http://127.0.0.1:8020",
  },
  "cloud-tasks-planner": {
    title: "Cloud Tasks Planner",
    prod: "Cloud Tasks queue rcsa-planner",
    local: "POST local al Planner Worker",
  },
  "planner-worker": {
    title: "Planner Worker",
    prod: "Cloud Run: rcsa-planner-worker",
    local: "http://127.0.0.1:8021",
  },
  interviewer: {
    title: "Interviewer",
    prod: "Cloud Run: rcsa-voice-agent",
    local: "http://127.0.0.1:8030",
  },
  "aws-notification-local": {
    title: "Notificacion externa",
    prod: "AWS Notification API via Workflows",
    local: "Evento stage.notification.requested simulado",
  },
  "flow-lab": {
    title: "Flow Lab",
    prod: "No aplica",
    local: "Servidor local de control",
  },
};

const flowMapNodeAliases = {
  backoffice: ["backoffice"],
  storage: ["cloud-storage"],
  preprocessor: ["preprocessor-api", "cloud-tasks-preprocessor", "preprocessor-worker"],
  "pubsub-1": ["pubsub-local", "eventarc-local", "workflow-local"],
  planner: ["planner-api", "cloud-tasks-planner", "planner-worker"],
  "pubsub-2": ["pubsub-local", "eventarc-local", "workflow-local"],
  interviewer: ["interviewer"],
  notification: ["aws-notification-local"],
};

document.addEventListener("DOMContentLoaded", () => {
  wireEvents();
  renderFlowMap();
  renderProgressRail();
  renderOperationalSummary();
  connectStreams();
  refreshConfig().catch((error) => showPayload({ error: error.message }));
  refreshServices();
  refreshFlows();
  refreshEvents();
  startServicesPolling();
  setInterval(() => {
    if (state.activeFlow) refreshDbSummary().catch(() => {});
  }, 7000);
  setInterval(() => {
    if (state.activeFlow?.status === "running") renderTimers();
  }, 1000);
});

function wireEvents() {
  $("#refreshServicesBtn").addEventListener("click", refreshServices);
  $("#startAllBtn").addEventListener("click", async () => {
    setServicesActionBusy(true);
    try {
      await action("/api/services/start-all", {});
      await refreshServices();
      startServicesPolling();
    } finally {
      setServicesActionBusy(false);
    }
  });
  $("#stopAllBtn").addEventListener("click", async () => {
    setServicesActionBusy(true);
    try {
      await action("/api/services/stop-all", {});
      await refreshServices();
    } finally {
      setServicesActionBusy(false);
    }
  });
  $("#reloadPathsBtn").addEventListener("click", () => refreshConfig().then(refreshServices));
  $("#pathsForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const data = new FormData(event.currentTarget);
    const payload = {
      repos: {
        preprocessor: String(data.get("preprocessor") || "").trim(),
        planner: String(data.get("planner") || "").trim(),
        interviewer: String(data.get("interviewer") || "").trim(),
      },
    };
    state.config = await action("/api/config/local-paths", payload);
    renderConfig();
    await refreshServices();
  });
  $("#logServiceFilter").addEventListener("change", (event) => {
    state.logFilter = event.target.value;
    renderLogs();
  });
  $("#refreshDbBtn").addEventListener("click", refreshDbSummary);
  $("#checkSubprocessBtn").addEventListener("click", refreshSubprocessSummary);
  $("#newFlowViewBtn")?.addEventListener("click", clearActiveFlowView);
  $("#refreshFlowsBtn")?.addEventListener("click", () => refreshFlows().catch((error) => showPayload({ error: error.message })));
  $("#flowForm").elements.subprocess_code.addEventListener("input", () => {
    state.subprocessSummary = null;
    renderSubprocessSummary();
  });
  $("#flowForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!areRequiredServicesReady()) {
      showPayload({
        error: "services_not_ready",
        message: "Espera que todos los servicios requeridos tengan health OK antes de iniciar el flujo.",
        services: requiredServices().map((service) => ({
          id: service.id,
          label: service.label,
          status: service.status,
          healthStatus: service.healthStatus,
          readiness: serviceReadiness(service).state,
        })),
      });
      return;
    }
    const data = new FormData(event.currentTarget);
    const payload = {
      subprocess_code: String(data.get("subprocess_code") || "").trim(),
      responsible_name: String(data.get("responsible_name") || "").trim(),
      responsible_email: String(data.get("responsible_email") || "").trim(),
      source_bucket: String(data.get("source_bucket") || "").trim(),
      include_mof: Boolean(data.get("include_mof")),
      mof_codes: String(data.get("mof_codes") || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean),
    };
    state.dbSummary = null;
    state.dbSummaryFlowId = null;
    state.flowViewCleared = false;
    renderPostgres();
    renderReadableOutputs();
    const flow = await action("/api/flows", payload);
    state.activeFlow = flow;
    renderFlow();
    renderFlowHistory();
  });
  $("#partialForm").addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = event.currentTarget;
    const data = new FormData(form);
    const payload = {
      action: String(data.get("action") || "").trim(),
      subprocess_code: String(data.get("subprocess_code") || "").trim(),
      preprocessor_run_id: String(data.get("preprocessor_run_id") || "").trim(),
      job_id: String(data.get("job_id") || "").trim(),
      plan_run_id: String(data.get("plan_run_id") || "").trim(),
      correlation_id: String(data.get("correlation_id") || "").trim(),
    };
    state.dbSummary = null;
    state.dbSummaryFlowId = null;
    state.flowViewCleared = false;
    renderPostgres();
    renderReadableOutputs();
    const flow = await action("/api/flows/partial", payload);
    state.activeFlow = flow;
    renderFlow();
    renderFlowHistory();
  });
  $("#useActiveFlowBtn").addEventListener("click", () => populatePartialFormFromFlow(state.activeFlow));
  $("#useDbLatestBtn").addEventListener("click", populatePartialFormFromDb);
}

function connectStreams() {
  connectSse("/api/logs/stream", (message) => {
    state.logs.push(message);
    if (state.logs.length > 1000) state.logs.splice(0, state.logs.length - 1000);
    renderLogs();
  });

  connectSse("/api/events/stream", (message) => {
    if (message.event) {
      state.events.unshift(message.event);
      if (state.events.length > 500) state.events.splice(500);
      renderEvents();
      renderAwsStates();
      renderOperationalSummary();
    }
  });

  connectSse("/api/flows/stream", (message) => {
    if (message.flow) {
      upsertFlow(message.flow);
      const shouldFocusFlow =
        !state.activeFlow ||
        state.activeFlow.id === message.flow.id ||
        message.flow.status === "running";
      if (shouldFocusFlow) {
        state.activeFlow = message.flow;
        state.flowViewCleared = false;
        renderFlow();
        renderAwsStates();
        renderPostgres();
        renderOperationalSummary();
      }
      renderFlowHistory();
    }
    if (message.service) {
      upsertService(message.service);
      renderServices();
    }
  });

  $("#serverStatus").textContent = "conectado";
  $("#serverStatus").className = "badge ok";
}

function connectSse(url, onMessage) {
  const source = new EventSource(url);
  source.onmessage = (event) => {
    if (!event.data) return;
    onMessage(JSON.parse(event.data));
  };
  source.onerror = () => {
    $("#serverStatus").textContent = "reconectando";
    $("#serverStatus").className = "badge failed";
  };
}

async function refreshServices() {
  const payload = await getJson("/api/services");
  state.services = payload.services || [];
  state.lastServicesRefreshAt = new Date();
  renderServices();
  renderReadiness();
  renderOperationalSummary();
}

async function refreshConfig() {
  state.config = await getJson("/api/config");
  renderConfig();
}

async function refreshFlows() {
  const payload = await getJson("/api/flows");
  state.flows = payload.flows || [];
  const currentId = state.activeFlow?.id;
  const current = currentId ? state.flows.find((flow) => flow.id === currentId) : null;
  const running = state.flows.find((flow) => flow.status === "running");
  if (current) {
    state.activeFlow = current;
  } else if (running) {
    state.activeFlow = running;
    state.flowViewCleared = false;
  } else if (!state.flowViewCleared) {
    state.activeFlow = state.flows[0] || null;
  }
  renderFlow();
  renderFlowHistory();
  if (state.activeFlow) {
    refreshDbSummary().catch(() => {});
  }
}

async function refreshEvents() {
  const payload = await getJson("/api/events");
  state.events = (payload.events || []).slice().reverse();
  renderEvents();
  renderAwsStates();
  renderOperationalSummary();
}

async function refreshDbSummary() {
  if (!state.activeFlow) {
    state.dbSummary = null;
    state.dbSummaryFlowId = null;
    renderPostgres();
    renderReadableOutputs();
    return;
  }
  state.dbSummary = await getJson(`/api/db/summary?flowId=${encodeURIComponent(state.activeFlow.id)}`);
  state.dbSummaryFlowId = state.activeFlow.id;
  renderPostgres();
  renderReadableOutputs();
}

async function refreshSubprocessSummary() {
  const code = String($("#flowForm").elements.subprocess_code.value || "").trim().toUpperCase();
  if (!code) {
    state.subprocessSummary = { available: false, error: "Ingresa un subprocess_code." };
    renderSubprocessSummary();
    return;
  }
  const button = $("#checkSubprocessBtn");
  button.disabled = true;
  button.textContent = "Revisando...";
  try {
    state.subprocessSummary = await getJson(`/api/db/subprocess?code=${encodeURIComponent(code)}`);
    renderSubprocessSummary();
  } finally {
    button.disabled = false;
    button.textContent = "Revisar datos existentes";
  }
}

function clearActiveFlowView() {
  state.activeFlow = null;
  state.dbSummary = null;
  state.dbSummaryFlowId = null;
  state.flowViewCleared = true;
  renderFlow();
  renderFlowHistory();
  renderPostgres();
  renderReadableOutputs();
  renderEvents();
  renderAwsStates();
  renderOperationalSummary();
  showPayload({
    view: "new-run",
    message: "Vista limpia para iniciar una corrida nueva. El historial sigue disponible abajo.",
  });
}

async function selectHistoryFlow(flowId) {
  const flow = await getJson(`/api/flows/${encodeURIComponent(flowId)}`);
  state.activeFlow = flow;
  state.dbSummary = null;
  state.dbSummaryFlowId = null;
  state.flowViewCleared = false;
  upsertFlow(flow);
  renderFlow();
  renderFlowHistory();
  await refreshDbSummary();
}

function renderServices() {
  const grid = $("#servicesGrid");
  grid.innerHTML = "";
  const filter = $("#logServiceFilter");
  const selected = filter.value;
  filter.innerHTML = `<option value="">Todos</option>`;

  for (const service of state.services) {
    const readiness = serviceReadiness(service);
    const option = document.createElement("option");
    option.value = service.id;
    option.textContent = service.label;
    filter.appendChild(option);

    const card = document.createElement("div");
    card.className = `service-card readiness-${readiness.state}`;
    card.innerHTML = `
      <div class="section-title">
        <h3>${escapeHtml(service.label)}</h3>
        <span class="badge ${readiness.className}">${escapeHtml(readiness.label)}</span>
      </div>
      <div class="service-meta">
        <div>URL: <code>${escapeHtml(service.url || "")}</code></div>
        <div>CWD: <code class="path-code">${escapeHtml(service.cwd || "")}</code></div>
        <div>PID: <code>${escapeHtml(service.pid || "-")}</code></div>
        <div>Estado: <code>${escapeHtml(service.status || "")}</code></div>
        <div>Health: <code>${escapeHtml(service.healthStatus || "unknown")}</code></div>
        <div>${escapeHtml(readiness.detail)}</div>
      </div>
      <div class="service-actions">
        ${renderServiceActionButtons(service)}
      </div>
    `;
    grid.appendChild(card);
  }

  filter.value = selected;

  grid.querySelectorAll("button").forEach((button) => {
    button.addEventListener("click", async () => {
      const actionName = button.dataset.action;
      if (actionName === "open") {
        window.open(button.dataset.url, "_blank");
        return;
      }
      const serviceId = button.dataset.service;
      await action(`/api/services/${serviceId}/${actionName}`, {});
      await refreshServices();
      startServicesPolling();
    });
  });
  renderReadiness();
}

function renderServiceActionButtons(service) {
  const isRunning = service.status === "running";
  const openButton = `<button class="secondary" data-action="open" data-url="${escapeHtml(service.url || "")}">Abrir</button>`;
  if (isRunning) {
    const stopButton = service.pid
      ? `<button class="danger" data-action="stop" data-service="${escapeHtml(service.id)}">Detener</button>`
      : `<button class="secondary" disabled>Detectado</button>`;
    return `
      ${stopButton}
      ${openButton}
    `;
  }
  return `
    <button data-action="start" data-service="${escapeHtml(service.id)}">Iniciar</button>
    ${openButton}
  `;
}

function renderReadiness() {
  const required = requiredServices();
  const ready = required.filter((service) => serviceReadiness(service).state === "ready");
  const running = required.filter((service) => service.status === "running");
  const notReady = required.filter((service) => serviceReadiness(service).state !== "ready");
  const allReady = required.length === requiredServiceIds.length && ready.length === requiredServiceIds.length;
  const anyStarting = required.some((service) => serviceReadiness(service).state === "starting");
  const badge = $("#servicesReadinessBadge");
  badge.textContent = `${ready.length}/${required.length || requiredServiceIds.length} listos`;
  badge.className = `badge ${allReady ? "ok" : anyStarting ? "running" : "failed"}`;

  const summary = $("#servicesReadinessSummary");
  if (!required.length) {
    summary.textContent = "Esperando estado de servicios.";
  } else if (allReady) {
    summary.textContent = "Todos los servicios requeridos estan listos para iniciar el flujo.";
  } else if (running.length) {
    summary.textContent = `Esperando health OK: ${notReady.map((service) => service.label).join(", ")}.`;
  } else {
    summary.textContent = "Los servicios estan detenidos. Usa Iniciar todo y espera que el contador llegue a 5/5.";
  }

  const submit = $("#flowSubmitBtn");
  if (submit) {
    submit.disabled = !allReady;
  }
  const hint = $("#flowReadinessHint");
  if (hint) {
    hint.textContent = allReady
      ? "Servicios listos. Puedes iniciar el flujo."
      : `Faltan servicios listos: ${notReady.map((service) => service.label).join(", ") || "cargando estado"}.`;
    hint.className = `flow-readiness ${allReady ? "ready" : "muted"}`;
  }
  renderOperationalSummary();
}

function areRequiredServicesReady() {
  const required = requiredServices();
  return required.length === requiredServiceIds.length && required.every((service) => serviceReadiness(service).state === "ready");
}

function requiredServices() {
  return requiredServiceIds.map((id) => state.services.find((service) => service.id === id)).filter(Boolean);
}

function serviceReadiness(service) {
  const health = service.healthStatus || "unknown";
  if (health === "ok") {
    return { state: "ready", label: "listo", className: "ok", detail: "Health check OK." };
  }
  if (service.status === "running") {
    const ageMs = service.startedAt ? Date.now() - new Date(service.startedAt).getTime() : 0;
    if (Number.isFinite(ageMs) && ageMs > startupGraceMs) {
      return {
        state: "unhealthy",
        label: "sin health",
        className: "failed",
        detail: `Proceso activo, pero health sigue ${health}. Revisar logs.`,
      };
    }
    return {
      state: "starting",
      label: "iniciando",
      className: "running",
      detail: `Proceso activo; esperando health OK (${health}).`,
    };
  }
  return { state: "stopped", label: "detenido", className: "down", detail: "Proceso local detenido." };
}

function startServicesPolling() {
  if (state.servicesPoller) return;
  state.servicesPoller = setInterval(() => {
    if (document.hidden) return;
    refreshServices().catch((error) => {
      showPayload({ error: error.message, source: "services-poller" });
    });
  }, 2500);
}

function setServicesActionBusy(isBusy) {
  $("#startAllBtn").disabled = isBusy;
  $("#stopAllBtn").disabled = isBusy;
}

function renderConfig() {
  const config = state.config;
  if (!config) return;
  const paths = config.localPaths || config.repos || {};
  const status = config.pathStatus || {};
  const form = $("#pathsForm");
  if (form) {
    form.elements.preprocessor.value = paths.preprocessor || "";
    form.elements.planner.value = paths.planner || "";
    form.elements.interviewer.value = paths.interviewer || "";
  }
  renderPathStatus("preprocessorPathStatus", status.preprocessor);
  renderPathStatus("plannerPathStatus", status.planner);
  renderPathStatus("interviewerPathStatus", status.interviewer);

  const allOk = Boolean(status.preprocessor && status.planner && status.interviewer);
  const badge = $("#pathsStatus");
  badge.textContent = allOk ? "rutas ok" : "revisar rutas";
  badge.className = `badge ${allOk ? "ok" : "failed"}`;
  $("#localConfigPath").innerHTML = `local.config.json: <code>${escapeHtml(config.localConfigPath || "")}</code>`;
}

function renderPathStatus(id, exists) {
  const el = $(`#${id}`);
  el.textContent = exists ? "existe" : "no existe";
  el.className = `badge ${exists ? "ok" : "failed"}`;
}

function renderFlowMap() {
  const container = $("#flowMap");
  container.innerHTML = "";
  const flow = state.activeFlow;
  const timeline = flow?.timeline || [];
  for (const node of flowMap) {
    const aliases = flowMapNodeAliases[node.id] || [node.id];
    const relatedTimeline = timeline.filter((item) => aliases.includes(item.node));
    const relatedIo = (flow?.ioRecords || []).filter((item) => aliases.includes(item.node));
    const isActive = Boolean(flow?.currentStep && aliases.includes(flow.currentStep));
    const failed = relatedTimeline.some((item) => item.status === "failed");
    const done = relatedTimeline.some((item) => ["completed", "warning"].includes(item.status));
    const stateLabel = isActive ? "activo" : failed ? "fallo" : done ? "listo" : "pendiente";
    const stateClass = isActive ? "map-active" : failed ? "map-failed" : done ? "map-done" : "";
    const el = document.createElement("div");
    el.className = `map-node ${stateClass}`;
    el.innerHTML = `
      <div class="map-title">
        <span>${escapeHtml(node.title)}</span>
        <span class="badge ${isActive ? "running" : failed ? "failed" : done ? "ok" : ""}">${escapeHtml(stateLabel)}</span>
      </div>
      <div class="map-prod"><span>Prod</span><strong>${escapeHtml(node.prod)}</strong></div>
      <div class="map-local"><span>Local</span><strong>${escapeHtml(node.local)}</strong></div>
      <div class="map-foot">
        <span>${escapeHtml(relatedTimeline.length)} pasos</span>
        <span>${escapeHtml(relatedIo.length)} I/O</span>
      </div>
    `;
    el.addEventListener("click", () => {
      showPayload({
        mapNode: node,
        aliases,
        status: stateLabel,
        timeline: relatedTimeline.slice(-10),
        ioRecords: relatedIo.slice(-10),
      });
    });
    container.appendChild(el);
  }
}

function renderProgressRail() {
  const container = $("#progressRail");
  if (!container) return;
  const flow = state.activeFlow;
  const timeline = flow?.timeline || [];
  container.innerHTML = "";

  flowMap.forEach((node, index) => {
    const aliases = flowMapNodeAliases[node.id] || [node.id];
    const relatedTimeline = timeline.filter((item) => aliases.includes(item.node));
    const relatedIo = (flow?.ioRecords || []).filter((item) => aliases.includes(item.node));
    const isActive = Boolean(flow?.currentStep && aliases.includes(flow.currentStep));
    const failed = relatedTimeline.some((item) => item.status === "failed");
    const done = relatedTimeline.some((item) => ["completed", "warning"].includes(item.status));
    const stateLabel = isActive ? "En ejecucion" : failed ? "Con falla" : done ? "Completado" : "Pendiente";
    const stateClass = isActive ? "active" : failed ? "failed" : done ? "done" : "";
    const el = document.createElement("div");
    el.className = `progress-step ${stateClass}`;
    el.innerHTML = `
      <div class="progress-step-head">
        <span class="progress-number">${index + 1}</span>
        <span class="badge ${isActive ? "running" : failed ? "failed" : done ? "ok" : ""}">${escapeHtml(stateLabel)}</span>
      </div>
      <div class="progress-title">${escapeHtml(node.title)}</div>
      <div class="progress-detail">${escapeHtml(node.prod)}</div>
    `;
    el.addEventListener("click", () => {
      showPayload({
        stage: node,
        status: stateLabel,
        aliases,
        timeline: relatedTimeline.slice(-10),
        ioRecords: relatedIo.slice(-10),
      });
    });
    container.appendChild(el);
  });
}

function renderOperationalSummary() {
  const servicesEl = $("#opsServices");
  if (!servicesEl) return;
  const required = requiredServices();
  const ready = required.filter((service) => serviceReadiness(service).state === "ready");
  const notReady = required.filter((service) => serviceReadiness(service).state !== "ready");
  const flow = state.activeFlow;
  const currentMeta = nodeMeta[flow?.currentStep] || null;
  const ioCount = flow?.ioRecords?.length || 0;
  const eventCount = state.events.filter(isEventForActiveFlow).length;

  servicesEl.textContent = `${ready.length}/${required.length || requiredServiceIds.length}`;
  $("#opsServicesDetail").textContent = notReady.length
    ? `Pendientes: ${notReady.map((service) => service.label).join(", ")}`
    : "Servicios listos";

  $("#opsFlow").textContent = flow ? flow.status : "Sin flujo";
  $("#opsFlowDetail").textContent = flow ? `${flow.subprocessCode} · ${flow.mode || "full"}` : "No hay corrida activa";

  $("#opsCurrent").textContent = currentMeta?.title || (flow?.currentStep ? flow.currentStep : "-");
  $("#opsCurrentDetail").textContent = flow?.currentStep
    ? `${currentMeta?.local || "Simulador local"}`
    : "Mapa pendiente";

  $("#opsObservability").textContent = `${eventCount} eventos`;
  $("#opsObservabilityDetail").textContent = `${ioCount} I/O registrados`;
}

function renderFlow() {
  const flow = state.activeFlow;
  const status = $("#flowStatus");
  const summary = $("#flowSummary");
  const timeline = $("#timeline");
  const launchLinks = $("#launchLinks");

  if (!flow) {
    status.textContent = "sin flujo";
    status.className = "badge";
    summary.textContent = "No hay una corrida activa.";
    timeline.innerHTML = "";
    launchLinks.innerHTML = "";
    $("#timelineCount").textContent = "0 pasos";
    renderFlowMap();
    renderProgressRail();
    renderIoRecords();
    renderReadableOutputs();
    renderTimers();
    renderOperationalSummary();
    return;
  }

  status.textContent = flow.status;
  status.className = `badge ${flow.status}`;
  summary.innerHTML = `
    <div>Flow: <code>${escapeHtml(flow.id)}</code></div>
    <div>Modo: <code>${escapeHtml(flow.mode || "full")}${flow.partialAction ? ` / ${escapeHtml(flow.partialAction)}` : ""}</code></div>
    <div>Subproceso: <code>${escapeHtml(flow.subprocessCode)}</code></div>
    <div>Correlation: <code>${escapeHtml(flow.correlationId || "-")}</code></div>
    <div>Preprocessor run: <code>${escapeHtml(flow.preprocessorRunId || "-")}</code></div>
    <div>Planner job: <code>${escapeHtml(flow.plannerJobId || "-")}</code></div>
    <div>Plan run: <code>${escapeHtml(flow.plannerPlanRunId || "-")}</code></div>
  `;

  timeline.innerHTML = "";
  const items = flow.timeline || [];
  $("#timelineCount").textContent = `${items.length} pasos`;
  for (const item of items.slice().reverse()) {
    const meta = nodeMeta[item.node] || {
      title: item.node,
      prod: "Componente productivo equivalente",
      local: "Simulador local",
    };
    const el = document.createElement("div");
    el.className = `timeline-item status-${item.status}`;
    el.innerHTML = `
      <div class="timeline-head">
        <span class="timeline-node">${escapeHtml(meta.title)}</span>
        <span class="timeline-time">${formatTime(item.ts)}</span>
      </div>
      <div class="timeline-label">${escapeHtml(item.label || "")}</div>
      <div class="timeline-tags">
        <span class="tag prod">Prod: ${escapeHtml(meta.prod)}</span>
        <span class="tag local">Local: ${escapeHtml(meta.local)}</span>
      </div>
    `;
    el.addEventListener("click", () => showPayload({ ...item, productionEquivalent: meta.prod, localImplementation: meta.local }));
    timeline.appendChild(el);
  }

  launchLinks.innerHTML = "";
  for (const launch of flow.launchTokens || []) {
    const card = document.createElement("div");
    card.className = "launch-card";
    card.innerHTML = `
      <div><strong>${escapeHtml(launch.launchToken || "launch")}</strong></div>
      <div>${escapeHtml(launch.intervieweeName || "")} ${escapeHtml(launch.intervieweeEmail || "")}</div>
      <div><a href="${escapeHtml(launch.launchUrl)}" target="_blank">Abrir entrevista</a></div>
      <div><a href="${escapeHtml(launch.apiUrl)}" target="_blank">Ver API payload</a></div>
    `;
    launchLinks.appendChild(card);
  }
  if (flow.outputs?.postgres) {
    state.dbSummary = flow.outputs.postgres;
    state.dbSummaryFlowId = flow.id;
  } else if (state.dbSummaryFlowId !== flow.id) {
    state.dbSummary = null;
    state.dbSummaryFlowId = null;
  }
  renderPostgres();
  renderAwsStates();
  renderTimers();
  renderFlowMap();
  renderProgressRail();
  renderIoRecords();
  renderReadableOutputs();
  renderOperationalSummary();
}

function renderFlowHistory() {
  const container = $("#flowHistory");
  const count = $("#flowHistoryCount");
  if (!container) return;
  const flows = state.flows || [];
  if (count) count.textContent = `${flows.length} corridas`;
  if (!flows.length) {
    container.className = "flow-history muted";
    container.textContent = "Todavia no hay corridas registradas en esta sesion local.";
    return;
  }
  container.className = "flow-history";
  container.innerHTML = flows
    .slice(0, 20)
    .map((flow) => {
      const active = state.activeFlow?.id === flow.id;
      const statusClass = flow.status === "completed" ? "ok" : flow.status === "failed" ? "failed" : flow.status;
      return `
        <button class="flow-history-item ${active ? "active" : ""}" type="button" data-flow-history-id="${escapeHtml(flow.id)}">
          <span>
            <strong>${escapeHtml(flow.subprocessCode || "-")}</strong>
            <small>${escapeHtml(flow.id)}</small>
          </span>
          <span>
            <span class="badge ${escapeHtml(statusClass || "")}">${escapeHtml(flow.status || "-")}</span>
            <small>${escapeHtml(formatTime(flow.updatedAt || flow.createdAt) || "")}</small>
          </span>
        </button>
      `;
    })
    .join("");
  container.querySelectorAll("[data-flow-history-id]").forEach((button) => {
    button.addEventListener("click", () => {
      selectHistoryFlow(button.dataset.flowHistoryId).catch((error) => showPayload({ error: error.message }));
    });
  });
}

function renderIoRecords() {
  const container = $("#ioRecords");
  const count = $("#ioCount");
  if (!container || !count) return;
  const flow = state.activeFlow;
  if (!flow) {
    count.textContent = "0 registros";
    container.innerHTML = `<div class="muted">No hay flujo activo.</div>`;
    return;
  }
  const records = flow.ioRecords || [];
  count.textContent = `${records.length} registros`;
  if (!records.length) {
    container.innerHTML = `<div class="muted">Aun no hay input/output registrado para este flujo.</div>`;
    return;
  }
  container.innerHTML = "";
  for (const record of records.slice().reverse()) {
    const meta = nodeMeta[record.node] || { title: record.node, prod: "Componente productivo equivalente", local: "Simulador local" };
    const card = document.createElement("div");
    card.className = `io-card io-${record.status || "completed"}`;
    card.innerHTML = `
      <div class="io-head">
        <div>
          <strong>${escapeHtml(record.title || record.stage)}</strong>
          <div class="io-meta">${escapeHtml(meta.title)} - ${escapeHtml(formatTime(record.ts))}</div>
        </div>
        <span class="badge ${record.status === "failed" ? "failed" : record.status === "warning" ? "running" : "ok"}">${escapeHtml(record.status || "completed")}</span>
      </div>
      <div class="timeline-tags">
        <span class="tag prod">Prod: ${escapeHtml(meta.prod)}</span>
        <span class="tag local">Local: ${escapeHtml(meta.local)}</span>
        <span class="tag">${escapeHtml(record.stage || "")}</span>
      </div>
      <div class="io-payload-grid">
        <div>
          <div class="io-label">Input</div>
          <pre>${escapeHtml(previewJson(record.input))}</pre>
        </div>
        <div>
          <div class="io-label">${record.error ? "Error" : "Output"}</div>
          <pre>${escapeHtml(previewJson(record.error || record.output))}</pre>
        </div>
      </div>
    `;
    card.addEventListener("click", () => showPayload(record));
    container.appendChild(card);
  }
}

function renderReadableOutputs() {
  const status = $("#readableOutputsStatus");
  const documentCount = $("#documentMarkdownCount");
  const plannerCount = $("#plannerOutputsCount");
  const documentContainer = $("#documentMarkdownOutputs");
  const plannerContainer = $("#plannerReadableOutputs");
  if (!status || !documentCount || !plannerCount || !documentContainer || !plannerContainer) return;
  renderReadableInterviewUrls();

  const summary =
    state.dbSummaryFlowId === state.activeFlow?.id ? state.dbSummary : state.activeFlow?.outputs?.postgres;
  if (!state.activeFlow) {
    status.textContent = "Sin flujo activo";
    documentCount.textContent = "0 documentos";
    plannerCount.textContent = "0 items";
    documentContainer.className = "readable-list muted";
    plannerContainer.className = "readable-list muted";
    documentContainer.textContent = "No hay flujo activo.";
    plannerContainer.textContent = "No hay flujo activo.";
    return;
  }
  if (!summary) {
    status.textContent = "DB pendiente";
    documentCount.textContent = "0 documentos";
    plannerCount.textContent = "0 items";
    documentContainer.className = "readable-list muted";
    plannerContainer.className = "readable-list muted";
    documentContainer.textContent = "Actualiza la DB para cargar Markdown extraido.";
    plannerContainer.textContent = "Actualiza la DB para cargar guiones y topicos.";
    return;
  }
  if (!summary.available) {
    status.textContent = "DB no disponible";
    documentCount.textContent = "0 documentos";
    plannerCount.textContent = "0 items";
    documentContainer.className = "readable-list muted";
    plannerContainer.className = "readable-list muted";
    documentContainer.textContent = summary.error || "No se pudo consultar PostgreSQL.";
    plannerContainer.textContent = summary.error || "No se pudo consultar PostgreSQL.";
    return;
  }

  const artifacts = summary.artifacts || {};
  const documents = artifacts.documents || [];
  const planner = artifacts.planner || {};
  const plannerItems = countPlannerReadableItems(planner);

  status.textContent = `Snapshot ${formatTime(summary.inspectedAt) || "DB"}`;
  documentCount.textContent = `${documents.length} documentos`;
  plannerCount.textContent = `${plannerItems} items`;

  documentContainer.className = "readable-list";
  plannerContainer.className = "readable-list";
  documentContainer.innerHTML = documents.length
    ? documents.map((document, index) => renderMarkdownDocument(document, index)).join("")
    : `<div class="muted">No hay documentos asociados al flujo actual.</div>`;
  plannerContainer.innerHTML = plannerItems
    ? renderPlannerReadableOutputs(planner)
    : `<div class="muted">No hay guiones o topicos Planner asociados al flujo actual.</div>`;

  documentContainer.querySelectorAll("[data-readable-document]").forEach((el) => {
    el.addEventListener("click", (event) => {
      if (event.target.closest("summary")) return;
      showPayload(documents[Number(el.dataset.readableDocument)]);
    });
  });
  plannerContainer.querySelectorAll("[data-readable-job]").forEach((el) => {
    el.addEventListener("click", () => showPayload((planner.jobs || [])[Number(el.dataset.readableJob)]));
  });
  plannerContainer.querySelectorAll("[data-readable-plan]").forEach((el) => {
    el.addEventListener("click", () => showPayload((planner.plans || [])[Number(el.dataset.readablePlan)]));
  });
}

function renderReadableInterviewUrls() {
  const container = $("#readableInterviewUrl");
  if (!container) return;
  const launches = state.activeFlow?.launchTokens || [];
  if (!state.activeFlow) {
    container.className = "readable-interview-url muted";
    container.textContent = "No hay flujo activo.";
    return;
  }
  if (!launches.length) {
    container.className = "readable-interview-url muted";
    container.textContent = "La URL relativa de entrevista aparecera cuando Interviewer resuelva el launch token.";
    return;
  }
  container.className = "readable-interview-url";
  container.innerHTML = `
    <div class="readable-interview-head">
      <div>
        <strong>URL relativa de entrevista</strong>
        <span>Sin base URL; esta es la ruta que se entregaria al frontend/consumidor.</span>
      </div>
      <span class="badge ok">${escapeHtml(launches.length)} launch${launches.length === 1 ? "" : "es"}</span>
    </div>
    <div class="readable-interview-list">
      ${launches.map(renderReadableInterviewUrl).join("")}
    </div>
  `;
  container.querySelectorAll("[data-open-interview-url]").forEach((button) => {
    button.addEventListener("click", (event) => {
      event.stopPropagation();
      window.open(button.dataset.openInterviewUrl, "_blank");
    });
  });
}

function renderReadableInterviewUrl(launch) {
  const relativeUrl = interviewRelativeUrl(launch);
  const fullUrl = interviewFullUrl(launch, relativeUrl);
  const identity = [launch.intervieweeName, launch.intervieweeEmail].filter(Boolean).join(" · ");
  return `
    <div class="readable-interview-card">
      <div class="readable-interview-copy">
        <code>${escapeHtml(relativeUrl)}</code>
        ${identity ? `<span>${escapeHtml(identity)}</span>` : ""}
      </div>
      <button type="button" class="secondary" data-open-interview-url="${escapeHtml(fullUrl)}">Abrir</button>
    </div>
  `;
}

function interviewRelativeUrl(launch) {
  const rawUrl = String(launch?.launchUrl || launch?.launch_url || "").trim();
  if (rawUrl) {
    try {
      const url = new URL(rawUrl, window.location.origin);
      return `${url.pathname}${url.search}${url.hash}`;
    } catch (_) {
      if (rawUrl.startsWith("/")) return rawUrl;
    }
  }
  const token = String(launch?.launchToken || launch?.launch_token || "").trim();
  return token ? `/planner-interviews/${encodeURIComponent(token)}` : "/planner-interviews";
}

function interviewFullUrl(launch, relativeUrl) {
  const rawUrl = String(launch?.launchUrl || launch?.launch_url || "").trim();
  if (/^https?:\/\//i.test(rawUrl)) return rawUrl;
  const interviewer = state.services.find((service) => service.id === "interviewer");
  const baseUrl = String(interviewer?.url || "").replace(/\/$/, "");
  if (baseUrl) return `${baseUrl}${relativeUrl.startsWith("/") ? relativeUrl : `/${relativeUrl}`}`;
  return new URL(relativeUrl, window.location.origin).href;
}

function renderMarkdownDocument(document, index) {
  const units = document.units || [];
  const unitBadges = [
    `${document.page_count ?? "-"} paginas`,
    `${document.unit_count ?? units.length ?? 0} unidades`,
    `${formatNumber(document.markdown_chars || 0)} chars`,
    document.parse_strategy || "",
  ].filter(Boolean);
  return `
    <article class="readable-card" data-readable-document="${index}">
      <div class="readable-card-head">
        <div>
          <h3>${escapeHtml(document.file_name || "Documento")}</h3>
          <div class="readable-meta">${escapeHtml(document.document_role || "-")} - ${escapeHtml(document.processing_status || "-")}</div>
        </div>
        <span class="badge ok">${escapeHtml(document.processing_status || "ok")}</span>
      </div>
      <div class="readable-tags">
        ${unitBadges.map((item) => `<span class="tag">${escapeHtml(item)}</span>`).join("")}
      </div>
      ${renderMarkdownExcerpt(document.markdown_excerpt)}
      ${
        units.length
          ? `<details class="readable-details">
              <summary>Unidades / paginas detectadas</summary>
              <div class="unit-list">
                ${units.map(renderDocumentUnit).join("")}
              </div>
            </details>`
          : ""
      }
    </article>
  `;
}

function renderDocumentUnit(unit) {
  const flags = [
    unit.page_number ? `pag. ${unit.page_number}` : "",
    unit.has_images ? "imagenes" : "",
    unit.has_tables ? "tablas" : "",
    unit.llm_used ? "LLM visual" : "",
    `${formatNumber(unit.markdown_chars || 0)} chars`,
  ].filter(Boolean);
  return `
    <div class="unit-card">
      <div class="unit-head">
        <strong>${escapeHtml(unit.unit_label || `Unidad ${unit.unit_number ?? "-"}`)}</strong>
        <span>${flags.map((item) => escapeHtml(item)).join(" · ")}</span>
      </div>
      ${renderMarkdownExcerpt(unit.markdown_excerpt, 80)}
    </div>
  `;
}

function renderPlannerReadableOutputs(planner) {
  const jobs = planner.jobs || [];
  const plans = planner.plans || [];
  return `
    ${jobs.map(renderPlannerJob).join("")}
    ${plans.map(renderPlannerPlan).join("")}
  `;
}

function renderPlannerJob(job, index) {
  const guides = job.guides || [];
  const health = getPlannerJobHealth(job, guides);
  const preprocessorRunId =
    job.preprocessor_run_id ||
    job.request_payload?.preprocessorRunId ||
    job.request_payload?.preprocessor_run_id ||
    "";
  const metaTags = [
    preprocessorRunId ? `preprocessorRunId: ${preprocessorRunId}` : "",
    health.chunksTotal !== null ? `chunks: ${health.chunksTotal}` : "",
    health.topicsDetected !== null ? `topicos: ${health.topicsDetected}` : "",
  ].filter(Boolean);
  return `
    <article class="readable-card planner-card" data-readable-job="${index}">
      <div class="readable-card-head">
        <div>
          <h3>${escapeHtml(job.job_id || "Planner job")}</h3>
          <div class="readable-meta">${escapeHtml(job.result_ref || "")}</div>
        </div>
        <span class="badge ${health.warning ? "warning" : "ok"}">${escapeHtml(job.guides_count ?? guides.length)} guiones</span>
      </div>
      ${
        metaTags.length
          ? `<div class="readable-tags planner-job-meta">${metaTags.map((item) => `<span class="tag">${escapeHtml(item)}</span>`).join("")}</div>`
          : ""
      }
      ${
        health.messages.length
          ? `<div class="planner-job-warning">${health.messages.map((message) => `<p>${escapeHtml(message)}</p>`).join("")}</div>`
          : ""
      }
      <div class="guide-list">
        ${guides.map((guide, guideIndex) => renderPlannerGuide(guide, guideIndex)).join("")}
      </div>
    </article>
  `;
}

function getPlannerJobHealth(job, guides) {
  const progress = job.progress_json || {};
  const chunksTotal = readProgressNumber(progress, ["chunksTotal", "chunks_total", "chunks"]);
  const topicsDetected = readProgressNumber(progress, ["topicsDetected", "topics_detected", "topics"]);
  const fallbackPhrases = [
    "no se dispone de insumos",
    "no se cuenta con insumos",
    "sin senales textuales",
    "sin señales textuales",
    "sin insumos especificos",
    "sin insumos específicos",
  ];
  const guideText = guides
    .map((guide) => `${guide.title || ""}\n${guide.description || ""}\n${guide.script || ""}`)
    .join("\n")
    .toLowerCase();
  const hasFallbackText = fallbackPhrases.some((phrase) => guideText.includes(phrase));
  const messages = [];
  if (chunksTotal === 0 && guides.length) {
    messages.push("Este job fue generado con 0 chunks documentales; los guiones pueden ser genericos aunque existan documentos procesados.");
  }
  if (hasFallbackText) {
    messages.push("Se detectaron frases de fallback en el guion. Para validar cambios, reejecuta Planner usando el preprocessorRunId correcto.");
  }
  return {
    chunksTotal,
    topicsDetected,
    warning: messages.length > 0,
    messages,
  };
}

function readProgressNumber(progress, keys) {
  for (const key of keys) {
    const value = progress?.[key];
    if (value === null || value === undefined || value === "") continue;
    const number = Number(value);
    if (Number.isFinite(number)) return number;
  }
  return null;
}

function renderPlannerPlan(plan, index) {
  const tracks = plan.tracks || [];
  const stepCount = tracks.reduce((total, track) => total + (track.steps || []).length, 0);
  return `
    <article class="readable-card planner-card" data-readable-plan="${index}">
      <div class="readable-card-head">
        <div>
          <h3>Plan v${escapeHtml(plan.plan_version ?? "-")} · ${escapeHtml(plan.process_code || "")}</h3>
          <div class="readable-meta">${escapeHtml(plan.plan_run_id || "")}</div>
        </div>
        <span class="badge ok">${escapeHtml(stepCount)} topicos</span>
      </div>
      ${plan.objective ? `<p class="readable-objective">${escapeHtml(plan.objective)}</p>` : ""}
      <div class="track-list">
        ${tracks.map(renderPlannerTrack).join("")}
      </div>
    </article>
  `;
}

function renderPlannerTrack(track) {
  return `
    <details class="track-card" open>
      <summary>
        <span>${escapeHtml(track.display_name || track.track_key || "Track")}</span>
        <small>${escapeHtml((track.steps || []).length)} pasos</small>
      </summary>
      ${track.description ? `<p>${escapeHtml(track.description)}</p>` : ""}
      <div class="guide-list">
        ${(track.steps || []).map((step, index) => renderPlannerGuide({
          title: step.title || step.short_label,
          description: step.description || step.analysis_goal,
          script: step.script_text,
          taxonomy: step.taxonomy_json,
        }, index)).join("")}
      </div>
    </details>
  `;
}

function renderPlannerGuide(guide, index) {
  return `
    <section class="guide-card">
      <div class="guide-head">
        <span>${escapeHtml(index + 1)}. ${escapeHtml(guide.title || "Topico")}</span>
      </div>
      ${guide.description ? `<p class="guide-description">${escapeHtml(guide.description)}</p>` : ""}
      ${guide.taxonomy ? `<div class="readable-tags">${renderTaxonomyTags(guide.taxonomy)}</div>` : ""}
      ${renderScriptText(guide.script)}
    </section>
  `;
}

function renderTaxonomyTags(taxonomy) {
  if (!taxonomy || typeof taxonomy !== "object") return "";
  return Object.entries(taxonomy)
    .filter(([, value]) => value)
    .map(([key, value]) => `<span class="tag">${escapeHtml(key)}: ${escapeHtml(value)}</span>`)
    .join("");
}

function countPlannerReadableItems(planner) {
  const jobGuides = (planner.jobs || []).reduce((total, job) => total + (job.guides || []).length, 0);
  const planSteps = (planner.plans || []).reduce((total, plan) => {
    return total + (plan.tracks || []).reduce((subtotal, track) => subtotal + (track.steps || []).length, 0);
  }, 0);
  return jobGuides + planSteps;
}

function populatePartialFormFromFlow(flow) {
  const form = $("#partialForm");
  if (!form || !flow) {
    $("#partialHint").textContent = "No hay flujo activo para copiar IDs.";
    return;
  }
  form.elements.subprocess_code.value = flow.subprocessCode || form.elements.subprocess_code.value || "";
  form.elements.preprocessor_run_id.value = flow.preprocessorRunId || "";
  form.elements.job_id.value = flow.plannerJobId || "";
  form.elements.plan_run_id.value = flow.plannerPlanRunId || "";
  form.elements.correlation_id.value = flow.correlationId || "";
  $("#partialHint").textContent = "IDs cargados desde el flujo activo.";
}

async function populatePartialFormFromDb() {
  const form = $("#partialForm");
  const code = String(form.elements.subprocess_code.value || $("#flowForm").elements.subprocess_code.value || "")
    .trim()
    .toUpperCase();
  if (!code) {
    $("#partialHint").textContent = "Ingresa un subproceso para consultar la DB.";
    return;
  }
  $("#partialHint").textContent = "Consultando ultimo dato DB...";
  try {
    state.subprocessSummary = await getJson(`/api/db/subprocess?code=${encodeURIComponent(code)}`);
  } catch (error) {
    $("#partialHint").textContent = `No se pudo consultar la DB: ${error.message}`;
    showPayload({ error: error.message, source: "populatePartialFormFromDb" });
    return;
  }
  renderSubprocessSummary();
  if (!state.subprocessSummary.available) {
    $("#partialHint").textContent = state.subprocessSummary.error || "No se pudo consultar la DB.";
    return;
  }
  const subprocessRun = firstTableRow("subprocess_runs");
  const plannerJob = firstTableRow("interview_job_runs");
  const planRun = firstTableRow("interview_plan_runs");
  form.elements.subprocess_code.value = code;
  form.elements.preprocessor_run_id.value =
    subprocessRun.id || subprocessRun.subprocess_run_id || subprocessRun.run_id || form.elements.preprocessor_run_id.value;
  form.elements.job_id.value = plannerJob.job_id || plannerJob.id || form.elements.job_id.value;
  form.elements.plan_run_id.value = planRun.id || planRun.plan_run_id || form.elements.plan_run_id.value;
  form.elements.correlation_id.value =
    subprocessRun.correlation_id || plannerJob.correlation_id || form.elements.correlation_id.value || "";
  $("#partialHint").textContent = "IDs cargados desde los ultimos registros encontrados en PostgreSQL.";
}

function firstTableRow(tableName) {
  const table = (state.subprocessSummary?.tables || []).find((item) => item.table === tableName);
  return table?.rows?.[0] || {};
}

function renderTimers() {
  const status = $("#timerStatus");
  const summary = $("#timerSummary");
  const dashboard = $("#timerDashboard");
  if (!status || !summary || !dashboard) return;

  const flow = state.activeFlow;
  if (!flow) {
    status.textContent = "sin flujo";
    status.className = "badge";
    summary.className = "timer-summary muted";
    summary.textContent = "Inicia un flujo para medir cada etapa.";
    dashboard.innerHTML = "";
    return;
  }

  const timers = flowTimerRows(flow);
  const totalMs = flowDurationMs(flow);
  const slowest = timers.reduce((best, item) => {
    return !best || item.durationMs > best.durationMs ? item : best;
  }, null);
  const completedCount = timers.filter((item) => item.status !== "running").length;
  const activeCount = timers.filter((item) => item.status === "running").length;
  const maxDuration = Math.max(1, ...timers.map((item) => item.durationMs || 0));

  status.textContent = flow.status === "running" ? "midiendo" : flow.status;
  status.className = `badge ${flow.status === "completed" ? "ok" : flow.status === "running" ? "running" : flow.status}`;
  summary.className = "timer-summary";
  summary.innerHTML = `
    <div class="timer-stat">
      <span>Total flujo</span>
      <strong>${escapeHtml(formatDuration(totalMs))}</strong>
    </div>
    <div class="timer-stat">
      <span>Etapas medidas</span>
      <strong>${escapeHtml(completedCount)} cerradas${activeCount ? ` / ${escapeHtml(activeCount)} activas` : ""}</strong>
    </div>
    <div class="timer-stat">
      <span>Etapa mas lenta</span>
      <strong>${escapeHtml(slowest ? `${timerTitle(slowest)} (${formatDuration(slowest.durationMs)})` : "-")}</strong>
    </div>
  `;

  dashboard.innerHTML = timers.length
    ? timers.map((item) => renderTimerCard(item, maxDuration)).join("")
    : `<div class="muted">Aun no hay timers registrados.</div>`;
  dashboard.querySelectorAll("[data-timer-index]").forEach((el) => {
    el.addEventListener("click", () => showPayload(timers[Number(el.dataset.timerIndex)]));
  });
}

function flowTimerRows(flow) {
  const completed = (flow.timerRuns || []).map((item, index) => normalizeTimer(item, index));
  const active = Object.values(flow.activeTimers || {}).map((item, index) => normalizeTimer(item, completed.length + index));
  return [...completed, ...active].sort((left, right) => {
    return new Date(left.startedAt || left.updatedAt || 0).getTime() - new Date(right.startedAt || right.updatedAt || 0).getTime();
  }).map((item, index) => ({ ...item, index }));
}

function normalizeTimer(item, index) {
  const startedAt = item.startedAt || item.updatedAt || item.endedAt;
  const endedAt = item.endedAt || (item.status === "running" ? new Date().toISOString() : item.updatedAt || startedAt);
  const durationMs = Number.isFinite(item.durationMs)
    ? Number(item.durationMs)
    : Math.max(0, new Date(endedAt).getTime() - new Date(startedAt).getTime());
  return {
    ...item,
    index,
    startedAt,
    endedAt,
    durationMs,
  };
}

function flowDurationMs(flow) {
  const timers = flowTimerRows(flow);
  const start = flow.startedAt || timers[0]?.startedAt || flow.createdAt;
  const end = flow.completedAt || (["completed", "failed"].includes(flow.status) ? flow.updatedAt : new Date().toISOString());
  if (!start || !end) return 0;
  return Math.max(0, new Date(end).getTime() - new Date(start).getTime());
}

function renderTimerCard(item, maxDuration) {
  const meta = nodeMeta[item.node] || { title: item.node, prod: "Componente productivo equivalente", local: "Simulador local" };
  const ratio = Math.max(0, Math.min(1, (item.durationMs || 0) / maxDuration));
  const width = Math.max(3, Math.min(100, Math.round(ratio * 100)));
  const bgLightness = Math.round(96 - ratio * 13);
  const borderLightness = Math.round(78 - ratio * 18);
  const barLightness = Math.round(62 - ratio * 14);
  return `
    <div
      class="timer-card timer-${escapeHtml(item.status || "unknown")}"
      style="--timer-bg: hsl(190 54% ${bgLightness}%); --timer-border: hsl(190 47% ${borderLightness}%); --timer-bar: hsl(190 58% ${barLightness}%);"
      data-timer-index="${item.index}"
    >
      <div class="timer-card-head">
        <span>${escapeHtml(meta.title)}</span>
        <strong>${escapeHtml(formatDuration(item.durationMs || 0))}</strong>
      </div>
      <div class="timer-label">${escapeHtml(item.label || "")}</div>
      <div class="timer-bar"><span style="width: ${width}%"></span></div>
      <div class="timer-tags">
        <span class="tag ${item.status === "running" ? "prod" : "local"}">${escapeHtml(item.status || "")}</span>
        <span class="tag prod">Prod: ${escapeHtml(meta.prod)}</span>
        <span class="tag local">Local: ${escapeHtml(meta.local)}</span>
      </div>
    </div>
  `;
}

function timerTitle(item) {
  const meta = nodeMeta[item.node] || { title: item.node };
  return meta.title || item.node;
}

function renderSubprocessSummary() {
  const container = $("#subprocessHistory");
  if (!container) return;
  const summary = state.subprocessSummary;
  if (!summary) {
    container.className = "subprocess-history muted";
    container.textContent = "No se ha consultado la DB para este subproceso.";
    return;
  }
  if (!summary.available) {
    container.className = "subprocess-history muted";
    container.innerHTML = `<div>No disponible.</div><div>${escapeHtml(summary.error || "No se pudo consultar PostgreSQL.")}</div>`;
    return;
  }

  const table = (name) => (summary.tables || []).find((item) => item.table === name) || {};
  const keyTables = [
    { name: "subprocess_runs", title: "Corridas preprocessor" },
    { name: "documents", title: "Documentos procesados" },
    { name: "document_units", title: "Unidades markdown" },
    { name: "interview_job_runs", title: "Jobs planner" },
    { name: "interview_plan_runs", title: "Planes" },
    { name: "interview_launches", title: "Launches entrevista" },
  ];
  const hasData = Boolean(summary.hasExistingData);
  container.className = `subprocess-history ${hasData ? "" : "muted"}`;
  container.innerHTML = `
    <div class="history-head">
      <strong>${escapeHtml(summary.subprocessCode || summary.identifiers?.subprocessCode || "")}</strong>
      <span class="badge ${hasData ? "ok" : ""}">${hasData ? "hay datos previos" : "sin datos previos"}</span>
    </div>
    <div class="history-note">
      Reprocesar crea una nueva corrida y conserva el historial. El plan activo puede apuntar al ultimo plan aprobado.
    </div>
    <div class="history-grid">
      ${keyTables.map((item, index) => renderHistoryCard(item.title, table(item.name), index)).join("")}
    </div>
  `;
  container.querySelectorAll("[data-history-table]").forEach((el) => {
    const tableName = el.getAttribute("data-history-table");
    showPayloadOnClick(el, table(tableName));
  });
}

function renderHistoryCard(title, table) {
  const rows = table.rows || [];
  const latest = rows[0] || {};
  return `
    <div class="history-card" data-history-table="${escapeHtml(table.table || "")}">
      <div class="history-title">
        <span>${escapeHtml(title)}</span>
        <span class="badge ${table.matchedCount ? "ok" : ""}">${escapeHtml(table.matchedCount ?? 0)}</span>
      </div>
      <div class="history-latest">
        ${latest.status ? `<span class="tag">${escapeHtml(latest.status)}</span>` : ""}
        ${latest.documents_status ? `<span class="tag">${escapeHtml(latest.documents_status)}</span>` : ""}
        ${latest.stage ? `<span class="tag">${escapeHtml(latest.stage)}</span>` : ""}
        ${latest.created_at ? `<span>${escapeHtml(formatTime(latest.created_at))}</span>` : ""}
      </div>
      <pre class="history-rows">${escapeHtml(JSON.stringify(rows, null, 2))}</pre>
    </div>
  `;
}

function showPayloadOnClick(element, payload) {
  element.addEventListener("click", () => showPayload(payload));
}

function renderEvents() {
  const list = $("#eventsList");
  list.innerHTML = "";
  const visibleEvents = state.activeFlow ? state.events.filter(isEventForActiveFlow) : state.events;
  $("#eventsCount").textContent = `${visibleEvents.length} eventos`;
  for (const event of visibleEvents.slice(0, 200)) {
    const el = document.createElement("div");
    el.className = "event-item";
    el.innerHTML = `
      <div class="event-head">
        <span class="event-type">${escapeHtml(event.event_type)}</span>
        <span class="event-time">${formatTime(event.occurred_at)}</span>
      </div>
      <div class="event-meta">${escapeHtml(event.producer || "")} - ${escapeHtml(event.correlation_id || "")}</div>
    `;
    el.addEventListener("click", () => showPayload(event));
    list.appendChild(el);
  }
}

function renderAwsStates() {
  const container = $("#awsStates");
  if (!container) return;
  const relatedEvents = state.events.filter((event) => {
    return event.event_type === "stage.notification.requested" && isEventForActiveFlow(event);
  });
  container.innerHTML = "";
  for (const checkpoint of awsCheckpoints) {
    const history = relatedEvents.filter((item) => {
      const payload = item.payload || {};
      return payload.stage === checkpoint.id;
    });
    const event = history[0];
    const payload = event?.payload || {};
    const card = document.createElement("div");
    card.className = `aws-card ${event ? "done" : ""}`;
    card.innerHTML = `
      <div class="aws-title">
        <span>${escapeHtml(checkpoint.title)}</span>
        <span class="badge ${event ? "ok" : ""}">${event ? escapeHtml(payload.status || "requested") : "pendiente"}</span>
      </div>
      <div class="aws-detail">
        <div><strong>Prod:</strong> ${escapeHtml(checkpoint.prod)}</div>
        <div><strong>Local:</strong> ${escapeHtml(checkpoint.local)}</div>
        ${
          event
            ? `<div><strong>Target:</strong> ${escapeHtml(payload.notification_target || "aws-notification-api")}</div>
               <div><strong>Evento origen:</strong> ${escapeHtml(payload.source_event_type || "-")}</div>
               <div><strong>Ultimo cambio:</strong> ${escapeHtml(event.occurred_at)}</div>`
            : ""
        }
        ${history.length ? `<div class="aws-history">${history.map(renderAwsHistoryItem).join("")}</div>` : ""}
      </div>
    `;
    card.addEventListener("click", () => showPayload(event || checkpoint));
    container.appendChild(card);
  }
}

function renderAwsHistoryItem(event) {
  const payload = event.payload || {};
  return `<span class="tag">${escapeHtml(payload.status || "requested")} - ${escapeHtml(formatTime(event.occurred_at))}</span>`;
}

function isEventForActiveFlow(event) {
  const flow = state.activeFlow;
  if (!flow) return true;
  return Boolean(
    (event.correlation_id && event.correlation_id === flow.correlationId) ||
      (event.subprocess_run_id && event.subprocess_run_id === flow.preprocessorRunId) ||
      (event.job_id && event.job_id === flow.plannerJobId) ||
      (event.plan_run_id && event.plan_run_id === flow.plannerPlanRunId),
  );
}

function renderPostgres() {
  const container = $("#postgresSummary");
  if (!container) return;
  const summary =
    state.dbSummaryFlowId === state.activeFlow?.id ? state.dbSummary : state.activeFlow?.outputs?.postgres;
  if (!state.activeFlow) {
    container.className = "postgres-summary muted";
    container.textContent = "Sin flujo activo.";
    return;
  }
  if (!summary) {
    container.className = "postgres-summary muted";
    container.textContent = "Aun no hay snapshot de PostgreSQL. Usa Actualizar DB.";
    return;
  }
  if (!summary.available) {
    container.className = "postgres-summary muted";
    container.innerHTML = `
      <div>No disponible.</div>
      <div>${escapeHtml(summary.error || "Configura FLOW_LAB_DATABASE_URL o los .env de los repos.")}</div>
    `;
    return;
  }
  const keyTables = new Set([
    "subprocess_runs",
    "processing_tasks",
    "task_inputs",
    "documents",
    "document_units",
    "integration_events",
    "interview_job_runs",
    "interview_job_events",
    "interview_plan_runs",
    "interview_plan_tracks",
    "interview_plan_steps",
    "active_interview_plans",
    "interview_launches",
    "voice_agent_workflow_runs",
    "voice_agent_step_runs",
    "voice_agent_sessions",
  ]);
  const interesting = (summary.tables || []).filter((table) => {
    return table.exists && (table.matchedCount > 0 || keyTables.has(table.table));
  });
  container.className = "postgres-summary";
  container.innerHTML = `
    <div class="db-detail">DB: <code>${escapeHtml(summary.database || "")}</code></div>
    <div class="db-detail">Snapshot: <code>${escapeHtml(summary.inspectedAt || "")}</code></div>
    <div class="db-detail">Identificadores: <code>${escapeHtml(JSON.stringify(summary.identifiers || {}))}</code></div>
    <div class="db-grid">
      ${interesting.map(renderDbTableCard).join("")}
    </div>
  `;
  container.querySelectorAll("[data-table-index]").forEach((el) => {
    el.addEventListener("click", () => showPayload(interesting[Number(el.dataset.tableIndex)]));
  });
}

function renderDbTableCard(table, index) {
  return `
    <div class="db-table-card" data-table-index="${index}">
      <div class="db-table-title">
        <span>${escapeHtml(table.table)}</span>
        <span class="badge ok">existe</span>
      </div>
      <div class="db-counts">
        <span class="tag">match: ${escapeHtml(table.matchedCount ?? "-")}</span>
        <span class="tag">total: ${escapeHtml(table.totalCount ?? "-")}</span>
      </div>
      <pre class="db-rows">${escapeHtml(JSON.stringify(table.rows || [], null, 2))}</pre>
    </div>
  `;
}

function renderLogs() {
  const lines = state.logs
    .filter((entry) => !state.logFilter || entry.serviceId === state.logFilter)
    .slice(-350)
    .map((entry) => `${formatTime(entry.ts)} [${entry.serviceLabel}] ${entry.line}`);
  $("#logs").textContent = lines.join("\n");
  $("#logs").scrollTop = $("#logs").scrollHeight;
}

function showPayload(payload) {
  $("#payloadViewer").textContent = JSON.stringify(payload ?? {}, null, 2);
}

async function getJson(url) {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url} -> ${response.status}`);
  return response.json();
}

async function action(url, body) {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  const payload = await response.json().catch(() => ({}));
  if (!response.ok) {
    showPayload(payload);
    throw new Error(`${url} -> ${response.status}`);
  }
  return payload;
}

function upsertService(service) {
  const index = state.services.findIndex((item) => item.id === service.id);
  if (index >= 0) state.services[index] = service;
  else state.services.push(service);
}

function upsertFlow(flow) {
  const index = state.flows.findIndex((item) => item.id === flow.id);
  if (index >= 0) state.flows[index] = flow;
  else state.flows.unshift(flow);
}

function badgeClass(service) {
  if (service.healthStatus === "ok") return "ok";
  if (service.status === "running") return "running";
  if (service.healthStatus === "down") return "down";
  return "";
}

function formatTime(value) {
  if (!value) return "";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return String(value);
  return date.toLocaleTimeString();
}

function formatNumber(value) {
  return new Intl.NumberFormat("es-CL").format(Number(value) || 0);
}

function formatDuration(ms) {
  const value = Math.max(0, Number(ms) || 0);
  if (value < 1000) return `${Math.round(value)} ms`;
  const seconds = value / 1000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 1 : 0)} s`;
  const minutes = Math.floor(seconds / 60);
  const remainingSeconds = Math.round(seconds % 60);
  if (minutes < 60) return `${minutes} min ${remainingSeconds}s`;
  const hours = Math.floor(minutes / 60);
  const remainingMinutes = minutes % 60;
  return `${hours} h ${remainingMinutes} min`;
}

function previewJson(value) {
  if (value === null || value === undefined || value === "") return "{}";
  const text = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  if (!text) return "{}";
  return text;
}

function renderMarkdownExcerpt(markdown) {
  const text = String(markdown || "").trim();
  if (!text) return `<div class="markdown-rendered muted">Sin Markdown disponible.</div>`;
  const lines = text.split(/\r?\n/);
  let html = "";
  let inList = false;
  let tableLines = [];
  const closeList = () => {
    if (inList) {
      html += "</ul>";
      inList = false;
    }
  };
  const flushTable = () => {
    if (!tableLines.length) return;
    html += renderMarkdownTable(tableLines);
    tableLines = [];
  };
  for (const rawLine of lines) {
    const line = rawLine.trimEnd();
    if (!line.trim()) {
      flushTable();
      closeList();
      continue;
    }
    const heading = line.match(/^(#{1,6})\s+(.+)$/);
    if (heading) {
      flushTable();
      closeList();
      const level = Math.min(6, Math.max(4, heading[1].length + 3));
      html += `<h${level}>${escapeHtml(heading[2])}</h${level}>`;
      continue;
    }
    const bullet = line.match(/^[-*]\s+(.+)$/) || line.match(/^\d+[.)]\s+(.+)$/);
    if (bullet) {
      flushTable();
      if (!inList) {
        html += "<ul>";
        inList = true;
      }
      html += `<li>${escapeHtml(bullet[1])}</li>`;
      continue;
    }
    if (line.includes("|") && line.split("|").length >= 3) {
      closeList();
      tableLines.push(line);
      continue;
    }
    flushTable();
    closeList();
    html += `<p>${escapeHtml(line)}</p>`;
  }
  flushTable();
  closeList();
  return `<div class="markdown-rendered">${html}</div>`;
}

function renderMarkdownTable(lines) {
  const rows = lines
    .map(parseMarkdownTableRow)
    .filter((row) => row.length && !isMarkdownSeparatorRow(row));
  if (!rows.length) return "";
  const maxCols = Math.max(...rows.map((row) => row.length));
  const normalizedRows = rows.map((row) => {
    const next = [...row];
    while (next.length < maxCols) next.push("");
    return next;
  });
  if (normalizedRows.length === 1) {
    return `<p>${escapeHtml(normalizedRows[0].filter(Boolean).join(" · "))}</p>`;
  }
  const [header, ...body] = normalizedRows;
  return `
    <div class="markdown-table-wrap">
      <table>
        <thead>
          <tr>${header.map((cell) => `<th>${escapeHtml(cell)}</th>`).join("")}</tr>
        </thead>
        <tbody>
          ${body.map((row) => `<tr>${row.map((cell) => `<td>${escapeHtml(cell)}</td>`).join("")}</tr>`).join("")}
        </tbody>
      </table>
    </div>
  `;
}

function parseMarkdownTableRow(line) {
  return line
    .trim()
    .replace(/^\|/, "")
    .replace(/\|$/, "")
    .split("|")
    .map((cell) => cell.trim())
    .filter((cell, index, cells) => cell || cells.length <= 3 || index > 0);
}

function isMarkdownSeparatorRow(row) {
  return row.length > 0 && row.every((cell) => /^:?-{2,}:?$/.test(cell.trim()));
}

function renderScriptText(script) {
  const text = String(script || "").trim();
  if (!text) return `<div class="script-rendered muted">Sin guion disponible.</div>`;
  const lines = text.split(/\r?\n/);
  let html = "";
  let inList = false;
  const closeList = () => {
    if (inList) {
      html += "</ol>";
      inList = false;
    }
  };
  for (const rawLine of lines) {
    const line = rawLine.trim();
    if (!line) {
      closeList();
      continue;
    }
    if (/^[A-ZÁÉÍÓÚÑ].{2,70}:$/.test(line)) {
      closeList();
      html += `<h4>${escapeHtml(line.replace(/:$/, ""))}</h4>`;
      continue;
    }
    const numbered = line.match(/^\d+[.)]\s+(.+)$/);
    const bullet = line.match(/^[-*]\s+(.+)$/);
    if (numbered || bullet) {
      if (!inList) {
        html += "<ol>";
        inList = true;
      }
      html += `<li>${escapeHtml((numbered || bullet)[1])}</li>`;
      continue;
    }
    closeList();
    html += `<p>${escapeHtml(line)}</p>`;
  }
  closeList();
  return `<div class="script-rendered">${html}</div>`;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}
