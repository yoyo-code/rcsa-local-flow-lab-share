import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync, appendFileSync, createReadStream, readdirSync, statSync } from "node:fs";
import { dirname, extname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = __dirname;
const PUBLIC_DIR = join(ROOT_DIR, "public");
const RUNTIME_DIR = join(ROOT_DIR, "runtime");
const LOG_DIR = join(RUNTIME_DIR, "logs");
const FLOW_DIR = join(RUNTIME_DIR, "flows");
const EVENTS_FILE = join(RUNTIME_DIR, "events.jsonl");
const DB_INSPECTOR_SCRIPT = join(ROOT_DIR, "tools", "db_inspector.py");
const IS_WINDOWS = process.platform === "win32";

mkdirSync(PUBLIC_DIR, { recursive: true });
mkdirSync(LOG_DIR, { recursive: true });
mkdirSync(FLOW_DIR, { recursive: true });

const DEFAULT_CONFIG_PATH = join(ROOT_DIR, "rcsa-flow-lab.config.json");
const LOCAL_CONFIG_PATH = join(ROOT_DIR, "local.config.json");
const config = loadConfig();
const services = new Map();
const flows = new Map();
const events = [];
const workflowCallbacks = new Map();
const sseClients = {
  logs: new Set(),
  events: new Set(),
  flows: new Set(),
};

for (const [id, service] of Object.entries(config.services || {})) {
  services.set(id, {
    id,
    ...service,
    process: null,
    pid: null,
    startedAt: null,
    stoppedAt: null,
    status: "stopped",
    healthStatus: "unknown",
    logs: [],
    logFile: join(LOG_DIR, `${id}.log`),
  });
}

loadPersistedFlows();

const server = createServer(async (req, res) => {
  try {
    await route(req, res);
  } catch (error) {
    console.error(error);
    sendJson(res, 500, {
      error: "internal_error",
      message: error instanceof Error ? error.message : String(error),
    });
  }
});

server.listen(config.port || 4400, config.host || "127.0.0.1", () => {
  console.log(`RCSA Local Flow Lab: http://${config.host || "127.0.0.1"}:${config.port || 4400}`);
});

process.on("SIGINT", async () => {
  await stopAllServices();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await stopAllServices();
  process.exit(0);
});

function loadConfig() {
  const base = JSON.parse(readFileSync(DEFAULT_CONFIG_PATH, "utf8"));
  if (!existsSync(LOCAL_CONFIG_PATH)) {
    return base;
  }
  const local = JSON.parse(readFileSync(LOCAL_CONFIG_PATH, "utf8"));
  return deepMerge(base, local);
}

function deepMerge(base, override) {
  if (!override || typeof override !== "object" || Array.isArray(override)) {
    return override ?? base;
  }
  const result = { ...base };
  for (const [key, value] of Object.entries(override)) {
    if (
      value &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      base &&
      typeof base[key] === "object" &&
      !Array.isArray(base[key])
    ) {
      result[key] = deepMerge(base[key], value);
    } else {
      result[key] = value;
    }
  }
  return result;
}

async function route(req, res) {
  const url = new URL(req.url, `http://${req.headers.host || "localhost"}`);
  const pathname = decodeURIComponent(url.pathname);

  if (pathname === "/api/logs/stream") return openSse(res, "logs");
  if (pathname === "/api/events/stream") return openSse(res, "events");
  if (pathname === "/api/flows/stream") return openSse(res, "flows");

  if (pathname === "/api/config" && req.method === "GET") {
    return sendJson(res, 200, publicConfig());
  }

  if (pathname === "/api/config/local-paths" && req.method === "POST") {
    const payload = await readJson(req);
    updateLocalPaths(payload.repos || payload.paths || payload);
    return sendJson(res, 200, publicConfig());
  }

  if (pathname === "/api/services" && req.method === "GET") {
    await refreshHealthForAll();
    return sendJson(res, 200, { services: [...services.values()].map(publicService) });
  }

  if (pathname === "/api/services/start-all" && req.method === "POST") {
    await startAllServices();
    return sendJson(res, 202, { services: [...services.values()].map(publicService) });
  }

  if (pathname === "/api/services/stop-all" && req.method === "POST") {
    await stopAllServices();
    return sendJson(res, 202, { services: [...services.values()].map(publicService) });
  }

  const serviceMatch = pathname.match(/^\/api\/services\/([^/]+)\/(start|stop|logs)$/);
  if (serviceMatch) {
    const [, id, action] = serviceMatch;
    if (action === "start" && req.method === "POST") {
      return sendJson(res, 202, publicService(await startService(id)));
    }
    if (action === "stop" && req.method === "POST") {
      return sendJson(res, 202, publicService(await stopService(id)));
    }
    if (action === "logs" && req.method === "GET") {
      const service = getService(id);
      return sendJson(res, 200, { serviceId: id, lines: service.logs });
    }
  }

  if (pathname === "/api/events" && req.method === "GET") {
    return sendJson(res, 200, { events: events.slice(-500) });
  }

  if (pathname === "/api/events" && req.method === "POST") {
    const payload = await readJson(req);
    const event = recordEvent(payload, { source: "http" });
    return sendJson(res, 202, event);
  }

  const workflowCallbackMatch = pathname.match(/^\/api\/workflow-callbacks\/([^/]+)$/);
  if (workflowCallbackMatch && req.method === "POST") {
    const payload = await readJson(req);
    const callback = recordWorkflowCallback(workflowCallbackMatch[1], payload);
    return sendJson(res, 202, callback);
  }

  if (pathname === "/api/db/summary" && req.method === "GET") {
    const flowId = url.searchParams.get("flowId") || "";
    const flow = flowId ? flows.get(flowId) : null;
    const summary = await inspectDatabase(flow);
    return sendJson(res, 200, summary);
  }

  if (pathname === "/api/db/subprocess" && req.method === "GET") {
    const subprocessCode = String(url.searchParams.get("code") || "").trim().toUpperCase();
    if (!subprocessCode) {
      return sendJson(res, 400, { error: "missing_code", message: "code es requerido" });
    }
    const summary = await inspectDatabase({ subprocessCode });
    return sendJson(res, 200, { ...summary, subprocessCode });
  }

  if (pathname === "/api/flows" && req.method === "GET") {
    const orderedFlows = [...flows.values()].sort((a, b) => {
      const aTime = new Date(a.updatedAt || a.createdAt || 0).getTime();
      const bTime = new Date(b.updatedAt || b.createdAt || 0).getTime();
      return bTime - aTime;
    });
    return sendJson(res, 200, { flows: orderedFlows.map(publicFlow) });
  }

  if (pathname === "/api/flows" && req.method === "POST") {
    const payload = await readJson(req);
    const flow = createFlow(payload);
    runFlow(flow.id).catch((error) => failFlow(flow.id, error));
    return sendJson(res, 202, publicFlow(flow));
  }

  if (pathname === "/api/flows/partial" && req.method === "POST") {
    const payload = await readJson(req);
    const flow = createPartialFlow(payload);
    runPartialFlow(flow.id, payload.action || payload.partialAction).catch((error) => failFlow(flow.id, error));
    return sendJson(res, 202, publicFlow(flow));
  }

  const flowMatch = pathname.match(/^\/api\/flows\/([^/]+)$/);
  if (flowMatch && req.method === "GET") {
    const flow = flows.get(flowMatch[1]);
    if (!flow) return sendJson(res, 404, { error: "not_found" });
    return sendJson(res, 200, publicFlow(flow));
  }

  const proxyMatch = pathname.match(/^\/api\/proxy\/([^/]+)(\/.*)?$/);
  if (proxyMatch) {
    return proxyRequest(req, res, proxyMatch[1], proxyMatch[2] || "/", url.search);
  }

  return serveStatic(pathname, res);
}

function publicConfig() {
  const localPaths = currentLocalPaths();
  return {
    host: config.host,
    port: config.port,
    pollIntervalMs: config.pollIntervalMs,
    repos: config.repos,
    localPaths,
    pathStatus: Object.fromEntries(Object.entries(localPaths).map(([key, value]) => [key, pathExists(value)])),
    localConfigPath: LOCAL_CONFIG_PATH,
    defaultConfigPath: DEFAULT_CONFIG_PATH,
    services: Object.fromEntries(
      [...services.entries()].map(([id, service]) => [
        id,
        {
          label: service.label || id,
          cwd: service.cwd,
          command: service.command,
          url: service.url,
          health: service.health,
          cwdExists: pathExists(service.cwd),
        },
      ]),
    ),
    dbInspector: {
      enabled: Boolean(config.dbInspector?.enabled),
      cwd: config.dbInspector?.cwd || "",
      cwdExists: pathExists(config.dbInspector?.cwd || ""),
    },
  };
}

function currentLocalPaths() {
  return {
    preprocessor: services.get("preprocessor-api")?.cwd || config.repos?.preprocessor || "",
    planner: services.get("planner-api")?.cwd || config.repos?.planner || "",
    interviewer: services.get("interviewer")?.cwd || config.repos?.interviewer || "",
  };
}

function updateLocalPaths(rawPaths = {}) {
  const current = currentLocalPaths();
  const paths = {
    preprocessor: normalizePathValue(rawPaths.preprocessor, current.preprocessor),
    planner: normalizePathValue(rawPaths.planner, current.planner),
    interviewer: normalizePathValue(rawPaths.interviewer, current.interviewer),
  };

  config.repos = { ...(config.repos || {}), ...paths };
  config.dbInspector = { ...(config.dbInspector || {}), cwd: paths.planner };

  const serviceCwds = {
    "preprocessor-api": paths.preprocessor,
    "preprocessor-worker": paths.preprocessor,
    "planner-api": paths.planner,
    "planner-worker": paths.planner,
    interviewer: paths.interviewer,
  };
  for (const [id, cwd] of Object.entries(serviceCwds)) {
    if (config.services?.[id]) config.services[id].cwd = cwd;
    const service = services.get(id);
    if (service) {
      service.cwd = cwd;
      emitSse("flows", { type: "service-updated", service: publicService(service) });
    }
  }

  const patch = {
    repos: paths,
    dbInspector: {
      cwd: paths.planner,
    },
    services: Object.fromEntries(Object.entries(serviceCwds).map(([id, cwd]) => [id, { cwd }])),
  };
  const existing = existsSync(LOCAL_CONFIG_PATH) ? JSON.parse(readFileSync(LOCAL_CONFIG_PATH, "utf8")) : {};
  const nextLocal = deepMerge(existing, patch);
  writeFileSync(LOCAL_CONFIG_PATH, `${JSON.stringify(nextLocal, null, 2)}\n`, "utf8");
}

function normalizePathValue(value, fallback) {
  const normalized = String(value || "").trim();
  if (!normalized) return fallback || "";
  return normalized;
}

function pathExists(value) {
  try {
    return Boolean(value && existsSync(value));
  } catch {
    return false;
  }
}

function createWorkflowCallback(flow, name) {
  const id = `cb_${randomUUID()}`;
  const callback = {
    id,
    flowId: flow.id,
    name,
    createdAt: new Date().toISOString(),
    url: `${callbackBaseUrl()}/api/workflow-callbacks/${id}`,
    callbacks: [],
  };
  workflowCallbacks.set(id, callback);
  return callback;
}

function callbackBaseUrl() {
  return `http://${config.host || "127.0.0.1"}:${config.port || 4400}`;
}

function recordWorkflowCallback(callbackId, payload) {
  const callback = workflowCallbacks.get(callbackId);
  if (!callback) {
    throw new Error(`Unknown workflow callback: ${callbackId}`);
  }
  const item = {
    id: `workflow-callback-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    receivedAt: new Date().toISOString(),
    ...payload,
  };
  callback.callbacks.push(item);
  const flow = flows.get(callback.flowId);
  if (flow) {
    recordIo(flow, {
      stage: `workflow-callback-${callback.name}`,
      node: "workflow-local",
      title: `Callback ${callback.name}`,
      status: String(payload.status || "").toLowerCase() === "failed" ? "failed" : "completed",
      input: {
        callbackId,
        callbackUrl: callback.url,
      },
      output: item,
    });
  }
  return item;
}

async function waitForWorkflowCallbacks(flow, callbackId, expected, timeoutMs) {
  const timeoutAt = Date.now() + timeoutMs;
  let lastCount = 0;
  while (Date.now() < timeoutAt) {
    const callback = workflowCallbacks.get(callbackId);
    const callbacks = callback?.callbacks || [];
    if (callbacks.length !== lastCount) {
      lastCount = callbacks.length;
      step(flow, "workflow-local", "running", `Callbacks recibidos ${callbacks.length}/${expected}`, {
        callbackId,
        expected,
        received: callbacks.length,
      }, { replaceKey: `workflow-callback-wait-${callbackId}` });
    }
    if (callbacks.length >= expected) {
      return callbacks.slice(0, expected);
    }
    await delay(config.pollIntervalMs || 2500);
  }
  throw new Error(`Timeout waiting for workflow callback ${callbackId}: expected=${expected}`);
}

function getService(id) {
  const service = services.get(id);
  if (!service) {
    throw new Error(`Unknown service: ${id}`);
  }
  return service;
}

function shellCommand(command) {
  if (IS_WINDOWS) {
    return {
      file: "powershell.exe",
      args: ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command],
    };
  }
  return {
    file: "/bin/sh",
    args: ["-lc", command],
  };
}

async function stopProcessTree(pid) {
  if (IS_WINDOWS) {
    await new Promise((resolveStop) => {
      const killer = spawn("taskkill.exe", ["/PID", String(pid), "/T", "/F"], {
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      });
      killer.on("exit", resolveStop);
      killer.on("error", resolveStop);
    });
    return;
  }

  try {
    process.kill(-pid, "SIGTERM");
  } catch {
    try {
      process.kill(pid, "SIGTERM");
    } catch {
      return;
    }
  }
  await delay(1500);
  try {
    process.kill(-pid, "SIGKILL");
  } catch {
    try {
      process.kill(pid, "SIGKILL");
    } catch {
      // Already stopped.
    }
  }
}

async function startService(id) {
  const service = getService(id);
  if (service.process && service.status === "running") {
    return service;
  }
  await refreshHealth(service);
  if (!service.process && service.healthStatus === "ok") {
    return service;
  }
  if (!existsSync(service.cwd)) {
    throw new Error(`Service cwd does not exist: ${service.cwd}`);
  }
  mkdirSync(LOG_DIR, { recursive: true });
  appendLog(service, `--- starting ${service.label || id} ---`);
  appendLog(service, `cwd=${service.cwd}`);
  appendLog(service, `command=${service.command}`);

  const shell = shellCommand(service.command);
  const child = spawn(shell.file, shell.args, {
    cwd: service.cwd,
    env: { ...process.env, ...(service.env || {}) },
    windowsHide: IS_WINDOWS,
    detached: !IS_WINDOWS,
    stdio: ["ignore", "pipe", "pipe"],
  });

  service.process = child;
  service.pid = child.pid;
  service.startedAt = new Date().toISOString();
  service.stoppedAt = null;
  service.status = "running";

  child.stdout.on("data", (chunk) => appendLog(service, chunk.toString("utf8"), "stdout"));
  child.stderr.on("data", (chunk) => appendLog(service, chunk.toString("utf8"), "stderr"));
  child.on("exit", (code, signal) => {
    appendLog(service, `--- exited code=${code ?? ""} signal=${signal ?? ""} ---`);
    service.process = null;
    service.pid = null;
    service.status = "stopped";
    service.stoppedAt = new Date().toISOString();
    emitSse("flows", { type: "service-stopped", service: publicService(service) });
  });

  emitSse("flows", { type: "service-started", service: publicService(service) });
  return service;
}

async function stopService(id) {
  const service = getService(id);
  if (!service.pid) {
    await refreshHealth(service);
    if (service.healthStatus !== "ok" && !service.process) {
      service.status = "stopped";
    }
    return service;
  }
  const pid = service.pid;
  appendLog(service, `--- stopping pid=${pid} ---`);
  await stopProcessTree(pid);
  service.process = null;
  service.pid = null;
  service.status = "stopped";
  service.stoppedAt = new Date().toISOString();
  emitSse("flows", { type: "service-stopped", service: publicService(service) });
  return service;
}

async function stopAllServices() {
  for (const id of services.keys()) {
    await stopService(id);
  }
}

async function startAllServices() {
  const orderedGroups = [
    ["preprocessor-api"],
    ["preprocessor-worker"],
    ["planner-api"],
    ["planner-worker"],
    ["interviewer"],
  ];
  for (const group of orderedGroups) {
    await Promise.all(group.map((id) => startServiceUntilHealthy(id, 2)));
  }
}

async function startServiceUntilHealthy(id, attempts = 2) {
  const service = getService(id);
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    await startService(id);
    await waitForServiceHealth(id, 90000).catch(() => service);
    if (service.healthStatus === "ok") {
      return service;
    }
    if (attempt < attempts) {
      appendLog(service, `--- health did not become OK; retrying start attempt ${attempt + 1}/${attempts} ---`);
      await stopService(id);
      await delay(3000);
    }
  }
  return service;
}

async function waitForServiceHealth(id, timeoutMs = 60000) {
  const service = getService(id);
  const timeoutAt = Date.now() + timeoutMs;
  while (Date.now() < timeoutAt) {
    await refreshHealth(service);
    emitSse("flows", { type: "service-health", service: publicService(service) });
    if (service.healthStatus === "ok") return service;
    if (service.status === "stopped") return service;
    await delay(1500);
  }
  return service;
}

function appendLog(service, text, stream = "system") {
  const lines = String(text)
    .replace(/\r/g, "")
    .split("\n")
    .filter((line) => line.length > 0);
  for (const line of lines) {
    const entry = {
      ts: new Date().toISOString(),
      serviceId: service.id,
      serviceLabel: service.label || service.id,
      stream,
      line,
    };
    service.logs.push(entry);
    if (service.logs.length > 800) service.logs.splice(0, service.logs.length - 800);
    appendFileSync(service.logFile, `${entry.ts} [${stream}] ${line}\n`, "utf8");
    emitSse("logs", entry);
  }
}

function publicService(service) {
  return {
    id: service.id,
    label: service.label || service.id,
    cwd: service.cwd,
    url: service.url,
    health: service.health,
    command: service.command,
    status: service.status,
    healthStatus: service.healthStatus,
    pid: service.pid,
    startedAt: service.startedAt,
    stoppedAt: service.stoppedAt,
    logs: service.logs.slice(-40),
  };
}

async function refreshHealthForAll() {
  await Promise.all([...services.values()].map(refreshHealth));
}

async function refreshHealth(service) {
  if (!service.health) {
    service.healthStatus = "unknown";
    return;
  }
  try {
    const response = await fetch(service.health, { signal: AbortSignal.timeout(1800) });
    service.healthStatus = response.ok ? "ok" : `http_${response.status}`;
    if (response.ok) {
      if (!service.process && !service.pid) {
        service.pid = await findListeningPid(service);
      }
      if (service.status === "stopped") {
        service.status = "running";
        service.startedAt = service.startedAt || new Date().toISOString();
        service.stoppedAt = null;
      }
    } else if (!service.process) {
      service.pid = null;
      service.status = "stopped";
    }
  } catch {
    service.healthStatus = "down";
    if (!service.process) {
      service.pid = null;
      service.status = "stopped";
    }
  }
}

async function findListeningPid(service) {
  const port = servicePort(service);
  if (!port) return null;
  const command = IS_WINDOWS
    ? `$conn = Get-NetTCPConnection -LocalPort ${port} -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1; if ($conn) { [Console]::Write($conn.OwningProcess) }`
    : `(command -v lsof >/dev/null 2>&1 && lsof -nP -tiTCP:${port} -sTCP:LISTEN 2>/dev/null | head -n 1) || (command -v ss >/dev/null 2>&1 && ss -ltnp 2>/dev/null | grep -E '[:.]${port}[[:space:]]' | grep -oE 'pid=[0-9]+' | head -n 1 | cut -d= -f2) || true`;
  const shell = shellCommand(command);
  return new Promise((resolvePid) => {
    let output = "";
    const child = spawn(shell.file, shell.args, { windowsHide: IS_WINDOWS, stdio: ["ignore", "pipe", "ignore"] });
    child.stdout.on("data", (chunk) => {
      output += chunk.toString("utf8");
    });
    child.on("exit", () => {
      const pid = Number(output.trim());
      resolvePid(Number.isFinite(pid) && pid > 0 ? pid : null);
    });
    child.on("error", () => resolvePid(null));
  });
}

function servicePort(service) {
  for (const candidate of [service.url, service.health]) {
    if (!candidate) continue;
    try {
      const parsed = new URL(candidate);
      const port = Number(parsed.port);
      if (Number.isFinite(port) && port > 0) return port;
    } catch {
      // Ignore non-URL values from user config.
    }
  }
  return null;
}

function createFlow(payload) {
  const subprocessCode = String(payload.subprocess_code || payload.subprocessCode || "S03808").trim().toUpperCase();
  const id = `flow_${compactTimestamp()}_${subprocessCode}`;
  const flow = {
    id,
    status: "queued",
    subprocessCode,
    correlationId: payload.correlation_id || payload.correlationId || `corr-local-${randomUUID()}`,
    responsibleName: payload.responsible_name || payload.responsibleName || "Responsable del Subproceso",
    responsibleEmail: payload.responsible_email || payload.responsibleEmail || "responsable@empresa.com",
    sourceBucket: payload.source_bucket || payload.sourceBucket || "agente-automatizacion-riesgos",
    includeMof: Boolean(payload.include_mof || payload.includeMof),
    mofCodes: Array.isArray(payload.mof_codes || payload.mofCodes) ? payload.mof_codes || payload.mofCodes : [],
    title: payload.planner_title || payload.title || `Guion de entrevista ${subprocessCode}`,
    objective:
      payload.planner_objective ||
      payload.objective ||
      `Generar guiones de entrevista para el responsable del subproceso ${subprocessCode}`,
    openInterview: Boolean(payload.openInterview),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    startedAt: null,
    completedAt: null,
    preprocessorRunId: null,
    plannerJobId: null,
    plannerPlanRunId: null,
    launchTokens: [],
    currentStep: "queued",
    timeline: [],
    timerRuns: [],
    activeTimers: {},
    ioRecords: [],
    events: [],
    outputs: {},
  };
  flows.set(id, flow);
  saveFlow(flow);
  emitFlow(flow);
  return flow;
}

function createPartialFlow(payload) {
  const flow = createFlow({
    ...payload,
    subprocess_code: payload.subprocess_code || payload.subprocessCode || "S03808",
    correlation_id: payload.correlation_id || payload.correlationId,
  });
  flow.mode = "partial";
  flow.partialAction = payload.action || payload.partialAction || "create-planner-job";
  flow.preprocessorRunId = payload.preprocessorRunId || payload.preprocessor_run_id || null;
  flow.plannerJobId = payload.plannerJobId || payload.job_id || null;
  flow.plannerPlanRunId = payload.plannerPlanRunId || payload.plan_run_id || null;
  flow.launchTokens = payload.launchToken
    ? [
        {
          launchToken: payload.launchToken,
        },
      ]
    : [];
  recordIo(flow, {
    stage: "partial-bootstrap",
    node: "backoffice",
    title: "Datos iniciales ejecucion parcial",
    status: "completed",
    input: payload,
    output: {
      flowId: flow.id,
      action: flow.partialAction,
      subprocessCode: flow.subprocessCode,
      preprocessorRunId: flow.preprocessorRunId,
      plannerJobId: flow.plannerJobId,
      plannerPlanRunId: flow.plannerPlanRunId,
    },
  });
  touchFlow(flow);
  return flow;
}

async function runFlow(flowId) {
  const flow = getFlow(flowId);
  flow.status = "running";
  flow.startedAt = new Date().toISOString();
  step(flow, "backoffice", "running", "Solicitud local creada");

  const preprocessorPayload = {
    subprocess_code: flow.subprocessCode,
    correlation_id: flow.correlationId,
    responsible_name: flow.responsibleName,
    responsible_email: flow.responsibleEmail,
    source_bucket: flow.sourceBucket,
    include_mof: flow.includeMof,
    mof_codes: flow.mofCodes,
    auto_dispatch_planner: false,
    planner_title: flow.title,
    planner_objective: flow.objective,
    options: {
      describe_visual_gaps: true,
      cleanup_source_files: true,
    },
  };

  step(flow, "preprocessor-api", "running", "Creando subprocess run", preprocessorPayload);
  const preprocessorRun = await httpJson("preprocessor-api", "POST", "/v1/subprocess-jobs", preprocessorPayload, {
    timeoutMs: 120000,
  });
  flow.preprocessorRunId = preprocessorRun.id;
  flow.outputs.preprocessorRun = preprocessorRun;
  recordIo(flow, {
    stage: "preprocessor-api-create-run",
    node: "preprocessor-api",
    title: "Crear subprocess run",
    status: "completed",
    input: preprocessorPayload,
    output: preprocessorRun,
  });
  step(flow, "backoffice", "completed", "Solicitud enviada al preprocessor");
  step(flow, "preprocessor-api", "completed", "Subprocess run creado", {
    runId: flow.preprocessorRunId,
    tasks: preprocessorRun.tasks || [],
  });

  step(flow, "cloud-storage", "completed", "Insumos resueltos", preprocessorRun.source_snapshot_json || {});
  recordIo(flow, {
    stage: "cloud-storage-source",
    node: "cloud-storage",
    title: "Resolver insumos documentales",
    status: "completed",
    input: {
      bucket: flow.sourceBucket,
      subprocess_code: flow.subprocessCode,
      include_mof: flow.includeMof,
      mof_codes: flow.mofCodes,
    },
    output: preprocessorRun.source_snapshot_json || {},
  });
  await refreshDbSnapshot(flow);
  const preprocessorStartedEvent = {
    event_type: "preprocessor.run.started",
    producer: "local-flow-lab",
    correlation_id: flow.correlationId,
    subprocess_code: flow.subprocessCode,
    subprocess_run_id: flow.preprocessorRunId,
    status: "processing_documents",
    documents_status: "processing",
  };
  recordIo(flow, {
    stage: "eventarc-preprocessor-start-route",
    node: "eventarc-local",
    title: "Enrutar run.started a Workflow",
    status: "completed",
    input: preprocessorStartedEvent,
    output: {
      trigger: "eventarc-local",
      workflow: "wf-local-preprocessor-run-coordinator",
      nextStage: "workflow-dispatch-preprocessor-tasks",
    },
  });
  step(flow, "eventarc-local", "completed", "Evento preprocessor.run.started enrutado a Workflow local");

  const preprocessorDispatchPayload = {
    event_id: `evt-local-preprocessor-dispatch-${randomUUID()}`,
    correlation_id: flow.correlationId,
    producer: "wf-local-preprocessor-run-coordinator",
    occurred_at: new Date().toISOString(),
  };
  const preprocessorCallback = createWorkflowCallback(flow, "preprocessor-document-tasks");
  preprocessorDispatchPayload.callbackUrl = preprocessorCallback.url;
  step(flow, "workflow-local", "running", "Solicitando dispatch de tasks documentales", preprocessorDispatchPayload);
  const preprocessorDispatchResponse = await httpJson(
    "preprocessor-api",
    "POST",
    `/internal/subprocess-runs/${flow.preprocessorRunId}/dispatch-tasks`,
    preprocessorDispatchPayload,
    { timeoutMs: 60000 },
  );
  step(flow, "workflow-local", "completed", "Workflow local solicito dispatch documental", preprocessorDispatchResponse);
  recordIo(flow, {
    stage: "workflow-dispatch-preprocessor-tasks",
    node: "workflow-local",
    title: "Workflow solicita Cloud Tasks documentales",
    status: "completed",
    input: preprocessorDispatchPayload,
    output: preprocessorDispatchResponse,
  });
  recordIo(flow, {
    stage: "cloud-tasks-preprocessor-dispatch",
    node: "cloud-tasks-preprocessor",
    title: "Dispatch local de tasks documentales",
    status: "completed",
    input: {
      subprocess_run_id: flow.preprocessorRunId,
      tasks: preprocessorRun.tasks || [],
    },
    output: {
      dispatch_backend: "workflow_managed_local_http",
      dispatched: Array.isArray(preprocessorRun.tasks) ? preprocessorRun.tasks.length : null,
      workerUrl: serviceUrl("preprocessor-worker", "/internal/tasks/process"),
    },
  });
  step(flow, "cloud-tasks-preprocessor", "running", "Tasks documentales despachadas localmente");
  step(flow, "preprocessor-worker", "running", "Procesando documentos");

  const expectedPreprocessorCallbacks = Number(
    preprocessorDispatchResponse.total_tasks || (Array.isArray(preprocessorRun.tasks) ? preprocessorRun.tasks.length : 0),
  );
  if (expectedPreprocessorCallbacks > 0) {
    step(flow, "workflow-local", "running", "Esperando callbacks de tasks documentales", {
      callbackId: preprocessorCallback.id,
      expected: expectedPreprocessorCallbacks,
    });
    const taskCallbacks = await waitForWorkflowCallbacks(
      flow,
      preprocessorCallback.id,
      expectedPreprocessorCallbacks,
      Number(config.flowTimeoutMinutes || 120) * 60 * 1000,
    );
    recordIo(flow, {
      stage: "workflow-preprocessor-task-callbacks",
      node: "workflow-local",
      title: "Callbacks recibidos desde Preprocessor worker",
      status: taskCallbacks.some((item) => item.status === "failed") ? "warning" : "completed",
      input: {
        callbackUrl: preprocessorCallback.url,
        expected: expectedPreprocessorCallbacks,
      },
      output: {
        received: taskCallbacks.length,
        callbacks: taskCallbacks,
      },
    });
    step(flow, "workflow-local", "completed", "Callbacks documentales recibidos", {
      received: taskCallbacks.length,
      expected: expectedPreprocessorCallbacks,
    });
  }

  const preprocessorStatus = await waitForPreprocessor(flow);
  step(flow, "cloud-tasks-preprocessor", "completed", "Tasks documentales consumidas por worker local");
  flow.outputs.preprocessorStatus = preprocessorStatus;
  recordIo(flow, {
    stage: "preprocessor-worker-result",
    node: "preprocessor-worker",
    title: "Resultado procesamiento documental",
    status: ["completed", "partial"].includes(preprocessorStatus.documents_status) ? "completed" : "failed",
    input: {
      subprocess_run_id: flow.preprocessorRunId,
      poll_url: serviceUrl("preprocessor-api", `/v1/subprocess-jobs/${flow.preprocessorRunId}/status`),
    },
    output: preprocessorStatus,
  });
  const documentsStatus = preprocessorStatus.documents_status;
  if (!["completed", "partial"].includes(documentsStatus)) {
    const failedEvent = recordEvent({
      event_type: "preprocessor.run.failed",
      producer: "local-flow-lab",
      correlation_id: flow.correlationId,
      subprocess_code: flow.subprocessCode,
      subprocess_run_id: flow.preprocessorRunId,
      status: preprocessorStatus.status,
      documents_status: documentsStatus,
    });
    recordIo(flow, {
      stage: "pubsub-preprocessor-event",
      node: "pubsub-local",
      title: "Publicar evento documental fallido",
      status: "failed",
      input: preprocessorStatus,
      output: failedEvent,
    });
    throw new Error(`Preprocessor finished with documents_status=${documentsStatus}`);
  }

  const preprocessorEventType = documentsStatus === "partial" ? "preprocessor.run.partial" : "preprocessor.run.completed";
  const preprocessorEvent = recordEvent({
    event_type: preprocessorEventType,
    producer: "local-flow-lab",
    correlation_id: flow.correlationId,
    subprocess_code: flow.subprocessCode,
    subprocess_run_id: flow.preprocessorRunId,
    status: preprocessorStatus.status,
    documents_status: documentsStatus,
  });
  recordIo(flow, {
    stage: "pubsub-preprocessor-event",
    node: "pubsub-local",
    title: "Publicar evento documental",
    status: "completed",
    input: {
      event_type: preprocessorEventType,
      preprocessorStatus,
    },
    output: preprocessorEvent,
  });
  recordAwsNotification(
    flow,
    "preprocessing_documents",
    documentsStatus,
    preprocessorEventType,
    "En produccion Workflows podria traducir el cierre documental a una notificacion de avance hacia AWS.",
  );
  await refreshDbSnapshot(flow);

  step(flow, "pubsub-local", "completed", `Evento ${preprocessorEventType}`);
  step(flow, "eventarc-local", "completed", "Evento enrutado a wf-local-planner-coordinator");
  recordIo(flow, {
    stage: "eventarc-preprocessor-route",
    node: "eventarc-local",
    title: "Enrutar evento documental a Workflow",
    status: "completed",
    input: preprocessorEvent,
    output: {
      trigger: "eventarc-local",
      workflow: "wf-local-planner-coordinator",
      nextStage: "planner-api-create-job",
    },
  });

  const plannerPayload = {
    eventId: `evt-local-planner-create-${randomUUID()}`,
    correlationId: flow.correlationId,
    title: flow.title,
    objective: flow.objective,
    processCode: flow.subprocessCode,
    preprocessorRunId: flow.preprocessorRunId,
    includeGeneralContext: true,
    generalContextStrategy: "full_document_ai",
  };

  step(flow, "workflow-local", "running", "Creando job Planner", plannerPayload);
  step(flow, "planner-api", "running", "Recibiendo solicitud de job Planner", plannerPayload);
  const plannerAccepted = await httpJson("planner-api", "POST", "/api/interview-jobs", plannerPayload, {
    timeoutMs: 120000,
  });
  flow.plannerJobId = plannerAccepted.jobId;
  flow.outputs.plannerAccepted = plannerAccepted;
  recordIo(flow, {
    stage: "workflow-create-planner-job",
    node: "workflow-local",
    title: "Workflow crea job Planner",
    status: "completed",
    input: preprocessorEvent,
    output: {
      request: plannerPayload,
      response: plannerAccepted,
    },
  });
  recordIo(flow, {
    stage: "planner-api-create-job",
    node: "planner-api",
    title: "Crear Planner job",
    status: "completed",
    input: plannerPayload,
    output: plannerAccepted,
  });
  step(flow, "planner-api", "completed", "Planner job creado", plannerAccepted);
  step(flow, "workflow-local", "completed", "Workflow local creo el job Planner", plannerAccepted);
  recordAwsNotification(
    flow,
    "planning_interview",
    "queued",
    "planner.job.created",
    "En produccion el job de Planner queda trazado; Workflows decide si notifica avance externo.",
  );
  await refreshDbSnapshot(flow);

  const plannerLinkedPayload = {
    event_id: `evt-local-planner-linked-${randomUUID()}`,
    correlation_id: flow.correlationId,
    producer: "local-flow-lab",
    planner_job_id: flow.plannerJobId,
    planner_status: "queued",
  };
  step(flow, "preprocessor-api", "running", "Marcando run enlazado con Planner", plannerLinkedPayload);
  const plannerLinkedResponse = await httpJson(
    "preprocessor-api",
    "POST",
    `/internal/subprocess-runs/${flow.preprocessorRunId}/planner-linked`,
    plannerLinkedPayload,
    { timeoutMs: 60000 },
  );
  recordIo(flow, {
    stage: "preprocessor-planner-linked",
    node: "preprocessor-api",
    title: "Enlazar preprocessor con Planner",
    status: "completed",
    input: plannerLinkedPayload,
    output: plannerLinkedResponse,
  });
  step(flow, "preprocessor-api", "completed", "Run enlazado con Planner", plannerLinkedResponse);

  step(flow, "cloud-tasks-planner", "running", "Despachando job al Planner worker");
  const plannerCallback = createWorkflowCallback(flow, "planner-worker");
  const plannerWorkerPayload = {
    jobId: flow.plannerJobId,
    eventId: `evt-local-planner-task-${randomUUID()}`,
    correlationId: flow.correlationId,
    callbackUrl: plannerCallback.url,
  };
  const dispatchPromise = httpJson(
    "planner-worker",
    "POST",
    "/internal/interview-jobs/process",
    plannerWorkerPayload,
    { timeoutMs: Number(config.flowTimeoutMinutes || 120) * 60 * 1000 },
  ).catch((error) => {
    step(flow, "planner-worker", "failed", "Planner worker devolvio error", { error: error.message });
    throw error;
  });
  recordIo(flow, {
    stage: "cloud-tasks-planner-dispatch",
    node: "cloud-tasks-planner",
    title: "Dispatch local a Planner worker",
    status: "completed",
    input: plannerWorkerPayload,
    output: {
      dispatched: true,
      workerUrl: serviceUrl("planner-worker", "/internal/interview-jobs/process"),
    },
  });
  step(flow, "cloud-tasks-planner", "completed", "Solicitud local enviada al Planner worker");

  step(flow, "workflow-local", "running", "Esperando callback del Planner worker", {
    callbackId: plannerCallback.id,
  });
  const plannerCallbacks = await waitForWorkflowCallbacks(
    flow,
    plannerCallback.id,
    1,
    Number(config.flowTimeoutMinutes || 120) * 60 * 1000,
  );
  recordIo(flow, {
    stage: "workflow-planner-worker-callback",
    node: "workflow-local",
    title: "Callback recibido desde Planner worker",
    status: plannerCallbacks[0]?.status === "completed" ? "completed" : "failed",
    input: {
      callbackUrl: plannerCallback.url,
      jobId: flow.plannerJobId,
    },
    output: plannerCallbacks[0],
  });
  step(flow, "workflow-local", "completed", "Callback Planner recibido", plannerCallbacks[0]);

  const plannerStatus = await waitForPlanner(flow);
  await dispatchPromise.catch(() => null);
  flow.outputs.plannerStatus = plannerStatus;
  recordIo(flow, {
    stage: "planner-worker-result",
    node: "planner-worker",
    title: "Resultado Planner worker",
    status: plannerStatus.status === "completed" ? "completed" : "failed",
    input: plannerWorkerPayload,
    output: plannerStatus,
  });

  if (plannerStatus.status !== "completed") {
    const plannerFailedEvent = recordEvent({
      event_type: "planner.job.failed",
      producer: "local-flow-lab",
      correlation_id: flow.correlationId,
      subprocess_code: flow.subprocessCode,
      subprocess_run_id: flow.preprocessorRunId,
      job_id: flow.plannerJobId,
      status: plannerStatus.status,
      stage: plannerStatus.stage,
      error: plannerStatus.error,
    });
    recordIo(flow, {
      stage: "pubsub-planner-event",
      node: "pubsub-local",
      title: "Publicar evento Planner fallido",
      status: "failed",
      input: plannerStatus,
      output: plannerFailedEvent,
    });
    throw new Error(`Planner finished with status=${plannerStatus.status}`);
  }

  flow.plannerPlanRunId = plannerStatus.planRunId || null;
  const plannerCompletedEvent = recordEvent({
    event_type: "planner.job.completed",
    producer: "local-flow-lab",
    correlation_id: flow.correlationId,
    subprocess_code: flow.subprocessCode,
    subprocess_run_id: flow.preprocessorRunId,
    job_id: flow.plannerJobId,
    plan_run_id: flow.plannerPlanRunId,
    status: plannerStatus.status,
    stage: plannerStatus.stage,
  });
  recordIo(flow, {
    stage: "pubsub-planner-event",
    node: "pubsub-local",
    title: "Publicar evento Planner",
    status: "completed",
    input: plannerStatus,
    output: plannerCompletedEvent,
  });
  recordIo(flow, {
    stage: "eventarc-planner-route",
    node: "eventarc-local",
    title: "Enrutar evento Planner a Workflow",
    status: "completed",
    input: plannerCompletedEvent,
    output: {
      trigger: "eventarc-local",
      workflow: "wf-local-planner-completion",
      nextStage: "preprocessor-planner-finalized",
    },
  });
  recordAwsNotification(
    flow,
    "planning_interview",
    "completed",
    "planner.job.completed",
    "En produccion Workflows consume planner.job.completed y solicita notificacion de etapa hacia AWS.",
  );

  const finalizedPayload = {
    event_id: `evt-local-planner-finalized-${randomUUID()}`,
    correlation_id: flow.correlationId,
    producer: "local-flow-lab",
    planner_status: "completed",
    planner_result_ref: serviceUrl("planner-api", `/api/interview-jobs/${flow.plannerJobId}/result`),
  };
  if (flow.plannerPlanRunId) {
    finalizedPayload.planner_plan_run_id = flow.plannerPlanRunId;
  }
  step(flow, "preprocessor-api", "running", "Marcando run finalizado por Planner", finalizedPayload);
  const finalizedResponse = await httpJson(
    "preprocessor-api",
    "POST",
    `/internal/subprocess-runs/${flow.preprocessorRunId}/planner-finalized`,
    finalizedPayload,
    {
      timeoutMs: 60000,
    },
  );
  recordIo(flow, {
    stage: "preprocessor-planner-finalized",
    node: "preprocessor-api",
    title: "Finalizar preprocessor desde Planner",
    status: "completed",
    input: finalizedPayload,
    output: finalizedResponse,
  });
  step(flow, "preprocessor-api", "completed", "Preprocessor marcado como finalizado por Planner", finalizedResponse);
  await refreshDbSnapshot(flow);

  try {
    step(flow, "planner-api", "running", "Cargando resultado Planner");
    flow.outputs.plannerResult = await httpJson("planner-api", "GET", `/api/interview-jobs/${flow.plannerJobId}/result`, null, {
      timeoutMs: 60000,
    });
    recordIo(flow, {
      stage: "planner-result",
      node: "planner-api",
      title: "Leer resultado Planner",
      status: "completed",
      input: {
        job_id: flow.plannerJobId,
        url: serviceUrl("planner-api", `/api/interview-jobs/${flow.plannerJobId}/result`),
      },
      output: flow.outputs.plannerResult,
    });
    step(flow, "planner-api", "completed", "Resultado Planner cargado");
  } catch (error) {
    recordIo(flow, {
      stage: "planner-result",
      node: "planner-api",
      title: "Leer resultado Planner",
      status: "warning",
      input: {
        job_id: flow.plannerJobId,
        url: serviceUrl("planner-api", `/api/interview-jobs/${flow.plannerJobId}/result`),
      },
      error: error instanceof Error ? error.message : String(error),
    });
    step(flow, "planner-api", "warning", "No se pudo cargar resultado Planner", { error: error.message });
  }

  step(flow, "interviewer", "running", "Buscando launch tokens");
  const launchesResponse = await httpJson("interviewer", "GET", "/api/planner-interviews?limit=200", null, {
    timeoutMs: 60000,
  });
  const allLaunches = launchesResponse.items || [];
  const matchingLaunches = allLaunches.filter((item) => {
    if (flow.plannerJobId) return item.job_id === flow.plannerJobId;
    if (flow.preprocessorRunId) return item.subprocess_run_id === flow.preprocessorRunId;
    return String(item.process_code || "").toUpperCase() === flow.subprocessCode;
  });
  flow.launchTokens = matchingLaunches.map((item) => ({
    launchToken: item.launch_token,
    launchUrl: serviceUrl("interviewer", item.launch_url || `/planner-interviews/${encodeURIComponent(item.launch_token)}`),
    apiUrl: serviceUrl("interviewer", item.api_url || `/api/planner-interviews/${encodeURIComponent(item.launch_token)}`),
    status: item.launch_status,
    intervieweeName: item.interviewee_name,
    intervieweeEmail: item.interviewee_email,
  }));
  flow.outputs.launches = matchingLaunches;
  recordIo(flow, {
    stage: "interviewer-launches",
    node: "interviewer",
    title: "Resolver launches Interviewer",
    status: "completed",
    input: {
      job_id: flow.plannerJobId,
      subprocess_run_id: flow.preprocessorRunId,
      subprocess_code: flow.subprocessCode,
      url: serviceUrl("interviewer", "/api/planner-interviews?limit=200"),
    },
    output: {
      matched: matchingLaunches,
      launchLinks: flow.launchTokens,
    },
  });
  step(flow, "interviewer", "completed", "Launch tokens resueltos", flow.launchTokens);
  recordAwsNotification(
    flow,
    "interview_ready",
    "ready",
    "interview.launch.ready",
    "En produccion AWS podria recibir que la entrevista ya tiene URL/token disponible.",
  );

  step(flow, "aws-notification-local", "completed", "Notificacion AWS simulada");

  flow.status = "completed";
  flow.currentStep = "completed";
  flow.completedAt = new Date().toISOString();
  touchFlow(flow);
}

async function runPartialFlow(flowId, action) {
  const flow = getFlow(flowId);
  flow.status = "running";
  flow.startedAt = new Date().toISOString();
  const selectedAction = action || flow.partialAction || "create-planner-job";
  step(flow, "backoffice", "running", `Ejecucion parcial: ${selectedAction}`, {
    action: selectedAction,
    subprocessCode: flow.subprocessCode,
    preprocessorRunId: flow.preprocessorRunId,
    plannerJobId: flow.plannerJobId,
    plannerPlanRunId: flow.plannerPlanRunId,
  });

  if (selectedAction === "wait-preprocessor") {
    requireFlowField(flow, "preprocessorRunId", "subprocess_run_id es requerido");
    step(flow, "preprocessor-worker", "running", "Leyendo estado de preprocessor existente");
    const preprocessorStatus = await waitForPreprocessor(flow);
    flow.outputs.preprocessorStatus = preprocessorStatus;
    recordIo(flow, {
      stage: "wait-preprocessor",
      node: "preprocessor-worker",
      title: "Estado de preprocessor existente",
      status: "completed",
      input: {
        subprocess_run_id: flow.preprocessorRunId,
      },
      output: preprocessorStatus,
    });
    step(flow, "preprocessor-worker", "completed", "Estado preprocessor resuelto", preprocessorStatus);
  } else if (selectedAction === "create-planner-job") {
    requireFlowField(flow, "preprocessorRunId", "subprocess_run_id es requerido");
    await createPlannerJobFromFlow(flow);
    await markPlannerLinkedFromFlow(flow);
  } else if (selectedAction === "process-planner-worker") {
    requireFlowField(flow, "plannerJobId", "planner_job_id es requerido");
    await processPlannerWorkerFromFlow(flow);
  } else if (selectedAction === "finalize-preprocessor") {
    requireFlowField(flow, "preprocessorRunId", "subprocess_run_id es requerido");
    requireFlowField(flow, "plannerJobId", "planner_job_id es requerido");
    await finalizePreprocessorFromFlow(flow);
  } else if (selectedAction === "fetch-planner-result") {
    requireFlowField(flow, "plannerJobId", "planner_job_id es requerido");
    await fetchPlannerResultFromFlow(flow);
  } else if (selectedAction === "resolve-launches") {
    await resolveLaunchesFromFlow(flow);
  } else {
    throw new Error(`Accion parcial no soportada: ${selectedAction}`);
  }

  step(flow, "backoffice", "completed", `Ejecucion parcial completada: ${selectedAction}`);
  flow.status = "completed";
  flow.currentStep = "completed";
  flow.completedAt = new Date().toISOString();
  touchFlow(flow);
}

function requireFlowField(flow, field, message) {
  if (!flow[field]) {
    throw new Error(message || `${field} es requerido`);
  }
}

async function createPlannerJobFromFlow(flow) {
  const plannerPayload = {
    eventId: `evt-local-planner-create-${randomUUID()}`,
    correlationId: flow.correlationId,
    title: flow.title,
    objective: flow.objective,
    processCode: flow.subprocessCode,
    preprocessorRunId: flow.preprocessorRunId,
    includeGeneralContext: true,
    generalContextStrategy: "full_document_ai",
  };
  step(flow, "workflow-local", "running", "Creando job Planner desde datos existentes", plannerPayload);
  step(flow, "planner-api", "running", "Recibiendo solicitud de job Planner", plannerPayload);
  const plannerAccepted = await httpJson("planner-api", "POST", "/api/interview-jobs", plannerPayload, {
    timeoutMs: 120000,
  });
  flow.plannerJobId = plannerAccepted.jobId;
  flow.outputs.plannerAccepted = plannerAccepted;
  recordIo(flow, {
    stage: "planner-api-create-job",
    node: "planner-api",
    title: "Crear Planner job",
    status: "completed",
    input: plannerPayload,
    output: plannerAccepted,
  });
  step(flow, "planner-api", "completed", "Planner job creado", plannerAccepted);
  step(flow, "workflow-local", "completed", "Workflow local creo el job Planner", plannerAccepted);
  recordAwsNotification(
    flow,
    "planning_interview",
    "queued",
    "planner.job.created",
    "Ejecucion parcial: job de Planner creado desde datos existentes.",
  );
  await refreshDbSnapshot(flow);
  return plannerAccepted;
}

async function markPlannerLinkedFromFlow(flow) {
  if (!flow.preprocessorRunId || !flow.plannerJobId) return null;
  const plannerLinkedPayload = {
    event_id: `evt-local-planner-linked-${randomUUID()}`,
    correlation_id: flow.correlationId,
    producer: "local-flow-lab",
    planner_job_id: flow.plannerJobId,
    planner_status: "queued",
  };
  step(flow, "preprocessor-api", "running", "Marcando run enlazado con Planner", plannerLinkedPayload);
  const response = await httpJson(
    "preprocessor-api",
    "POST",
    `/internal/subprocess-runs/${flow.preprocessorRunId}/planner-linked`,
    plannerLinkedPayload,
    { timeoutMs: 60000 },
  );
  recordIo(flow, {
    stage: "preprocessor-planner-linked",
    node: "preprocessor-api",
    title: "Enlazar preprocessor con Planner",
    status: "completed",
    input: plannerLinkedPayload,
    output: response,
  });
  step(flow, "preprocessor-api", "completed", "Run enlazado con Planner", response);
  return response;
}

async function processPlannerWorkerFromFlow(flow) {
  step(flow, "cloud-tasks-planner", "running", "Despachando job al Planner worker");
  const plannerCallback = createWorkflowCallback(flow, "planner-worker-partial");
  const workerPayload = {
    jobId: flow.plannerJobId,
    eventId: `evt-local-planner-task-${randomUUID()}`,
    correlationId: flow.correlationId,
    callbackUrl: plannerCallback.url,
  };
  const dispatchPromise = httpJson("planner-worker", "POST", "/internal/interview-jobs/process", workerPayload, {
    timeoutMs: Number(config.flowTimeoutMinutes || 120) * 60 * 1000,
  }).catch((error) => {
    step(flow, "planner-worker", "failed", "Planner worker devolvio error", { error: error.message });
    throw error;
  });
  recordIo(flow, {
    stage: "cloud-tasks-planner-dispatch",
    node: "cloud-tasks-planner",
    title: "Dispatch local a Planner worker",
    status: "completed",
    input: workerPayload,
    output: {
      dispatched: true,
      workerUrl: serviceUrl("planner-worker", "/internal/interview-jobs/process"),
    },
  });
  step(flow, "cloud-tasks-planner", "completed", "Solicitud local enviada al Planner worker");
  step(flow, "workflow-local", "running", "Esperando callback del Planner worker", {
    callbackId: plannerCallback.id,
  });
  const plannerCallbacks = await waitForWorkflowCallbacks(
    flow,
    plannerCallback.id,
    1,
    Number(config.flowTimeoutMinutes || 120) * 60 * 1000,
  );
  recordIo(flow, {
    stage: "workflow-planner-worker-callback",
    node: "workflow-local",
    title: "Callback recibido desde Planner worker",
    status: plannerCallbacks[0]?.status === "completed" ? "completed" : "failed",
    input: {
      callbackUrl: plannerCallback.url,
      jobId: flow.plannerJobId,
    },
    output: plannerCallbacks[0],
  });
  step(flow, "workflow-local", "completed", "Callback Planner recibido", plannerCallbacks[0]);
  const plannerStatus = await waitForPlanner(flow);
  await dispatchPromise.catch(() => null);
  flow.outputs.plannerStatus = plannerStatus;
  flow.plannerPlanRunId = plannerStatus.planRunId || flow.plannerPlanRunId || null;
  recordIo(flow, {
    stage: "planner-worker-result",
    node: "planner-worker",
    title: "Resultado Planner worker",
    status: plannerStatus.status === "completed" ? "completed" : "failed",
    input: workerPayload,
    output: plannerStatus,
  });
  if (plannerStatus.status !== "completed") {
    throw new Error(`Planner finished with status=${plannerStatus.status}`);
  }
  recordEvent({
    event_type: "planner.job.completed",
    producer: "local-flow-lab",
    correlation_id: flow.correlationId,
    subprocess_code: flow.subprocessCode,
    subprocess_run_id: flow.preprocessorRunId,
    job_id: flow.plannerJobId,
    plan_run_id: flow.plannerPlanRunId,
    status: plannerStatus.status,
    stage: plannerStatus.stage,
  });
  recordAwsNotification(
    flow,
    "planning_interview",
    "completed",
    "planner.job.completed",
    "Ejecucion parcial: Planner worker completo y listo para notificar avance.",
  );
  await refreshDbSnapshot(flow);
  return plannerStatus;
}

async function finalizePreprocessorFromFlow(flow) {
  const finalizedPayload = {
    event_id: `evt-local-planner-finalized-${randomUUID()}`,
    correlation_id: flow.correlationId,
    producer: "local-flow-lab",
    planner_status: "completed",
    planner_result_ref: flow.plannerJobId ? serviceUrl("planner-api", `/api/interview-jobs/${flow.plannerJobId}/result`) : null,
  };
  if (flow.plannerPlanRunId) {
    finalizedPayload.planner_plan_run_id = flow.plannerPlanRunId;
  }
  step(flow, "preprocessor-api", "running", "Marcando run finalizado por Planner", finalizedPayload);
  const response = await httpJson(
    "preprocessor-api",
    "POST",
    `/internal/subprocess-runs/${flow.preprocessorRunId}/planner-finalized`,
    finalizedPayload,
    { timeoutMs: 60000 },
  );
  recordIo(flow, {
    stage: "preprocessor-planner-finalized",
    node: "preprocessor-api",
    title: "Finalizar preprocessor desde Planner",
    status: "completed",
    input: finalizedPayload,
    output: response,
  });
  step(flow, "preprocessor-api", "completed", "Preprocessor marcado como finalizado por Planner", response);
  await refreshDbSnapshot(flow);
  return response;
}

async function fetchPlannerResultFromFlow(flow) {
  step(flow, "planner-api", "running", "Cargando resultado Planner");
  const result = await httpJson("planner-api", "GET", `/api/interview-jobs/${flow.plannerJobId}/result`, null, {
    timeoutMs: 60000,
  });
  flow.outputs.plannerResult = result;
  recordIo(flow, {
    stage: "planner-result",
    node: "planner-api",
    title: "Leer resultado Planner",
    status: "completed",
    input: {
      job_id: flow.plannerJobId,
      url: serviceUrl("planner-api", `/api/interview-jobs/${flow.plannerJobId}/result`),
    },
    output: result,
  });
  step(flow, "planner-api", "completed", "Resultado Planner cargado");
  return result;
}

async function resolveLaunchesFromFlow(flow) {
  step(flow, "interviewer", "running", "Buscando launch tokens");
  const launchesResponse = await httpJson("interviewer", "GET", "/api/planner-interviews?limit=200", null, {
    timeoutMs: 60000,
  });
  const allLaunches = launchesResponse.items || [];
  const matchingLaunches = allLaunches.filter((item) => {
    return (
      item.job_id === flow.plannerJobId ||
      item.subprocess_run_id === flow.preprocessorRunId ||
      String(item.process_code || "").toUpperCase() === flow.subprocessCode
    );
  });
  flow.launchTokens = matchingLaunches.map((item) => ({
    launchToken: item.launch_token,
    launchUrl: serviceUrl("interviewer", item.launch_url || `/planner-interviews/${encodeURIComponent(item.launch_token)}`),
    apiUrl: serviceUrl("interviewer", item.api_url || `/api/planner-interviews/${encodeURIComponent(item.launch_token)}`),
    status: item.launch_status,
    intervieweeName: item.interviewee_name,
    intervieweeEmail: item.interviewee_email,
  }));
  flow.outputs.launches = matchingLaunches;
  recordIo(flow, {
    stage: "interviewer-launches",
    node: "interviewer",
    title: "Resolver launches Interviewer",
    status: "completed",
    input: {
      job_id: flow.plannerJobId,
      subprocess_run_id: flow.preprocessorRunId,
      subprocess_code: flow.subprocessCode,
      url: serviceUrl("interviewer", "/api/planner-interviews?limit=200"),
    },
    output: {
      matched: matchingLaunches,
      launchLinks: flow.launchTokens,
    },
  });
  step(flow, "interviewer", "completed", "Launch tokens resueltos", flow.launchTokens);
  recordAwsNotification(
    flow,
    "interview_ready",
    "ready",
    "interview.launch.ready",
    "Ejecucion parcial: la entrevista ya tiene URL/token disponible.",
  );
  await refreshDbSnapshot(flow);
  return matchingLaunches;
}

async function waitForPreprocessor(flow) {
  const timeoutAt = Date.now() + Number(config.flowTimeoutMinutes || 120) * 60 * 1000;
  let lastStatus = null;
  while (Date.now() < timeoutAt) {
    const status = await httpJson("preprocessor-api", "GET", `/v1/subprocess-jobs/${flow.preprocessorRunId}/status`, null, {
      timeoutMs: 30000,
    });
    lastStatus = status;
    step(flow, "preprocessor-worker", "running", `Documentos: ${status.documents_status}`, status, { replaceKey: "preprocessor-status" });
    if (["completed", "partial", "failed"].includes(status.documents_status) || status.status === "failed") {
      step(flow, "preprocessor-worker", status.documents_status === "failed" ? "failed" : "completed", "Procesamiento documental terminado", status);
      return status;
    }
    await delay(config.pollIntervalMs || 2500);
  }
  throw new Error(`Timeout waiting for preprocessor run ${flow.preprocessorRunId}. Last status: ${JSON.stringify(lastStatus)}`);
}

async function waitForPlanner(flow) {
  const timeoutAt = Date.now() + Number(config.flowTimeoutMinutes || 120) * 60 * 1000;
  let lastStatus = null;
  while (Date.now() < timeoutAt) {
    const status = await httpJson("planner-api", "GET", `/api/interview-jobs/${flow.plannerJobId}/status`, null, {
      timeoutMs: 30000,
    });
    lastStatus = status;
    step(flow, "planner-worker", "running", `Planner: ${status.stage} ${status.progress?.percent ?? 0}%`, status, {
      replaceKey: "planner-status",
    });
    if (["completed", "failed", "cancelled"].includes(status.status)) {
      step(flow, "planner-worker", status.status === "completed" ? "completed" : "failed", "Planner terminado", status);
      return status;
    }
    await delay(config.pollIntervalMs || 2500);
  }
  throw new Error(`Timeout waiting for planner job ${flow.plannerJobId}. Last status: ${JSON.stringify(lastStatus)}`);
}

function step(flow, node, status, label, payload = null, options = {}) {
  const now = new Date().toISOString();
  const entry = {
    id: options.replaceKey || `${node}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    ts: now,
    node,
    status,
    label,
    payload,
  };
  if (options.replaceKey) {
    const index = flow.timeline.findIndex((item) => item.id === options.replaceKey);
    if (index >= 0) {
      entry.startedAt = flow.timeline[index].startedAt || flow.timeline[index].ts;
      flow.timeline[index] = entry;
    } else {
      entry.startedAt = now;
      flow.timeline.push(entry);
    }
  } else {
    entry.startedAt = now;
    flow.timeline.push(entry);
  }
  updateFlowTimer(flow, entry, options);
  flow.currentStep = node;
  touchFlow(flow);
}

function recordIo(flow, record = {}) {
  flow.ioRecords = Array.isArray(flow.ioRecords) ? flow.ioRecords : [];
  const entry = {
    id: record.id || `io-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    ts: record.ts || new Date().toISOString(),
    stage: record.stage || record.node || "unknown",
    node: record.node || "flow-lab",
    title: record.title || record.stage || "Input / output",
    status: record.status || "completed",
    input: record.input ?? null,
    output: record.output ?? null,
    error: record.error ?? null,
    meta: record.meta || {},
  };
  flow.ioRecords.push(entry);
  if (flow.ioRecords.length > 250) flow.ioRecords.splice(0, flow.ioRecords.length - 250);
  touchFlow(flow);
  return entry;
}

function updateFlowTimer(flow, entry, options = {}) {
  flow.timerRuns = Array.isArray(flow.timerRuns) ? flow.timerRuns : [];
  flow.activeTimers = flow.activeTimers && typeof flow.activeTimers === "object" ? flow.activeTimers : {};
  const key = options.timerKey || entry.node;
  if (entry.status === "running") {
    const active = flow.activeTimers[key];
    if (active) {
      active.label = entry.label;
      active.updatedAt = entry.ts;
      active.payload = entry.payload;
      return;
    }
    flow.activeTimers[key] = {
      id: `${key}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
      key,
      node: entry.node,
      label: entry.label,
      status: "running",
      startedAt: entry.startedAt || entry.ts,
      updatedAt: entry.ts,
      payload: entry.payload,
    };
    return;
  }

  if (!isTimerTerminalStatus(entry.status)) {
    return;
  }

  const active = flow.activeTimers[key];
  if (active) {
    const startedAt = active.startedAt || entry.startedAt || entry.ts;
    const endedAt = entry.ts;
    flow.timerRuns.push({
      ...active,
      label: entry.label || active.label,
      status: entry.status,
      endedAt,
      updatedAt: endedAt,
      durationMs: Math.max(0, new Date(endedAt).getTime() - new Date(startedAt).getTime()),
      payload: entry.payload ?? active.payload,
    });
    delete flow.activeTimers[key];
    return;
  }

  flow.timerRuns.push({
    id: `${key}-${Date.now()}-${Math.random().toString(16).slice(2)}`,
    key,
    node: entry.node,
    label: entry.label,
    status: entry.status,
    startedAt: entry.startedAt || entry.ts,
    endedAt: entry.ts,
    updatedAt: entry.ts,
    durationMs: 0,
    payload: entry.payload,
  });
}

function isTimerTerminalStatus(status) {
  return ["completed", "failed", "warning", "cancelled"].includes(status);
}

function failFlow(flowId, error) {
  const flow = flows.get(flowId);
  if (!flow) return;
  for (const active of Object.values(flow.activeTimers || {})) {
    step(flow, active.node || active.key, "failed", `Interrumpido: ${active.label || active.key}`, {
      error: error instanceof Error ? error.message : String(error),
    });
  }
  flow.status = "failed";
  flow.currentStep = "failed";
  flow.completedAt = new Date().toISOString();
  step(flow, "flow-lab", "failed", error instanceof Error ? error.message : String(error));
}

function recordAwsNotification(flow, stage, status, sourceEventType, description) {
  const awsPayload = {
    correlation_id: flow.correlationId,
    subprocess_code: flow.subprocessCode,
    subprocess_run_id: flow.preprocessorRunId,
    planner_job_id: flow.plannerJobId,
    plan_run_id: flow.plannerPlanRunId,
    stage,
    status,
    source_event_type: sourceEventType,
    occurred_at: new Date().toISOString(),
  };
  const event = recordEvent({
    event_type: "stage.notification.requested",
    producer: "local-flow-lab",
    correlation_id: flow.correlationId,
    subprocess_code: flow.subprocessCode,
    subprocess_run_id: flow.preprocessorRunId,
    job_id: flow.plannerJobId,
    plan_run_id: flow.plannerPlanRunId,
    stage,
    status,
    source_event_type: sourceEventType,
    notification_target: "aws-notification-api",
    aws_payload: awsPayload,
    simulated: true,
    description,
  });
  recordIo(flow, {
    stage: `aws-notification-${stage}`,
    node: "aws-notification-local",
    title: `Payload hacia AWS: ${stage}`,
    status: "completed",
    input: {
      source_event_type: sourceEventType,
      workflow_description: description,
    },
    output: {
      event,
      awsPayload,
    },
  });
  step(flow, "aws-notification-local", "completed", `AWS notification: ${stage} -> ${status}`, {
    description,
    event,
    productionBehavior:
      "En produccion Workflows publicaria/consumiria stage.notification.requested y ejecutaria el conector HTTP hacia AWS Notification API.",
    localBehavior: "En local se registra el evento y se muestra el payload que se enviaria.",
    awsPayload,
  });
}

function getFlow(flowId) {
  const flow = flows.get(flowId);
  if (!flow) throw new Error(`Unknown flow: ${flowId}`);
  return flow;
}

function touchFlow(flow) {
  flow.updatedAt = new Date().toISOString();
  saveFlow(flow);
  emitFlow(flow);
}

function saveFlow(flow) {
  writeFileSync(join(FLOW_DIR, `${flow.id}.json`), JSON.stringify(flow, null, 2), "utf8");
}

function loadPersistedFlows() {
  const files = readdirSync(FLOW_DIR)
    .filter((name) => name.endsWith(".json"))
    .map((name) => {
      const path = join(FLOW_DIR, name);
      return { path, mtimeMs: statSync(path).mtimeMs };
    })
    .sort((a, b) => b.mtimeMs - a.mtimeMs)
    .slice(0, 30);
  for (const file of files) {
    try {
      const flow = JSON.parse(readFileSync(file.path, "utf8"));
      if (flow?.id) {
        flow.activeTimers = {};
        flows.set(flow.id, flow);
      }
    } catch {
      // Ignore malformed historical files; the active demo server should still boot.
    }
  }
}

function publicFlow(flow) {
  return {
    id: flow.id,
    status: flow.status,
    mode: flow.mode || "full",
    partialAction: flow.partialAction || null,
    subprocessCode: flow.subprocessCode,
    correlationId: flow.correlationId,
    preprocessorRunId: flow.preprocessorRunId,
    plannerJobId: flow.plannerJobId,
    plannerPlanRunId: flow.plannerPlanRunId,
    launchTokens: currentLaunchTokens(flow),
    currentStep: flow.currentStep,
    createdAt: flow.createdAt,
    startedAt: flow.startedAt,
    updatedAt: flow.updatedAt,
    completedAt: flow.completedAt,
    timeline: flow.timeline,
    timerRuns: flow.timerRuns || [],
    activeTimers: flow.activeTimers || {},
    ioRecords: currentIoRecords(flow),
    events: flow.events,
    outputs: currentOutputs(flow),
  };
}

function matchingRawLaunches(flow) {
  const launches = Array.isArray(flow.outputs?.launches) ? flow.outputs.launches : [];
  return launches.filter((item) => {
    if (flow.plannerJobId) return item.job_id === flow.plannerJobId;
    if (flow.preprocessorRunId) return item.subprocess_run_id === flow.preprocessorRunId;
    return String(item.process_code || "").toUpperCase() === flow.subprocessCode;
  });
}

function currentLaunchTokens(flow) {
  const filtered = matchingRawLaunches(flow);
  if (filtered.length) {
    return filtered.map((item) => ({
      launchToken: item.launch_token,
      launchUrl: serviceUrl("interviewer", item.launch_url || `/planner-interviews/${encodeURIComponent(item.launch_token)}`),
      apiUrl: serviceUrl("interviewer", item.api_url || `/api/planner-interviews/${encodeURIComponent(item.launch_token)}`),
      status: item.launch_status,
      intervieweeName: item.interviewee_name,
      intervieweeEmail: item.interviewee_email,
    }));
  }
  return flow.launchTokens || [];
}

function currentOutputs(flow) {
  const outputs = { ...(flow.outputs || {}) };
  const filtered = matchingRawLaunches(flow);
  if (filtered.length) {
    outputs.launches = filtered;
  }
  return outputs;
}

function currentIoRecords(flow) {
  const launchTokens = currentLaunchTokens(flow);
  const filteredLaunches = matchingRawLaunches(flow);
  return (flow.ioRecords || []).map((record) => {
    if (record.stage !== "interviewer-launches") return record;
    return {
      ...record,
      output: {
        ...(record.output || {}),
        matched: filteredLaunches.length ? filteredLaunches : record.output?.matched,
        launchLinks: launchTokens,
      },
    };
  });
}

async function refreshDbSnapshot(flow) {
  try {
    flow.outputs.postgres = await inspectDatabase(flow);
    touchFlow(flow);
  } catch (error) {
    flow.outputs.postgres = {
      available: false,
      error: error instanceof Error ? error.message : String(error),
    };
    touchFlow(flow);
  }
}

async function inspectDatabase(flow) {
  if (!config.dbInspector?.enabled) {
    return { available: false, error: "dbInspector deshabilitado" };
  }
  const dbUrl = resolveDatabaseUrl();
  if (!dbUrl) {
    return {
      available: false,
      error:
        "No se encontro URL de PostgreSQL. Configura FLOW_LAB_DATABASE_URL, local.config.json o los .env de los repos.",
    };
  }
  const cwd = config.dbInspector.cwd || config.repos?.planner || ROOT_DIR;
  const input = flow
    ? {
        scope: "flow",
        flowId: flow.flowId || flow.id,
        subprocessCode: flow.subprocessCode,
        correlationId: flow.correlationId,
        preprocessorRunId: flow.preprocessorRunId,
        plannerJobId: flow.plannerJobId,
        plannerPlanRunId: flow.plannerPlanRunId,
        launchToken: flow.launchTokens?.[0]?.launchToken,
      }
    : {};
  const result = await spawnCollect("uv", ["run", "python", DB_INSPECTOR_SCRIPT], {
    cwd,
    env: { ...process.env, FLOW_LAB_DATABASE_URL: dbUrl },
    input: JSON.stringify(input),
    timeoutMs: 45000,
  });
  if (result.code !== 0) {
    return {
      available: false,
      inspectedAt: new Date().toISOString(),
      error: compactInspectorError(result.stderr || result.stdout || `db inspector exited with ${result.code}`),
    };
  }
  const summary = JSON.parse(result.stdout || "{}");
  summary.inspectedAt = new Date().toISOString();
  return summary;
}

function compactInspectorError(raw) {
  const text = String(raw || "").replace(/\s+/g, " ").trim();
  if (!text) return "DB inspector fallo sin detalle.";
  if (/exceeded the data transfer quota/i.test(text)) {
    return "PostgreSQL no disponible: Neon indica que el proyecto excedio la cuota de transferencia de datos.";
  }
  if (/Access Denied/i.test(text)) {
    return text.match(/Access Denied:[^.]+[.]/i)?.[0] || "Acceso denegado al consultar la fuente de datos.";
  }
  if (/connection failed/i.test(text)) {
    return text.match(/connection failed:[^.]+[.]/i)?.[0] || "No se pudo conectar a PostgreSQL.";
  }
  return text.slice(0, 700);
}

function resolveDatabaseUrl() {
  const envCandidates = [
    process.env.FLOW_LAB_DATABASE_URL,
    process.env.PLANNER_DB_URL,
    process.env.PROCESS_DOCUMENTS_DB_URL,
    process.env.VOICE_AGENT_DB_URL,
    process.env.DATABASE_URL,
  ];
  for (const value of envCandidates) {
    if (value) return value;
  }

  const configCandidates = [];
  for (const service of Object.values(config.services || {})) {
    const env = service.env || {};
    configCandidates.push(
      env.FLOW_LAB_DATABASE_URL,
      env.PLANNER_DB_URL,
      env.PROCESS_DOCUMENTS_DB_URL,
      env.VOICE_AGENT_DB_URL,
      env.DATABASE_URL,
    );
  }
  for (const value of configCandidates) {
    if (value) return value;
  }

  const envFiles = [
    join(config.repos?.preprocessor || "", ".env"),
    join(config.repos?.planner || "", ".env"),
    join(config.repos?.interviewer || "", ".env"),
  ];
  for (const file of envFiles) {
    const parsed = parseEnvFile(file);
    const value =
      parsed.PLANNER_DB_URL || parsed.PROCESS_DOCUMENTS_DB_URL || parsed.VOICE_AGENT_DB_URL || parsed.DATABASE_URL;
    if (value) return value;
  }
  return null;
}

function parseEnvFile(filePath) {
  if (!filePath || !existsSync(filePath)) return {};
  const parsed = {};
  const content = readFileSync(filePath, "utf8");
  for (const rawLine of content.split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index <= 0) continue;
    const key = line.slice(0, index).trim();
    let value = line.slice(index + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    parsed[key] = value;
  }
  return parsed;
}

function spawnCollect(command, args, options = {}) {
  return new Promise((resolveSpawn) => {
    const child = spawn(command, args, {
      cwd: options.cwd || ROOT_DIR,
      env: options.env || process.env,
      windowsHide: true,
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    const timeout = setTimeout(() => {
      child.kill();
      resolveSpawn({ code: -1, stdout, stderr: stderr || "timeout" });
    }, options.timeoutMs || 30000);
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString("utf8");
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString("utf8");
    });
    child.on("error", (error) => {
      clearTimeout(timeout);
      resolveSpawn({ code: -1, stdout, stderr: error.message });
    });
    child.on("exit", (code) => {
      clearTimeout(timeout);
      resolveSpawn({ code: code ?? 0, stdout, stderr });
    });
    if (options.input) {
      child.stdin.write(options.input);
    }
    child.stdin.end();
  });
}

function emitFlow(flow) {
  emitSse("flows", { type: "flow-updated", flow: publicFlow(flow) });
}

function recordEvent(payload, meta = {}) {
  const event = {
    event_id: payload.event_id || payload.eventId || `evt-local-${randomUUID()}`,
    event_type: payload.event_type || payload.eventType || "unknown",
    occurred_at: payload.occurred_at || new Date().toISOString(),
    producer: payload.producer || "unknown",
    correlation_id: payload.correlation_id || payload.correlationId || null,
    subprocess_code: payload.subprocess_code || payload.subprocessCode || null,
    subprocess_run_id: payload.subprocess_run_id || payload.preprocessorRunId || null,
    job_id: payload.job_id || payload.jobId || null,
    plan_run_id: payload.plan_run_id || payload.planRunId || null,
    payload,
    meta,
  };
  events.push(event);
  if (events.length > 2000) events.splice(0, events.length - 2000);
  appendFileSync(EVENTS_FILE, `${JSON.stringify(event)}\n`, "utf8");
  attachEventToFlow(event);
  emitSse("events", { type: "event", event });
  return event;
}

function attachEventToFlow(event) {
  for (const flow of flows.values()) {
    if (
      (event.correlation_id && event.correlation_id === flow.correlationId) ||
      (event.subprocess_run_id && event.subprocess_run_id === flow.preprocessorRunId) ||
      (event.job_id && event.job_id === flow.plannerJobId)
    ) {
      flow.events.push(event);
      if (flow.events.length > 500) flow.events.splice(0, flow.events.length - 500);
      touchFlow(flow);
    }
  }
}

async function httpJson(serviceId, method, path, body, options = {}) {
  const service = getService(serviceId);
  const url = serviceUrl(serviceId, path);
  const response = await fetch(url, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(options.timeoutMs || 60000),
  });
  const text = await response.text();
  let payload = null;
  if (text) {
    try {
      payload = JSON.parse(text);
    } catch {
      payload = { raw: text };
    }
  }
  if (!response.ok) {
    throw new Error(`${method} ${url} -> ${response.status}: ${text.slice(0, 1000)}`);
  }
  return payload;
}

function serviceUrl(serviceId, path = "") {
  const service = getService(serviceId);
  if (/^https?:\/\//i.test(path)) return path;
  return `${service.url.replace(/\/$/, "")}/${String(path).replace(/^\//, "")}`;
}

async function proxyRequest(req, res, serviceId, restPath, search) {
  const serviceMap = {
    preprocessor: "preprocessor-api",
    "preprocessor-api": "preprocessor-api",
    planner: "planner-api",
    "planner-api": "planner-api",
    interviewer: "interviewer",
  };
  const resolvedService = serviceMap[serviceId];
  if (!resolvedService) return sendJson(res, 404, { error: "unknown_proxy_service" });
  const targetUrl = serviceUrl(resolvedService, `${restPath}${search || ""}`);
  const body = ["GET", "HEAD"].includes(req.method || "GET") ? undefined : await readRaw(req);
  const response = await fetch(targetUrl, {
    method: req.method,
    headers: copyProxyHeaders(req.headers),
    body,
  });
  res.writeHead(response.status, Object.fromEntries(response.headers.entries()));
  const buffer = Buffer.from(await response.arrayBuffer());
  res.end(buffer);
}

function copyProxyHeaders(headers) {
  const copied = {};
  for (const [key, value] of Object.entries(headers)) {
    if (["host", "connection", "content-length"].includes(key.toLowerCase())) continue;
    copied[key] = value;
  }
  return copied;
}

function openSse(res, channel) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    Connection: "keep-alive",
    "Access-Control-Allow-Origin": "*",
  });
  res.write(": connected\n\n");
  const client = res;
  sseClients[channel].add(client);
  res.on("close", () => sseClients[channel].delete(client));
}

function emitSse(channel, payload) {
  const text = `data: ${JSON.stringify(payload)}\n\n`;
  for (const client of sseClients[channel]) {
    client.write(text);
  }
}

async function readJson(req) {
  const raw = await readRaw(req);
  if (!raw.length) return {};
  return JSON.parse(raw.toString("utf8"));
}

async function readRaw(req) {
  const chunks = [];
  for await (const chunk of req) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

function sendJson(res, status, payload) {
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
  });
  res.end(JSON.stringify(payload, null, 2));
}

function serveStatic(pathname, res) {
  let filePath = pathname === "/" ? join(PUBLIC_DIR, "index.html") : join(PUBLIC_DIR, pathname);
  filePath = resolve(filePath);
  if (!filePath.startsWith(PUBLIC_DIR)) {
    res.writeHead(403);
    return res.end("Forbidden");
  }
  if (!existsSync(filePath)) {
    res.writeHead(404);
    return res.end("Not found");
  }
  res.writeHead(200, { "Content-Type": contentType(filePath) });
  createReadStream(filePath).pipe(res);
}

function contentType(filePath) {
  const ext = extname(filePath).toLowerCase();
  if (ext === ".html") return "text/html; charset=utf-8";
  if (ext === ".js") return "application/javascript; charset=utf-8";
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".json") return "application/json; charset=utf-8";
  if (ext === ".svg") return "image/svg+xml";
  return "application/octet-stream";
}

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

function compactTimestamp() {
  return new Date().toISOString().replace(/[-:TZ.]/g, "").slice(0, 14);
}
