import { cpSync, mkdirSync, readFileSync, readdirSync, rmSync, statSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT_DIR = dirname(SCRIPT_DIR);
const OUT_DIR = join(dirname(ROOT_DIR), "rcsa-local-flow-lab-share");

const EXCLUDED = new Set([
  ".git",
  "node_modules",
  "runtime",
  "local.config.json",
  ".DS_Store",
]);

rmSync(OUT_DIR, { recursive: true, force: true });
mkdirSync(OUT_DIR, { recursive: true });
copyDirectory(ROOT_DIR, OUT_DIR);
sanitizeShareConfig();

console.log(`Carpeta lista para compartir: ${OUT_DIR}`);
console.log("No incluye runtime/, node_modules/, .git ni local.config.json.");
console.log("Comprímela y compartela junto con las instrucciones de README.md.");

function copyDirectory(source, target) {
  for (const name of readdirSync(source)) {
    if (EXCLUDED.has(name)) continue;
    const sourcePath = join(source, name);
    const targetPath = join(target, name);
    const stats = statSync(sourcePath);
    if (stats.isDirectory()) {
      mkdirSync(targetPath, { recursive: true });
      copyDirectory(sourcePath, targetPath);
    } else if (stats.isFile()) {
      cpSync(sourcePath, targetPath);
    }
  }
}

function sanitizeShareConfig() {
  const configPath = join(OUT_DIR, "rcsa-flow-lab.config.json");
  const config = JSON.parse(readFileSync(configPath, "utf8"));
  config.repos = {
    preprocessor: "",
    planner: "",
    interviewer: "",
  };
  if (config.dbInspector) config.dbInspector.cwd = "";
  for (const serviceId of [
    "preprocessor-api",
    "preprocessor-worker",
    "planner-api",
    "planner-worker",
    "interviewer",
  ]) {
    if (config.services?.[serviceId]) {
      config.services[serviceId].cwd = "";
    }
  }
  writeFileSync(configPath, `${JSON.stringify(config, null, 2)}\n`, "utf8");
}
