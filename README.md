# RCSA Local Flow Lab

Laboratorio local para ejecutar y visualizar el flujo RCSA completo sin desplegar Pub/Sub, Eventarc, Workflows ni Cloud Tasks.

## Requisitos

- Node.js 20 o superior.
- `uv` disponible en PATH.
- Los tres repos de servicios clonados localmente:
  - `coe-genai-rcsa-doc-preprocessor-impl`
  - `coe-genai-rcsa-planner-impl`
  - `coe-genai-rcsa-interviewer-impl`
- Los `.env` de cada repo configurados con las credenciales y bases necesarias.
- Acceso local a las credenciales externas que usan los servicios reales, por ejemplo PostgreSQL, GCS, BigQuery, Vertex/LiteLLM o Document AI segun corresponda.

No se requiere `npm install`: el Flow Lab usa solo modulos nativos de Node.js.

## Compatibilidad

El Flow Lab es compatible con Windows, macOS y Linux.

- Windows: usa PowerShell para iniciar servicios y `taskkill` para detenerlos.
- macOS/Linux: usa `/bin/sh` para iniciar servicios y grupos de proceso para detenerlos.
- Para detectar PID por puerto en macOS/Linux se usa `lsof` si existe, o `ss` en Linux. Si no estan instalados, la UI igual puede validar servicios por health check, pero puede no mostrar el PID detectado de un proceso iniciado fuera del Flow Lab.

## Arranque

Windows:

```powershell
cd "C:\ruta\a\rcsa-local-flow-lab"
.\START_HERE.bat
```

macOS:

```bash
cd "/ruta/a/rcsa-local-flow-lab"
chmod +x START_HERE.sh START_HERE.command
./START_HERE.sh
```

Linux:

```bash
cd "/ruta/a/rcsa-local-flow-lab"
chmod +x START_HERE.sh
./START_HERE.sh
```

Abrir:

```text
http://127.0.0.1:4400
```

Tambien puedes usar directamente:

```bash
node server.js
```

## Compartir con el equipo

Forma recomendada:

1. Desde esta carpeta, genera una copia limpia:

   ```bash
   npm run prepare:share
   ```

2. Se creara una carpeta hermana:

   ```text
   rcsa-local-flow-lab-share
   ```

3. Comprime esa carpeta y compartela con el equipo.

La copia limpia excluye:

- `runtime/`
- `.git/`
- `node_modules/`
- `local.config.json`

Cada colega debe tener los tres repos de servicios en su maquina y luego configurar sus rutas desde la UI, en el panel `Rutas locales`.

Si prefieren configurar a mano, pueden copiar:

```text
local.config.example.json
```

como:

```text
local.config.json
```

y reemplazar las rutas absolutas por las de su maquina.

## Que hace

- Levanta y detiene:
  - Preprocessor API
  - Preprocessor Worker
  - Planner API
  - Planner Worker
  - Interviewer
- Muestra readiness, health, PID y logs.
- Bloquea `Iniciar flujo` hasta que los cinco servicios requeridos tengan health OK.
- Ejecuta un flujo por `subprocess_code`.
- Permite revisar datos existentes de un `subprocess_code` antes de reprocesar.
- Registra timers por etapa y muestra un mini dashboard de duracion al terminar.
- Simula localmente:
  - Pub/Sub
  - Eventarc
  - Workflows
  - Cloud Tasks
  - estados/notificaciones hacia AWS
- Muestra una timeline con el equivalente productivo y la implementacion local de cada paso.
- Muestra `Input / Output por etapa` para comparar payload de entrada, respuesta, error y componente productivo equivalente.
- Permite ejecutar desde una etapa intermedia usando IDs previos de PostgreSQL o del flujo activo.
- Muestra tarjetas de estados hacia AWS con historial por etapa.
- Muestra snapshots de PostgreSQL para ver que tablas se poblan durante el flujo.
- Guarda eventos y corridas en `runtime/`.

Para agentes o personas que necesiten operar/modificar el lab con contexto completo, ver:

```text
AGENTS_COMPUTER_USE_GUIDE.md
```

## Configuracion

La config base esta en:

```text
rcsa-flow-lab.config.json
```

Para cambios personales, crear:

```text
local.config.json
```

`local.config.json` se mezcla encima de la config base y no se versiona. Ejemplo:

```json
{
  "services": {
    "preprocessor-api": {
      "env": {
        "DATABASE_URL": "postgresql://..."
      }
    },
    "planner-api": {
      "env": {
        "PROCESS_DOCUMENTS_DB_URL": "postgresql://...",
        "PLANNER_DB_URL": "postgresql://..."
      }
    },
    "interviewer": {
      "env": {
        "VOICE_AGENT_DB_URL": "postgresql://..."
      }
    }
  }
}
```

Tambien se puede configurar desde la UI en el panel `Rutas locales`. Al guardar, se escribe `local.config.json` con las rutas de esa maquina.
Para compartir la demo con el equipo, compartir esta carpeta completa y cada persona ajusta sus rutas locales desde ese panel.

Las rutas que se deben configurar son los `cwd` desde donde se ejecutan los comandos:

- Preprocessor: carpeta que contiene `apps/api/main.py` y `apps/worker/main.py`.
- Planner: carpeta raiz del repo planner.
- Interviewer: carpeta raiz del repo interviewer.

## Modo local recomendado

Preprocessor API:

```text
TASK_DISPATCH_BACKEND=local_http
LOCAL_TASKS_WORKER_URL=http://127.0.0.1:8011/internal/tasks/process
LOCAL_EVENT_SINK_URL=http://127.0.0.1:4400/api/events
```

Planner:

```text
INTERVIEW_JOB_DISPATCH_BACKEND=cloud_tasks
WORKFLOW_MANAGED_DISPATCH=true
LOCAL_EVENT_SINK_URL=http://127.0.0.1:4400/api/events
```

Interviewer:

```text
LOCAL_EVENT_SINK_URL=http://127.0.0.1:4400/api/events
```

## Estados hacia AWS

El panel `Estados hacia AWS` muestra cuando Workflows enviaria estados a la API externa en produccion.
En local no llama a AWS: registra eventos `stage.notification.requested` con el payload que se enviaria.

Etapas visibles:

- `preprocessing_documents`
- `planning_interview`
- `interview_ready`
- `interview_execution`

Cada tarjeta queda pendiente hasta que entra un evento de esa etapa. Si hay mas de un cambio para la misma etapa, la tarjeta muestra el historial de estados.

## Testing por etapa

El panel `Ejecutar desde una etapa` sirve para probar cambios puntuales sin repetir todo el flujo.

Opciones disponibles:

- `Leer estado preprocessor existente`: consulta `/v1/subprocess-jobs/{id}/status`.
- `Crear job Planner desde run preprocessor`: llama Planner API y luego marca el run como `planner-linked`.
- `Ejecutar Planner worker`: simula Cloud Tasks local contra el worker y espera el resultado.
- `Finalizar run preprocessor`: llama `planner-finalized`.
- `Leer resultado Planner`: consulta `/api/interview-jobs/{job_id}/result`.
- `Resolver launches Interviewer`: busca launches existentes para el job/run/subproceso.

Los botones `Usar flujo activo` y `Usar ultimo dato DB` completan los IDs necesarios para ejecutar esa etapa.

## PostgreSQL

El panel `PostgreSQL` ejecuta un inspector read-only y resume las tablas principales del preprocessor, planner e interviewer.
Durante un flujo activo se refresca cada 7 segundos y tambien se puede refrescar manualmente con `Actualizar DB`.
En `Nuevo flujo`, el boton `Revisar datos existentes` consulta la misma DB por `subprocess_code` y muestra corridas, documentos, jobs, planes y launches ya existentes.

La URL de base se resuelve en este orden:

- `FLOW_LAB_DATABASE_URL`
- `PLANNER_DB_URL`
- `PROCESS_DOCUMENTS_DB_URL`
- `VOICE_AGENT_DB_URL`
- `DATABASE_URL`
- variables equivalentes en `local.config.json`
- `.env` de los repos configurados

Para forzar una DB especifica sin tocar los repos:

```json
{
  "services": {
    "planner-api": {
      "env": {
        "FLOW_LAB_DATABASE_URL": "postgresql://..."
      }
    }
  }
}
```

## Reproceso de subprocesos

El flujo actual no borra ni sobreescribe las corridas anteriores del preprocessor. Cada reproceso crea un nuevo `subprocess_run_id`, nuevas tasks y nuevos documentos vinculados a esa corrida.

En Planner tambien se crea un nuevo `job_id` y un nuevo `plan_run_id`. La tabla `active_interview_plans` si apunta al ultimo plan activo por proceso, por lo que el historial queda, pero el plan activo puede quedar actualizado al ultimo reproceso exitoso.

## Puertos

| Servicio | Puerto |
|---|---:|
| Flow Lab | 4400 |
| Preprocessor API | 8010 |
| Preprocessor Worker | 8011 |
| Planner API | 8020 |
| Planner Worker | 8021 |
| Interviewer | 8030 |

## Flujo esperado

```text
subprocess_code
  -> Preprocessor API
  -> Preprocessor Worker local
  -> evento local preprocessor.run.completed/partial
  -> Eventarc local
  -> Workflow local
  -> Planner API
  -> Planner Worker local
  -> evento local planner.job.completed
  -> Preprocessor planner-finalized
  -> Interviewer launch_token
```

## Timers

El panel `Tiempos` mide las etapas registradas por el simulador local:

- APIs locales
- Workers
- Cloud Tasks local
- Pub/Sub local
- Eventarc local
- Workflow local
- Notificaciones AWS simuladas

Durante una corrida muestra tiempos activos; al terminar conserva el total del flujo, la etapa mas lenta y la duracion por componente.

## Notas

Este laboratorio hace local la infraestructura de orquestacion. Si los repos estan configurados para usar GCS, Document AI, Vertex AI o LiteLLM, esas llamadas siguen siendo reales salvo que se agregue un modo mock/local en esos repos.
