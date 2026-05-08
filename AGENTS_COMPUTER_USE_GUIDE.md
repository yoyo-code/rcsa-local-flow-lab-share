# Guia para agentes: RCSA Local Flow Lab

Esta guia es para agentes que lleguen sin contexto y necesiten usar Computer Use / navegador local para operar, diagnosticar o modificar el laboratorio local del flujo RCSA.

## Objetivo del lab

`rcsa-local-flow-lab` es una app local para demostrar y probar el flujo RCSA completo sin desplegar Pub/Sub, Eventarc, Workflows ni Cloud Tasks reales.

La app:

- Levanta y detiene servicios locales de Preprocessor, Planner e Interviewer.
- Ejecuta el flujo end-to-end desde un `subprocess_code`.
- Permite ejecutar desde una etapa intermedia usando datos previos.
- Simula la infraestructura de orquestacion productiva:
  - Cloud Tasks
  - Pub/Sub
  - Eventarc
  - Workflows
  - Notificaciones hacia AWS Notification API
- Muestra timeline, eventos, tiempos, logs, payloads input/output por etapa y snapshots de PostgreSQL.

## Ubicacion

Lab local:

```text
<ruta-local>/rcsa-local-flow-lab
```

Repos de implementacion:

```text
<ruta-local>/coe-genai-rcsa-doc-preprocessor-impl/cr-preprocessor
<ruta-local>/coe-genai-rcsa-planner-impl
<ruta-local>/coe-genai-rcsa-interviewer-impl
```

Repos IAC relacionados:

```text
<ruta-local>/coe-genai-rcsa-doc-preprocessor-iac
<ruta-local>/coe-genai-rcsa-planner-iac
<ruta-local>/coe-genai-rcsa-interviewer-iac
```

Documento de arquitectura de referencia:

```text
<ruta-local>/arquitectura-giorgio-modificada-atr.md
```

## Como abrirlo con Computer Use

1. Confirmar que el servidor local este arriba:

```powershell
Invoke-WebRequest -Uri "http://127.0.0.1:4400/" -UseBasicParsing
```

2. Si no esta arriba:

```powershell
cd "<ruta-local>/rcsa-local-flow-lab"
node server.js
```

3. Abrir en el navegador:

```text
http://127.0.0.1:4400/
```

4. Usar la UI por fases:

- `Preparar entorno local`: validar rutas y levantar servicios.
- `Ejecutar y probar`: correr flujo completo o una etapa parcial.
- `Monitorear corrida`: ver flujo activo, timers, timeline y eventos.
- `Inspeccionar datos`: ver input/output, estados AWS y PostgreSQL.
- `Depurar`: revisar logs y payload seleccionado.

## Servicios y puertos

| Servicio | Puerto | Health |
|---|---:|---|
| Flow Lab | 4400 | `/` |
| Preprocessor API | 8010 | `/healthz` |
| Preprocessor Worker | 8011 | `/healthz` |
| Planner API | 8020 | `/health` |
| Planner Worker | 8021 | `/health` |
| Interviewer | 8030 | `/health` |

La configuracion base esta en:

```text
<ruta-local>/rcsa-local-flow-lab/rcsa-flow-lab.config.json
```

La configuracion personal de cada maquina queda en:

```text
<ruta-local>/rcsa-local-flow-lab/local.config.json
```

No asumir que `local.config.json` sirve para otra maquina. Cada usuario debe guardar sus rutas desde la UI.

## Flujo local

Flujo completo simulado por `server.js`:

```text
UI / Backoffice local
  -> Preprocessor API
  -> Preprocessor Worker via local_http
  -> evento local preprocessor.run.completed/partial
  -> Pub/Sub local
  -> Eventarc local
  -> Workflow local
  -> Planner API
  -> Planner Worker via dispatch local
  -> evento local planner.job.completed
  -> Preprocessor planner-finalized
  -> Interviewer launch token
  -> stage.notification.requested hacia AWS simulado
```

Puntos importantes:

- El lab no despliega infraestructura GCP.
- Pub/Sub/Eventarc/Workflows/Cloud Tasks son simulados por el servidor Node.
- GCS, Document AI, Vertex AI, LiteLLM y bases de datos pueden seguir siendo reales si los repos estan configurados asi.
- Los eventos locales se guardan en `runtime/events.jsonl`.
- Las corridas se guardan en `runtime/flows/*.json`.

## Flujo productivo real

La idea productiva equivalente es:

```text
Backoffice / AWS
  -> Preprocessor Cloud Run API
  -> Cloud Tasks preprocessor
  -> Preprocessor Cloud Run Worker
  -> Pub/Sub topic preprocessor.*
  -> Eventarc trigger
  -> Workflows coordinador
  -> Planner Cloud Run API
  -> Cloud Tasks planner
  -> Planner Cloud Run Worker
  -> Pub/Sub topic planner.*
  -> Eventarc trigger
  -> Workflows completion/finalizacion
  -> Preprocessor internal planner-finalized
  -> Interviewer Cloud Run
  -> AWS Notification API
```

Los repos IAC son los lugares correctos para revisar nombres reales de topics, triggers, queues, servicios y workflows productivos.

## Como operar una demo

1. Abrir `http://127.0.0.1:4400/`.
2. Revisar `Rutas locales`.
3. Click en `Iniciar todo`.
4. Esperar que `Servicios` llegue a `5/5`.
5. En `Nuevo flujo`, usar un `Subproceso`, por ejemplo `S03808`.
6. Opcional: click en `Revisar datos existentes`.
7. Click en `Iniciar flujo`.
8. Monitorear:
   - `Progreso de extremo a extremo`
   - `Mapa tecnico del flujo`
   - `Flujo activo`
   - `Timeline`
   - `Eventos`
   - `Tiempos`
   - `Input / Output por etapa`
   - `PostgreSQL`
   - `Logs`

Si algo falla, hacer click sobre la tarjeta o evento fallido. El payload completo aparece en `Payload seleccionado`.

## Ejecutar una etapa parcial

Usar el panel `Ejecutar desde una etapa` cuando se quiera probar un cambio puntual sin repetir todo.

Acciones soportadas:

| Accion UI | Que hace |
|---|---|
| Leer estado preprocessor existente | Consulta `/v1/subprocess-jobs/{id}/status`. |
| Crear job Planner desde run preprocessor | Llama Planner API y luego marca `planner-linked` en Preprocessor. |
| Ejecutar Planner worker | Simula Cloud Tasks local contra Planner Worker y espera resultado. |
| Finalizar run preprocessor | Llama `planner-finalized` en Preprocessor. |
| Leer resultado Planner | Consulta `/api/interview-jobs/{job_id}/result`. |
| Resolver launches Interviewer | Busca launch tokens por job/run/subproceso. |

Formas de poblar IDs:

- `Usar flujo activo`: copia IDs de la corrida actual.
- `Usar ultimo dato DB`: consulta PostgreSQL por subproceso y usa el registro mas reciente.
- Completar manualmente:
  - `preprocessor_run_id`
  - `job_id`
  - `plan_run_id`
  - `correlation_id`

## Mapa de codigo del lab

| Necesidad | Archivo / funcion |
|---|---|
| Cambiar flujo end-to-end | `server.js` -> `runFlow` |
| Cambiar ejecuciones parciales | `server.js` -> `runPartialFlow` |
| Cambiar creacion de Planner job | `server.js` -> `createPlannerJobFromFlow` |
| Cambiar dispatch a Planner worker | `server.js` -> `processPlannerWorkerFromFlow` |
| Cambiar resolucion de launch tokens | `server.js` -> `resolveLaunchesFromFlow` |
| Cambiar payloads AWS simulados | `server.js` -> `recordAwsNotification` |
| Cambiar input/output por etapa | `server.js` -> `recordIo` y llamadas a `recordIo(...)` |
| Cambiar timers/timeline | `server.js` -> `step`, `updateFlowTimer` |
| Cambiar rutas/servicios/comandos/env | `rcsa-flow-lab.config.json` |
| Cambiar UI/estructura de pantalla | `public/index.html` |
| Cambiar render del mapa/progreso/I/O | `public/app.js` |
| Cambiar estilos/espaciado | `public/styles.css` |
| Cambiar inspector PostgreSQL | `tools/db_inspector.py` |

## Mapa de codigo de repos externos

Preprocessor:

```text
<ruta-local>/coe-genai-rcsa-doc-preprocessor-impl/cr-preprocessor
```

Puntos usuales:

- API: `apps/api/main.py`
- Worker: `apps/worker/main.py`
- Config local/prod: buscar `LOCAL_TASKS_WORKER_URL`, `LOCAL_EVENT_SINK_URL`, `TASK_DISPATCH_BACKEND`
- Dispatch local Cloud Tasks: buscar `task_queue`
- Publicacion de eventos: buscar `event_publisher`

Planner:

```text
<ruta-local>/coe-genai-rcsa-planner-impl
```

Puntos usuales:

- API: `apps/api/main.py`
- Worker: `apps/worker/main.py`
- Config: buscar `LOCAL_EVENT_SINK_URL`, `WORKFLOW_MANAGED_DISPATCH`, `INTERVIEW_JOB_DISPATCH_BACKEND`
- Eventos: buscar `event_publisher`
- Jobs: buscar endpoints `/api/interview-jobs`
- Worker processing: buscar `/internal/interview-jobs/process`

Interviewer:

```text
<ruta-local>/coe-genai-rcsa-interviewer-impl
```

Puntos usuales:

- API principal: `app/main.py`
- Launches: buscar `/api/planner-interviews`
- Eventos: buscar `event_publisher` y `LOCAL_EVENT_SINK_URL`

IAC:

- Si piden cambiar infraestructura productiva, revisar los repos `*-iac`, no solo este lab.
- Buscar recursos por nombres: `pubsub`, `eventarc`, `workflow`, `cloud_tasks`, `cloud_run`.

## Endpoints utiles del Flow Lab

| Endpoint | Uso |
|---|---|
| `GET /api/config` | Ver config activa, rutas y servicios. |
| `POST /api/config/local-paths` | Guardar rutas locales en `local.config.json`. |
| `GET /api/services` | Health/status de servicios. |
| `POST /api/services/start-all` | Levantar todos los servicios. |
| `POST /api/services/stop-all` | Detener todos los servicios. |
| `GET /api/events` | Eventos registrados localmente. |
| `POST /api/events` | Sink local para eventos de repos. |
| `POST /api/flows` | Ejecutar flujo completo. |
| `POST /api/flows/partial` | Ejecutar una etapa parcial. |
| `GET /api/db/summary?flowId=...` | Snapshot DB para flujo activo. |
| `GET /api/db/subprocess?code=S03808` | Buscar datos existentes de un subproceso. |

## Verificacion despues de cambios

Minimo recomendado:

```powershell
cd "<ruta-local>/rcsa-local-flow-lab"
node --check server.js
node --check public/app.js
Invoke-WebRequest -Uri "http://127.0.0.1:4400/" -UseBasicParsing
Invoke-RestMethod -Uri "http://127.0.0.1:4400/api/services"
```

Si se cambia UI:

- Recargar `http://127.0.0.1:4400/`.
- Revisar consola del navegador.
- Confirmar que existen las secciones:
  - `Preparar entorno local`
  - `Ejecutar y probar`
  - `Monitorear corrida`
  - `Inspeccionar datos`
  - `Depurar`

Si se cambia flujo:

- Probar primero con ejecucion parcial.
- Luego probar flujo completo.
- Revisar `Input / Output por etapa`.
- Revisar logs de API y workers.
- Revisar PostgreSQL.

## Criterios para decidir donde cambiar

- Si el problema es visual, espaciado o jerarquia de la pantalla: modificar `public/index.html`, `public/styles.css` y quizas `public/app.js`.
- Si el problema es que una etapa local no se ejecuta o se ejecuta en orden incorrecto: modificar `server.js`.
- Si el problema es que un servicio real no expone endpoint, no acepta payload o falla internamente: modificar el repo correspondiente (`preprocessor`, `planner` o `interviewer`).
- Si el problema es que el flujo productivo real tiene un topic, queue, trigger o workflow distinto: revisar y modificar IAC.
- Si el problema es que PostgreSQL no muestra datos esperados: revisar `tools/db_inspector.py` y luego confirmar tablas reales en los repos.

## Precauciones

- No borrar `runtime/` si se necesita historial de la demo actual.
- No asumir que reprocesar sobreescribe todo: normalmente se crean nuevas corridas, jobs y planes; `active_interview_plans` puede apuntar al ultimo plan activo.
- No editar `local.config.json` como configuracion compartida; es por maquina.
- No convertir la simulacion local en dependencia productiva. Las banderas `LOCAL_*` deben ser opt-in local.
- Si se toca un repo externo, mantener los cambios compatibles con despliegue: local debe activarse por env vars, no por comportamiento hardcodeado.
