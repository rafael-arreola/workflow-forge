# workflow-forge — Diseño

> Motor de workflows declarativos estilo BPMN, definido, validado y extendido con
> JSON Schema + JSONPath. Escrito en Rust, embebible como librería.
> Estado (2026-06-12): **v0.1 implementado**. Spec 1.0 + schemas publicados;
> core completo (executor, validación, gateways, foreach, sub-workflows,
> perfiles, blobs, secrets, observabilidad); extensiones `util`/`data`/`http`/
> `sftp`/`tabular`; fachada con feature flags; ejemplos ejecutables, harness de
> pruebas y CI. Decisiones fundacionales tomadas el 2026-06-09; ver el detalle
> de la implementación en `docs/` (manuales de uso y de arquitectura).

## Visión

Que cualquier persona pueda **definir un workflow como un documento JSON**,
validarlo con esquemas publicados (sin necesitar el engine), y ejecutarlo
embebiendo el core en su aplicación Rust. El core publica dos contratos:

1. **Esquema de ejecución**: el JSON Schema que describe qué es un workflow
   válido (nodos, aristas, gateways, retry, mappings).
2. **Esquema de extensión**: el manifiesto con el que cualquier extensión
   declara sus tareas — config, input/output — como JSON Schema. Esto habilita
   validación estática, documentación generada y, a futuro, editores visuales,
   sin acoplarse al código de la extensión.

## Decisiones fundacionales

| # | Tema | Decisión | Alternativas descartadas |
|---|------|----------|--------------------------|
| 1 | Forma de producto | Librería embebible primero; CLI/runtime/server en fases posteriores sobre el mismo core | Runtime standalone desde v1; ambos en paralelo |
| 2 | Spec | Esquema propio, inspirado en BPMN / Step Functions / CNCF Serverless Workflow | Implementar la spec CNCF; capa de import |
| 3 | Estructura | Grafo explícito `nodes` + `edges` | Pasos anidados (árbol); híbrido con azúcar sintáctico |
| 4 | Ejecución v1 | Efímera en memoria, run-to-completion | Durable desde v1 |
| 5 | Flujo de datos | Contexto global por ejecución + mappings JSONPath; el output de cada nodo se publica en `$.nodes.<id>.output` | Dataflow puro por puertos; híbrido |
| 6 | Branching | Nodo `gateway` explícito (exclusive / parallel / join) con condiciones en el nodo y aristas etiquetadas | Condiciones en aristas; ambos |
| 7 | Extensiones | Traits Rust compile-time en v1; el contrato JSON Schema se diseña para que WASM sea solo un loader nuevo (roadmap) | WASM desde v1; sin API de extensiones |
| 8 | Errores | Política por nodo (`retry`, `timeout_ms`) + arista `on: error` opcional; sin compensación/sagas en v1 | Solo fallo del workflow; modelo con compensación |
| 9 | Extensiones v1 | HTTP/S client, transformación de datos, SFTP/FS | (utilidades delay/log quedan como nice-to-have) |
| 10 | Licencia | Dual MIT / Apache-2.0 | MIT solo, Apache solo, AGPL |
| 11 | Versionado | Campo `spec` en cada workflow + JSON Schemas publicados y versionados (vía `$id`); semver del crate independiente | Acoplar spec al semver del crate |
| 12 | Condiciones | Mini-DSL JSON propio (`eq`, `gt`, `in`, `and`/`or`/`not`, … sobre paths JSONPath) — validable con JSON Schema y construible por UI | CEL, JSONata, mini-DSL + CEL opcional |
| 13 | Join | `wait_all` con fallo rápido: espera todas las ramas; si una falla sin `on_error`, el workflow falla y las demás se cancelan | Modos configurables `all`/`any`/`count(n)`; sin paralelismo en v1 |
| 14 | Mappings | JSONPath puro + literales; toda transformación es un nodo `data.*` explícito | Helpers `$concat`/`$default`; templates de string inline |
| 15 | Sub-workflows | Post-v1, pero `kind: "subworkflow"` queda reservado en la spec 1.0 | Implementarlo en v1; no reservarlo |
| 16 | Extensiones v1 (ampliado) | `http`, `data`, `util`, `sftp`, `tabular` (csv+xlsx); `compress`/`crypto`/`storage`/`smtp` en v1.x, `db`/`queue` post-v1 | Solo http+data+util en v1 |
| 17 | Fachada | Crate `workflow-forge` con feature flags (`http`, `data`, …) que re-exporta core y registra extensiones; un solo `cargo add` para adoptar | Que el usuario dependa de core + cada extensión |
| 19 | Terminología | Se llaman **extensiones** (no "plugins"): crates `workflow-forge-ext-*` bajo `crates/extensions/` | "Plugins" |
| 18 | Binarios/archivos | Convención `$blob`: las tareas pasan referencias `{"$blob": "<id>", ...}`; el core define un trait `BlobStore` (v1: temp dir por ejecución, limpiado al terminar) | Paths planos; base64 inline |
| 20 | Perfiles de tarea | Instancias nombradas y reusables de una tarea (`TaskProfile`): config horneada (`bind`) + schemas propios; se registran en el registry o inline en la sección `tasks` del workflow | Repetir config en cada nodo; solo registry; solo inline |
| 21 | Secrets | Convención `{"$secret": "NOMBRE"}` en el `bind` de perfiles, resuelta al registrar vía trait `SecretProvider` (default: variables de entorno) | Tokens literales en JSON; diferir a v1.x |
| 22 | Iteración | Nodo `foreach`: invoca una tarea por cada elemento de un array, con concurrencia elegible (default secuencial), throttle entre arranques y políticas `fail`/`collect` por elemento | Nodo de loop genérico; iteración dentro de data.map; sin iteración en v1 |
| 23 | Observabilidad | Trait `ExecutionObserver` + eventos tipados serializables (`ExecutionEvent`) emitidos por el executor; `InMemoryHistory` → `ExecutionReport` integrado. Los eventos son la semilla del journal durable (post-v1) | Solo tracing; reporte sin streaming; diferir todo a la fase durable |
| 24 | Panics | Tercera salida por nodo: `on: "panic"`. Un panic en una extensión se captura (`catch_unwind`), no tumba la ejecución, **no reintenta** (es bug, no fallo transitorio) y **no cae en `on: error`** (pudo dejar efectos a medias); sin arista panic el workflow falla. En foreach un panic de un elemento siempre aborta el nodo (aun con `collect`) | Que el panic tumbe el proceso; tratarlo como error normal; fallback a `on: error` |
| 25 | HTTP completo | `http.request` cubre todos los cuerpos — `body` (JSON), `form` (urlencoded), `text` (raw), `body_blob` (binario streamed) y `multipart` (form-data con partes texto/json/blob) — mutuamente excluyentes; y `response_body: auto\|text\|blob` para descargar binarios al BlobStore. Blobs siempre por streaming | Solo JSON; cargar binarios a memoria/base64; extensión aparte para multipart |
| 26 | Conversiones por campo | Tarea `data.cast`: conversiones declarativas por campo (fechas con formato, números con separadores, int/bool/string, trim/upper/lower/replace, defaults) sobre un objeto o array de filas; `on_invalid: fail\|null\|collect` decide qué pasa con valores inconvertibles | Helpers de conversión en mappings (rompe #14); pipes estilo template; dejar la conversión al host |
| 27 | Sub-workflows | `kind: "subworkflow"` (deja de estar reservado): el `input` resuelto es el trigger del hijo y el output final del hijo es el output del nodo. Registro dual como los perfiles: `WorkflowRegistry` compartido + sección `workflows` inline (precedencia). Las 3 salidas aplican (error/panic del hijo rutean). BlobStore compartido, eventos con `parent_execution_id`, ciclos y profundidad >8 rechazados al construir | Solo inline; solo registry; retry/timeout en el nodo subworkflow (diferido); ejecución aislada sin compartir blobs |
| 28 | Autoría de tareas | Tres niveles sobre el trait `Task`: `register_typed` (closure tipada, schemas de input/output **derivados** de los tipos vía `schemars`, validados por el engine; refs anidados/enums se aplican), `register_fn` (closure sobre JSON crudo, sin schema) y `impl Task` (struct con estado/dependencias y acceso al `WorkflowContext` completo). Las closures reciben un `TaskCtx` por valor (execution_id, blobs, idempotency_key) para no lidiar con lifetimes. Validación tolerante por defecto; estricta con `#[serde(deny_unknown_fields)]` | Solo el trait a mano; schemas siempre escritos a mano; pasar `&WorkflowContext` a las closures (lifetimes) |
| 29 | Pruebas | Feature `testing` con `MockTask` (`returning`/`failing`/`with_fn`) + `CallLog`: dry-run de un workflow contra tareas simuladas, asertando salida y qué se habría llamado, sin red/FS/secrets. Es el complemento de "declarar sin programar": probar flujos sin dependencias vivas | Sin utilidades de prueba; que cada quien mockee con `register_fn` a mano |
| 30 | Idempotencia | Convención de clave **determinista y content-addressed**: `idempotency::key_for(value)` deriva un UUID v5 estable del payload (mismo payload → misma clave, entre reintentos y re-runs). Declarativa vía `util.idempotency_key` (`{ value } → { key }`) cableada al `bind` del perfil/tarea creadora; en código vía `TaskCtx::idempotency_key`. El alcance se compone metiendo discriminadores en el value (execution_id, sistema destino) | Clave por (execution+node) que solo dedupe reintentos de una corrida; hook con estado por-nodo (carrera bajo paralelismo); dejar la idempotencia 100% al host |
| 31 | Observabilidad durable | Adaptadores `ExecutionObserver` listos: `TracingObserver` (emite por `tracing` → cualquier subscriber/OTel del host) y `JsonlObserver` (una línea JSON por evento a un `Write`/archivo, append-only, replayable). Síncronos (como el trait); `JsonlObserver` hace flush por evento. La persistencia/cola sigue siendo del host, pero ya no se reescribe desde cero | Solo `InMemoryHistory`; obligar a cada host a implementar el trait; persistencia asíncrona dentro del core |
| 32 | Cancelación y deadline | `run_with(trigger, RunOptions)` añade `deadline` (timeout total de la ejecución) y `cancel` (`CancellationToken` cooperativo). **Por defecto ilimitado**: `run` no impone límites; el host decide. Al vencer/cancelar se abandona la ejecución (descarte de futuros: cooperativo en los `await`) y se devuelve `EXECUTION_TIMEOUT`/`EXECUTION_CANCELLED`; los blobs se limpian igual. Resuelve la pregunta abierta #1 | Límites por defecto; cancelación forzada (kill de tareas a media syscall); timeout solo por nodo |

## Modelo conceptual

### Workflow

Un documento JSON con grafo dirigido:

```json
{
  "spec": "1.0",
  "name": "sync-users",
  "version": "0.1.0",
  "nodes": [ ... ],
  "edges": [ ... ]
}
```

### Nodos

| Kind | Rol |
|------|-----|
| `start` | Punto de entrada; recibe el `trigger` (input inicial de la ejecución) |
| `end` | Terminación; puede mapear el output final del workflow |
| `task` | Invoca una tarea registrada (`"task": "http.request"`) con un `input` mapeado por JSONPath |
| `gateway` | Control de flujo: `exclusive` (if/else), `parallel` (fan-out), `join` (fan-in) |
| `subworkflow` | Ejecuta otro workflow como si fuera una tarea (ver "Sub-workflows") |

Ejemplo de task con política de errores:

```json
{
  "id": "fetch-user",
  "kind": "task",
  "task": "http.request",
  "input": {
    "url": "$.trigger.api_base",
    "method": "GET"
  },
  "retry": { "max": 3, "backoff": "exponential", "initial_ms": 500 },
  "timeout_ms": 10000
}
```

Ejemplo de gateway exclusivo:

```json
{
  "id": "check-status",
  "kind": "gateway",
  "gateway": "exclusive",
  "branches": [
    { "when": { "path": "$.nodes.fetch-user.output.status", "eq": 200 }, "edge": "ok" },
    { "else": true, "edge": "fail" }
  ]
}
```

### Sub-workflows

Un nodo `kind: "subworkflow"` ejecuta otro workflow como si fuera una tarea —
el bloque de reuso por encima de los perfiles: "parsear → validar → enviar"
se define una vez y cada integración lo invoca con su propio mapping.

```json
{
  "id": "procesa",
  "kind": "subworkflow",
  "workflow": "normalizar-y-enviar",
  "input": { "fecha": "$.trigger.fecha_pedido", "total": "$.trigger.importe" }
}
```

Semántica:

- El `input` resuelto (o el token del predecesor, si no hay `input`) se
  convierte en el **trigger** del hijo; el output final del hijo es el output
  del nodo. El schema del start del hijo valida ese trigger.
- **Resolución por nombre, registro dual** (espejo de los perfiles): la
  sección `workflows` inline del documento tiene precedencia; después se
  busca en el `WorkflowRegistry` compartido
  (`WorkflowExecutor::builder(...).workflows(registry)`).
- Todo se resuelve y valida **al construir el executor**, nunca en runtime:
  nombre inexistente (`SUBWORKFLOW_NOT_FOUND`), hijo inválido (errores con
  contexto), ciclos A→B→A (`SUBWORKFLOW_CYCLE`) y anidamiento más allá de 8
  niveles (`SUBWORKFLOW_DEPTH_EXCEEDED`).
- **Las tres salidas aplican**: un fallo del hijo rutea por `on: error` del
  nodo; un `TASK_PANIC` dentro del hijo rutea por `on: panic` (mismas reglas:
  sin retry, sin fallback). El nodo subworkflow no acepta `retry`/`timeout_ms`
  en v1 — las políticas viven en los nodos del hijo.
- La ejecución hija tiene su propio `execution_id` (UUID v7) y su propio
  documento de estado, pero **comparte el BlobStore** de la ejecución raíz
  (las referencias `$blob` cruzan la frontera; la raíz limpia al final) y el
  contador `seq` de eventos (orden total del árbol completo).
- Observabilidad: los eventos del hijo llevan `parent_execution_id`;
  `InMemoryHistory::report()` resume solo la ejecución raíz (el nodo
  subworkflow aparece como un nodo más) y `report_for(execution_id)` /
  `executions()` dan el detalle por hijo.
- Los perfiles inline (`tasks`) de un documento son locales a ese documento:
  el hijo se construye con el registry original del host, no con el scoped
  del padre.

### Condiciones (mini-DSL)

Las condiciones son objetos JSON validables con JSON Schema y construibles por
una UI. Una condición es un comparador sobre un path, o una composición lógica:

```json
{
  "and": [
    { "path": "$.nodes.fetch.output.status", "eq": 200 },
    { "or": [
      { "path": "$.trigger.priority", "in": ["high", "urgent"] },
      { "path": "$.trigger.retry_count", "gt": 3 }
    ]}
  ]
}
```

Operadores v1 (propuesta inicial, a cerrar en Fase 0):
- Comparación: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`
- Pertenencia: `in`, `contains`
- Existencia/tipo: `exists`, `is_null`
- Strings: `starts_with`, `ends_with`, `matches` (regex)
- Lógicos: `and`, `or`, `not`

Un path que no resuelve no es error: `exists` da `false` y los comparadores
no-existencia dan `false` (el workflow no truena por un path ausente en una
condición).

### Semántica de `parallel` / `join`

- `parallel` hace fan-out: todas sus aristas salientes se ejecutan
  concurrentemente.
- `join` es `wait_all`: espera **todas** sus ramas entrantes. Su output es un
  objeto `{ "<nodo_origen>": output }` con el resultado de cada rama.
- Fallo rápido: si una rama falla sin ruta `on_error`, el workflow falla y las
  ramas hermanas se cancelan.
- Modos `any` / `count(n)` quedan para post-v1 (extensión compatible).

### Aristas

```json
{ "from": "check-status", "label": "ok", "to": "notify" }
{ "from": "fetch-user", "on": "error", "to": "alert" }
{ "from": "fetch-user", "on": "panic", "to": "cleanup" }
```

- `label` conecta ramas de un gateway.
- `on: "error"` define la ruta cuando un nodo agota sus reintentos.
- `on: "panic"` define la ruta cuando la tarea panickea (bug en la extensión;
  error `TASK_PANIC`). Cada nodo que ejecuta tareas tiene así **tres salidas
  ruteables**: success (default), error y panic. Semántica:
  - El panic se captura con `catch_unwind`: el runtime sobrevive.
  - **Nunca reintenta**, aunque el nodo tenga `retry` (un bug no es transitorio).
  - **Sin fallback**: sin arista `on: panic` el workflow falla; jamás cae en
    `on: error`, porque un panic pudo dejar efectos a medias y la rama de
    errores operacionales no debe tratarlo como caso conocido.
  - En `foreach`, el panic de un elemento aborta el nodo completo (incluso con
    `on_item_error: collect`) y rutea por el `on: panic` del foreach.
  - Las aristas con trigger (`on: error`/`on: panic`) solo salen de nodos
    `task`/`foreach` y no pueden entrar a un gateway `join`.

### Contexto y datos

Cada ejecución posee un documento de estado:

```
$.trigger              → input inicial del workflow
$.nodes.<id>.output    → resultado de cada nodo ejecutado
$.workflow             → metadata (id, nombre, ids de ejecución)
```

Los `input` de cada nodo son mappings donde los strings que empiezan con `$.`
se resuelven como JSONPath contra el contexto; el resto son literales. Reglas:

- `"$.trigger.url"` → se resuelve contra el contexto.
- `"hola"`, `42`, `true`, objetos/arrays anidados → literales (los strings
  dentro de objetos anidados también se resuelven si empiezan con `$.`).
- `"$$.no.es.path"` → escape: produce el literal `"$.no.es.path"`.
- Un path de mapping que **no resuelve es error** (`MAPPING_PATH_NOT_FOUND`):
  en un input es casi siempre un bug de definición. (Contraste deliberado con
  las condiciones, donde un path ausente da `false`.) Los valores opcionales
  se preparan con un nodo `data.*` previo.
- Los paths de mapping son **singulares**: se toma el primer match; wildcards
  no están soportados en mappings v1.
- **No hay helpers de transformación en el mapping**: concatenar, formatear o
  poner defaults se hace con un nodo `data.*` previo. Un solo lugar donde
  ocurren transformaciones.

El input y output de cada tarea se validan contra los JSON Schemas declarados
en su manifiesto (errores `TASK_INPUT_INVALID` / `TASK_OUTPUT_INVALID`).
Un nodo task sin `input` recibe el output de su predecesor.

### Extensiones

- v1: una extensión es un crate que implementa el trait `Task` y se registra
  en el `TaskRegistry`. Las oficiales viven en el workspace
  (`crates/extensions/*`) detrás de feature flags.
- Toda tarea declara un **manifiesto** (`TaskManifest`): id namespaced
  (`http.request`), descripción, JSON Schema de input/output. El manifiesto es
  serializable: `TaskRegistry::catalog()` exporta el catálogo completo de
  tareas disponibles como JSON (base para tooling y editores).
- Roadmap: loader WASM (extism/wasmtime) que consume el mismo manifiesto, y
  por tanto no cambia la spec.

### Extensiones v1

| Namespace | Tareas | Notas |
|-----------|--------|-------|
| `http` | `http.request` | Métodos estándar, headers, auth basic/bearer, body JSON |
| `data` | `data.transform`, `data.map`, `data.merge`, `data.template`, `data.cast` | Reshape con JSONPath, plantillas de strings, conversiones por campo |
| `util` | `util.delay`, `util.log`, `util.noop` | Debug, pruebas y ejemplos |
| `sftp` | `sftp.get`, `sftp.put`, `sftp.list` | Integraciones empresariales; usa `$blob` |
| `tabular` | `tabular.parse`, `tabular.write` | CSV y XLSX ↔ JSON; usa `$blob` |

Roadmap de extensiones: `fs` (`fs.read`/`fs.write` local, complementa sftp,
usa `$blob`), `compress` (zip/gzip), `crypto` (hash/HMAC), `storage`
(S3-compatible) y `smtp` en v1.x; `db` (SQL) y `queue` (AMQP/Kafka) post-v1.

### Nodo `foreach`

Iteración con invocación de tarea por elemento — el complemento de `data.map`
(que solo reshapea). El caso típico: "una llamada HTTP por cada fila del lote
del cliente".

```json
{
  "id": "crear_ordenes",
  "kind": "foreach",
  "items": "$.nodes.adaptar.output",
  "task": "miapi.crear_envio",
  "concurrency": 5,
  "throttle_ms": 200,
  "on_item_error": "collect",
  "retry": { "max": 2 },
  "timeout_ms": 10000
}
```

Semántica:

- `items`: mapping (reglas `$.` de los inputs) que debe resolver a array
  (`FOREACH_ITEMS_NOT_ARRAY` si no).
- Cada elemento es el **input directo** de la tarea; el reshape por elemento
  se hace antes con `data.map` (decisión #14: un solo lugar transforma).
  Input/output de cada elemento se validan contra los schemas de la tarea.
- `concurrency` (default 1 = secuencial) limita los elementos en vuelo;
  `throttle_ms` separa los **arranques** de elementos entre sí, también bajo
  concurrencia — es el rate limit hacia el destino.
- `retry`/`timeout_ms` aplican por elemento.
- `on_item_error: "fail"` (default): el primer error cancela los elementos en
  vuelo y el nodo falla (aplican aristas `on: error`). Output: array de
  outputs en el orden de `items`.
- `on_item_error: "collect"`: se ejecutan todos; output
  `{ "ok": [...], "failed": [{ "index", "item", "error" }] }` y el nodo no
  falla — los fallos se rutean con gateways
  (ej. `{ "path": "$.nodes.x.output.failed[0]", "exists": true }`).

### Observabilidad

El executor emite eventos tipados a un `ExecutionObserver` registrado con
`WorkflowExecutor::with_observer` (cero costo sin observer). Los eventos
serializan a JSON con discriminador `type` y llevan `execution_id`, `seq`
(orden total por ejecución, estable bajo paralelismo) y `elapsed_ms`:

`workflow_started` · `node_started` · `task_attempt_started` ·
`task_attempt_failed` · `node_completed` · `node_failed` ·
`foreach_item_completed` · `foreach_item_failed` · `workflow_completed` ·
`workflow_failed`

- Los payloads (trigger, inputs, outputs, errores) van **completos** en los
  eventos; el observer decide qué persistir/truncar. `on_event` es síncrono y
  no debe bloquear (el host bufferiza si persiste lento).
- `InMemoryHistory` es el observer integrado: acumula eventos y produce un
  `ExecutionReport` serializable — status global + por nodo (status,
  attempts, duración, output/error, conteos ok/failed de foreach).
- **Roadmap durable (#8)**: estos mismos eventos son el journal del executor
  event-sourced post-v1 — un `EventStore` que persista `ExecutionEvent` y un
  replay que reconstruya el estado. El contrato de eventos se congela aquí
  para no rediseñar.

### Perfiles de tarea

El bloque de construcción para integraciones repetibles: un **perfil**
(`TaskProfile`) es una instancia nombrada y reusable de una tarea registrada,
con la configuración horneada y un contrato de input/output específico. El
caso típico: un endpoint concreto de un cliente como tarea de primera clase.

```json
{
  "id": "acme.crear_orden",
  "extends": "http.request",
  "description": "Crea una orden en Acme",
  "input_schema": { "type": "object", "required": ["sku", "qty"] },
  "output_schema": { "type": "object", "required": ["order_id"] },
  "bind": {
    "url": "https://api.acme.com/orders",
    "method": "POST",
    "auth": { "type": "bearer", "token": { "$secret": "ACME_TOKEN" } },
    "fail_on_error_status": true,
    "body": "@"
  },
  "output": "@.body"
}
```

Semántica:

- `extends`: id de una tarea ya registrada; puede ser otro perfil (cadenas de
  especialización). No hay ciclos posibles: la base debe existir al registrar.
- `bind`: shape que construye el input de la base a partir del input del
  perfil, con la misma convención de `data.transform`: `@` es el input
  completo, `@.path` un subpath (ausente → null), `@@.` escapa, el resto son
  literales. Sin `bind`, el input pasa tal cual.
- `output`: shape opcional sobre el output de la base (ej. `"@.body"`); el
  resultado se valida contra `output_schema`.
- Secrets: los objetos `{"$secret": "NOMBRE"}` del `bind` se resuelven **al
  registrar** el perfil vía el trait `SecretProvider` (default `EnvSecrets`,
  variables de entorno). Las definiciones versionadas no llevan credenciales.
- Registro: `TaskRegistry::register_profile()` para perfiles compartidos entre
  workflows, o la sección `tasks` del documento del workflow para perfiles
  locales (el executor los registra en una copia `scoped()` del registry: el
  registry compartido no se contamina).
- Un perfil registrado es una tarea más: aparece en el catálogo con sus
  schemas y el executor valida su input/output como a cualquier tarea. Si el
  `bind` produce un input que la base rechaza, el error es
  `PROFILE_BIND_INVALID` (otros errores: `PROFILE_BASE_NOT_FOUND`,
  `PROFILE_ID_CONFLICT`, `SECRET_NOT_FOUND`).
- Spec: `schemas/1.0/profile.schema.json` publica el contrato del perfil; el
  schema del workflow lo embebe en su sección `tasks`.

El patrón de integración resultante: `trigger` (JSON del cliente en su
formato) → nodo `data.transform` que lo reconvierte campo a campo → perfil
preconfigurado que ejecuta la llamada. Cada pieza es declarativa, validable y
reusable.

### Binarios: convención `$blob`

El contexto es JSON; los archivos grandes no viajan inline. Una tarea que
produce un archivo devuelve una referencia:

```json
{ "file": { "$blob": "01J…", "name": "ventas.csv.gz", "size": 52428800 } }
```

El core define un trait `BlobStore` con el ciclo de vida atado a la ejecución
(v1: directorio temporal por ejecución, limpiado al terminar; futuro: S3 u
otros backends). Las tareas leen/escriben blobs a través del contexto, nunca
tocan el filesystem por su cuenta.

### Layout del workspace

```
crates/
  core/                  → workflow-forge-core (spec, executor, manifest, BlobStore)
  extensions/
    http/                → workflow-forge-ext-http
    data/                → workflow-forge-ext-data
    util/                → workflow-forge-ext-util
    sftp/                → workflow-forge-ext-sftp
    tabular/             → workflow-forge-ext-tabular
  forge/                 → workflow-forge (fachada con feature flags)
  cli/                   → workflow-forge-cli (binario `forge`: run/validate/catalog)
```

Reglas: cada extensión depende solo de core (nunca de otra extensión), expone
`register(&TaskRegistry)` y sus manifiestos, y trae sus propios tests.

## Versionado del contrato

- Cada workflow declara `"spec": "1.0"`.
- Los JSON Schemas (workflow + manifiesto de extensión + cada extensión
  oficial) se publican en el repo bajo `schemas/<version>/` con `$id` estable, de modo
  que un workflow es validable con cualquier validador estándar, sin el engine.
- Los crates evolucionan con semver propio; un bump de crate no implica bump
  de spec.

## Open source

- **Licencia**: dual MIT / Apache-2.0 (estándar del ecosistema Rust).
- **Workspace**: ver "Layout del workspace" arriba.
- **Mínimos de release**: README con quickstart, schemas publicados, ejemplos
  ejecutables, CI (fmt + clippy + test), CHANGELOG, publicación en crates.io.

## Roadmap

### Fase 0 — Spec draft ✅
JSON Schema del workflow (nodos, gateways, edges, retry, mappings), JSON Schema
del manifiesto de extensión, y workflows de ejemplo que validan contra ellos.

### Fase 1 — Core alineado a la spec ✅
Executor con contexto global, resolución JSONPath de mappings, gateways
(exclusive/parallel/join), foreach, retry/timeout/on_error/on_panic, sub-workflows,
perfiles, blobs, secrets, validación del grafo y catálogo exportable de tareas.

### Fase 2 — Extensiones oficiales ✅
`ext-util`, `ext-data`, `ext-http`, `ext-tabular`, `ext-sftp` + fachada, cada
una con manifiesto, schemas y tests de integración.

### Fase 3 — Pulido OSS y v1.0 ✅ (v0.1 lista)
Docs (manuales de uso y arquitectura en `docs/`), ejemplos ejecutables, CI
(fmt + clippy + tests), licencias, metadata de crates.io. Pendiente solo el
`cargo publish` final.

### Hecho post-0.1 (aditivo, sin tocar la spec)
- Autoría de baja fricción: `register_typed`/`register_fn` con schemas derivados.
- Harness de pruebas: feature `testing` con `MockTask`/`CallLog`.
- Idempotencia: `idempotency::key_for` + tarea `util.idempotency_key`.
- Observabilidad durable: adaptadores `TracingObserver` y `JsonlObserver`.
- Cancelación + deadline de ejecución (`run_with`/`RunOptions`), ilimitado por
  defecto.
- Reuso de conexiones a volumen: `http::register_with_client` (Client afinable;
  el default ya hace pooling) y `sftp::register_pooled` + `SftpPool` (reutiliza
  sesiones SSH autenticadas, con liveness y reconexión).
- CLI runtime: crate `workflow-forge-cli`, binario `forge` (`run`/`validate`/
  `catalog`).

### Futuro (post-v1)
- Extensiones WASM instalables sin recompilar
- Durabilidad: executor event-sourced detrás de un trait de storage; espera de
  eventos externos
- Servidor con API / triggers; scheduling
- Despacho dinámico a sub-workflows por campo (resuelto en runtime)
- Editor visual (el grafo + schemas + catálogo lo hacen posible)

## Contratos de extensiones (decididos en implementación)

- **`data.transform`**: `{ source, shape }`. Los strings de `shape` con prefijo
  `@.` son JSONPath **relativos al source** (sin colisión con los mappings `$.`
  del executor); `@` solo es el source completo; `@@.` escapa; un path ausente
  produce `null`. La resolución de shapes vive en `core::shape` (compartida
  con el `bind`/`output` de los perfiles).
- **`data.map`**: `{ items, shape }`, aplica el shape a **cada elemento** del
  array (paths `@.` relativos al elemento; ausente → null). Es la respuesta v1
  al caso "reestructurar filas de un CSV/array" sin nodo de iteración genérico.
- **`data.merge`**: `{ objects: [...] }`, merge profundo en orden, llaves
  posteriores ganan; arrays/escalares se reemplazan completos.
- **`data.cast`**: `{ source, fields, on_invalid? }`. `source` es un objeto o
  un array de filas; `fields` mapea paths con puntos (relativos a cada fila) a
  una lista de operaciones encadenables: `date` (`from`/`to` strftime, default
  ISO), `number` (`decimal`/`thousands`), `int`, `bool`
  (true/false/1/0/yes/no/si/sí), `string`, `trim`, `upper`, `lower`,
  `replace` (`from`/`to` literal) y `default` (`value`). Null/ausente
  **atraviesa** las ops sin error (solo `default` lo sustituye; un campo
  ausente que sigue null no se inserta). `on_invalid`: `fail` (default —
  `CAST_FIELD_INVALID` con fila y campo), `null` (el campo queda null) o
  `collect` (output `{ ok: [...], failed: [{ index, item, errors }] }`, espejo
  del foreach). Config malformada es `CAST_INPUT_INVALID`.
- **`data.template`**: `{ template, values }`, placeholders `{path.con.puntos}`,
  `{{`/`}}` escapan; placeholder ausente es error; output string.
- **`util.log` / `util.delay`**: devuelven su `value` (o null) como output,
  para no romper la cadena del token.
- **`http.request`**: un status 4xx/5xx NO es error por default (el status es
  dato, se rutea con gateways); con `fail_on_error_status: true` la tarea
  falla y aplican `retry`/`on_error` del nodo. Cuerpos de petición mutuamente
  excluyentes (`HTTP_INPUT_INVALID` si hay más de uno): `body` (JSON), `form`
  (urlencoded), `text` (raw; content-type vía headers, default `text/plain`),
  `body_blob` (binario **streamed** desde un `$blob`; default
  `application/octet-stream`) y `multipart` (form-data; partes
  `"texto"` | `{text, content_type?}` | `{json}` | `{blob, filename?, content_type?}`,
  los blobs van streamed con su longitud). Respuesta: `response_body` =
  `auto` (default: JSON si el content-type es json, string en otro caso) |
  `text` (fuerza string) | `blob` (descarga streamed al BlobStore; el `body`
  del output es la referencia `{"$blob", name, size}`, con `name` tomado del
  `content-disposition` si viene).

## Preguntas abiertas

_(Las dos preguntas originales quedaron resueltas en la implementación.)_

1. ~~**Cancelación**: ¿API de cancelación cooperativa en v1?~~ **Resuelta**
   (decisión #32): `run_with(trigger, RunOptions)` con `deadline` y
   `CancellationToken`, ilimitado por defecto. No afectó la spec JSON.
2. ~~**Operadores definitivos del mini-DSL de condiciones**~~ **Resuelta**:
   13 operadores built-in (`eq`/`ne`/`gt`/`gte`/`lt`/`lte`/`in`/`contains`/
   `exists`/`is_null`/`starts_with`/`ends_with`/`matches`) + registro de
   operadores custom del host (`expr::operators`), validado en build-time.

Pendientes nuevos (de uso real como plataforma de integración, ver decisiones
y "Futuro"): reuso de conexiones (pools en extensiones HTTP/SFTP a volumen) y
despacho dinámico a sub-workflows por campo. Ambos son aditivos.
