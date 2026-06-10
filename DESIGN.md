# workflow-forge — Diseño

> Motor de workflows declarativos estilo BPMN, definido, validado y extendido con
> JSON Schema + JSONPath. Escrito en Rust, embebible como librería.
> Estado: decisiones fundacionales tomadas el 2026-06-09. Pre-spec.

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

`kind: "subworkflow"` queda **reservado** en la spec 1.0 (no implementado en
v1): el validador lo rechaza con "no soportado aún", pero ninguna extensión
puede ocupar ese kind.

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
```

- `label` conecta ramas de un gateway.
- `on: "error"` define la ruta cuando un nodo agota sus reintentos.

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
| `data` | `data.transform`, `data.merge`, `data.template` | Reshape con JSONPath, plantillas de strings |
| `util` | `util.delay`, `util.log`, `util.noop` | Debug, pruebas y ejemplos |
| `sftp` | `sftp.get`, `sftp.put`, `sftp.list` | Integraciones empresariales; usa `$blob` |
| `tabular` | `tabular.parse`, `tabular.write` | CSV y XLSX ↔ JSON; usa `$blob` |
| `fs` | `fs.read`, `fs.write` | Local, complementa sftp; usa `$blob` |

Roadmap de extensiones: `compress` (zip/gzip), `crypto` (hash/HMAC), `storage`
(S3-compatible) y `smtp` en v1.x; `db` (SQL) y `queue` (AMQP/Kafka) post-v1.

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
- **Workspace**: ver "Layout del workspace" arriba (+ futuro `crates/cli`).
- **Mínimos de release**: README con quickstart, schemas publicados, ejemplos
  ejecutables, CI (fmt + clippy + test), CHANGELOG, publicación en crates.io.

## Roadmap

### Fase 0 — Spec draft
JSON Schema del workflow (nodos, gateways, edges, retry, mappings), JSON Schema
del manifiesto de extensión, y 3–5 workflows de ejemplo que validen contra
ellos. *La spec se diseña sobre ejemplos, no al revés.*

### Fase 1 — Core alineado a la spec
Evolucionar el executor actual: contexto global, resolución JSONPath de
mappings, gateways (exclusive/parallel/join), retry/timeout/on_error,
validación del grafo (ciclos, nodos huérfanos, aristas a ids inexistentes),
catálogo exportable de tareas.

### Fase 2 — Extensiones oficiales
`ext-util`, `ext-data`, `ext-http`, `ext-tabular`, `ext-sftp` + fachada, cada
una con manifiesto, schemas y tests de integración.

### Fase 3 — Pulido OSS y v1.0
Docs, ejemplos, CI, licencias, publicación en crates.io, anuncio.

### Futuro (post-v1)
- CLI runtime (`forge run workflow.json`)
- Extensiones WASM instalables sin recompilar
- Durabilidad: executor event-sourced detrás de un trait de storage; espera de
  eventos externos
- Servidor con API / triggers
- Editor visual (el grafo + schemas lo hacen posible)

## Preguntas abiertas

1. **Cancelación**: ¿API de cancelación cooperativa de una ejecución en v1?
   (No afecta la spec JSON; se decide al diseñar la API del executor.)
2. **Operadores definitivos del mini-DSL de condiciones**: la lista propuesta
   arriba se cierra en Fase 0 contra los workflows de ejemplo.
3. **Diseño fino de `data.transform` / `data.template`**: dado que todo el peso
   de las transformaciones cae en estas tareas (decisión #14), su input schema
   merece diseño cuidadoso en Fase 0.
