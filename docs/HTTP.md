# Workflow Forge — contrato HTTP F-4

Anexo normativo de [TDD-10/12](TDD.md#11-tdd-10--librería-y-servicio), bajo P-05/P-08 del [PRD](PRD.md). Describe la implementación F-4; el avance verificado vive en [PROJECT](PROJECT.md). El servicio es un Adapter de `WorkflowApplication`. Protocolo y engine no dependen de HTTP ni de Axum.

## 1. Instancia, confianza y preparación

`workflow-forge-service` usa Axum/Tokio y recibe una `EngineAssembly`, un `RequestAuthenticator` y opciones del host. Una instancia conserva `EngineRuntime`; los handlers comparten clones de `WorkflowApplication`. El ejecutable de referencia usa SQLite por defecto y permite memoria mediante configuración explícita. No cambia de perfil por un fallo de apertura. Al exponer el servicio fuera de loopback, el host termina TLS; este primer listener sirve HTTP.

El autenticador transforma cabeceras en `AccessContext`; scope, actor, permisos y recursos proceden de configuración confiable, nunca del JSON de negocio. El proveedor inicial acepta una sola cabecera `Authorization: Bearer TOKEN`, compara hashes de tokens con una primitiva de tiempo constante y no registra la credencial. El host le proporciona tokens de 32–4096 bytes ASCII gráficos, actores y grants; el ejecutable los obtiene mediante referencias de entorno. Un autenticador alternativo implementa el contrato del servicio sin cambiar el engine. Cada instancia atiende un scope; varios actores del mismo scope no constituyen multitenencia hostil. Revocar una credencial no cancela runs previamente aceptados.

Salud no requiere autenticación y solo publica disponibilidad. Todas las rutas `/v2` requieren autenticación. El engine vuelve a comprobar scope, permisos y recursos del comando. La composición se valida antes de escuchar; el scope del servicio debe coincidir con el del engine. Catálogo y capacidades requieren `read`; preparar requiere `prepare`; iniciar y cargar artefactos, `start`; consultar resultados/artefactos, `read`. Recursos adicionales se comprueban según el plan o `artifacts` para cargar/leer contenido. Cancelar, señalar y reconciliar usan sus permisos del protocolo, incluido `stop_tracking` cuando corresponde.

El plan se referencia por `{id, revision}` (`WorkflowRevision`). El servicio retiene planes preparados en una caché por instancia, máximo 1 000 por defecto; no serializa `PreparedWorkflow`. Las definiciones obligatorias del arranque se preparan antes de readiness y se incorporan a esa caché. Preparar la misma revisión no permite sustituir su contenido. Tras reiniciar, las definiciones del bootstrap o una nueva preparación habilitan nuevos starts; los runs ya aceptados recuperan su propio paquete, aunque no esté en esa caché.

## 2. Rutas y DTOs

JSON usa nombres `snake_case`, IDs del protocolo y milisegundos Unix para fechas. Los DTOs de control rechazan campos desconocidos; los valores de negocio conservan su JSON Schema. No se admite un campo de credenciales, actor, permisos o scope para elevar autoridad. DTOs Rust públicos del crate de servicio constituyen la representación wire; no se exportan checkpoints internos como respuesta HTTP.

| Método y ruta | Entrada | Respuesta correcta |
|---|---|---|
| `GET /health/live` | Ninguna. | `200 {"live":true}` mientras responde el servidor. |
| `GET /health/ready` | Ninguna. | `200 {"ready":true}` con engine/transporte listos; `503` con `false` en otro caso. |
| `GET /v2/catalog` | `limit`, `cursor` opcionales. | `200 {capabilities, items, next_cursor}`; descriptores filtrados por grants. |
| `POST /v2/workflows/prepare` | `{definition}` formato 2. | `200 {workflow:{id,revision}, diagnostics}`. No inicia un run. |
| `POST /v2/runs` | `{workflow:{id,revision}, input, options?}`. | `202 StartReceipt`, también al devolver un duplicado; `Location` apunta al run. |
| `GET /v2/runs/{id}` | RunId en la ruta. | `200 RunStatus`: estado, revisión, workflow, fechas, cancelación, conteos, error e incertidumbre. |
| `GET /v2/runs/{id}/invocations` | Paginación. | `200 {items, next_cursor}`: ruta, identidad/intento, estado, operación, certeza, error y próximo intento. |
| `GET /v2/runs/{id}/waits` | Paginación. | `200 {items, next_cursor}`: reserva, correlación, deadline, estado y acuse; sin payload entregado. |
| `GET /v2/runs/{id}/audit` | Paginación. | `200 {items, next_cursor}`: tipo, fecha, actor/comando, invocación/intento y resultado de la decisión; sin outputs de observaciones tardías. |
| `GET /v2/runs/{id}/result` | RunId. | `200 {run_id, output}` o error del engine si aún no existe resultado confirmado. |
| `POST /v2/runs/{id}/cancel` | Sin body. | `202 {run_id, cancellation_requested:true}`; no promete reversión remota. |
| `POST /v2/runs/{id}/signals` | `SignalCommand` del protocolo; run_id debe coincidir con la ruta. | `200 SignalReceipt`; duplicado conserva el acuse. |
| `POST /v2/runs/{id}/effects/inspect` | `{invocation_id}`. | `200 EffectInspection`; la consulta no aplica una decisión. |
| `POST /v2/runs/{id}/effects/reconcile` | `ReconcileCommand`; run_id coincide con la ruta. | `200 ReconcileReceipt`, incluida deduplicación. |
| `POST /v2/artifacts` | Body binario, `Content-Type` opcional. | `201 ArtifactRef`; no hay referencia antes de publicar todos los bytes. |
| `POST /v2/artifacts/read` | `{artifact:ArtifactRef}`. | `200` con stream binario, media type y longitud del artefacto; disposición attachment. |

La lectura de artefactos recibe la referencia completa que devolvió el motor. Así usa el mismo puerto público que Rust, valida metadatos/scope y funciona después de reiniciar sin otra caché de referencias ni consultas SQL del servicio. Las entradas cargadas se declaran en `options.artifacts` al iniciar el run; publicarlas no les asigna aún propiedad de un run. Rigen la gracia/retención y cuotas de CONTRACTS §10.3.

`options` tiene `require_durable`, `timeout_ms`, `receipt_key` y `artifacts`, con el significado del protocolo. En HTTP `require_durable` vale **true por defecto**, incluso si se omite `options`; para usar el perfil efímero el cliente lo indica como false. El acuse publica la garantía real. No se inventa estado `accepted` en un duplicado que ya pudo terminar. `receipt_key` vive en el body; no se añade una segunda capa de idempotencia en cabeceras.

Fragmento H-01 — iniciar un plan ya preparado:

```json
{
  "workflow": {"id": "reference.customer_lookup", "revision": "r1"},
  "input": {"request_id": "request-42", "customer": " C-9 ", "items": [{"sku": "A-1", "quantity": 2}]},
  "options": {"require_durable": true, "receipt_key": "request-42"}
}
```

La identidad debe corresponder a una definición cargada/preparada. Un cliente conserva el RunId del acuse y consulta su resultado. La desconexión o timeout HTTP no cancelan un run aceptado. El cliente puede reenviar la misma recepción dentro de su ventana para recuperar un acuse perdido; un contenido diferente produce conflicto. El servidor no hace retries ocultos de `start`, `signal` o `reconcile`.

## 3. Paginación, límites y errores

Catálogo se ordena por `OperationRevision`; invocaciones por ruta, esperas por WaitId y auditoría por orden registrado. `limit` predeterminado 50, máximo 100, mínimo 1. El cursor es opaco, está limitado a 1 024 bytes y vincula colección, instancia, actor/scope/grants y, para datos de run, revisión del snapshot. Un cursor ajeno o inválido se rechaza; si cambió la revisión, `409 state.conflict` exige consultar desde el principio. El cursor no otorga permisos. Cambiar límite entre páginas no cambia el significado del siguiente offset.

Límites iniciales del transporte, configurables por el host: 64 requests autenticados simultáneos, 2 MiB de JSON entrante, 8 MiB por respuesta JSON, 8 MiB por transferencia de artefacto, 30 s para un comando/transferencia y 30 s de drenado HTTP. Las cuotas del engine siguen vigentes y pueden rechazar entradas más pequeñas. El límite de bytes cuenta contenido recibido, incluso sin Content-Length. La serialización de respuesta está acotada; el cliente puede reducir `limit` si una página excede su presupuesto. Saturación se rechaza antes de acreditar aceptación. Un timeout después de un commit puede perder su acuse; se recupera por los comandos/recibos del engine.

Una carga cancelada, desconectada, vencida o excesiva no publica referencia y su staging se limpia sin exigir reiniciar un host sano. Un stream de descarga que falla después de enviar cabeceras termina con error de transporte; no se presenta como descarga completa ni puede cambiar entonces a JSON. No se registran bodies, tokens ni valores secretos. Estado/telemetría exponen metadatos y diagnósticos; el resultado y los bytes se obtienen mediante sus rutas explícitas.

Errores JSON conservan `ForgeError {diagnostics:[...]}` con código, phase, ubicación, clasificación y retryable; todos los mensajes libres se normalizan a textos estables, incluidos los de validación y de operaciones. El adaptador no devuelve texto de parsers HTTP ni cuerpos recibidos como error. Códigos/ubicaciones fuera de los límites publicados por el adaptador se rechazan o normalizan. La paridad con Rust se verifica sobre los campos estructurados, sin exigir igualdad del mensaje libre.

| HTTP | Casos |
|---|---|
| 400 | `http.invalid_request`, `http.cursor_invalid`, IDs incoherentes o query inválida. |
| 401 | `http.unauthenticated`; credencial ausente, duplicada o rechazada. |
| 403 | `access.denied`. |
| 404 | `not_found`, `resource.not_found`, `resource.missing`, `wait.not_found`; referencia/plan/espera o ruta no disponible. |
| 405 | `http.method_not_allowed`. |
| 409 | `state.conflict`, `not_ready`, incertidumbre que requiere resolución. |
| 413 | `http.body_too_large`, `http.response_too_large`. |
| 415 | `http.content_type` para un comando que requiere JSON. |
| 422 | Validación de definición/schema/mapping, capacidades, presupuestos lógicos y error de operación. |
| 429 | `admission.full`, `http.busy`. |
| 503 | `runtime.*`, `store.*`, `http.timeout`, servidor en cierre. |
| 500 | Error interno del adaptador; detalle interno no se publica. |

## 4. Fachada y lifecycle

La fachada lógica agrega `capabilities(access)`, `write_artifact(access, stream, media_type)` y `read_artifact(access, reference)`. Expone protocolo/formato, durabilidad y límites sin entregar objetos de infraestructura. Esas operaciones usan la composición del engine, validan grants y rechazan artefactos sin coordinación cuando el perfil es durable. Están disponibles para Rust y HTTP; ninguna depende del transporte.

El proveedor de artefactos declara `host_access()` e implementa `write_for_host(owner, ...)`/`read_for_host(owner, ...)` para estas transferencias anteriores a un run. SQLite comprueba ese propietario también por fragmento; un handle anterior no hereda la autoridad de otro boot. Un proveedor durable que no implemente esa frontera anuncia transferencias de host no disponibles y devuelve `capability.unsupported`; las operaciones dentro de un run conservan `ArtifactAccess`. Un error conocido del stream limpia staging antes de devolver el error; abandono forzado conserva la recuperación/limpieza de F-3 sin borrar una publicación cuyo acuse se perdió.

Arranque del servicio: validar opciones/autenticador, boot con definiciones obligatorias, comprobar scope/capacidades, cargar caché de planes, enlazar listener y publicar readiness. Un error después de boot espera shutdown del engine. No se ejecutan automáticamente los workflows cargados. El supervisor observa engine, servidor y solicitud de cierre; retirar readiness y cerrar admisión preceden al drenado. Después espera cierre del transporte y shutdown del runtime bajo sus plazos. El reporte identifica cierre forzado y runs pendientes. Drop aborta recursos supervisados, pero no acredita drenado: el host espera el cierre explícito.

Las tareas que sobreviven a una petición, incluidas cargas con cleanup pendiente, pertenecen al servicio y se drenan/cancelan bajo supervisión. No usar tareas desprendidas sin propietario. El servicio no vuelve a abrir el store con un handle viejo ni habilita admisión mientras recupera. Señales/comandos durante drenado siguen la política del engine y del listener, sin confirmar trabajo que el perfil no pueda conservar.

## 5. Evidencia exigida y referencias

V-10: C-01/C-02 equivalentes por Rust y HTTP, duplicado/conflicto y desconexión sin cancelar trabajo aceptado. V-16: instancia compartida, readiness, bind fallido, tarea esencial fallida, cierre, handles viejos y liberación del store. V-13: observador lento/fallido no modifica resultados; diagnósticos sin secretos. V-17/V-19: extensión/decorador sobre los puertos públicos, grants, errores, paginación y límites de body/stream comprobados. Las pruebas de router no sustituyen las de sockets y lifecycle.

Fuentes primarias: [Axum serve y shutdown](https://docs.rs/axum/latest/axum/serve/struct.Serve.html), [extractores y límites](https://docs.rs/axum/latest/axum/extract/struct.DefaultBodyLimit.html). Los límites de extractores JSON no sustituyen el límite de un stream binario propio. El mecanismo del framework no modifica las garantías de aceptación, efectos o recuperación del engine.

## 6. Ejecutable y composición del host

El [ejecutable](../crates/service/src/main.rs) recibe la ruta de un JSON de [configuración](../crates/service/src/host.rs). [config.json](../examples/service/config.json) carga un workflow echo y SQLite. Las rutas de workflows, base de datos y raíces de archivos son relativas al archivo de configuración. El directorio de la base debe existir. Omitir storage selecciona SQLite; `{"kind":"memory"}` lo cambia expresamente. Un error de configuración/apertura termina el proceso, sin fallback.

```sh
export WORKFLOW_FORGE_TOKEN="$(openssl rand -hex 32)"
cargo run -p workflow-forge-service -- examples/service/config.json
```

Un cliente que tenga el mismo token puede iniciar el workflow cargado:

```sh
curl -X POST http://127.0.0.1:7070/v2/runs \
  -H "Authorization: Bearer $WORKFLOW_FORGE_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"workflow":{"id":"example.echo","revision":"r1"},"input":{"hello":"forge"},"options":{"receipt_key":"example-1"}}'
```

El acuse contiene RunId; consultar `/v2/runs/{id}/result` con la misma credencial. Ctrl-C o SIGTERM en Unix cierra admisión, drena y libera propiedad. La salida estándar emite JSON Lines de `ExecutionEvent`: run, scope, revisión y estado, sin inputs/outputs. La observación tiene colas/plazos acotados y puede perder eventos; el estado confirmado se consulta en el motor. Stderr comunica readiness/cierre y códigos de fallo, sin imprimir configuración ni mensajes arbitrarios de proveedores.

`identities` requiere `token_env`, actor, permisos y recursos. `secrets` mapea nombres lógicos a variables de entorno; se leen al componer el host. `http` y `files` contienen perfiles de INTEGRATIONS; `csv: {}` habilita el parser con sus defaults. `engine_limits`, `transport` y `storage.options` aceptan overrides parciales de sus límites y rechazan campos desconocidos. La configuración está limitada a 1 MiB; los workflows respetan document_bytes, máximo 1 000 y 16 MiB agregados durante bootstrap.

Para un host Rust que ya construye sus proveedores, el fragmento esencial es:

```rust
let service = ServiceRuntime::boot(
    builder.build()?,
    authenticator, // Arc<dyn RequestAuthenticator> del host
    ServiceOptions { definitions, ..Default::default() },
).await?;
let app = service.application(); // comparte la instancia con otros consumidores
// conservar service mientras atiende la aplicación
let report = service.shutdown().await?;
```

El fragmento omite configuración y gestión de errores. El [bootstrap ejecutable](../crates/service/src/host.rs) y las [pruebas de proceso](../crates/service/tests/bootstrap.rs) muestran el recorrido completo y sus fallos; los handlers no reconstruyen proveedores ni runtimes.
