# Contrato de integración embebida y resultados

Este documento fija el reajuste del producto: **librería Rust dentro del host**. Sustituye el alcance histórico de servicio/CLI. El engine no abre listeners, instala señales del proceso ni administra un servicio autónomo. Los conectores salientes y las tareas internas supervisadas siguen siendo capacidades de la librería.

## Propiedad y ejecución

El host crea `EngineRuntime` una vez, comparte `WorkflowApplication` y espera `shutdown` antes de cerrar su executor Tokio. Un clon de la fachada no adquiere propiedad independiente del runtime.

```rust
let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
let app = runtime.application();
let result = async {
    let plan = app.prepare(access.clone(), definition).await?;
    app.execute(access, StartRunRequest::new(plan, input), cancellation).await
}.await;
let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
// Conservar ambos resultados; cerrar incluso si prepare/execute falla.
```

`cancellation` es un `CancellationToken` del host. `execute` retorna `Result<Value, ForgeError>` después de éxito, fallo o bloqueo que requiere intervención. El timeout se toma de `StartOptions.timeout_ms` y nunca supera `Limits.run_timeout_ms`. Cada intento, retry, loop, grupo y espera conserva sus límites.

Descartar el future solicita cancelación. La aceptación y limpieza continúan bajo supervisión del runtime, evitando abandonar una transacción de aceptación a la mitad. Cancelar no revierte un efecto remoto. El shutdown espera las llamadas propias y aborta sus tareas si debe forzar el cierre. Las operaciones y proveedores deben cooperar; un bucle Rust sin cesión o una syscall bloqueante no se pueden detener por la fuerza desde el engine.

`execute` requiere permisos Start, Read y Cancel. No admite `receipt_key`: una llamada de propiedad exclusiva no puede cancelar una ejecución deduplicada que pertenece a otro consumidor. Para identidades compartidas, señales o consultas explícitas, el host puede elegir `start`/`wait`/`cancel`; en ese caso conserva la responsabilidad de cancelar el run. No se crea un servicio remoto.

## Arranque y recuperación

`BootOptions::default()` usa `RecoveryPolicy::RejectUnfinished`. Si el store contiene trabajo pendiente devuelve `recovery.required`, libera el store y no ejecuta operaciones. No borra ni reanuda pendientes silenciosamente. El host puede optar por `BootOptions { recovery: RecoveryPolicy::Resume, ..Default::default() }`. La recuperación conserva revisión, intención, certeza del efecto y deadline original; no concede una vida ilimitada ni reinicia el plazo.

Persistir auditoría/resultados no significa autorizar reanudación. Las esperas solo progresan mientras existe un host ejecutando el motor, o después de su decisión explícita de recuperar. El motor no incorpora cron ni disparadores autónomos.

## Resultados y rutas

Una operación produce `OperationOutput.value` conforme a su `output_schema`, o un `OperationError { code, class, certainty, message }`. Las decisiones usan valores estructurados y códigos estables, nunca mensajes de texto. El módulo clasifica lo que sabe; el workflow decide la reacción de negocio.

`forge.data.equals` compara `{left, right}` y devuelve booleano. Usa igualdad estructural JSON de `serde_json::Value`: distingue tipos y ausencia de `null`; no realiza coerciones numéricas ni textuales. Un `decision` consume ese booleano. Las alternativas se prueban en orden declarado; se ejecuta la primera verdadera, o `fallback`. Sin coincidencia ni fallback devuelve `control.no_match`, tratable por un `try` exterior o por el host. Paralelismo se declara mediante `parallel`, nunca por múltiples coincidencias implícitas.

Un control `try` delimita el cuerpo protegido:

```json
{
  "id": "consulta", "kind": "try",
  "input": {"select": {"source": "input", "pointer": ""}},
  "body": "<BodyDefinition del request y su evaluación>",
  "catches": [
    {"id": "timeout", "code": "http.timeout", "body": "<BodyDefinition alternativo>"}
  ],
  "fallback": {"id": "general", "body": "<BodyDefinition de error no previsto>"}
}
```

El fragmento muestra campos; los marcadores de `body` deben reemplazarse por cuerpos reales. El [schema](../schemas/2/workflow.schema.json) define el formato completo. Un handler recibe `{"input": <entrada del try>, "error": <ForgeError>}`. Coincide por el código de `operation_error` del primer diagnóstico, o por su código de engine cuando no hay error de operación. Códigos e IDs no pueden repetirse. `fallback` es obligatorio para un `try`; `catches` puede estar vacío.

La salida del control es `{"outcome":"success","output":...}` o `{"outcome":"handled","handler":"id","output":...}`. El workflow puede seleccionar `/output` para normalizar ambas rutas. El engine no reemplaza esa salida por una respuesta HTTP.

Para manejo local se envuelve una operación; para manejo general se envuelve el cuerpo raíz. Los handlers pueden usar los mismos controles y operaciones. Si falla un handler, el error se propaga al `try` exterior; no vuelve a entrar en el mismo handler. La selección y el error se guardan antes de ejecutar el handler: recuperar una espera dentro de él no repite el request fallido.

## Límites que un handler no puede eludir

`try` no captura fallos de infraestructura, suspensión, efectos inciertos, cancelación, límites de recursos ni errores de operación Internal/Resource/Cancelled. Solo captura errores de operación con certeza `NotApplied`; una escritura con efecto incierto continúa bloqueada. El deadline global y la cancelación se revisan antes de seleccionar y antes de ejecutar el handler. Un timeout de intento de lectura puede tratarse si aún queda presupuesto global.

Los retries declarados de la operación se resuelven antes de entregar el fallo a `try`. El handler no autoriza repetir una escritura cuyo resultado se desconoce. Si no se define un `try`, el fallo se devuelve estructurado al host: esa es la salida genérica segura de la librería.

## HTTP es una capacidad saliente

`forge.http.<perfil>` contrato **2** devuelve `{status, body}` también para respuestas JSON 4xx/5xx. El workflow puede considerar un 404 como dato esperado, un 429 como alternativa o un 500 como rechazo. No hay retry implícito por status. El módulo JSON sigue rechazando contenido no JSON, payload inválido, exceso de bytes o fallos de transporte mediante códigos como `http.content_type`, `http.invalid_json`, `http.transport` y `http.timeout`. Un 204 devuelve `body: null`.

Una UI futura genera el mismo documento y consulta catálogo/schema. El código Rust implementa capacidades reutilizables; el documento configura sus relaciones, decisiones y recuperación. No se añade un intérprete de código ni una interfaz gráfica al engine.

## Determinismo

Con una revisión fijada y los mismos resultados observados, el control de decisiones sigue reglas explícitas. Esto no garantiza que una API remota devuelva siempre lo mismo, que dos ramas paralelas terminen en el mismo orden ni que el tiempo transcurra igual. La ejecución acotada es una garantía separada, impuesta por presupuestos y cooperación de módulos.
