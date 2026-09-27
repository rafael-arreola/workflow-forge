# workflow-forge

Librería de integración agnóstica **para aplicaciones Rust**. Tu aplicación embebe el motor, registra módulos, carga workflows JSON y ejecuta sus capacidades mediante contratos JSON Schema. El host conserva el control del ciclo de vida; la librería no levanta un servicio.

**En desarrollo, sin versión pública.** Incluye secuencias, decisiones, `try` con manejadores de errores, paralelo, foreach, loops, subworkflows, esperas y señales. Los módulos oficiales aportan operaciones de datos, requests HTTP/JSON y archivos/CSV. Memoria es el perfil inicial; SQLite y recuperación explícita son elecciones del host.

## Integración

```rust
use workflow_forge::prelude::*;

let runtime = EngineRuntime::boot(WorkflowBuilder::standard().build()?, BootOptions::default()).await?;
let app = runtime.application();
let access = AccessContext::trusted("default"); // permisos que decide tu host
let result = async {
    let plan = app.prepare(access.clone(), definition).await?;
    app.execute(access, StartRunRequest::new(plan, input), cancellation).await
}.await;
let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
// Procesar ambos resultados; el cierre ocurre también cuando falla la ejecución.
```

`cancellation` es un `CancellationToken` del host. El ejemplo completo está en [v2_customer.rs](crates/forge/examples/v2_customer.rs). Descartar el future de `execute` solicita cancelación; las tareas de aceptación y limpieza quedan supervisadas. Los límites acotan intentos, duración, reintentos, datos y concurrencia. No se promete detener por la fuerza código Rust que bloquee el executor.

`start`/`wait`/`cancel` quedan disponibles para el host que necesita controlar señales, recibos deduplicados o seguimiento explícito. `BootOptions::default()` rechaza pendientes anteriores; reanudarlos requiere `RecoveryPolicy::Resume` y conserva el deadline original.

## Probar los recorridos locales

```sh
cargo run -p workflow-forge --example v2_customer
cargo run -p workflow-forge --example v2_outcomes
```

El segundo ejemplo usa respuestas de muestra, compara un estado, elige una ruta y maneja una entrada inesperada mediante el workflow. No conecta sistemas reales. [EXAMPLES](EXAMPLES.md) contiene recorridos de SQLite, artefactos, señales y autoría.

## Documentación

- [Manual práctico](manual/index.html): integración, módulos, datos y operación.
- [Contrato embebido y resultados](docs/EMBEDDING.md): propiedad, cancelación, `try`, HTTP y determinismo.
- [PRD](docs/PRD.md), [arquitectura](docs/ARCHITECTURE.md) y [TDD](docs/TDD.md): producto, comunicación y mecanismos.
- [Contratos](docs/CONTRACTS.md), [patrones](docs/PATTERNS.md) y [módulos oficiales](docs/INTEGRATIONS.md): reglas para extensiones.
- [Estado y evidencia](docs/PROJECT.md): comprobaciones actuales y límites.

## Crates

| Crate | Responsabilidad |
|---|---|
| `protocol` | Tipos, traits, descriptores y contratos públicos. |
| `engine` | Validación, preparación, ejecución y lifecycle. |
| `modules` | Operaciones y proveedores oficiales. |
| `forge` | Fachada `workflow-forge` y composición estándar. |
| `conformance` | Verificación reutilizable de contratos. |

Las extensiones son Rust compilado en el proceso y dependen del protocolo público. Una herramienta visual externa puede usar catálogo, schema y `presentation` para generar definiciones; las reglas del negocio permanecen en los módulos y workflows.

## Verificación

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo test --workspace --all-features --locked
```

Durante cambios pequeños se seleccionan las pruebas esenciales y casos borde afectados. Las mediciones históricas de capacidad no acreditan el rendimiento de otra carga ni del motor modificado. No es necesario ejecutar benchmarks para integrar la librería.

## License

MIT OR Apache-2.0.
