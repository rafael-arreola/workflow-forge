# workflow-forge

Motor agnóstico de integración en Rust. Las definiciones JSON conectan operaciones mediante contratos JSON Schema; el host decide qué módulos, recursos y transportes habilita.

**En desarrollo, sin versión pública.** `workflow_forge::v2` ejecuta secuencias, decisiones, paralelo, foreach, loop y subworkflows, con retry e inspección/resolución de efectos. Incluye estado/artefactos SQLite, recuperación, señales y timers. El servicio HTTP comparte esos contratos y agrega autenticación, consultas y transferencias acotadas; los módulos oficiales integran HTTP/JSON y archivos/CSV. La [evidencia y los límites](docs/PROJECT.md) distinguen pruebas locales de metas de producción y casos reales pendientes en el [ROADMAP](docs/ROADMAP.md).

## Ejecutar el primer recorrido

Desde este checkout:

```sh
cargo run -p workflow-forge --example v2_customer
```

Carga una definición, normaliza el cliente `" C-9 "`, consulta una extensión y devuelve `{"customer":"C-9","active":true}`. El [ejemplo completo](crates/forge/examples/v2_customer.rs) conserva el runtime y espera el apagado incluso si falla el recorrido.

La instancia se construye una vez al arrancar el host:

```rust
use workflow_forge::v2::*;

let mut builder = WorkflowBuilder::standard();
builder.register_bundle(host_operations)?;
let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
let app = runtime.application(); // clonar y compartir con los handlers
let access = AccessContext::trusted("default");
let plan = app.prepare(access.clone(), definition).await?;
let receipt = app.start(access.clone(), StartRunRequest::new(plan, input)).await?;
let completed = app.wait(access, receipt.run_id).await?;
runtime.shutdown(ShutdownOptions::default()).await?;
```

El fragmento omite la creación de la contribución y los datos. `build` es inactivo; `boot` establece propiedad y supervisión antes de readiness. `start` acusa aceptación; `wait` devuelve el estado final o bloqueo. El host autoriza el acceso; los plugins compilados son código confiable. El perfil en memoria no sobrevive a la caída del proceso.

Para recorrer una integración con lotes y efectos:

```sh
cargo run --release -p workflow-forge --example v2_inventory -- 100 3
```

El [workflow de inventario](examples/workflows/inventory_import.v2.json) lee un artefacto CSV, aplica filas mediante un destino inyectado y publica un reporte JSONL. La [extensión](examples/reference-module/src/inventory/mod.rs) mantiene el parseo y las reglas de inventario fuera del engine. El [ejecutable](crates/forge/examples/v2_inventory.rs) mide tres composiciones independientes; en un servicio se conserva una instancia durante su vida, como en `v2_customer`. Un efecto incierto bloquea la continuación hasta su resolución; `collect` recoge errores conocidos.

El incremento F-3 incorpora estado y artefactos SQLite:

```sh
cargo run -p workflow-forge --features sqlite --example v2_sqlite -- /tmp/forge-example.sqlite
```

El [host durable](crates/forge/examples/v2_sqlite.rs) ejecuta C-01A y solicita aceptación durable. Repetir el comando con la misma clave recupera su recibo dentro de la ventana de deduplicación. Un solo runtime reclama el archivo local; `shutdown` libera su propiedad. Para CSV/reportes, el mismo proveedor se instala en ambos puertos y se declaran las referencias de entrada en `StartOptions.artifacts`; [CONTRACTS §10.3](docs/CONTRACTS.md#artefactos-durables) muestra la composición. Las [pruebas C-02](crates/forge/tests/v2_sqlite_inventory.rs) recuperan caídas entre lotes, durante streaming y después de publicación. Un store durable rechaza dependencias de artefactos efímeros o sin coordinación de retención.

Para conservar una espera entre dos arranques, ejecutar ambos comandos dentro de cinco minutos:

```sh
cargo run -p workflow-forge --features sqlite --example v2_signal -- /tmp/forge-approval.sqlite start
cargo run -p workflow-forge --features sqlite --example v2_signal -- /tmp/forge-approval.sqlite signal
```

El [ejemplo](crates/forge/examples/v2_signal.rs) usa un [workflow de aprobación](examples/workflows/approval.v2.json). `start` imprime la reserva y cierra el host; `signal` recupera el mismo run, valida la aprobación y devuelve `{"start":null,"signal":{"approved":true}}`. Repetir `signal` conserva el acuse con `duplicate:true`. Es una demostración local con acceso confiable; el transporte y autenticación de callbacks los aporta el host. [CONTRACTS §11](docs/CONTRACTS.md#esperas-durables) explica el inicio opcional, los límites y las carreras.

## Contratos y extensiones

- [CONTRACTS](docs/CONTRACTS.md): documento, mappings explícitos, errores, recursos y cuotas.
- [Schema de formato 2](schemas/2/workflow.schema.json) y [definición ejecutable](examples/workflows/customer_lookup.v2.json).
- [Extensión externa](examples/reference-module/src/lib.rs): depende del protocolo y un cliente inyectado, sin importar internos del engine.
- [PATTERNS](docs/PATTERNS.md): Builder, Adapter, Decorator y reglas de evolución.
- [HTTP](docs/HTTP.md): arranque del servicio, credenciales, rutas, cuotas y cierre.
- [INTEGRATIONS](docs/INTEGRATIONS.md): perfiles de conectores, revisiones fijadas y ejemplos de composición.
- [Kit de conformidad](crates/conformance/src/lib.rs): checks públicos para proveedores sustitutos.

`standard()` instala `forge.data.identity`, `forge.text.trim`, estado/secretos/artefactos en memoria y un observador vacío. La configuración se congela antes del arranque. No se sobrescriben operaciones por orden de registro ni se descargan referencias de schemas desde la red.

## Ejecutar como servicio

Con `WORKFLOW_FORGE_TOKEN` definido por el host (32–4096 bytes ASCII gráficos):

```sh
cargo run -p workflow-forge-service -- examples/service/config.json
```

La [configuración de ejemplo](examples/service/config.json) usa SQLite y carga un workflow echo antes de publicar readiness en `127.0.0.1:7070`. El proceso conserva una instancia, autentica Bearer y atiende `POST /v2/runs`; Ctrl-C o SIGTERM inicia el cierre supervisado. [HTTP §6](docs/HTTP.md#6-ejecutable-y-composición-del-host) muestra la petición y las opciones. El perfil efímero requiere elegir `storage.kind = memory` y aceptar `require_durable: false` en el cliente.

## Organización

| Crate | Responsabilidad |
|---|---|
| `protocol` | Traits, DTOs, descriptores y puertos públicos. |
| `engine` | Preparación, coordinación, validación y lifecycle; depende de puertos. |
| `modules` | Operaciones y proveedores oficiales. |
| `forge` | Fachada y composición estándar. |
| `conformance` | Verificaciones reutilizables de proveedores. |
| `service` | Adapter HTTP, autenticación y ejecutable de composición/supervisión. |
| `examples/reference-module` | Extensión independiente del motor. |

El prototipo spec 1.0 (`core`, `extensions`, CLI y [EXAMPLES](EXAMPLES.md)) permanece temporalmente para caracterizar el comportamiento anterior. Su retiro acompaña la migración de consumidores y módulos en F-4/F-5; no se garantiza compatibilidad automática. El namespace `v2` permite identificar la nueva API durante esa transición.

## Verificación

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo run --release -p workflow-forge --example v2_measure -- 10 1024 1000 8
```

El benchmark separa preparación y ejecución, realiza 100 calentamientos y exige al menos 1 000 muestras. El test SFTP del prototipo requiere un servidor externo; permanece ignorado en las pruebas locales generales. Consultar [PROJECT](docs/PROJECT.md) para comandos, resultados y entorno realmente medidos.

Los dos ejecutables de medición admiten SQLite con `--features sqlite`. Después de los argumentos numéricos, `v2_measure` acepta `--sqlite RUTA_NUEVA.sqlite` y `v2_inventory` acepta `--sqlite DIRECTORIO`. El directorio padre debe existir; cada base de medición debe ser nueva. Ambos solicitan aceptación durable y conservan las comprobaciones de resultados. C-02 usa lotes de 100, concurrencia de filas 4 y presupuestos explícitos de 12 000 activaciones y 15 minutos por run. Tres muestras de C-02 no equivalen a percentiles; las secuencias usan al menos 1 000 observaciones.

El [mapa documental](docs/README.md) conecta PRD, arquitectura, TDD y hoja de ruta. Los casos reales y objetivos de producción siguen sujetos a validación con el implementador.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
