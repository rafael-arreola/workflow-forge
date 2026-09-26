# workflow-forge

Motor agnóstico de integración en Rust. Las definiciones JSON conectan operaciones mediante contratos JSON Schema; el host decide qué módulos, recursos y transportes habilita.

**En desarrollo, sin versión pública.** `workflow_forge::v2` ejecuta secuencias, decisiones, paralelo, foreach, loop y subworkflows en memoria, con retry e inspección/resolución de efectos. F-2 incluye importación CSV por lotes y mediciones verificadas de 100 y 10 000 filas. Persistencia y servicio tienen fases posteriores en el [ROADMAP](docs/ROADMAP.md). La [evidencia y los límites](docs/PROJECT.md) distinguen capacidades probadas de diseño pendiente.

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

## Contratos y extensiones

- [CONTRACTS](docs/CONTRACTS.md): documento, mappings explícitos, errores, recursos y cuotas.
- [Schema de formato 2](schemas/2/workflow.schema.json) y [definición ejecutable](examples/workflows/customer_lookup.v2.json).
- [Extensión externa](examples/reference-module/src/lib.rs): depende del protocolo y un cliente inyectado, sin importar internos del engine.
- [PATTERNS](docs/PATTERNS.md): Builder, Adapter, Decorator y reglas de evolución.
- [Kit de conformidad](crates/conformance/src/lib.rs): checks públicos para proveedores sustitutos.

`standard()` instala `forge.data.identity`, `forge.text.trim`, estado/secretos/artefactos en memoria y un observador vacío. La configuración se congela antes del arranque. No se sobrescriben operaciones por orden de registro ni se descargan referencias de schemas desde la red.

## Organización

| Crate | Responsabilidad |
|---|---|
| `protocol` | Traits, DTOs, descriptores y puertos públicos. |
| `engine` | Preparación, coordinación, validación y lifecycle; depende de puertos. |
| `modules` | Operaciones y proveedores oficiales. |
| `forge` | Fachada y composición estándar. |
| `conformance` | Verificaciones reutilizables de proveedores. |
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

El [mapa documental](docs/README.md) conecta PRD, arquitectura, TDD y hoja de ruta. Los casos reales y objetivos de producción siguen sujetos a validación con el implementador.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
