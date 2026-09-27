# Workflow Forge — recorridos ejecutables

Estos ejemplos usan `workflow_forge::v2` y `forge.workflow/2`; la fachada raíz y `prelude` exponen la misma API. Se ejecutan desde el checkout. [ADOPTION](docs/ADOPTION.md) explica cómo componer el host, crear extensiones y migrar consumidores. [PROJECT](docs/PROJECT.md) conserva las verificaciones y límites reales.

## JSON: normalizar, consultar y responder

```sh
cargo run -p workflow-forge --example v2_customer
```

El [host](crates/forge/examples/v2_customer.rs) registra una [extensión externa](examples/reference-module/src/lib.rs) y prepara [customer_lookup.v2.json](examples/workflows/customer_lookup.v2.json). Normaliza `" C-9 "`, consulta un directorio inyectado y devuelve `{"customer":"C-9","active":true}`. Conserva una instancia y espera su cierre. El directorio es una referencia local; sustituir su cliente no cambia el engine.

## Autoría mediante catálogo y JSON Schema

```sh
cargo run -p workflow-forge-authoring-example --example round_trip
```

El [consumidor](examples/authoring-client/src/lib.rs) construye un documento desde el catálogo, conserva metadata visual, exporta/importa y usa preparación pública. El [programa](examples/authoring-client/examples/round_trip.rs) publica el documento JSON en stdout y comprueba `ID-42`. La [operación prefix](examples/reference-module/src/text.rs) muestra un plugin con configuración reusable y datos independientes por invocación. No requiere UI ni imports internos del engine.

## Estado durable y deduplicación

```sh
cargo run -p workflow-forge --features sqlite --example v2_sqlite -- /tmp/forge-example.sqlite
```

El [programa](crates/forge/examples/v2_sqlite.rs) reclama un store SQLite local y pide aceptación durable. Repetir el comando con la misma recepción conserva el RunId dentro de la ventana de deduplicación. Solo un runtime posee el archivo; un error de apertura no cambia el perfil a memoria.

## CSV por lotes, efectos y reporte

```sh
cargo run --release -p workflow-forge --example v2_inventory -- 100 3
```

[inventory_import.v2.json](examples/workflows/inventory_import.v2.json) procesa un artefacto CSV, aplica filas a un destino inyectado y publica un reporte JSONL correlacionado. El [programa](crates/forge/examples/v2_inventory.rs) mide tres composiciones locales de 100 filas; [la extensión](examples/reference-module/src/inventory/mod.rs) conserva las reglas de negocio fuera del motor. Una escritura incierta bloquea la continuación hasta una resolución; `collect` recoge fallos conocidos. Los módulos genéricos [archivos/CSV](docs/INTEGRATIONS.md) no conocen SKU ni reglas de inventario.

## Espera entre arranques

Ejecutar ambos comandos dentro de cinco minutos:

```sh
cargo run -p workflow-forge --features sqlite --example v2_signal -- /tmp/forge-approval.sqlite start
cargo run -p workflow-forge --features sqlite --example v2_signal -- /tmp/forge-approval.sqlite signal
```

El [host](crates/forge/examples/v2_signal.rs) recupera [approval.v2.json](examples/workflows/approval.v2.json), valida la señal y conserva el acuse duplicado. La demostración usa acceso local confiable; [HTTP](docs/HTTP.md) aporta la frontera autenticada de un servicio.

## Servicio HTTP

Con `WORKFLOW_FORGE_TOKEN` definido por el host:

```sh
cargo run -p workflow-forge-service -- examples/service/config.json
```

La [configuración](examples/service/config.json) carga un echo y usa SQLite. [HTTP §6](docs/HTTP.md#6-ejecutable-y-composición-del-host) detalla credenciales, petición, rutas relativas y apagado; [INTEGRATIONS](docs/INTEGRATIONS.md) muestra cómo agregar perfiles HTTP/JSON, archivos y CSV. El ejecutable registra solo metadata de eventos.

Con ese servicio en ejecución y la credencial en otra terminal:

```sh
cargo run -p workflow-forge-cli -- validate examples/service/echo.v2.json
cargo run -p workflow-forge-cli -- run examples/service/echo.v2.json --input '{"hello":"world"}'
```

La [CLI](docs/CLI.md) usa las mismas rutas. `--no-wait` devuelve el recibo; `wait`, `status`, `waits`, `signal`, `inspect` y `reconcile` continúan el seguimiento con ese RunId. La salida del cliente no cancela el trabajo aceptado.

## Capacidad, retención y mediciones

```sh
cargo build --release -p workflow-forge --features sqlite --example v2_measure --example v2_capacity
target/release/examples/v2_measure 1 1048576 1000 32 --terminal-runs 64
target/release/examples/v2_capacity saturation 32
target/release/examples/v2_capacity release 32 1048576
```

[v2_measure](crates/forge/examples/v2_measure.rs) separa preparación y ejecución de cadenas. [v2_capacity](crates/forge/examples/v2_capacity.rs) comprueba admisión llena y expiración de resultados conservando una instancia viva. Ambos admiten `--sqlite RUTA_NUEVA`; cada experimento durable exige una base nueva. El modo release consulta RSS con `ps`, disponible en macOS/Linux. Usar la herramienta de medición del sistema sobre el binario compilado para registrar además su pico RSS.

Los comandos publican JSON y fallan ante resultados inesperados. [ACCEPTANCE §5](docs/ACCEPTANCE.md#5-medir-temprano-y-publicar-límites-comprobados) define muestras, retención, cuotas y tiempos artificiales; [PROJECT](docs/PROJECT.md) distingue campañas completadas de pendientes. La espera controlada de saturación permite probar límites; su throughput no representa la capacidad máxima.

## Control, fallos y recuperación

| Recorrido | Prueba pública reproducible |
|---|---|
| Decisión, paralelo, foreach, loop y subworkflows | [v2_control.rs](crates/forge/tests/v2_control.rs) |
| Escritura incierta, inspección y reconciliación | [v2_effects.rs](crates/forge/tests/v2_effects.rs) |
| Caída de proceso con efecto/acuse pendiente | [v2_sqlite.rs](crates/forge/tests/v2_sqlite.rs) |
| CSV durable entre lotes y publicación de artefacto | [v2_sqlite_inventory.rs](crates/forge/tests/v2_sqlite_inventory.rs) |
| Paridad Rust/HTTP con memoria y SQLite | [parity.rs](crates/service/tests/parity.rs) |

Los escenarios del prototipo que dependían de XLSX, SFTP, compresión o expresiones JSONPath requieren una implementación expresa bajo el protocolo nuevo. El código anterior y sus recetas históricas se conservan en Git, hasta `6806fa0`. No se presenta esa compatibilidad como entregada.
