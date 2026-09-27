# Workflow Forge — estado y continuidad

## Punto actual

Reajuste implementado y verificado para el alcance confirmado: librería Rust embebida, control del host, ejecución acotada y workflows que deciden rutas de datos y errores. [EMBEDDING](EMBEDDING.md) es el contrato vigente y [ROADMAP](ROADMAP.md) contiene los criterios de cierre.

Implementado en el árbol de trabajo:

- Retiro de los crates `service` y `cli`, ejemplos de servicio, contratos HTTP/CLI y medición del transporte retirado.
- `WorkflowApplication::execute` con cancelación del host, cancelación al descartar y tareas de aceptación/limpieza propiedad del runtime. No acepta recibos deduplicados.
- `RecoveryPolicy::RejectUnfinished` predeterminado; recuperación solamente mediante `Resume` explícito.
- Control `try` con códigos únicos, fallback obligatorio, selección persistida y propagación de fallos del handler. Mantiene prioridad de cancelación, deadlines, recursos y efectos inciertos.
- HTTP/JSON contrato 2: status/body son datos también para 4xx/5xx; errores técnicos siguen estructurados. Utilidad `forge.data.equals` en la composición estándar.
- Ejemplo local `v2_outcomes` para decisiones y fallback; ejemplo principal y AGENTS alineados con embedding.

## Evidencia del reajuste

Comandos ejecutados en esta sesión; no equivalen a garantías de producción:

| Comprobación | Resultado |
|---|---|
| `cargo check --workspace --all-targets --all-features` | Pasó tras los cambios de API y control. |
| `cargo test -p workflow-forge --test v2_control --all-features` | 16 pruebas pasaron, incluyendo rutas y cancelación. |
| `cargo test -p workflow-forge --all-features --test v2_control boot_requires_explicit_recovery` | Pasó: boot rechaza pendientes y Resume continúa el handler sin repetir operación. |
| `cargo test -p workflow-forge --all-features --test v2_http_outcomes` | Pasó: estados 200/404/429/500/204 y errores de JSON/content type. |
| `cargo test --workspace --all-features --locked` y continuación por paquetes/targets | La primera pasada detectó un fixture SQLite que esperaba recuperación implícita. Corregido para elegir Resume; la regresión pasó y se completaron los targets restantes sin repetir los ya aprobados. |
| `cargo test -p workflow-forge --all-features --test v2_failure_boundaries owned_call_dropped_during_acceptance` | Pasó: descartar durante aceptación termina cancelando el run y permite cierre sin pendientes. |
| `cargo test -p workflow-forge --all-features --test v2_effects workflow_fallback_cannot_hide` | Pasó: el fallback no oculta ni repite una escritura incierta. |
| `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` | Pasó tras corregir la lectura parcial en el fixture HTTP. |
| `cargo fmt --all --check` y `git diff --check` | Pasaron. |
| Ejemplos `v2_customer`, `v2_outcomes` y `manual/ejemplos` bin `inicio` | Ejecutados; salidas y assertions correctas. |
| Enlaces de documentación y manual | 700 referencias locales verificadas, sin rutas ni anchors HTML rotos. |
| Archify `arranque` y `arquitectura` | Entrega y comprobación final: 9/9 checks, cero errores/advertencias; cuatro viewports de navegador sin overflow. Revisión visual clara/oscura separada, paleta neutra conservada. |

La continuación de tests usó `v2_sqlite` filtrado a la regresión de reconciliación, después `v2_sqlite_inventory`, `v2_sqlite_waits`, `v2_wait_races`, `v2_waits`; `cargo test --workspace --all-features --exclude workflow-forge` y el doctest de la fachada completaron los demás crates. La primera pasada ya había aprobado aceptación, controles, efectos, fronteras de fallo, HTTP, inventario y paquetes de recuperación. No se atribuye éxito al comando inicial que terminó con fallo.

## Evidencia por requisito

| Requisito | Código o artefacto y evidencia |
|---|---|
| Solo librería Rust | [Workspace](../Cargo.toml) sin service/cli y [fachada](../crates/forge/src/lib.rs) con ejemplo embebido. |
| Llamada propia, cancelación y cierre | [execute](../crates/engine/src/runtime/execute.rs), [lifecycle](../crates/engine/src/runtime/lifecycle.rs); controles y fronteras de aceptación verificadas. |
| Recuperación explícita | `BootOptions.recovery`; prueba `boot_requires_explicit_recovery_and_resumes_a_selected_error_handler` y fixtures SQLite actualizados. |
| Rutas específicas y genéricas | [Formato](../crates/protocol/src/definition.rs), [schema](../schemas/2/workflow.schema.json), compilador y [control](../crates/engine/src/runtime/control.rs); éxito, catch, fallback, fallo de handler, deadline y recuperación verificados. |
| HTTP y utilidades reutilizables | [HTTP](../crates/modules/src/integrations/http.rs), [datos](../crates/modules/src/data.rs), fixtures de status/contenido y [ejemplo](../crates/forge/examples/v2_outcomes.rs). |
| Conservación de efectos | `workflow_fallback_cannot_hide_an_uncertain_write_or_repeat_it`; suites de efectos y recuperación. |
| Documentación y agente | PRD, arquitectura, TDD, contratos, patrones, manual y [AGENTS](../AGENTS.md) alineados; instrucciones del agente en inglés. |
| Diagramas | [Arranque](../manual/diagramas/arranque.html), [arquitectura](../manual/diagramas/arquitectura.html) y [recibos/resumen](../manual/diagramas/verificacion.json). |

Los recibos `delivery.json` acreditan el original Archify; `theme.json`, `check.json` y `visual-check.json` corresponden al HTML final adaptado. Las especificaciones son `sequence` y `architecture`; hashes de especificación/artefacto y alcance visual quedan en `verificacion.json`. El texto es español y los controles fijos del visor están en inglés.

## Evidencia histórica y límites

F-0–F-5, sus resultados y el servicio retirado permanecen en Git. Las mediciones locales conservadas en `docs/measurements` son históricas y no acreditan rendimiento de esta versión ni de otra carga. No se ejecutan nuevos benchmarks como parte del reajuste.

Las integraciones reales y capacidad de producción siguen diferidas por decisión del usuario. SQLite es un proveedor local para un coordinador propietario; no coordinación distribuida. Los módulos compilados deben cooperar con cancelación y no bloquear el executor.
