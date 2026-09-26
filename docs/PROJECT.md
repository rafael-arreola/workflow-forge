# Workflow Forge — estado y continuidad

Actualizado: 2026-09-26. Esta página registra evidencia; el producto objetivo está en [PRD](PRD.md), sus límites en [ARCHITECTURE](ARCHITECTURE.md) y mecanismos en [TDD](TDD.md).

## Punto actual

**Implementación autorizada el 2026-09-26:** avanzar F-0 a F-5 en orden y crear un commit por fase. La planificación quedó conservada en `66d39a5`; F-0 en `d5901a1`. F-1 está completada: fmt, Clippy predeterminado/all-features, tests del workspace y primeros benchmarks verificados. La API nueva se encuentra en `workflow_forge::v2`; la CLI y los módulos spec 1.0 permanecen temporalmente durante su migración.

F-1 incorpora protocolo público, engine, módulos oficiales, fachada, kit de conformidad y extensión externa. El recorrido C-01A recibe JSON, normaliza un identificador, consulta un cliente inyectado y devuelve un resultado validado. `build` permanece inactivo, `boot` reclama el store y supervisa ejecución, los handles comparten la instancia y `shutdown` cierra admisión y drena.

Los patrones se concretan en código: Builder en la composición, Adapter en la extensión, Factory Function en las contribuciones, Facade en el handle, Command en la invocación y Decorator en la prueba de observación. No se requiere importar internos del engine para extender operaciones o sustituir proveedores. El compilador fija revisiones y rechaza las capacidades fuera del perfil F-1.

El alcance completo sigue en ROADMAP. F-2 incorpora control y efectos; F-3 durabilidad/esperas; F-4 servicio y módulos de integración; F-5 adopción. P-01 aún requiere contrastar las referencias con dos sistemas reales del usuario y P-07 cerrar sus metas de despliegue. El éxito de los fixtures no acredita esas integraciones.

## Evidencia F-1

| Contrato / verificación aplicable | Evidencia |
|---|---|
| C-01A; V-01/V-02/V-03/V-04 | [Extensión pública](../examples/reference-module/src/lib.rs), [fixture](../examples/workflows/customer_lookup.v2.json) y [schema](../schemas/2/workflow.schema.json). Round-trip, metadatos visuales, validación antes de aceptar, salida global validada y compatibilidad desconocida explícita. |
| C-03; V-04/V-05/V-17 | [Aceptación](../crates/forge/tests/v2_acceptance.rs): sustitución del cliente, Decorator transparente, 32 runs independientes, registro atómico y recursos autorizados. [Kit público](../crates/conformance/src/lib.rs) ejecutado contra memoria y wrapper alternativo. |
| V-15, parte en memoria | Revisiones inmutables, plan ligado a composición y recuperación del trabajo aceptado cuyo solicitante desaparece antes del acuse. Revisión durable ausente corresponde a F-3. |
| V-16, parte embebida | Boot fallido libera propiedad, doble propietario rechazado, fallo esencial elimina readiness, cierre forzado y cleanup tras fallo del supervisor. |
| V-19 | Scopes/permisos, referencias de schemas sin red, diferencia ausente/null/literal, cuotas de datos/admisión, garantía durable rechazada explícitamente. |
| Retención y cancelación | [Fronteras de fallo](../crates/forge/tests/v2_failure_boundaries.rs): recibos sobreviven a resultados expirados, CAS y terminal inmutable, aceptación sin conexión del solicitante, artefactos acotados y cierre del runtime. |
| Host ejecutable | `target/release/examples/v2_customer`: exit 0; estado `Succeeded`, salida `{"active":true,"customer":"C-9"}`. |
| Checks de cierre | `cargo fmt --all --check` y Clippy workspace/all-targets con features predeterminadas y all-features, usando `-D warnings`: exit 0. Tests workspace/all-features: 241 pasan, 0 fallan, 1 SFTP ignorada. Enlaces locales, fences y JSON documental válidos; no se volvió a renderizar Mermaid. |
| Suite dirigida | `cargo test -p workflow-forge --test v2_acceptance --test v2_failure_boundaries`: 21 pruebas pasan. |

Los tests usan únicamente API/puertos públicos para inyectar fallos. No acreditan durabilidad: en F-1 un proceso perdido pierde su store en memoria. El perfil de schemas y los límites adicionales están publicados en CONTRACTS. El kit de stores es secuencial; sus casos no sustituyen las pruebas de concurrencia y caída requeridas para un backend durable.

### Primera medición V-14

Ejecutada el 2026-09-26 sobre el cambio F-1 basado en `d5901a1`: Apple M1 Max (10 CPU, 64 GiB), macOS 27.0, `rustc 1.98.1`, build `release`. Datos de una pasada local, sin red ni latencia simulada. No son objetivos de producción ni comparación entre máquinas.

Comandos: `cargo build --release -p workflow-forge --example v2_measure --example v2_customer`; después `/usr/bin/time -l target/release/examples/v2_measure N BYTES 1000 CONCURRENCY`. Cada proceso ejecuta 100 preparaciones y runs de calentamiento, más 1 000 muestras medidas. Preparación recompila el documento contra catálogo fijo; ejecución reutiliza el plan. El calentamiento termina antes de comenzar a contar throughput. Se comprueba estado y contenido del resultado.

| Nodos / JSON / concurrencia | Preparación p50 / p95 / p99 (µs) | Ejecución p50 / p95 / p99 (µs) | Runs/s | RSS máximo (bytes) |
|---|---|---|---|---|
| 1 / 1 KiB / 1 | 16.792 / 38.000 / 61.917 | 105.000 / 141.166 / 182.958 | 8 848.53 | 19 595 264 |
| 10 / 1 KiB / 8 | 45.958 / 66.417 / 84.250 | 7 563.834 / 11 186.875 / 12 334.250 | 932.82 | 35 962 880 |
| 100 / 1 KiB / 32 | 387.417 / 440.542 / 486.250 | 385 809.625 / 606 257.083 / 625 247.166 | 51.97 | 251 166 720 |
| 1 / 64 KiB / 1 | 14.792 / 28.958 / 48.084 | 628.958 / 726.916 / 789.584 | 1 534.17 | 218 103 808 |

Cero resultados incorrectos o runs fallidos en esas cuatro combinaciones. Defaults de CONTRACTS, incluyendo retención de hasta 1 000 resultados: el RSS incluye resultados retenidos y preparación. `/usr/bin/time -l` mide el proceso ejecutable, sin compilador; se habilitó la consulta de estadísticas del SO después de que el sandbox la bloqueara. La pasada anterior sin estadísticas completas no se usa en la tabla.

La carga de 100 nodos muestra un coste de coordinación que habrá que perfilar antes de prometer eficiencia de producción. Las cargas cambian también concurrencia; no permiten atribuir toda la diferencia al número de nodos. Esta entrega fija una comparación inicial; la matriz completa de ACCEPTANCE, la latencia simulada y las cargas durables se amplían en las fases correspondientes.

## Evidencia del prototipo

Revisión estática inicial del 2026-09-26; no equivale a una suite ejecutada ni a auditoría exhaustiva.

| Evidencia local | Lectura y brecha |
|---|---|
| [Task y manifiesto](../crates/core/src/task/mod.rs) | Abstracción de operación existente; schemas opcionales y sin todas las revisiones del diseño nuevo. |
| [Registro](../crates/core/src/task/registry.rs) y [schemas compilados](../crates/core/src/runtime/schemas.rs) | Registro sustituible por ID y validadores construidos antes del run; fijar referencias evita divergencia. Es inferencia estática, no reproducción de fallo. |
| [Executor](../crates/core/src/runtime/executor.rs) | Coordinación/contexto en memoria; requiere separación para recuperación del nuevo diseño. |
| [Política](../crates/core/src/runtime/policy.rs) | Validación, timeout y retry existentes; falta formalizar incertidumbre y seguridad de repetición del nuevo diseño. |
| [Gateways](../crates/core/src/runtime/handlers/gateway.rs) | Joins por entradas del grafo; revisar activación de ramas con el contrato propuesto. |
| [Observación](../crates/core/src/observe/mod.rs) | Eventos y observers; no sustituyen commit transaccional. |
| [Idempotencia](../crates/core/src/idempotency.rs) | Helper basado en contenido; el nuevo diseño distingue invocación y clave de negocio. |
| [I/O](../crates/core/src/io/mod.rs), [extensiones](../crates/extensions) y [fachada](../crates/forge/src/lib.rs) | Recursos/conectores y experiencia integrada que sirven como base de refactorización. |
| [Pruebas del core](../crates/core/tests) | Casos existentes para baseline y caracterización; no se reporta conteo ni éxito sin ejecutarlos. |

## Estado por fase

| Fase | Estado |
|---|---|
| F-0 | Completada: diseño/casos registrados, formato y benchmark corregidos; fmt, ambas variantes de Clippy y 220 pruebas all-features pasan. |
| F-1 | Completada: C-01A/C-03, garantías aplicables en memoria, 21 pruebas nuevas y primera medición V-14. |
| F-2 a F-5 | Pendientes según dependencias y criterios de ROADMAP. |

## Cobertura de los puntos revisados

| Punto | Evidencia documental y límite |
|---|---|
| Primera entrega usable | CONTRACTS §2 y ROADMAP F-1, con capacidades incluidas/rechazadas y casos de salida. |
| Integraciones representativas | ACCEPTANCE C-01/C-02 define datos, pasos, efectos y fallos; el contraste con sistemas reales permanece explícito en P-01. |
| Contratos concretos | CONTRACTS §3–6 y E-01: documento, revisiones, mappings, schema dialect, operación y errores. |
| Confianza y recursos tempranos | CONTRACTS §7, P-08 y V-19 desde F-1; no se promete sandbox de plugins compilados. |
| Ejecuciones inciertas | TDD-06, C-04 y V-18: evidencia, permisos, transiciones, concurrencia, auditoría y cierre con incertidumbre. |
| Evidencia y mediciones tempranas | Baseline de abajo y ACCEPTANCE §5; mediciones iniciales ejecutadas arriba; matriz completa aún pendiente. |

La lectura por rol está en README; PATTERNS §8 orienta la elección por necesidad y ARCHITECTURE §1.1 explica motivos/alternativas. Se conserva un registro único de decisiones P-* y se enlazan los detalles sin duplicar sus estados.

## Verificaciones históricas del baseline documental

Las diferencias de fmt/Clippy de esta tabla se corrigieron en F-0; no describen el estado actual.

| Verificación | Estado |
|---|---|
| Revisión documental: enlaces locales, anchors, IDs, cobertura PRD→TDD→V y referencias de fases | Verificada el 2026-09-26 con script local Python: 15 requisitos, 12 contratos y 19 verificaciones previstas; enlaces/anchors válidos y cobertura de requisitos conservada en nueve documentos activos. |
| Ampliación arquitectónica E-01 a E-10 | Verificada el 2026-09-26: diez fragmentos únicos, JSON ilustrativo parseable, enlaces y referencias a verificaciones válidos; participantes de las secuencias identificados. Ocho diagramas en ARCHITECTURE. |
| Guía de patrones PAT-01 a PAT-10 | Verificada el 2026-09-26: diez IDs de patrón y cinco fragmentos PX únicos, referencias y enlaces válidos; conformidad V-17 enlazada a TDD-02 y fases. Los fragmentos Rust no se compilaron. |
| `git diff --check` | Sin errores el 2026-09-26; los documentos nuevos también se revisan directamente porque aún no están bajo seguimiento. |
| Renderizado visual de diagramas Mermaid | No verificado; CLI `mmdc` no disponible. Se revisó texto y referencias de participantes, sin afirmar parseo/renderizado Mermaid. |
| `cargo test --workspace` | Ejecutado el 2026-09-26, exit 0: 216 pruebas exitosas, 0 fallidas y 1 ignorada, incluyendo doctests. |
| `cargo test --workspace --all-features` | Ejecutado el 2026-09-26, exit 0: 220 pruebas exitosas, 0 fallidas y 1 ignorada, incluyendo doctests. |
| `cargo fmt --all --check` | Exit 1: diferencias preexistentes en 10 archivos Rust. Se registran como baseline; no se reformateó código durante esta tarea documental. |
| `cargo clippy --workspace --all-targets -- -D warnings` | Exit 101: benchmark importa `criterion`, dependencia opcional no activada en esta variante; error derivado de `main` ausente. |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | Exit 101: `crates/core/benches/workflow_bench.rs:22` usa `to_async`, no disponible con las features actuales de Criterion. No se acredita lint limpio del workspace. |
| SFTP real | Prueba `put_list_get_roundtrip` ignorada en ambas ejecuciones locales; requiere servidor y variables `WF_SFTP_*`. No se ejecutó ni se atribuye cobertura de esa frontera externa. |
| Recuperación, paridad API/librería y benchmarks objetivo | No implementados/verificados por esta entrega. |

## Siguiente punto de reanudación

Conservar el checkpoint F-1; después concretar las instrucciones de F-2 bajo P-09, las cuotas de P-07 y TDD-05/06 antes de implementarlas. Contrastar referencias con integraciones reales cuando haya datos. La implementación y los checkpoints por fase ya están autorizados.

Baseline reproducido sobre HEAD `392303b`, con los cambios documentales del worktree, macOS y `rustc 1.98.1` / `cargo 1.98.1`. La ejecución inicial aislada no podía preparar dependencias; las pruebas se completaron después con acceso autorizado. La CI existente usa all-features y un job SFTP separado. El manifiesto del core declara Criterion opcional bajo `benchmarks`; el target de benchmark no declara `required-features`. Antes de medir rendimiento, corregir la configuración de ese target y habilitar su soporte async compatible, volver a comprobar Clippy y registrar el resultado. El formato pendiente debe tratarse en un cambio explícito de código, sin confundirlo con deuda introducida por esta documentación.

Al comenzar implementación, verificar las instrucciones locales y el estado de Git. La eliminación del diseño en raíz y de tres HTML bajo `docs/` ya estaba presente antes de la refactorización documental; no se restauraron esos archivos.
