# Workflow Forge — estado y continuidad

Actualizado: 2026-09-26. Esta página registra evidencia; el producto objetivo está en [PRD](PRD.md), sus límites en [ARCHITECTURE](ARCHITECTURE.md) y mecanismos en [TDD](TDD.md).

## Punto actual

**Implementación autorizada el 2026-09-26:** avanzar F-0 a F-5 en orden y crear un commit por fase. El worktree inicial estaba limpio en `66d39a5`, que ya conserva la planificación. F-0 corrigió la configuración async de Criterion, declaró `required-features` en el benchmark y aplicó rustfmt al baseline. `cargo fmt --all --check`, Clippy con features predeterminadas y all-features, y `cargo test --workspace --all-features` pasan (220 pruebas, 1 SFTP ignorada). F-1 es el siguiente trabajo; las verificaciones históricas de abajo conservan el diagnóstico anterior.

El usuario solicitó completar primero la refactorización documental. Se crearon PRD, ARCHITECTURE, TDD, mapa documental y ROADMAP a partir de las decisiones de la conversación y el plan inicial. No se implementó la arquitectura objetivo ni se cambiaron contratos de código en este trabajo.

Después de consolidar la especificación, se eliminaron el diseño anterior, el borrador inicial y los manuales sustituidos por solicitud del usuario. Los ejemplos y schemas se conservan porque siguen siendo utilizados por las pruebas del prototipo.

La arquitectura se amplió con matriz de comunicación, propiedad por ámbito, secuencias de preparación/ejecución/recuperación/señales, recetas de extensión y ocho fragmentos orientativos E-*. ROADMAP desarrolla los paquetes de trabajo por fase. Se tomó como referencia la presentación de contratos y recorridos de `memory-forge`, sin adoptar su dominio o infraestructura. Los fragmentos no se compilaron ni representan API entregada.

Se agregó el contrato de instancia y bootstrap: composición inactiva, boot asíncrono, runtime propiedad del host y handles compartidos. E-09/E-10 muestran registro/carga, readiness, supervisión y apagado; E-06 se ajustó al mismo lifecycle. V-16 cubre arranque y cierre, con integración prevista en F-1/F-3/F-4.

Se añadió PATTERNS como desarrollo de la arquitectura: diez patrones/mecanismos con participantes, límites y conformidad, cinco fragmentos PX-* y reglas de registro, compatibilidad y retiro de extensiones. TDD-02 incorpora el contrato de contribuciones; V-17 verifica su crecimiento sin cambios de negocio en el engine. P-03 ahora adopta crates compilados; la carga dinámica queda fuera de la primera composición.

Las respuestas confirmadas permiten rehacer API/formato y exigen librería + servicio, protocolos + implementaciones + extensiones, composición integrada y mecanismos sustituibles de recuperación. El plan adopta UI separada, plugins compilados, SQLite local como referencia durable y HTTP/JSON para servicio; no se presentan como preferencias originalmente confirmadas por el usuario.

Se cerró el diseño de la primera entrega en CONTRACTS: formato 2, bindings pequeños, schema dialect/refs, frontera async, errores, confianza, recursos y defaults. ACCEPTANCE concreta dos integraciones de referencia, sustitución de módulos y esperas/resolución, con fixtures y protocolo de medición desde F-1. TDD-06 define reconciliación autorizada, evidencia, carreras y auditoría; V-18/V-19 amplían su verificación. PATTERNS agrega una guía por necesidad del desarrollador. No se programó el motor nuevo.

P-01 sigue esperando contraste con sistemas reales del usuario. Las referencias permiten construir contratos sin inventar hechos de esos sistemas. Las metas de producción, codecs/DDL durables y rutas/DTOs del servicio conservan condiciones de cierre por fase en PRD.

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
| F-1 a F-5 | Planificadas; implementación no iniciada por este trabajo. |

## Cobertura de los puntos revisados

| Punto | Evidencia documental y límite |
|---|---|
| Primera entrega usable | CONTRACTS §2 y ROADMAP F-1, con capacidades incluidas/rechazadas y casos de salida. |
| Integraciones representativas | ACCEPTANCE C-01/C-02 define datos, pasos, efectos y fallos; el contraste con sistemas reales permanece explícito en P-01. |
| Contratos concretos | CONTRACTS §3–6 y E-01: documento, revisiones, mappings, schema dialect, operación y errores. |
| Confianza y recursos tempranos | CONTRACTS §7, P-08 y V-19 desde F-1; no se promete sandbox de plugins compilados. |
| Ejecuciones inciertas | TDD-06, C-04 y V-18: evidencia, permisos, transiciones, concurrencia, auditoría y cierre con incertidumbre. |
| Evidencia y mediciones tempranas | Baseline de abajo y ACCEPTANCE §5; mediciones del motor nuevo aún por implementar, con cargas definidas desde F-1. |

La lectura por rol está en README; PATTERNS §8 orienta la elección por necesidad y ARCHITECTURE §1.1 explica motivos/alternativas. Se conserva un registro único de decisiones P-* y se enlazan los detalles sin duplicar sus estados.

## Verificaciones

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

Continuar F-1 con [CONTRACTS](CONTRACTS.md) y C-01A/C-03 de [ACCEPTANCE](ACCEPTANCE.md). Convertir el diseño a tipos/schemas y recorrido de F-1, incluyendo confianza y mediciones; mantener las garantías de recuperación previstas. Contrastar referencias con integraciones reales cuando haya datos. La implementación y los checkpoints por fase ya están autorizados.

Baseline reproducido sobre HEAD `392303b`, con los cambios documentales del worktree, macOS y `rustc 1.98.1` / `cargo 1.98.1`. La ejecución inicial aislada no podía preparar dependencias; las pruebas se completaron después con acceso autorizado. La CI existente usa all-features y un job SFTP separado. El manifiesto del core declara Criterion opcional bajo `benchmarks`; el target de benchmark no declara `required-features`. Antes de medir rendimiento, corregir la configuración de ese target y habilitar su soporte async compatible, volver a comprobar Clippy y registrar el resultado. El formato pendiente debe tratarse en un cambio explícito de código, sin confundirlo con deuda introducida por esta documentación.

Al comenzar implementación, verificar las instrucciones locales y el estado de Git. La eliminación del diseño en raíz y de tres HTML bajo `docs/` ya estaba presente antes de la refactorización documental; no se restauraron esos archivos.
