# Workflow Forge — estado y continuidad

Actualizado: 2026-09-26. Esta página registra evidencia; el producto objetivo está en [PRD](PRD.md), sus límites en [ARCHITECTURE](ARCHITECTURE.md) y mecanismos en [TDD](TDD.md).

## Punto actual

**Implementación autorizada el 2026-09-26:** avanzar F-0 a F-5 en orden y crear un commit por fase. La planificación quedó conservada en `66d39a5`; F-0 en `d5901a1` y F-1 en `00bcad9`. F-2 completa los controles, efectos y C-02 en memoria: fmt, ambas variantes de Clippy, 282 pruebas del workspace y mediciones verificadas. Su cierre se conserva en el commit de esta fase. La API nueva se encuentra en `workflow_forge::v2`; la CLI y los módulos spec 1.0 permanecen temporalmente durante su migración.

F-1 incorpora protocolo público, engine, módulos oficiales, fachada, kit de conformidad y extensión externa. El recorrido C-01A recibe JSON, normaliza un identificador, consulta un cliente inyectado y devuelve un resultado validado. `build` permanece inactivo, `boot` reclama el store y supervisa ejecución, los handles comparten la instancia y `shutdown` cierra admisión y drena.

Los patrones se concretan en código: Builder en la composición, Adapter en la extensión, Factory Function en las contribuciones, Facade en el handle, Command en la invocación y Decorator en la prueba de observación. F-2 añade State para control/efectos, Composite para cuerpos/subworkflows y Strategy de backoff después de clasificar la seguridad de repetición. No se requiere importar internos del engine para extender operaciones o sustituir proveedores. El compilador fija revisiones y rechaza capacidades fuera del perfil implementado.

El alcance completo sigue en ROADMAP. La siguiente fase es F-3, durabilidad/esperas; F-4 entrega servicio y módulos de integración; F-5 adopción. P-01 aún requiere contrastar las referencias con dos sistemas reales del usuario y P-07 cerrar sus metas de despliegue. El éxito de los fixtures no acredita esas integraciones.

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

## Evidencia F-2

La fase basada en `00bcad9` incorpora instrucciones tipadas, retry/backoff y contratos de inspección/resolución. La compilación recursiva resuelve cuerpos y subworkflows bajo revisiones exactas, rechaza dependencias ausentes/cíclicas y reúne los recursos necesarios antes de autorizar preparación o ejecución. El schema público contempla todas las instrucciones F-2 y rechaza campos ajenos a cada variante.

El runtime ejecuta decisiones, paralelo/join, foreach, loop y subworkflows en memoria. `planner` conserva decisiones puras; `control`/`groups` coordinan ámbitos y estado; `steps` se ocupa de intentos y efectos. Los checkpoints guardan selección, cursor, estado de loop y causa de detención. Los hijos no consumen otro permiso de run. Las claves de invocación usan rutas etiquetadas y escapadas, documentadas en CONTRACTS §9.2, para evitar colisiones entre ramas/índices.

Las escrituras conservan InvocationId/effect key entre intentos, con AttemptId distinto. La Strategy de backoff es sustituible desde el builder; se consulta después de clasificar la repetición y su vencimiento se guarda antes del siguiente despacho. `inspect_effect` usa un proveedor separado, registrado atómicamente con su módulo. `reconcile` aplica CAS, revisión/intento esperado, evidencia, auditoría y acuse; repetir un comando idéntico del mismo actor devuelve el acuse previo. `StopTracking` requiere permiso adicional y conserva `unresolved_effects`. Una respuesta tardía solo agrega evidencia acotada y no modifica un resultado confirmado.

[Pruebas de efectos](../crates/forge/tests/v2_effects.rs): 15 casos pasan. Además de intentos, resolución concurrente, auditoría, permisos y respuestas tardías, cubren C-01B activo/inactivo, resolución de varios efectos inciertos dentro de foreach/subworkflows, cancelación de una escritura en paralelo con `fail_fast` y continuación de un loop sin repetir la vuelta confirmada. Una detención por fallo conocido sigue vigente después de resolver otro efecto del grupo.

[Pruebas de control](../crates/forge/tests/v2_control.rs): 12 casos pasan. Cubren prioridad/fallback/condición booleana, correlación ante finalización fuera de orden, aislamiento de rutas, duplicados por índice, límites de concurrencia, `collect`/`fail_fast`, políticas de loop, catálogo/ciclos, permisos transitivos, schemas del hijo y cuotas de datos/plan/anidamiento. Una cuota de cuerpos activos se rechaza sin interbloqueo. El fallo al envolver un resultado de control queda registrado sin alterar sus hijos confirmados.

[Pruebas de inventario](../crates/forge/tests/v2_inventory.rs): cinco casos cubren C-02. El fixture de tres filas devuelve 3/2/1, sin llamar al destino por la fila inválida; cabecera, UTF-8 y excesos se rechazan antes de los efectos. Se comprueban CSV con comillas/saltos de línea, SKU repetido, archivo con solo cabecera y columnas inválidas. Un lote de 201 filas conserva orden con hasta cuatro llamadas activas y deduplica una respuesta perdida. Con 101 filas y el último efecto incierto, quedan 100 filas en reporte parcial y la fila pendiente como `Unknown` en el snapshot; inspeccionar/resolver continúa sin repetir las primeras 100 ni publicar éxito prematuro. El [módulo externo](../examples/reference-module/src/inventory/mod.rs) usa exclusivamente el protocolo, CSV y un cliente inyectado.

[Fronteras de fallo](../crates/forge/tests/v2_failure_boundaries.rs): ocho casos. Se añaden carreras entre commits completos y de invocación, y cancelación confirmada mientras se intenta finalizar un run. Una confirmación obsoleta no sobreescribe la cancelación; los outputs de nodos confirmados permanecen. El [kit de conformidad](../crates/conformance/src/lib.rs) compara snapshots y vistas, contadores, aislamiento de prefijos, revisión/propietario y terminales inmutables, tanto con proyecciones nativas como con el fallback público.

Checks de cierre: `cargo test --workspace --all-features` termina con **282 pruebas exitosas, 0 fallidas y 1 SFTP ignorada**. Incluye 62 pruebas del protocolo y motor nuevos, módulos, aceptación, efectos, control, inventario y fronteras de fallo. `cargo fmt --all --check`, `git diff --check` y Clippy workspace/all-targets con features predeterminadas y all-features, usando `-D warnings`, pasan. Logs locales: `/tmp/workflow-forge-f2-close-workspace.log`, `/tmp/workflow-forge-f2-close-clippy-default.log` y `/tmp/workflow-forge-f2-close-clippy-all.log`. La suite completa se ejecutó con permisos para sockets HTTP locales, necesarios para tests del prototipo. No se ejecutó el servidor SFTP externo.

La revisión documental comprueba 127 enlaces locales/anchors en diez documentos, fences balanceados, tres bloques JSON y los fixtures/schema formato 2. Sin errores; no se volvió a renderizar Mermaid.

### Mediciones F-2 y comparación

Mismo entorno de F-1: Apple M1 Max, 10 CPU, 64 GiB, macOS 27.0, Rust 1.98.1, release. Build: `cargo build --release -p workflow-forge --example v2_measure --example v2_inventory --example v2_customer`. Se mide el ejecutable con `/usr/bin/time -l`, sin compilación ni red. Ejecución conserva validación de resultados y cuotas; las cifras no acreditan SQLite, HTTP ni un destino real.

Comando secuencial: `/usr/bin/time -l target/release/examples/v2_measure N BYTES 1000 CONCURRENCY`. Mantiene el protocolo F-1: 100 calentamientos, 1 000 muestras, plan reutilizado en ejecución, defaults y retención de hasta 1 000 resultados. Cero runs fallidos o resultados incorrectos.

| Nodos / JSON / concurrencia | Preparación p50 / p95 / p99 (µs) | Ejecución p50 / p95 / p99 (µs) | Runs/s | RSS máximo (bytes) |
|---|---|---|---|---|
| 1 / 1 KiB / 1 | 19.042 / 34.500 / 50.709 | 88.125 / 116.875 / 144.916 | 10 424.62 | 23 838 720 |
| 10 / 1 KiB / 8 | 52.541 / 78.875 / 102.625 | 3 110.084 / 3 727.125 / 3 929.250 | 2 249.59 | 40 632 320 |
| 100 / 1 KiB / 32 | 424.917 / 515.167 / 592.292 | 71 821.792 / 94 318.375 / 96 130.417 | 339.47 | 284 426 240 |
| 1 / 64 KiB / 1 | 16.458 / 24.708 / 37.083 | 648.791 / 740.375 / 800.708 | 1 496.92 | 219 856 896 |

El umbral de investigación del 15% detectó diferencias en preparación/memoria y una regresión inicial del caso 64 KiB. Se repitieron las cuatro cargas F-2 y se recompiló F-1 desde `git archive 00bcad9` en un directorio temporal, con el mismo toolchain y sin alterar el código del worktree. Se reconstruyeron los ejecutables F-2 al terminar. La repetición F-1 obtuvo preparación p95 de 34.833/65.542/469.500/35.833 µs, ejecución p95 de 159.833/11 831.750/622 166.916/825.000 µs y RSS de 19 742 720/35 946 496/253 362 176/219 414 528 bytes, en el orden de la tabla.

Contra esa repetición, ejecución p95 baja 26.9%/68.5%/84.8%/10.3%. Preparación de diez nodos sube 20.3%, y el RSS de un nodo de 1 KiB sube 20.7% (4.1 MB). La revisión de código identifica trabajo añadido de contabilidad del plan expandido, definiciones fijadas y metadatos de invocaciones/índices. Esa es una explicación por inspección, no una atribución medida por allocator o profiler. Se conserva el coste explícito para vigilarlo en F-3/F-5; no se eliminan cuotas ni garantías para reducir la cifra. El caso 64 KiB dejó de mostrar una regresión mayor al umbral después de evitar recálculos de datos inmutables y consultar contadores de la misma revisión CAS al finalizar.

Para C-02: `/usr/bin/time -l target/release/examples/v2_inventory ROWS 3`. Lotes de 100, concurrencia de filas 4, memoria para estado/artefactos/destino, límite de 12 000 activaciones y 900 000 ms por run; demás defaults. Cada muestra construye una composición independiente. Son **tres observaciones sin calentamiento**, no percentiles ni el protocolo estadístico completo de V-14. La ejecución medida incluye `start` hasta `wait`; luego se comprueba cada fila del reporte y exactamente un efecto/intento por fila.

| Filas | Preparación de cada muestra (µs) | Ejecución de cada muestra (s) | Activaciones / transiciones por run | RSS máximo del proceso (bytes) |
|---|---|---|---|---|
| 100 | 710.125 / 294.458 / 274.250 | 0.017623 / 0.016481 / 0.013246 | 105 / 313 | 16 302 080 |
| 10 000 | 289.500 / 254.917 / 306.041 | 1.319349 / 1.302917 / 1.305651 | 10 302 / 30 706 | 148 881 408 |

El input/reporte final mide 995/13 332 bytes para 100 filas y 137 797/1 491 144 para 10 000. El RSS incluye estado retenido, reportes por lote, artefacto final, destino simulado y verificación. Las cotas de memoria proceden de cuotas, no de prometer consumo constante al aumentar filas. El cursor guarda una referencia de reporte, mientras los registros de invocaciones se conservan bajo el presupuesto del run.

La primera medición C-02 de 10 000 filas, anterior a optimizar acceso al estado, terminó correctamente en 618.774 s y 1 068 892 160 bytes de RSS. Las transiciones copiaban y serializaban el historial completo. `ExecutionStore::view`, `unfinished_heads` y `commit_invocation` permiten confirmar un nodo con contadores atómicos; los snapshots completos quedan para consultas/recuperación y transiciones de cabecera. Las tres muestras posteriores conservan las mismas 10 302 activaciones, 30 706 transiciones y 10 000 efectos correctos. No se omiten commits para conseguir la mejora.

Logs locales de medición: `/tmp/workflow-forge-f2-final-sequence-*.log`, `/tmp/workflow-forge-f1-recheck-*.log`, `/tmp/workflow-forge-f2-final-inventory-*.log` y `/tmp/workflow-forge-f2-inventory-10000-before-views.log`. Los comandos y tablas permiten repetir la comparación aunque esos logs temporales caduquen. La matriz completa de cargas, consumo tras liberar resultados, latencia externa y metas de producción siguen en V-14/P-07; estas muestras no cierran F-5.

La recuperación de una intención tras fallo del supervisor usa un proveedor en memoria conservado por el host. Las pruebas de reanudación de controles conservan la misma composición. No acreditan caída de proceso, SQLite ni recuperación de dependencias entre composiciones: F-3 debe persistir el paquete de revisiones resueltas y probar cambios/ausencias de dependencias antes de prometer esa garantía.

Siguiente implementación: diseño durable F-3 según ROADMAP. Los dos casos reales P-01 y objetivos del despliegue P-07 siguen pendientes de datos del implementador, sin bloquear este trabajo del motor.

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
| F-2 | Completada en memoria: C-01B/C-02, V-06/V-07/V-18, controles/efectos y comparación de rendimiento. 282 pruebas pasan; fmt y ambas variantes de Clippy limpios. |
| F-3 a F-5 | Pendientes según dependencias y criterios de ROADMAP. |

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

Comenzar F-3 concretando codec/DDL/migraciones SQLite, propiedad del store, paquete de revisiones fijadas y retención de artefactos antes de implementar el backend. Después, probar caídas en C-01B/C-02 y conectar reserva de callback, señales y timers según TDD-08. Los subworkflows recuperados deben usar las dependencias aceptadas, sin resolver silenciosamente versiones nuevas. Contrastar referencias con integraciones reales cuando haya datos. La implementación y los checkpoints por fase ya están autorizados.

Baseline reproducido sobre HEAD `392303b`, con los cambios documentales del worktree, macOS y `rustc 1.98.1` / `cargo 1.98.1`. La ejecución inicial aislada no podía preparar dependencias; las pruebas se completaron después con acceso autorizado. La CI existente usa all-features y un job SFTP separado. F-0 corrigió las features del target de Criterion, su soporte async y el formato preexistente, antes de medir el motor nuevo.

Al comenzar implementación, verificar las instrucciones locales y el estado de Git. La eliminación del diseño en raíz y de tres HTML bajo `docs/` ya estaba presente antes de la refactorización documental; no se restauraron esos archivos.
