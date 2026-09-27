# Workflow Forge — estado y continuidad

Actualizado: 2026-09-27. Esta página registra evidencia; el producto objetivo está en [PRD](PRD.md), sus límites en [ARCHITECTURE](ARCHITECTURE.md) y mecanismos en [TDD](TDD.md).

## Punto actual

**Implementación autorizada el 2026-09-26:** avanzar F-0 a F-5 en orden y crear un commit por fase. La planificación quedó conservada en `66d39a5`; F-0 en `d5901a1`, F-1 en `00bcad9`, F-2 en `dcb1d34`, F-3 en `2ed3970` y F-4 en `6806fa0`. La fachada raíz, `prelude` y `workflow_forge::v2` exponen el mismo motor. F-5 completa autoría, extensión y adopción, migra la CLI a HTTP y retira los consumidores/implementación spec 1.0. Su cierre se conserva en el commit de esta fase.

**Alcance vigente, ajustado por el usuario el 2026-09-27:** terminar con pruebas esenciales y casos borde, conservando contratos/errores, recuperación/efectos, cuotas/concurrencia, paridad y checks apropiados. Las nueve combinaciones de secuencias/lecturas SQLite y tres de capacidad SQLite sin completar quedan diferidas, igual que nuevas repeticiones y comparaciones históricas de rendimiento. Solo se retomarían ante una necesidad de capacidad real; no son criterios de cierre técnico de F-5. La evidencia ya obtenida permanece abajo y las combinaciones omitidas no se acreditan como exitosas. La verificación funcional final pasó: 151 pruebas del workspace y una regresión focalizada adicional, con Clippy y formato limpios. La aclaración posterior del usuario en esta misma fecha separa las integraciones reales del cierre del engine: P-01/P-07 quedan para una etapa posterior, cuando el usuario decida integrar sus sistemas. F-5 queda completada con los casos de referencia y la evidencia técnica registrada; no se acredita aceptación ni capacidad de producción.

F-1 incorpora protocolo público, engine, módulos oficiales, fachada, kit de conformidad y extensión externa. El recorrido C-01A recibe JSON, normaliza un identificador, consulta un cliente inyectado y devuelve un resultado validado. `build` permanece inactivo, `boot` reclama el store y supervisa ejecución, los handles comparten la instancia y `shutdown` cierra admisión y drena.

Los patrones se concretan en código: Builder en la composición, Adapter en la extensión, Factory Function en las contribuciones, Facade en el handle, Command en la invocación y Decorator en la prueba de observación. F-2 añade State para control/efectos, Composite para cuerpos/subworkflows y Strategy de backoff después de clasificar la seguridad de repetición. No se requiere importar internos del engine para extender operaciones o sustituir proveedores. El compilador fija revisiones y rechaza capacidades fuera del perfil implementado.

F-3 está completa: paquete de recuperación fijado, estado/artefactos SQLite, migraciones, señales/timers y reconciliación durable, con 331 pruebas del workspace y mediciones comparables registradas abajo. F-4 está completa: servicio HTTP, módulos de integración y ejecutable, con 357 pruebas del workspace y los checks de cierre registrados abajo. F-5 completa adopción, autoría y validación esencial del engine. Después de este cierre, P-01 permitirá contrastar las referencias con sistemas reales del usuario y P-07 fijar sus metas de despliegue. El éxito de los fixtures no acredita esas integraciones ni obliga a iniciarlas ahora.

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

Logs locales de medición: `/tmp/workflow-forge-f2-final-sequence-*.log`, `/tmp/workflow-forge-f1-recheck-*.log`, `/tmp/workflow-forge-f2-final-inventory-*.log` y `/tmp/workflow-forge-f2-inventory-10000-before-views.log`. Los comandos y tablas permiten reproducir la comparación aunque esos logs temporales caduquen. Al cerrar F-2 se previó ampliar V-14 en F-5; el ajuste del 2026-09-27 deja las campañas de rendimiento sin completar como opcionales. Estas muestras históricas no acreditan las metas reales P-07.

La recuperación de una intención tras fallo del supervisor usa un proveedor en memoria conservado por el host. Las pruebas de reanudación de controles conservan la misma composición. No acreditan caída de proceso, SQLite ni recuperación de dependencias entre composiciones: F-3 debe persistir el paquete de revisiones resueltas y probar cambios/ausencias de dependencias antes de prometer esa garantía.

Al cerrar F-2, el siguiente trabajo fue durabilidad F-3 según ROADMAP. Los dos casos reales P-01 y objetivos del despliegue P-07 siguen pendientes de datos del implementador.

## Evidencia F-3

Después de `dcb1d34`, el checkpoint formato 2 incorpora el paquete inmutable de definiciones, descriptores y schemas aceptados. La aceptación y la identidad de recepción lo incluyen. Boot y recuperación de una aceptación sin acuse compilan desde ese paquete, sin sustituir subworkflows desde el catálogo del nuevo host. Reconciliación valida resultados con los schemas originales. Una implementación ausente/incompatible deja `recovery.unavailable`; reinstalarla permite recuperar ese bloqueo sin borrar un efecto incierto.

[Pruebas del paquete](../crates/forge/tests/v2_recovery_package.rs): cuatro casos pasan, usando un store en memoria conservado entre composiciones. Cubren round-trip JSON, catálogo cambiado/schema de autoría ausente, preservación de un paso confirmado, revisión de operación ausente y reinstalada, descriptor incompatible y confirmación de efecto bajo el schema externo original. El kit público también rechaza mutar el paquete bajo CAS. Son pruebas de reconstrucción de dependencias, **no caída de proceso ni durabilidad**.

Verificación inicial del paquete: 59 casos únicos de aceptación, control, efectos, inventario, fronteras de fallo y paquete pasan. Logs `/tmp/workflow-forge-f3-package-regression.log` y `/tmp/workflow-forge-f3-package-tests.log`; el segundo vuelve a ejecutar las ocho fronteras de fallo al ampliar conformidad. La suite completa de 282 casos y las mediciones anteriores corresponden al commit F-2; no se atribuyen al nuevo formato de checkpoint.

El [adaptador SQLite](../crates/modules/src/sqlite/mod.rs) ya implementa estado/aceptación durable, detrás de la feature `sqlite`. La configuración permanece inactiva; el primer reclamo inicia su actor y abre/migra el archivo bajo lock exclusivo del SO. Cola acotada, conexión en hilo propio, propietario contrastado dentro de transacciones, WAL/FULL y límites de páginas/registro. El codec valida versión antes de interpretar payload; los paquetes se conservan por hash, los nodos por fila y las proyecciones mantienen contadores en el mismo commit. Memoria y SQLite comparten validaciones puras del protocolo para creación/sucesión; el backend conserva la responsabilidad de su atomicidad.

Las [pruebas del store](../crates/modules/tests/sqlite_store.rs) pasan seis casos: kit público y reapertura, propietario/revisión concurrentes, rollback de recibo/run por cuota de disco, schema/codec desconocidos y corrupción, base ajena sin sobrescritura y reclamo cancelado sin lock retenido. La cancelación observa que el reclamo está activo mediante el lock del SO antes de abandonar su future; no depende de suponer progreso por un sleep.

Las [pruebas del engine SQLite](../crates/forge/tests/v2_sqlite.rs) pasan seis casos, uno de ellos el punto de entrada de los procesos hijos. La matriz termina cuatro procesos reales mediante `exit(73)`, sin destructores Rust, después de aceptación sin acuse, intención, efecto en un ledger externo y resultado confirmado. La aceptación/clave persiste; una intención o escritura sin resultado bloquea; inspección y resolución permiten continuar con exactamente un efecto final. Resultado confirmado no se repite. Un segundo arranque conserva snapshot, auditoría y recibo de resolución. También se prueban rechazo de artefactos efímeros y cancelación de boot después de reclamar el store, manteniendo vivo el proveedor para demostrar liberación de propiedad. También se verifican descarte del runtime con handles vivos y abandono de `shutdown` después de tomar su supervisor: se libera el store y el trabajo no confirmado permanece recuperable. Estas pruebas no simulan pérdida física de energía o fallos del hardware.

Comandos: `cargo test -p workflow-forge-modules --features sqlite --test sqlite_store` y `cargo test -p workflow-forge --features sqlite --test v2_sqlite`; logs `/tmp/workflow-forge-f3-sqlite-store-tests.log` y `/tmp/workflow-forge-f3-sqlite-process-tests.log`. La regresión dirigida de siete suites v2 pasó 62 casos antes de añadir el test de cancelación de boot; ese test y dos casos de cierre pasan en la suite SQLite posterior. Son 71 casos únicos entre esas suites y el store, no una nueva ejecución de todo el workspace. La comprobación de ciclo de vida está en `/tmp/workflow-forge-f3-sqlite-lifecycle-tests.log`: 29 casos entre aceptación, fronteras de fallo y SQLite. Log de regresión `/tmp/workflow-forge-f3-sqlite-regression.log`. Clippy workspace/all-targets/all-features con `-D warnings` pasa, incluido el ejemplo durable (`/tmp/workflow-forge-f3-sqlite-workspace-clippy.log`).

[Host ejecutable](../crates/forge/examples/v2_sqlite.rs): build con `cargo build -p workflow-forge --features sqlite --example v2_sqlite`; dos ejecuciones contra `/tmp/workflow-forge-sqlite-host-xL853dcD/state.sqlite` devolvieron `Succeeded`, `{"active":true,"customer":"C-9"}`, `durable:true` y el mismo RunId. La segunda informó `duplicate:true`. Los datos temporales no son un despliegue ni sustituyen mediciones release. Dependencias fijadas en Cargo.lock: rusqlite 0.39.0 y libsqlite3-sys 0.37.0 con SQLite incluido; sin dependencia SQL dentro del engine.

### Incremento de artefactos durables

El mismo proveedor SQLite implementa estado y artefactos mediante un actor compartido. `artifact_domain` comprueba coordinación sin imports SQL en el engine; dos instancias separadas sobre un mismo path no son intercambiables con clones de ese proveedor. `StartOptions.artifacts` declara referencias que se validan/fijan con run y recibo. La lista es inmutable y participa en cuotas e identidad de recepción. El contexto pasa propietario/RunId en cada acceso: un stream viejo no hereda la autoridad de un arranque posterior.

La migración SQL 2 agrega metadatos, bloques de hasta 64 KiB y propietarios por run. `staging` nunca devuelve una referencia; `ready` exige todos sus bytes confirmados. Hay cuotas de cantidad/bytes, limpieza de escrituras interrumpidas, expiración de cargas sin propietario y retención compartida mientras exista algún run propietario. La publicación cuyo acuse se pierde permanece vinculada hasta la retención del run. Un checkpoint JSON de schema 1 conserva contenido, contadores y recibo después de migrar.

[Pruebas del proveedor de artefactos](../crates/modules/tests/sqlite_artifacts.rs): seis casos pasan. Cubren streaming de 200 003 bytes, reapertura y propietarios compartidos, rollback de aceptación/vínculos/recibo, metadatos/scope, acceso no declarado, creación desde un run, propietario obsoleto, cancelación con evidencia de bloque persistido, cuotas, migración y referencia vencida antes del GC. El kit de ejecución también rechaza alterar las referencias aceptadas. Se ejecutan junto con los seis casos del store: `/tmp/workflow-forge-f3-artifact-provider-tests.log`.

[Pruebas C-02 durables](../crates/forge/tests/v2_sqlite_inventory.rs): tres casos pasan, incluido el punto de entrada hijo. Tres procesos terminan mediante `exit(73)` después del primer lote, durante streaming del reporte final y después de publicarlo sin devolver el acuse. Al reiniciar se conserva la recepción, se recuperan las 201 filas y el reporte ordenado, con 201 llamadas/efectos en el ledger del destino. El caso `staging` recupera con cuota exacta de cinco artefactos; el publicado sin acuse ocupa su sexta plaza hasta la retención. Un segundo arranque conserva snapshot y contenido, y el GC posterior invalida el reporte. También se rechaza combinar dos coordinadores aunque ambos anuncien durabilidad. Log final `/tmp/workflow-forge-f3-artifact-process-tests.log`; el fixture cuenta los intentos interrumpidos y fija tres intentos para operaciones repetibles.

Regresión dirigida del incremento: 26 casos de aceptación/fronteras/C-02 durable y 15 de inventario/paquete/SQLite pasan; logs `/tmp/workflow-forge-f3-artifact-integration-tests.log` y `/tmp/workflow-forge-f3-artifact-engine-regression.log`. Clippy workspace/all-targets con defaults y all-features, `-D warnings`, pasa: `/tmp/workflow-forge-f3-artifact-clippy-default.log` y `/tmp/workflow-forge-f3-artifact-clippy.log`. No se presenta como nueva suite completa del workspace ni como medición de rendimiento. El ejemplo JSON del host sigue publicado; CONTRACTS §10.3 añade el fragmento para componer ambos puertos y declarar la entrada CSV.

### Incremento de esperas, señales y timers

CONTRACTS §11 ya se conecta al protocolo, compilador, schema y runtime: `await_signal` reserva antes de su operación de inicio opcional; `timer` conserva el input y un vencimiento absoluto. Se mantienen las rutas de ámbito y la política existente de efectos. El Command de señal valida permiso/scope, correlación, schema offline, cuotas y adjuntos; conserva payload/acuse antes de responder. Un inicio incierto permanece bloqueado aunque reciba el callback. El consumo y resultado del control comparten un commit completo; los proveedores rechazan modificar el nodo de espera mediante el puerto de commits individuales.

La suspensión es distinta de error y libera capacidad de run/ámbito; se propaga por controles anidados y deja asentarse a las ramas activas. `RunHead.next_wakeup_at_ms` permite reanudar por señal, timer o deadline incluso si se pierde una notificación en memoria. Un grupo detenido por fallo conocido cierra atómicamente sus reservas abiertas, incluidos hijos ya suspendidos; no afecta otras ramas. La cancelación del run cierra reservas junto con su marca de cancelación y conserva acuses/incertidumbre.

[Pruebas de esperas](../crates/forge/tests/v2_waits.rs): diez casos pasan. Cubren liberación con `active_runs = 1`, validación/deduplicación/permisos, callback anterior a confirmar el inicio, incertidumbre y resolución, expiración, paralelo/foreach/loop/subworkflow, ámbitos detenidos y cuotas de reservas/activaciones/deadline/payload. Una entrega en plazo sobrevive al vencimiento de la espera mientras se confirma el inicio; vencer sin entrega durante una escritura incierta bloquea hasta resolverla. El caso de grupo detenido reprodujo una reserva indebidamente abierta y pasa después de cerrar reservas junto con la marca de detención. Logs `/tmp/workflow-forge-f3-wait-boundary-tests.log` y `/tmp/workflow-forge-f3-wait-budget-tests.log`; las 15 pruebas de efectos también pasan con ese cambio.

[Pruebas de carreras](../crates/forge/tests/v2_wait_races.rs): cuatro casos pasan. Un Decorator retiene un candidato antes del CAS y deja confirmar al competidor para comprobar señal durante la suspensión, dos entregas idénticas/conflictivas y señal frente a cancelación/expiración, tanto en memoria como en SQLite. El caso de adjuntos SQLite verifica rollback de todos los vínculos/acuse tras una referencia inválida y una entrega válida posterior. La retención elimina run, acuse y artefacto cuando ya no tienen propietario. Logs `/tmp/workflow-forge-f3-wait-race-tests.log` y `/tmp/workflow-forge-f3-wait-retention-tests.log`.

[Pruebas de reinicio de esperas](../crates/forge/tests/v2_sqlite_waits.rs): tres casos, incluido el punto de entrada hijo. La matriz termina tres procesos con `exit(73)` después de reservar, aceptar señal y consumirla antes de su sucesor. Al arrancar conserva identidad/deadline, acuse durable, adjuntos y un solo efecto de inicio/sucesor. Funciona sin el schema del catálogo de autoría original, usando el paquete aceptado. Un segundo arranque conserva resultado y acuse; un timer conserva su fecha original después de shutdown/boot. Log `/tmp/workflow-forge-f3-wait-process-tests.log`.

El checkpoint y esquema SQL avanzan a 3. La migración transaccional del formato 2 actualiza sobres y hashes/referencias de paquetes, preservando los datos anteriores. Las [ocho pruebas del store](../crates/modules/tests/sqlite_store.rs) incluyen migrar intención/salida/artefacto/recibo y forzar un fallo posterior para comprobar rollback de contenido, DDL y versión; reparar el registro permite reclamar después. Las seis pruebas de artefactos siguen pasando, incluida migración desde schema SQL 1. El kit de conformidad comprueba proyección coherente y consumo/resultado inseparables sobre memoria, fallback y SQLite. Logs `/tmp/workflow-forge-f3-wait-migration-tests.log`, `/tmp/workflow-forge-f3-wait-conformance-tests.log` y `/tmp/workflow-forge-f3-wait-store-conformance.log`.

El [ejemplo `v2_signal`](../crates/forge/examples/v2_signal.rs) se construyó con `cargo build -p workflow-forge --features sqlite --example v2_signal`. Tres procesos contra `/tmp/workflow-forge-signal-example-8a7aY7Kp/state.sqlite` ejecutaron `start`, `signal` y `signal`: primero `Waiting`, después `Succeeded` con `{"start":null,"signal":{"approved":true}}` y finalmente el mismo acuse con `duplicate:true`. Conservan RunId/WaitId. Es un host local con acceso confiable, sin servicio HTTP ni medición release.

Verificación del incremento: `cargo test --workspace --all-features` pasa **326 pruebas, 0 fallos y 1 SFTP ignorada**; log `/tmp/workflow-forge-f3-waits-workspace.log`. Incluye los cambios de protocolo/codec, controles y cancelación. La posterior ampliación del caso de GC de señales pasa en su suite dirigida de cuatro casos; no cambia código de producción ni el conteo. La suite completa usa permisos para sockets HTTP locales del prototipo. No se ejecutó el servidor SFTP externo.

`cargo fmt --all --check`, `git diff --check` y Clippy workspace/all-targets con defaults y all-features, `-D warnings`, pasan. Logs `/tmp/workflow-forge-f3-waits-clippy-default.log` y `/tmp/workflow-forge-f3-waits-clippy-all.log`. Revisión documental: diez documentos, 158 enlaces locales/anchors, cuatro bloques JSON y fixtures/schema formato 2 parseables; fences balanceados, sin errores. No se renderizó Mermaid. Estas comprobaciones no sustituyen las mediciones release de F-3 ni prueban otro toolchain.

### Reconciliación durable V-18

La suite SQLite amplía su matriz a nueve casos; los tres nuevos están en [sqlite_resolution](../crates/forge/tests/support/sqlite_resolution.rs), usando exclusivamente la fachada y puertos públicos. Pasan con `cargo test -p workflow-forge --features sqlite --test v2_sqlite`; log `/tmp/workflow-forge-f3-resolution-tests.log`. El fixture declara una salida entera para comprobar la resolución de efectos con outputs incompatibles.

Una inspección de ausencia sin quiescencia conserva bloqueo; evidencia vacía, ámbito ajeno y permisos ausentes no modifican el snapshot. Dos investigaciones sobre la misma revisión aceptan una sola decisión; después de reiniciar se conservan ambas entradas previas de auditoría, el acuse del ganador y el rechazo de cambios de contenido/actor. La no aplicación definitiva sin retry termina el run sin nuevos intentos ni efectos. La prueba de output inválido conserva `Applied` y `Unknown` después de reiniciar, rechaza una posterior negación del efecto y solo habilita el sucesor al confirmar un output válido.

La matriz de pérdida de acuse ejecuta seis pares de procesos: el primero cae en intención/efecto y el segundo antes o después del commit de resolución. Antes del commit no aparecen decisión, auditoría ni acuse; después aparecen juntos y repetir el Command devuelve el recibo persistido. Cubre aplicación, retry seguro, investigación inconclusa y `StopTracking` tanto fallido como cancelado. El cierre mantiene identidad/intento/clave en `unresolved_effects`, no despacha el sucesor ni permite reabrir el run. Un segundo reinicio mantiene cada snapshot/acuse y exactamente un efecto en el ledger. Se conservan las cuatro fronteras de caída de la matriz original.

### Memoria compartida y comprobaciones de cierre

La comparación de RSS detectó duplicación del paquete de recuperación en cada run retenido. El proveedor en memoria ahora comparte contenido idéntico mediante `Arc<ResolvedPackage>`; mantiene progreso por run y materializa copias completas al cruzar el puerto público. La igualdad del paquete y la transición siguen comprobándose bajo el lock/CAS. Un índice débil se limpia con la retención. [PAT-11](PATTERNS.md#pat-11--flyweight-compartir-datos-inmutables) explica la aplicación de Flyweight y sus límites.

La prueba añadida de retención usa memoria y SQLite: completar/eliminar un run no pierde el paquete de otro, modificar una copia pública no permite reemplazarlo y una aceptación posterior funciona después de liberar todos los runs anteriores. La suite de carreras pasa cinco casos. Logs `/tmp/workflow-forge-f3-shared-package-tests.log` y `/tmp/workflow-forge-f3-shared-package-retention-tests.log`.

La ampliación final V-15 en [v2_sqlite](../crates/forge/tests/v2_sqlite.rs) lleva esa suite a diez casos. Un proceso cae después de confirmar el resultado de una escritura; arrancar sin su plugin bloquea con `recovery.unavailable` y conserva el registro confirmado. Reutilizar su revisión con otro schema también bloquea, sin avanzar al sucesor. Reinstalar el contrato original permite terminar con el mismo recibo/run y un solo efecto externo. Otra reapertura conserva exactamente el snapshot final.

Checks de cierre, posteriores a esa ampliación: `cargo test --workspace --all-features` pasa **331 pruebas, 0 fallos y 1 SFTP ignorada**. Formato, `git diff --check` y Clippy workspace/all-targets con defaults y all-features, `-D warnings`, pasan. Logs `/tmp/workflow-forge-f3-close-{workspace,fmt,diff,clippy-default,clippy-all}.log`. La suite requiere sockets HTTP locales del prototipo; no acredita el servidor SFTP externo ni el MSRV declarado, porque se ejecutó con Rust 1.98.1.

La revisión documental final verifica diez documentos, 163 enlaces locales/anchors, cuatro bloques JSON y cuatro archivos de fixtures/schema; sin errores y con fences balanceados. `cargo fmt --all --check` también pasa después del último ajuste del test. No se renderizó Mermaid ni se presentan los fragmentos Rust de diseño como programas compilados.

### Mediciones F-3

Entorno local del 2026-09-26: Apple M1 Max, 10 CPU, 64 GiB, macOS 27.0 (26A428), Rust 1.98.1, release. Sin red ni latencia externa simulada. Las mediciones se ejecutan en serie, sin builds/tests simultáneos. `/usr/bin/time -l` mide RSS del ejecutable, excluyendo Cargo. Las secuencias conservan los presupuestos predeterminados, incluidos 1 000 resultados/1 h, 16 MiB por run y 5 minutos activos; preparación y ejecución se miden separadas, con 100 calentamientos y 1 000 observaciones cada una.

Árbol de fuentes usado para medir F-3 después de compartir paquetes: SHA-256 `be7e7f6f5f461e89ec8d99f9cc7a6b03c0d157e88a6d7342dd24e6c9c6b64135`. Se calculó sobre 193 rutas únicas ordenadas de `git ls-files -co --exclude-standard`, seleccionando `.rs`, `.sql`, `.json`, `Cargo.toml` y `Cargo.lock`; cada entrada aporta ruta UTF-8, NUL, contenido y NUL. Las mediciones SQLite pequeñas preceden al cambio exclusivo de memoria y corresponden a `904f70f1e0ee4fa95831c1064be254bcacc28c7fe18024548c1ed6d9288ffbb4`. La posterior ampliación de la prueba de reinstalación de plugins no cambia los binarios medidos.

Comandos para secuencias: `cargo build --release -p workflow-forge --example v2_measure --example v2_inventory` y `/usr/bin/time -l target/release/examples/v2_measure N BYTES 1000 CONCURRENCY`. Para SQLite se agrega `--features sqlite` al build y `--sqlite NUEVA_RUTA.sqlite` al ejecutable; se instala el mismo proveedor en estado/artefactos y se exige un acuse durable. Las rutas de bases son nuevas, dentro de un directorio existente. WAL/FULL y fullfsync permanecen activos; no se comparan sus garantías con memoria como si fueran equivalentes.

Primera pasada F-3 con paquetes compartidos, **cero runs fallidos o incorrectos**:

| Nodos / JSON / concurrencia | Preparación p50 / p95 / p99 (µs) | Ejecución p50 / p95 / p99 (µs) | Runs/s | RSS máximo (bytes) |
|---|---|---|---|---|
| 1 / 1 KiB / 1 | 20.958 / 28.000 / 39.375 | 91.917 / 130.500 / 158.917 | 9 576.87 | 24 592 384 |
| 10 / 1 KiB / 8 | 61.500 / 73.708 / 94.000 | 3 402.916 / 4 157.125 / 4 416.875 | 2 054.46 | 42 270 720 |
| 100 / 1 KiB / 32 | 518.917 / 572.458 / 645.083 | 85 028.708 / 110 336.917 / 112 791.334 | 292.78 | 312 459 264 |
| 1 / 64 KiB / 1 | 21.292 / 38.375 / 53.958 | 690.375 / 789.041 / 885.625 | 1 396.07 | 223 461 376 |

La investigación del umbral de 15% repitió F-2 (`dcb1d34`, exportado a otro directorio) en esta máquina y F-3 dos veces más. No se elige solo la mejor pasada:

| Carga | F-2 repetida: ejecución p95 (µs) / RSS (bytes) | F-3: rango p95 de tres pasadas (µs) | F-3: rango RSS (bytes) |
|---|---|---|---|
| 1 / 1 KiB / 1 | 110.500 / 23 822 336 | 128.250–134.000 | 24 576 000–24 592 384 |
| 10 / 1 KiB / 8 | 3 795.042 / 40 747 008 | 3 852.000–4 157.125 | 42 237 952–42 287 104 |
| 100 / 1 KiB / 32 | 94 169.292 / 287 342 592 | 98 533.583–110 336.917 | 308 592 640–312 459 264 |
| 1 / 64 KiB / 1 | 720.709 / 220 168 192 | 713.959–789.041 | 219 873 280–223 461 376 |

Antes de compartir el paquete, la carga de 100 nodos usó 380 944 384 bytes; después, 308 592 640–312 459 264: reducción aproximada del 18–19%, todavía 7.4–8.7% sobre F-2 repetida. La latencia p95 de esa carga varía entre +4.6% y +17.2%. Un nodo de 1 KiB conserva una regresión de +16.1–21.3%, unos 18–24 µs. Preparación de 100 nodos pasa de 443.416 µs p95 en F-2 a 572.458–604.625 µs; 64 KiB, de 23.459 a 27.750–38.375 µs. La revisión local identifica trabajo nuevo de copia/serialización del paquete durante preparación, validación de su inmutabilidad y materialización de snapshots completos; es una explicación por inspección, no un perfil de CPU que atribuya cada microsegundo. Se conserva ese coste para mantener recuperación y contratos públicos. No se declara paridad total ni cumplimiento de metas de producción aún pendientes P-07.

SQLite, **cero runs fallidos o incorrectos** en las cuatro cargas:

| Nodos / JSON / concurrencia | Preparación p50 / p95 / p99 (µs) | Ejecución p50 / p95 / p99 (µs) | Runs/s | RSS máximo (bytes) |
|---|---|---|---|---|
| 1 / 1 KiB / 1 | 20.375 / 24.875 / 27.708 | 38 842.500 / 52 790.500 / 63 885.125 | 25.19 | 17 809 408 |
| 10 / 1 KiB / 8 | 61.208 / 73.709 / 82.292 | 1 159 958.417 / 1 337 221.542 / 1 461 621.792 | 6.57 | 18 890 752 |
| 100 / 1 KiB / 32 | 486.208 / 505.584 / 538.667 | 41 913 124.125 / 45 882 614.542 / 46 990 404.875 | 0.76 | 33 816 576 |
| 1 / 64 KiB / 1 | 21.250 / 27.833 / 39.292 | 45 412.542 / 74 753.958 / 88 924.875 | 20.68 | 21 364 736 |

La carga durable de 100 nodos completó 100 calentamientos y 1 000 muestras en 1 456.68 s totales. Su p95 de 45.883 s corresponde al run completo con 32 ejecuciones concurrentes; no es latencia por nodo. Hubo una consulta SQLite de solo lectura de progreso durante la carga de 10 nodos y dos durante la de 100 nodos; estas son mediciones locales, no una campaña aislada de producción.

C-02 usa `/usr/bin/time -l target/release/examples/v2_inventory FILAS 3`, agregando `--sqlite DIRECTORIO` para crear `sample-0.sqlite` a `sample-2.sqlite`. Son tres composiciones independientes sin calentamiento; no se infieren percentiles. Lotes de 100, concurrencia de filas 4, 12 000 activaciones y 900 000 ms por run. El cronómetro de ejecución va de `start` a `wait`; la carga inicial del CSV y la lectura de comprobación quedan fuera, pero el RSS incluye el proceso completo. Se comprueban todas las filas del reporte y los efectos del destino de referencia en memoria.

| Perfil / filas | Ejecución de cada muestra (s) | RSS máximo (bytes) | Filas y efectos por muestra |
|---|---|---|---|
| F-2 repetida, memoria / 100 | 0.021929 / 0.016625 / 0.013883 | 16 384 000 | 100 |
| F-3, memoria / 100 | 0.016089 / 0.013845 / 0.012032 | 17 465 344 | 100 |
| F-2 repetida, memoria / 10 000 | 1.222469 / 1.281648 / 1.318629 | 150 700 032 | 10 000 |
| F-3, memoria / 10 000 | 1.333590 / 1.329217 / 1.341853 | 163 151 872 | 10 000 |
| F-3, SQLite / 100 | 2.026744 / 2.045761 / 2.039876 | 19 496 960 | 100 |
| F-3, SQLite / 10 000 | 227.738979 / 227.039823 / 225.216731 | 195 477 504 | 10 000 |

Las tres muestras durables de 10 000 filas terminaron con 10 000 filas y efectos correctos cada una; entre 43.91 y 44.40 filas/s. Los runs de 100/10 000 filas conservan 105/10 302 activaciones y 313/30 706 transiciones. CSV de 995/137 797 bytes; reporte de 13 332/1 491 144 bytes. Las mediciones en memoria de 10 000 filas suben 8.3% en RSS frente a F-2 repetida; la evidencia de recuperación C-02 sigue siendo la matriz con ledger externo de 201 filas, no el destino volátil de este benchmark.

Logs locales bajo `/tmp/workflow-forge-f3-measure-uiYwceCq`: `shared-memory-*.log`, `shared-memory-repeat{2,3}-*.log`, `f2-repeat-*.log`, `shared-inventory-memory-*.log`, `sqlite-*.log` e `inventory-sqlite-*.log`. Los comandos/tablas se conservan aunque caduquen los temporales. La ampliación prevista de 1 MiB, latencia externa, saturación sostenida y RSS tras liberación obtuvo la evidencia F-5 registrada abajo; su parte sin completar quedó opcional por el ajuste del 2026-09-27. Los casos funcionales de admisión y GC verifican comportamiento, sin acreditar capacidad de producción.

### Criterios de salida F-3

| Criterio de ROADMAP | Evidencia de cierre |
|---|---|
| C-04; V-09 | Reservas antes del inicio, señales tempranas/duplicadas, carreras, cancelación y timers; caída en reserva, recepción y consumo. |
| Fronteras de C-01B/C-02; V-08 | Procesos terminados antes/después de intención, efecto y resultado; lotes/reporte recuperados sin repetir efectos confirmados. |
| V-12 | Artefactos durables, streaming, referencias retenidas, fencing del propietario y GC; migración/rollback probados. |
| V-15 | Paquete y schemas aceptados conservados; plugin ausente o incompatible bloquea y reinstalarlo recupera sin repetir el paso confirmado. |
| V-18 durable | Decisión/auditoría/acuse atómicos, resoluciones concurrentes/duplicadas y cierre con incertidumbre persistente. |
| Proveedor conforme | Kit público, CAS, cuotas y exclusión de propietario en SQLite; sin fallback a memoria. |
| Medición P-07 aplicable | Cuatro cargas de secuencias en ambos perfiles y C-02 de 100/10 000 filas; investigación de regresiones y límites publicados. |

F-3 quedó cerrada en `2ed3970` con los checks anteriores. Los resultados F-4 se registran a continuación; el contraste con casos reales y metas de producción quedó posteriormente diferido hasta después del cierre del engine por instrucción del usuario.

## Evidencia F-4

Los anexos [HTTP](HTTP.md) e [INTEGRATIONS](INTEGRATIONS.md) concretan los contratos adoptados antes de implementar sus adaptadores. El [servicio](../crates/service/src/lib.rs) depende de la fachada pública; no accede a internals del engine ni a tablas SQL. `HostConfig` compone proveedores/módulos y definiciones; `ServiceRuntime` conserva un engine y distribuye clones de su handle.

| Verificación | Evidencia |
|---|---|
| V-10, paridad | [C-01A/C-02 y señales](../crates/service/tests/parity.rs) por sockets reales y Rust, tanto con memoria como con SQLite. Resultados/reportes equivalentes, recibos duplicados, conflicto de recepción y errores con las mismas ubicaciones/códigos. |
| V-19, acceso y cuotas | [Autenticación, grants y DTOs](../crates/service/tests/access.rs): no se toma actor/scope/permisos del body, catálogo filtrado, cursor ligado al actor/instancia/colección/revisión, límites de JSON, respuestas y planes. La autenticación comparte deadline/capacidad de las peticiones. |
| Artefactos y propiedad | [Streams](../crates/service/tests/streams.rs): carga interrumpida/excesiva/vencida limpia staging en SQLite sin reiniciar, metadatos verificados, streams y handles viejos no siguen al nuevo propietario. Descargas retienen el cupo hasta cerrar el body y un cierre forzado interrumpe I/O. La fachada usa nuevos métodos de host del puerto; proveedores durables deben declararlos expresamente. |
| V-18, operadores | [Inspección/reconciliación](../crates/service/tests/effects.rs): incertidumbre visible, resolución/deduplicación y auditoría sin repetir el efecto. Mensajes arbitrarios de operaciones y notas libres no aparecen en las vistas de metadata. |
| V-16, lifecycle | [Supervisión](../crates/service/tests/lifecycle.rs): bind/scope fallidos liberan el store; fallo esencial retira readiness y cierra transporte; Drop y shutdown invalidan handles. Perder la conexión después de aceptación conserva el run y permite recuperar su recibo sin otra invocación. |
| V-13/PAT-07 | [Observadores](../crates/service/tests/observation.rs): lento, error y panic no alteran 32 resultados independientes ni el cierre. El ejecutable publica solo `ExecutionEvent` mediante un canal acotado; no es un outbox durable. |
| V-17, HTTP/JSON | [Conectores](../crates/service/tests/integrations_http.rs): clientes compartidos y Decorator sobre el contrato público, revisiones derivadas de perfiles, secretos por puerto, redirects desactivados, bytes reales acotados y ningún retry oculto. Una escritura incierta queda con un intento y exige decisión explícita. |
| V-17, archivos/CSV | [Lectura/parsing](../crates/service/tests/integrations_files.rs): raíz con capacidad, escape/symlink externo rechazados, archivo confirmado independiente de cambios posteriores, quoting/multilinea/UTF-8, lotes y cuotas. Dieciséis runs concurrentes conservan sus datos. Las reglas de inventario siguen fuera del engine. |
| Bootstrap y defaults | [Procesos reales](../crates/service/tests/bootstrap.rs): config relativa al archivo, SQLite predeterminado, memoria expresa, arranque fatal ante store/credencial inválidos, SIGTERM con drenado, recibos/resultados conservados tras reiniciar y consulta de runs sin recargar su definición en caché. |

`cargo test --workspace --all-features`: **357 pruebas exitosas, 0 fallidas y 1 SFTP ignorada**, incluyendo doctests. Después de precisar la traducción HTTP a 404 de un artefacto/espera ausente, `cargo test -p workflow-forge-service` vuelve a pasar sus 26 casos. Las ejecuciones usan sockets locales y procesos del ejecutable; no llaman a destinos de producción. Logs locales: `/tmp/workflow-forge-f4-close-workspace-final.log` y `/tmp/workflow-forge-f4-close-service.log`.

`cargo fmt --all --check`, `git diff --check` y Clippy workspace/all-targets con defaults y all-features, `-D warnings`, pasan. Revisión documental: doce documentos, 201 enlaces locales/anchors, cinco bloques JSON y seis archivos JSON parseables; fences balanceados y cero errores. No se renderizó Mermaid. Los checks se ejecutaron con Rust 1.98.1.

Una primera pasada completa detectó que pruebas paralelas de F-3 podían usar el mismo timestamp para sus directorios SQLite; la reserva ahora usa contador y creación atómica, sin depender de resolución del reloj. No cambió el comportamiento del engine por ese ajuste.

Los límites siguen siendo explícitos: un scope y coordinador por instancia, plugins compilados confiables, HTTP sin TLS directo, bearer inicial, escrituras HTTP genéricas `Unsafe` y CSV acotado que se vuelve a analizar por lote. No se acreditan destinos reales, rendimiento HTTP de producción, MSRV 1.85 ni el servidor SFTP externo mediante estas pruebas locales. El contraste P-01/P-07 queda para la adopción posterior al cierre del engine; la comparación de transporte ya realizada se registra abajo.

## Evidencia F-5 — completada

El [consumidor de autoría](../examples/authoring-client/src/lib.rs) usa solo protocolo/JSON en su biblioteca. Su host de demostración y [cuatro pruebas](../examples/authoring-client/tests/consumer.rs) obtienen el catálogo público, seleccionan una revisión exacta, generan una definición, conservan metadata visual al exportar/importar y ejecutan mediante la fachada. No adivina configuración requerida ni elige otra revisión cuando falta la solicitada. Un Decorator comprueba que preparar o rechazar datos de configuración no invoca la operación.

La [extensión de texto](../examples/reference-module/src/text.rs) agrega configuración reusable y un contrato Pure/Safe desde un crate externo. [ADOPTION](ADOPTION.md) publica las recetas de composición, plugins, autoría, versionado y retiro. [EXAMPLES](../EXAMPLES.md) ahora apunta a recorridos formato 2; sus quince workflows históricos se conservaron temporalmente como fixtures de caracterización. Se retiraron con sus consumidores en el incremento posterior; los originales están en `6806fa0:EXAMPLES.md`.

La sustitución técnica de C-02 se comprueba mediante el puerto público [InventoryDestination](../examples/reference-module/src/inventory/destination.rs) y el bootstrap inyectable de [las pruebas de inventario](../crates/forge/tests/v2_inventory.rs). `multiple_pages_preserve_order_bound_concurrency_and_retry_without_duplicate_effect` registra `ObservedDestination` en lugar de `MemoryInventory`, ejecuta la misma definición y verifica 201 efectos, 202 intentos, orden del reporte y concurrencia acotada. El caso de reconciliación continúa sin repetir la página confirmada. Ambos pasan en el log `/tmp/workflow-forge-f5-rust98-workspace.log`. El sustituto es un Decorator de prueba sobre `MemoryInventory`; demuestra la sustitución por contrato sin cambiar el coordinador, no un segundo sistema de negocio real ni el cierre de P-01.

El consumidor descubrió una ubicación demasiado genérica en errores de preparación. El compilador ahora conserva diagnósticos ya ubicados y señala `/operation`, `/retry`, `/config`, `/input/literal` y `/start/config`, con JSON Pointer del dato cuando aplica. Las pruebas comprueban esos campos y cero ejecución durante validación. El ejemplo de autoría cubre un nodo; no acredita un editor completo de controles anidados.

Verificación de este incremento: `cargo test --workspace --all-features` termina con **361 pruebas exitosas, 0 fallidas y 1 SFTP ignorada**. `cargo run -p workflow-forge-authoring-example --example round_trip` exporta el documento y verifica `ID-42`. Formato y Clippy workspace/all-targets con defaults y all-features, `-D warnings`, pasan. El benchmark posterior se compiló en release y ejecutó las combinaciones de abajo. Log de tests: `/tmp/workflow-forge-f5-authoring-workspace.log`. No se comprobó MSRV 1.85 ni se ejecutó el servidor SFTP externo.

La ejecución completa también reprodujo una colisión de directorios por timestamps en otro fixture SQLite. Los cuatro fixtures del motor comparten ahora reserva atómica con contador, incluidos procesos de caída; no se cambió la semántica de ejecución por este ajuste.

Revisión documental del incremento: catorce documentos, 250 enlaces locales/anchors, cinco bloques JSON y seis archivos de configuración/fixtures/schema parseables, sin errores y con fences balanceados. No se renderizó Mermaid. El archivo de mediciones enlazado se genera como JSON a partir de los resultados completos de los procesos.

### Comparación de transporte F-5

El [ejecutable de medición](../crates/service/examples/service_measure.rs) compara una operación identity por Rust y HTTP/1.1 loopback. Mismo entorno de F-3: Apple M1 Max, 10 CPU, 64 GiB, macOS 27.0 (26A428), Rust 1.98.1, release. Una pasada por combinación: 100 calentamientos y 1 000 muestras, en lotes de 1/8/32 que terminan antes de iniciar el siguiente. Cada SQLite es nueva; sin observer de logging ni destinos externos.

Build: `cargo build --release -p workflow-forge-service --example service_measure`. Comando: `/usr/bin/time -l target/release/examples/service_measure rust|http BYTES 1000 CONCURRENCY [RUTA_SQLITE_NUEVA]`. Ejemplo concreto: `/usr/bin/time -l target/release/examples/service_measure http 1024 1000 8`. Omitir ruta mide memoria. Los [datos completos](measurements/f5_transport.json) publican preparación inicial y p50/p95/p99 de preparación, aceptación y recorrido completo, throughput, defaults, huellas SHA-256 del código/binario y cada comando.

**24 combinaciones; 26 400 runs correctos, incluidos calentamientos.** No hubo un RunId repetido ni un acuse con garantía diferente del perfil solicitado. Resumen de p95; el JSON enlazado conserva los demás percentiles:

| Perfil / JSON / concurrencia | Aceptación p95 Rust / HTTP (µs) | Completo p95 Rust / HTTP (µs) | RSS Rust / HTTP (bytes) | Consultas HTTP/run |
|---|---|---|---|---|
| Memoria / 1 KiB / 1 | 42.75 / 163.25 | 152.209 / 295.75 | 26017792 / 28557312 | 1.003 |
| Memoria / 1 KiB / 8 | 97.542 / 1274.833 | 1249.834 / 4266.958 | 26345472 / 30048256 | 1.988 |
| Memoria / 1 KiB / 32 | 155.167 / 6172.125 | 4548.5 / 9441.125 | 26902528 / 33800192 | 1.764 |
| Memoria / 64 KiB / 1 | 139.333 / 692.875 | 753.084 / 4254.5 | 224755712 / 225280000 | 2 |
| Memoria / 64 KiB / 8 | 442.458 / 2554.208 | 2575 / 5953.625 | 226181120 / 240664576 | 2.133 |
| Memoria / 64 KiB / 32 | 626.583 / 13571.042 | 11496.5 / 17811.417 | 227721216 / 260423680 | 2.353 |
| SQLite / 1 KiB / 1 | 8251.791 / 8781.208 | 42313.375 / 46108.417 | 18726912 / 21151744 | 4.862 |
| SQLite / 1 KiB / 8 | 125857.542 / 148421.333 | 293047.584 / 298160.834 | 18825216 / 22429696 | 6.411 |
| SQLite / 1 KiB / 32 | 928144.333 / 935601.333 | 1118223.667 / 1098096.75 | 19136512 / 25722880 | 6.572 |
| SQLite / 64 KiB / 1 | 10765.917 / 11085.209 | 58879.459 / 57834.833 | 22003712 / 25247744 | 4.892 |
| SQLite / 64 KiB / 8 | 169848.958 / 161400.208 | 346213.083 / 328880.292 | 26066944 / 33587200 | 6.563 |
| SQLite / 64 KiB / 32 | 1100029.5 / 1040478.167 | 1318243.833 / 1229950.667 | 32309248 / 55836672 | 6.72 |

Aceptación termina al recibir el acuse. El recorrido completo incluye comprobar el resultado: Rust espera con `wait`; HTTP consulta `/result`, con pausa solicitada de 1 ms tras `not_ready`. El tiempo real de esa pausa depende del scheduler. Por tanto, la diferencia incluye polling, autenticación, traducción y transporte; estas mediciones no atribuyen su coste individual. Que algún p95 HTTP durable sea menor en esta pasada no demuestra que HTTP acelere el engine. Hace falta repetir para sostener diferencias pequeñas.

El RSS procede de `/usr/bin/time -l` e incluye el proceso completo, preparación/calentamiento y retención predeterminada; en HTTP incluye el cliente local. No es memoria por run ni consumo tras liberar resultados. Esta herramienta usa futures por lotes y un workflow de un nodo; no sustituye la comparación histórica de cadenas de `v2_measure`. Logs locales: `/tmp/workflow-forge-f5-measure-ejH7QR/transport.log`.

La evidencia adicional de memoria y SQLite se registra abajo. Por el ajuste autorizado del 2026-09-27, las campañas de rendimiento sin completar quedan diferidas y opcionales. La verificación funcional final está registrada al final de esta sección. El contraste con integraciones reales P-01 y sus metas P-07 se realizará después del cierre del engine, cuando el usuario decida comenzar esa etapa.

### Migración de consumidores y retiro del prototipo

La [CLI](CLI.md) consume el protocolo HTTP: catálogo paginado, preparación, inicio/espera, estado/resultado, consultas paginadas, cancelación, señales, inspección/reconciliación y artefactos. Su dependencia normal es el protocolo y transporte; el engine solo aparece en fixtures de desarrollo. Por defecto pide durabilidad, mantiene separados los deadlines de petición/run/espera y no cancela al desconectarse. `wait` consulta el estado autoritativo para distinguir fallo, espera y bloqueo de intervención, incluidos efectos inciertos.

Las [pruebas contra el servicio](../crates/cli/tests/service.rs) pasan seis casos con procesos CLI reales: 65 operaciones adicionales para paginación, preparación sin ejecutar, diagnósticos, credencial rechazada, recibo SQLite tras reiniciar sin repetir la operación, aceptación expresa de memoria, timeout/SIGINT sin cancelación, señal y cancelación explícita, efecto inspeccionado/resuelto una sola vez y stream de 200 003 bytes con referencia declarada. Las [pruebas de transporte](../crates/cli/tests/wire.rs) pasan tres casos: redirects/retries deshabilitados, plazo de petición, bytes reales sin Content-Length, JSON/argumentos inválidos sin reflejar datos y descargas truncadas/excesivas sin destino parcial ni archivos temporales residuales. `cargo test -p workflow-forge-cli` pasa nueve casos antes de retirar las dependencias anteriores.

La [fachada](../crates/forge/src/lib.rs) y `prelude` ahora exponen el motor formato 2. Se eliminaron `core`, los seis crates `extensions/*`, los schemas 1.0, tres ejemplos Rust y las pruebas exclusivas de esos contratos, además del job SFTP y dependencias que ya no tienen consumidores. El namespace `v2` conserva los mismos tipos/implementación. [ADOPTION §5](ADOPTION.md#5-migrar-desde-el-prototipo) publica las features actuales y las diferencias: SFTP/XLSX/ZIP/GZIP/plantillas/conversiones antiguas no se presentan como migradas. Los historiales de abajo y mediciones previas conservan sus resultados originales, no el conteo de la suite actual.

Verificación posterior al retiro y al ajuste de Rust, previa a la pasada final: `cargo test --workspace --all-features --locked` pasa **151 pruebas, 0 fallos y 0 ignoradas** con Rust/Cargo 1.98.1. El conteo excluye las pruebas eliminadas del prototipo e incluye los nueve casos CLI. Clippy workspace/all-targets con defaults y all-features, `-D warnings`, pasa; formato y `git diff --check` también. El perfil `cargo check -p workflow-forge --no-default-features` compila. `cargo tree -p workflow-forge-cli --edges normal --prefix none --depth 1` confirma protocolo/transporte sin dependencia normal del engine o servicio.

Por instrucción del usuario se usa la serie instalada Rust 1.98, concretamente 1.98.1, fijada en `rust-toolchain.toml`, el mínimo del workspace y CI. La declaración antigua 1.85 no representaba las dependencias resueltas: ICU/IDNA ya exigen 1.86 según sus manifests. Se revisaron siete sugerencias mecánicas de Clippy habilitadas por el mínimo nuevo (condiciones `if let` y `is_multiple_of`), y la suite anterior pasó después de aplicarlas. No se acredita compatibilidad con toolchains anteriores. La configuración CI se actualizó; la evidencia indicada es local, no una ejecución remota de GitHub Actions.

La revisión documental del retiro verifica 15 documentos, 253 enlaces locales/anchors, cinco bloques JSON y seis archivos JSON de fixtures/configuración/schema, sin errores. La posterior publicación de secuencias lleva los enlaces a 256 y valida por separado sus 30 comandos únicos, contadores y documento JSON. No se renderizó Mermaid. La ampliación del benchmark tiene su protocolo en ACCEPTANCE; no altera los resultados históricos ni completa por sí sola V-14. Log de la suite Rust 1.98.1: `/tmp/workflow-forge-f5-rust98-workspace.log`.

### Secuencias y lectura simulada en memoria

El [informe de secuencias](measurements/f5_sequences.json) registra **30 combinaciones y 33 000 runs verificados**, incluyendo calentamiento: 27 000 muestras exitosas, 3 000 muestras rechazadas por cuota como se esperaba y 3 000 calentamientos. No hubo resultados inesperados ni RunIds repetidos dentro de cada combinación. Los rechazos son runs aceptados que terminan `Failed/resource.limit`, sin output; no son 30 000 ejecuciones exitosas.

Comando base: `/usr/bin/time -l target/release/examples/v2_measure N BYTES 1000 CONCURRENCY --terminal-runs 64`. Se añade `--expect-resource-limit` a 100 nodos de 1 MiB y `--latency-ms 10` a una primera lectura de 1 KiB. Se midieron cadenas puras de 1/10/100 nodos × 1/64/1024 KiB × concurrencia 1/8/32, más tres lecturas simuladas. Se conservan los demás defaults, incluidos 16 MiB retenidos por run y 1 MiB por valor. El perfil de esta matriz es memoria; las mediciones SQLite realizadas y las omitidas se distinguen en la sección siguiente.

Apple M1 Max, diez CPU, 64 GiB, macOS 27.0/26A428 y Rust 1.98.1 release. Una pasada por combinación, 100 calentamientos y 1000 muestras. La tabla muestra **concurrencia 1 / 8 / 32**, p95 de start→wait en milisegundos y pico RSS del proceso en MiB; el JSON conserva p50/p95/p99, throughput, límites, hashes y comandos.

| Nodos / payload | p95 ms, 1 / 8 / 32 | Pico RSS MiB, 1 / 8 / 32 | Resultado exigido |
|---|---|---|---|
| 1 / 1 KiB | 0.080 / 1.127 / 3.589 | 13.2 / 14.0 / 14.0 | Resultado correcto |
| 1 / 64 KiB | 0.753 / 3.011 / 10.412 | 33.6 / 35.6 / 37.8 | Resultado correcto |
| 1 / 1024 KiB | 10.182 / 31.486 / 102.514 | 355.9 / 343.1 / 447.8 | Resultado correcto |
| 10 / 1 KiB | 0.328 / 3.331 / 13.737 | 14.9 / 15.9 / 16.8 | Resultado correcto |
| 10 / 64 KiB | 4.088 / 26.835 / 75.545 | 108.4 / 102.3 / 124.0 | Resultado correcto |
| 10 / 1024 KiB | 77.558 / 355.399 / 1075.934 | 1246.1 / 1534.7 / 1568.6 | Resultado correcto |
| 100 / 1 KiB | 5.971 / 36.150 / 129.456 | 35.9 / 40.5 / 52.1 | Resultado correcto |
| 100 / 64 KiB | 53.518 / 166.079 / 619.982 | 697.0 / 733.3 / 824.1 | Resultado correcto |
| 100 / 1024 KiB | 79.426 / 200.470 / 795.683 | 1737.1 / 1568.5 / 1743.2 | resource.limit |
| 1 / 1 KiB / lectura 10 ms | 12.764 / 14.281 / 14.622 | 13.1 / 13.8 / 14.8 | Resultado correcto |

El campo `max_retained_data_bytes` mide snapshots finales; no es el máximo intermedio de un run ni RSS. `failed_runs` cuenta resultados inesperados, no los estados Failed exigidos por las pruebas de cuota. La comprobación de output, tamaño final e identidad ocurre después del cronómetro start→wait y entra en throughput. El tiempo de lectura incluye el timer y scheduling de Tokio, sin una red real.

El calentamiento rechazó inicialmente una expectativa incorrecta para diez nodos de 1 MiB: el motor ya libera inputs de nodos confirmados y termina con 12 582 976 bytes retenidos. Se corrigió la expectativa del experimento; no se cambió el engine. El JSON distingue los dos binarios del harness: el segundo solo agrega diagnósticos de calentamiento y estado/código al registro de muestra. La matriz completa compara comportamientos exigidos explícitamente, sin convertir ese intento descartado en un fallo del motor.

La retención de 64 terminales difiere de los 1000 de los benchmarks históricos; estas cifras no acreditan una mejora/regresión frente a ellos. Una comparación futura necesitaría la misma configuración, pero no se exige realizarla para cerrar técnicamente F-5 bajo el ajuste del 2026-09-27. Los picos incluyen allocator, planes, snapshots y resultados retenidos; no son una medición de memoria tras GC. Logs completos: `/tmp/workflow-forge-f5-sequences-8Bkfgg/memory.log`. Las observaciones de saturación/liberación se registran abajo; las metas reales P-07 siguen abiertas.

### Ampliación durable y capacidad de una instancia viva — evidencia conservada

El [informe SQLite](measurements/f5_sqlite_sequences.json) conserva tres de las doce combinaciones previstas: un nodo de 1 MiB, concurrencia 1/8/32, 100 calentamientos y 1000 muestras por combinación. **3300 runs correctos**, sin resultados inesperados. p95 start→wait: 230.702/992.566/3907.690 ms; pico RSS: 68.750/119.375/186.578 MiB. Cada base nueva declara 2 GiB de cuota, mantiene WAL/FULL/fullfsync y retiene hasta 64 terminales. Los tamaños finales de las bases son 206012416/215482368/218632192 bytes, sin sidecars WAL/SHM al salir; no representan el pico de disco. Las nueve combinaciones de cadenas de 10/100 nodos y lecturas simuladas quedaron diferidas por el ajuste del 2026-09-27; no tienen resultados acreditados.

El [harness de capacidad](../crates/forge/examples/v2_capacity.rs) implementa el protocolo de ACCEPTANCE mediante una operación pública y consultas al store. Formato, Clippy del ejemplo con SQLite y build release pasaron con Rust 1.98.1 antes del último ajuste del consumidor descrito abajo. No modifica el engine. El [informe de capacidad](measurements/f5_capacity.json) conserva las observaciones terminadas y distingue las combinaciones diferidas, sin exigir reanudar la campaña.

Las tres saturaciones en memoria, con 1/8/32 plazas activas y 2/16/64 pendientes, verifican 1002/6984/27456 ejecuciones medidas y 2004/13968/54912 rechazos `admission.full`. Cada combinación suma además 100 calentamientos correctos y al menos 30 segundos con la cola llena. Todas las aceptaciones tienen resultado e invocación única; no se excede la concurrencia. La espera de 100 ms por ciclo forma parte del recorrido completo y throughput. Estas cifras verifican límites y no son capacidad máxima de producción. No se atribuye liberación a los picos de estas saturaciones; la serie correspondiente se registra a continuación.

La saturación SQLite con una plaza activa y dos pendientes completó **1002 ejecuciones medidas y 100 calentamientos**, 2004 rechazos `admission.full`, 334 ciclos y 34.806 segundos con la cola llena, sin resultados incorrectos. El p95 del recorrido completo fue 223559.834 µs y el pico RSS 16400384 bytes. También incluye la espera artificial del harness; no acredita capacidad máxima de producción. Las saturaciones SQLite con concurrencia 8/32 quedaron diferidas.

La expiración en memoria con 32 runs concurrentes y 1 MiB pasa los 1100 IDs de cada una de dos observaciones iniciales: `not_found`, store sin pendientes y readiness vigente. El RSS supera el umbral de investigación: +27.46%/+24.57% frente al calentamiento. Una tercera observación amplía a 50 ciclos (5100 runs verificados) y conserva todas las muestras: desde el ciclo 25 al 50, RSS pasa de 499449856 a 499695616 bytes, mientras cada ciclo expira sus 100 IDs. La ventana inicial sí crece; no se borra ni se presenta como estable.

Dos observaciones adicionales de 50 ciclos incorporan contadores de malloc y luego un probe final al soltar IDs/vectores de latencia del propio harness. En la última, cada expiración reduce las asignaciones en uso en **201835936 bytes (192.5 MiB)**. Los últimos 20 ciclos tienen RSS entre 464830464 y 466108416 bytes, variación de **0.275%**. Soltar la metadata de medición libera otros 636480 bytes; quedan 1967056 bytes de heap en uso, frente a 1745120 tras calentamiento (+12.72%). Permanecen vivos el runtime, plan, input reusable e informe compacto. La memoria reservada por malloc queda en 486539264 bytes y RSS en 466206720 bytes; ambos son distintos de los bytes aún asignados.

La investigación identifica liberación de payloads, estabilización local de RSS y retención de reserva del allocator, con la metadata del instrumento contabilizada por separado. No se cambió el engine ni se forzó liberación del allocator. El campo nativo `max_size_in_use` devuelve cero en este host y no se usa como pico; el pico RSS procede de `/usr/bin/time`. Las cinco observaciones de liberación y tres saturaciones en memoria suman **53242 runs verificados**; son cuatro combinaciones de memoria, con repeticiones diagnósticas. Al agregar la saturación SQLite, el informe conserva nueve observaciones sobre cinco de las ocho combinaciones previstas: **54344 runs verificados**, incluidos 53444 medidos, y 72888 rechazos de admisión. Las otras tres combinaciones SQLite —saturación 8/32 y liberación 32— quedaron diferidas. La conclusión se limita a las ventanas/cargas medidas y las metas P-07 siguen pendientes. El informe conserva comandos, configuraciones, series completas, revisiones del harness y huellas de código/binario.

El intento de liberación SQLite con concurrencia 32 terminó durante el calentamiento en 3.23 segundos con `not_found`, sin producir muestras. El consumidor del harness esperaba a que acabaran todos los `start` antes de observar resultados, mientras su TTL era de 2000 ms; resultados tempranos podían expirar antes de consultarse. Se corrigió el consumidor para encadenar `start → wait → validate` por tarea. La campaña no se repitió: la verificación del cambio se limita a una regresión focalizada de ocho runs SQLite con TTL de dos segundos, que pasó en 2.42 segundos con timeout de diez segundos. La prueba no fuerza un desfase entre aceptaciones ni reproduce estadísticamente la campaña. El intento inicial no cuenta entre las nueve observaciones completadas ni acredita liberación durable.

Clippy workspace/all-targets con all-features y `--locked -D warnings` pasa después de incorporar los probes; el ejemplo release compila con Rust 1.98.1. La dependencia libc ya resuelta se declara solo para desarrollo en macOS. No se agregaron dependencias normales al motor ni se sustituyó el allocator. La revisión de enlaces/JSON del incremento conserva 15 documentos válidos; F-5 queda cerrada bajo el alcance técnico acordado.

### Verificación esencial final — 2026-09-27

Con Rust 1.98.1 instalado, `cargo test --workspace --all-features --locked` termina con **151 pruebas exitosas, cero fallidas y cero ignoradas**, incluidos doctests. La regresión focalizada del ejemplo `v2_capacity` añade **una prueba exitosa**: 152 verificaciones en total, sin contarla dos veces. La suite del workspace no ejecuta ese test del ejemplo; CI incorpora un paso específico para conservar su cobertura. La evidencia es local, no una ejecución remota de GitHub Actions.

`cargo clippy --workspace --all-targets --all-features --locked -- -D warnings`, `cargo fmt --all --check` y `git diff --check` pasan. Logs: `/tmp/workflow-forge-f5-essential-workspace.log`, `/tmp/workflow-forge-f5-essential-sqlite.log` y `/tmp/workflow-forge-f5-essential-clippy.log`. La verificación técnica acordada queda satisfecha; no quedan pruebas ni mediciones por completar bajo este alcance salvo que un cambio o fallo nuevo lo justifique. P-01/P-07 corresponden a la etapa posterior de integraciones reales y despliegue; no condicionan este cierre por la aclaración del usuario del 2026-09-27.

La revisión documental final valida 15 documentos, 265 enlaces locales/anchors, cinco bloques JSON y seis fixtures/configuraciones/schema, sin errores. Los dos informes de campañas parciales son JSON parseable y sus contadores están comprobados. Conservan las combinaciones realizadas y las diferidas; no se renderizó Mermaid.

## Evidencia del prototipo

Revisión estática inicial del 2026-09-26; no equivale a una suite ejecutada ni a auditoría exhaustiva. Las rutas de esta tabla son históricas, recuperables con `git show 6806fa0:RUTA`; el retiro F-5 no convierte esos archivos en documentación vigente.

| Evidencia local | Lectura y brecha |
|---|---|
| `crates/core/src/task/mod.rs` | Abstracción de operación existente; schemas opcionales y sin todas las revisiones del diseño nuevo. |
| `crates/core/src/task/registry.rs` y `crates/core/src/runtime/schemas.rs` | Registro sustituible por ID y validadores construidos antes del run; fijar referencias evita divergencia. Es inferencia estática, no reproducción de fallo. |
| `crates/core/src/runtime/executor.rs` | Coordinación/contexto en memoria; requiere separación para recuperación del nuevo diseño. |
| `crates/core/src/runtime/policy.rs` | Validación, timeout y retry existentes; falta formalizar incertidumbre y seguridad de repetición del nuevo diseño. |
| `crates/core/src/runtime/handlers/gateway.rs` | Joins por entradas del grafo; revisar activación de ramas con el contrato propuesto. |
| `crates/core/src/observe/mod.rs` | Eventos y observers; no sustituyen commit transaccional. |
| `crates/core/src/idempotency.rs` | Helper basado en contenido; el nuevo diseño distingue invocación y clave de negocio. |
| `crates/core/src/io/mod.rs`, `crates/extensions` y `crates/forge/src/lib.rs` históricos | Recursos/conectores y experiencia integrada que sirven como base de refactorización. |
| `crates/core/tests` | Casos existentes para baseline y caracterización; no se reporta conteo ni éxito sin ejecutarlos. |

## Estado por fase

| Fase | Estado |
|---|---|
| F-0 | Completada: diseño/casos registrados, formato y benchmark corregidos; fmt, ambas variantes de Clippy y 220 pruebas all-features pasan. |
| F-1 | Completada: C-01A/C-03, garantías aplicables en memoria, 21 pruebas nuevas y primera medición V-14. |
| F-2 | Completada en memoria: C-01B/C-02, V-06/V-07/V-18, controles/efectos y comparación de rendimiento. 282 pruebas pasan; fmt y ambas variantes de Clippy limpios. |
| F-3 | Completada: paquete fijado, estado/artefactos SQLite, migración 3, señales/timers y caídas C-01B/C-02/C-04/V-15/V-18 probadas. 331 pruebas pasan; fmt y ambas variantes de Clippy limpios. Mediciones en memoria/SQLite y regresiones documentadas. |
| F-4 | Completada: paridad Rust/HTTP, servicio con bootstrap/supervisión, módulos HTTP/JSON y archivos/CSV, observación y permisos. 357 pruebas pasan; fmt y ambas variantes de Clippy limpios. |
| F-5 | Completada: autoría/extensión, guías de adopción, CLI migrada y prototipo retirado; verificación técnica final de 151 pruebas más una regresión focalizada, Clippy y formato limpios. Se conservan 24 mediciones Rust/HTTP, 30 combinaciones de secuencias/lectura en memoria, tres SQLite y cinco combinaciones de capacidad. Campañas restantes opcionales e integraciones reales posteriores al cierre del engine, según las instrucciones del usuario del 2026-09-27. |

## Cobertura de los puntos revisados

| Punto | Evidencia documental y límite |
|---|---|
| Primera entrega usable | CONTRACTS §2 y ROADMAP F-1, con capacidades incluidas/rechazadas y casos de salida. |
| Integraciones representativas | ACCEPTANCE C-01/C-02 define datos, pasos, efectos y fallos; el contraste con sistemas reales permanece explícito en P-01. |
| Contratos concretos | CONTRACTS §3–6 y E-01: documento, revisiones, mappings, schema dialect, operación y errores. |
| Confianza y recursos tempranos | CONTRACTS §7, P-08 y V-19 desde F-1; no se promete sandbox de plugins compilados. |
| Ejecuciones inciertas | TDD-06, C-04 y V-18: evidencia, permisos, transiciones, concurrencia, auditoría y cierre con incertidumbre. |
| Evidencia y mediciones tempranas | Baseline de abajo y ACCEPTANCE §5; conservar las mediciones realizadas y sus límites. Las combinaciones omitidas son opcionales bajo el ajuste del 2026-09-27 y no se acreditan como realizadas. |

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

F-0 a F-5 están completas: el engine y sus consumidores tienen validación técnica esencial con Rust 1.98.1. El usuario realizará las integraciones reales cuando considere listo el motor; entonces se concretarán P-01/P-07 para esos sistemas y su despliegue. No queda trabajo requerido para cerrar la refactorización actual. No repetir pruebas sin un cambio o fallo nuevo ni reanudar las nueve secuencias/lecturas SQLite, las tres combinaciones de capacidad diferidas o comparaciones históricas sin una necesidad concreta. Las mediciones realizadas y sus límites permanecen como evidencia.

Baseline reproducido sobre HEAD `392303b`, con los cambios documentales del worktree, macOS y `rustc 1.98.1` / `cargo 1.98.1`. La ejecución inicial aislada no podía preparar dependencias; las pruebas se completaron después con acceso autorizado. La CI del baseline usaba all-features y un job SFTP separado; este último se retiró en F-5. F-0 corrigió las features del target de Criterion, su soporte async y el formato preexistente, antes de medir el motor nuevo.

Al comenzar implementación, verificar las instrucciones locales y el estado de Git. La eliminación del diseño en raíz y de tres HTML bajo `docs/` ya estaba presente antes de la refactorización documental; no se restauraron esos archivos.
