# Workflow Forge — casos de referencia y evidencia

Diseño de aceptación, actualizado el 2026-09-27. Anexo del [PRD](PRD.md) y del [TDD](TDD.md). Son escenarios de referencia elegidos para probar generalidad, **no integraciones reales atribuidas al usuario**. Por decisión del usuario, P-01 conserva el contraste con sus sistemas para después del cierre técnico del engine. Las cifras y resultados esperados describen los protocolos de aceptación; PROJECT registra las ejecuciones acreditadas.

## 1. C-01 — recibir, transformar, consultar y crear

Caso: alta de un pedido en un destino de pruebas. El vocabulario de pedidos pertenece al módulo; el engine solo ve schemas, operaciones y relaciones. Se puede sustituir por alta de clientes, tickets, envíos o facturas sin agregar instrucciones de negocio al engine.

Entrada de referencia:

```json
{"request_id":"req-17","customer":" C-9 ","items":[{"sku":"A-1","quantity":2}]}
```

| Paso | Dueño | Entrada → salida | Efecto |
|---|---|---|---|
| Recibir | Host/adaptador | Solicitud → input y clave de recepción `req-17` dentro de su ámbito | Aceptación del run. |
| Normalizar | `forge.text.trim` | `" C-9 "` → `"C-9"`; items permanece disponible en el input global | Puro. |
| Consultar cliente | Adaptador de lectura | `"C-9"` → `{"active":true}` | Lectura repetible, no constante. |
| Crear pedido | Adaptador del destino | Cliente/items/clave de efecto → `{"external_id":"O-41"}` | Escritura idempotente solo si el destino lo demuestra. |
| Construir respuesta | Mapping | Output confirmado → `{"order_id":"O-41"}` | Puro. |

**C-01A, F-1:** terminar después de la consulta y devolver `{"customer":"C-9","active":true}`. La lectura usa un fake del kit público y se sustituye por otro adaptador sin cambiar el engine. El mapping selecciona `/customer` para trim; la operación transforma una string y otro mapping construye el input de consulta. Las reglas de pedidos no se insertan en el compilador.

**C-01B, F-2/F-3/F-4:** incluir creación, política de efectos, recuperación y transporte. El destino simulado conserva un registro de solicitudes y deduplica por clave. Dos runs intencionales con payload idéntico y claves de recepción diferentes pueden crear dos pedidos; dos recepciones con la misma clave/contenido devuelven el mismo run dentro de su ventana.

| Variante | Resultado esperado |
|---|---|
| Falta `customer`, tipo inválido o `quantity <= 0` | Error localizado; cero llamadas de creación. |
| Cliente inactivo | Rechazo de negocio declarado, sin creación. En F-1 lo expresa una operación; F-2 puede modelar una rama. |
| Destino crea y se pierde respuesta | Mismo effect key en retry seguro, o `Unknown` y reconciliación; nunca crear a ciegas. |
| Misma clave de recepción con otro payload | Conflicto, no sobrescribir el input previo. |
| Reinicio después del resultado confirmado | Recuperar sucesor, no repetir creación. |
| Output inválido después de crear | Error de contrato conserva que pudo existir el efecto; no retry automático por validación. |

Schemas de aceptación: objeto de entrada sin campos extra, `request_id`/`customer` strings no vacías, `items` array no vacío; cada item exige `sku` string no vacía y `quantity` entero positivo. El schema de la respuesta exige `order_id` string no vacía. El adaptador comprueba reglas del destino además del schema. Fixtures de prueba fijan cada respuesta de la tabla, sin depender de una cuenta externa.

Trazabilidad: UC-01; V-02/V-03/V-04/V-07/V-08/V-10. La clave de recepción pertenece a `start`; la clave de efecto pertenece a una invocación. No usar el nombre de campo `request_id` como regla interna universal.

## 2. C-02 — importar un archivo y entregar un reporte

Caso: actualización de inventario desde un CSV. La misma composición sirve para migraciones, conciliación de datos, catálogos y reportes cambiando operaciones/schemas. El host registra un artefacto; el workflow recibe su referencia, no una ruta arbitraria del servidor.

Fixture CSV, UTF-8, separador coma, cabecera obligatoria:

```csv
sku,quantity
A-1,2
B-2,error
C-3,4
```

| Paso | Contrato observable |
|---|---|
| Leer y parsear | Valida cabecera; conserva número de fila y errores. Convierte cantidad explícitamente; no depende de coerción JSON Schema. |
| Formar lotes | Máximo 100 filas por lote, con cursor/identidad persistible. Array acotado de resultados o referencia de lote. |
| Validar filas | `A-1`/`C-3` válidas; `B-2` produce error de dato sin llamar al destino. |
| Procesar | `foreach` con hasta 4 invocaciones concurrentes, o el límite menor del host. Una clave lógica por fila/lote, estable tras reinicio. |
| Acumular | Política `collect`: resultados por índice de origen, independientemente del orden de respuesta. |
| Publicar reporte | Artefacto consultable y resumen `{"rows":3,"succeeded":2,"failed":1}`. |

El resumen anterior aplica al fixture sin efectos inciertos. Si un destino deja una escritura incierta, el run queda `Blocked` y el reporte se marca parcial con `unknown`; no se contabiliza como fallo seguro ni se publica éxito final. La política `collect` recoge errores conocidos, no resuelve incertidumbre.

Repetir el mismo SKU en filas distintas conserva invocaciones distintas; la política de negocio del conector determina si son dos ajustes o reemplazos. El orden de resultados no implica orden de efectos: un destino que necesita actualizaciones ordenadas usa concurrencia 1 o partición explícita por clave.

Aceptación en F-2: parseo acotado, iteración y resultado con proveedor en memoria. En F-3: reiniciar entre lotes mantiene cursor, filas confirmadas y disponibilidad de artefactos. En F-4: proveedor oficial de archivos/CSV y destino seleccionado. En F-5: sustituir el destino y repetir sin editar el coordinador.

Errores de lectura/cabecera fallan antes de los efectos; errores por fila siguen `collect`. Caída después de escribir reporte y antes de confirmar referencia deja un artefacto provisional recuperable/limpiable. No eliminar input, lotes o reporte mientras el run o su retención los requieran. Streaming entre operaciones usa artefactos y cursores serializables; no persistir un stream/future vivo.

Trazabilidad: UC-02; V-06/V-07/V-08/V-12/V-14. Los tamaños son fixtures de aceptación; no límites universales del motor.

## 3. C-03 — demostrar sustitución y crecimiento

Un crate de ejemplo fuera de los internos implementa `reference.customer_lookup` con schema y descriptor. Solo depende del protocolo público y de su cliente inyectado. El host lo registra mediante una contribución de módulo y ejecuta C-01A. Luego sustituye el fake por otro cliente con la misma semántica y repite el caso.

La revisión debe mostrar: ningún cambio de negocio en el engine; mismas reglas para módulo oficial/externo; dos runs concurrentes sin mezcla de datos; conflicto de registro sin publicación parcial; errores clasificados y límites respetados. Agregar métricas mediante Decorator no modifica input/output, identidad ni número de llamadas. Un proveedor de estado alternativo pasa su suite de garantías; cambiarlo no exige reescribir el workflow.

Trazabilidad: UC-04; V-04/V-05/V-17. El kit de conformidad será público y reusable; no requiere acceso a funciones privadas para ejecutar estos casos.

## 4. C-04 — resolver una espera y un resultado incierto

Registrar una espera correlacionada antes de iniciar el trabajo que enviará el callback. Iniciar el trabajo mediante su adaptador. Reiniciar el proceso, recibir señal válida y continuar una sola vez. Señal duplicada devuelve el acuse previo; señal desconocida no se acepta. El host debe garantizar reentrega si eligió un destino que puede avisar antes de que exista la espera.

Para un intento `Unknown`, usar los comandos y tabla de resolución de TDD-06: evidencia de aplicación confirma el resultado, evidencia de no aplicación habilita una decisión segura y evidencia inconclusa mantiene bloqueo. Probar dos resoluciones concurrentes, payload de evidencia inválido, comando duplicado y acceso de otro ámbito. Ninguno puede sobrescribir un resultado ya confirmado.

Trazabilidad: UC-03; V-07/V-08/V-09/V-18. En F-3 estos casos tienen pruebas de caída reales del proceso y acuses persistidos.

## 5. Medir temprano y publicar límites comprobados

**Alcance vigente, 2026-09-27:** por instrucción del usuario, el cierre técnico se limita a pruebas esenciales y casos borde. Las matrices de rendimiento, sus repeticiones y la comparación histórica dejan de ser requisitos de cierre. Se conservan los resultados obtenidos; las combinaciones no realizadas quedan diferidas, sin contarlas como aprobadas. Los protocolos de medición de esta sección quedan disponibles para necesidades concretas de capacidad u optimización.

En una decisión posterior del mismo día, el usuario sitúa sus integraciones reales después de comprobar el engine. Los casos de referencia, conformidad, autoría/extensión, paridad, recuperación y verificaciones esenciales permiten cerrar F-5 y crear su commit. P-01/P-07 continúan en la etapa posterior, sin bloquear el cierre técnico ni atribuir sistemas reales o capacidad de producción verificados.

### Cierre técnico esencial

Usar la suite funcional existente para comprobar estas garantías; no crear otra matriz de pruebas que duplique sus escenarios:

| Garantía | Casos borde necesarios |
|---|---|
| Contratos y autoría | Schema/configuración inválidos rechazados antes de ejecutar; revisión exacta, round-trip y diagnóstico localizado. |
| Coordinación | Secuencia, ramas, joins, iteraciones y subworkflows con resultado correcto aunque cambie el orden de finalización. |
| Efectos | Duplicados, timeout, cancelación, respuesta tardía e incertidumbre conservan identidad y no repiten escrituras sin garantía. |
| Durabilidad y lifecycle | Reinicio en fronteras de confirmación, señales/timers, readiness y cierre; un store durable fallido no cae a memoria. |
| Acceso y recursos | Scope/permisos, entrada/artefactos excesivos, cola llena y concurrencia acotada. |
| Extensión y consumidores | Sustitución mediante contratos públicos, revisión ausente/restaurada, paridad Rust/HTTP y CLI. |

La verificación de cierre comprende una pasada final de `cargo test --workspace --all-features --locked`, la regresión focalizada del consumidor SQLite de `v2_capacity`, formato, Clippy y verificación documental; PROJECT conserva los resultados ya obtenidos. Solo repetir ante un cambio relevante, fallo o preocupación concreta sin resolver. La regresión del benchmark comprueba comportamiento con pocas ejecuciones y no publica percentiles como si fueran una campaña de rendimiento. P-01/P-07 corresponden a la aceptación posterior con sistemas y metas reales.

### Campañas de rendimiento opcionales

Cuando se decida ejecutar una campaña, separar preparación fría, reutilización de plan y ejecución. Registrar versión de código, toolchain, sistema/CPU, perfil release, configuración, número de muestras y comando. Para percentiles comparables, realizar 100 ejecuciones de calentamiento y al menos 1 000 observaciones por combinación elegida; reportar p50/p95/p99, throughput y pico de memoria con el método de medición. No mezclar latencia simulada de red con overhead local. Esas cantidades no aplican a las pruebas funcionales ni obligan a recorrer toda la tabla.

| Carga de referencia | Qué verifica | Fase |
|---|---|---|
| Cadenas de 1/10/100 operaciones puras; JSON de 1/64/1024 KiB | Coste de preparar, coordinar, validar y retener datos; rechazos previstos por presupuesto. | F-1 |
| 1/8/32 runs concurrentes con operación sin I/O y lectura simulada de 10 ms | Uso de capacidad y separación de espera externa. | F-1/F-2 |
| C-02 con 100/10 000 filas y lotes de 100 | Memoria acotada y correlación; sin cargar todos los outputs indefinidamente. | F-2/F-3 |
| Store lento/fallido, señales duplicadas y reinicio | No perder aceptados ni confundir telemetría con commit. | F-3 |
| Misma definición por Rust y HTTP | Overhead del transporte separado de ejecución. | F-4 |

Cada combinación declara el presupuesto utilizado; para medir 100 outputs de 1 MiB se eleva explícitamente la retención del run o se espera rechazo, nunca se desactiva el límite sin registrarlo. La cola se llena hasta su capacidad y una solicitud adicional debe rechazarse antes de acreditar aceptación.

Criterios de estas campañas: resultados correctos bajo concurrencia, ninguna aceptación perdida, rechazo de exceso predecible, observación del consumo al liberar runs/resultados y tiempos medidos reproducibles. La primera medición crea una línea de comparación. Si se estudia una regresión de más del 15% en p95 o memoria, investigar mediante repeticiones equivalentes; ese umbral no obliga a abrir nuevas campañas ni bloquea por sí solo el cierre técnico. Las metas comerciales de latencia/carga siguen en P-07 hasta disponer de escenarios reales.

### Comparación Rust/HTTP en F-5

Una operación identity, el mismo documento/input y límites predeterminados, con perfiles de memoria y SQLite medidos por separado. Cada proceso elige Rust o HTTP sobre loopback; conserva cliente/instancia, realiza 100 calentamientos y al menos 1 000 muestras. Publica preparación inicial, p50/p95/p99 de preparación repetida, aceptación y recorrido completo, throughput, consultas por run y cero resultados/recibos incorrectos. El RSS del proceso HTTP incluye servidor y cliente locales; no representa la memoria de un cliente remoto.

Aceptación termina cuando se recibe `StartReceipt`; el recorrido completo verifica el resultado. Rust usa `wait`, mientras HTTP consulta `/result` y espera 1 ms únicamente tras `not_ready`. Por eso la diferencia de recorrido completo incluye polling y scheduling; no se presenta toda ella como coste de serialización o del socket. No se añaden retries a los comandos. Los tamaños de esta comparación son 1/64 KiB, con concurrencia 1/8/32; la campaña de 1 MiB, saturación, liberación de memoria y latencia externa sigue separada.

### Extensión de la medición de secuencias

`v2_measure N BYTES 1000 CONCURRENCY` conserva los límites predeterminados y admite `--latency-ms 10`: sustituye solamente la primera operación por una lectura simulada Safe de 10 ms; el resto sigue siendo identity. El tiempo incluye el timer/scheduling de Tokio y no representa una red real. `--sqlite RUTA_NUEVA` conserva el perfil durable existente. El resultado declara latencia, límites y perfil junto a los percentiles.

`--terminal-runs N` declara una retención por cantidad menor para las campañas de payload grande (entre la concurrencia elegida y 1000). No cambia el presupuesto por run. Las cargas grandes usan 64 terminales retenidos para acotar memoria/disco durante la campaña; compararlas exige repetir la referencia con esa misma retención. El default sigue siendo 1000 para conservar los comandos históricos. Los IDs de los 100 calentamientos y 1000 muestras deben ser distintos.

Para la campaña durable de payload grande, `--sqlite-bytes 2147483648` declara una cuota de base de 2 GiB, adicional a `--sqlite RUTA_NUEVA`. Los 512 MiB predeterminados no alojan 64 resultados de unos 12–16 MiB más runs activos. El override es exclusivo del benchmark y se publica en `provider_options`; no cambia WAL/FULL/fullfsync, el máximo de registro ni los límites del engine. Sin el argumento se conservan las opciones del proveedor. Rechazar el argumento si no se eligió SQLite. Cada combinación usa una base nueva; registrar tamaño de base/sidecars al terminar y RSS máximo por separado. El tamaño final no acredita el pico de disco.

La ampliación SQLite originalmente prevista en F-5 comprendía 1/10/100 nodos de 1 MiB a concurrencia 1/8/32, más una lectura simulada de 10 ms con un nodo de 1 KiB a las mismas concurrencias: 12 combinaciones nuevas. Las tres combinaciones realizadas se conservan; las otras nueve se difieren por el recorte autorizado. Las cargas pequeñas y comparación de transporte ya tienen campañas separadas. No presentar esta ampliación parcial como el producto cartesiano completo de tamaños, nodos y perfiles.

En el JSON, `max_retained_data_bytes` corresponde a snapshots finales, no al máximo intermedio ni al RSS del proceso. `failed_runs` cuenta resultados inesperados. RSS se obtiene separadamente con la herramienta del sistema. Validar output/identidad y contar bytes finales queda fuera del cronómetro start→wait y dentro del throughput total.

`--expect-resource-limit` mide rechazos durante la ejecución bajo los mismos presupuestos. Cada run debe haber sido aceptado, terminar `Failed` con `resource.limit`, conservar sus datos dentro de `run_bytes` y no publicar output. Una ejecución exitosa, un error diferente o un recibo de durabilidad incorrecto invalida esa combinación. Se cuentan por separado los rechazos comprobados y resultados inesperados, también durante los 100 calentamientos. Este modo permite comprobar las cadenas de 1 MiB que exceden 16 MiB retenidos; no acredita completar esas cadenas ni sustituye una medición con presupuestos mayores declarados.

### Saturación y liberación en una instancia viva

El ejemplo `v2_capacity` debe usar los contratos públicos y una sola instancia por proceso. La operación de prueba es una lectura Safe que conserva el input y espera un permiso controlado por el host; no se inyecta lógica de negocio ni instrumentación privada en el coordinador. El JSON publica configuración, muestras y comprobaciones. Un fallo invalida la combinación y aun así intenta cerrar el runtime.

En modo `saturation C`, fijar `active_runs = concurrent_attempts = C` y `pending_runs = 2C`, para C = 1/8/32, en memoria y SQLite. Tras 100 calentamientos, llenar 3C plazas sin liberar la operación; comprobar C llamadas activas, 3C runs no terminales y rechazo `admission.full` de otros 3C intentos. Mantener llena la cola al menos 100 ms por ciclo, repetir el rechazo antes de liberar y verificar que no aparezca otro aceptado ni llamada adicional. Liberar los 3C permisos y comprobar una invocación y un resultado correcto por recibo. Repetir ciclos hasta reunir al menos 1000 runs medidos y 30 segundos con la cola llena. Reportar aceptación, rechazo y recorrido completo por separado; la latencia completa y throughput incluyen la espera artificial, por lo que no son capacidad máxima del motor.

En modo `release C BYTES`, usar 100 calentamientos y diez ciclos de 100 ejecuciones, con retención máxima de 64 terminales y expiración de 2000 ms. Cada tarea debe comenzar `wait` inmediatamente después de recibir su propio acuse; reunir el lote cuando sus resultados ya estén comprobados. Esperar todas las aceptaciones antes de observar la primera introduce una demora que puede consumir la retención. Después de cada ciclo, soltar snapshots/resultados y esperar la ventana de retención: cada ID conocido debe devolver `not_found`, no debe quedar trabajo pendiente y readiness debe conservarse. Registrar RSS después del calentamiento y antes/después de esa espera, con el runtime aún vivo. Medir 1 MiB y concurrencia 32 en memoria y SQLite; ampliar o repetir si la serie revela crecimiento. En macOS/Linux, `ps -o rss= -p PID` devuelve KiB del propio proceso y se convierte a bytes; `/usr/bin/time` registra además el pico del proceso completo. Los probes quedan fuera de los percentiles de ejecución.

Los nombres históricos `before_expiry_*` significan después del lote y antes de esperar/comprobar ausencia; no afirman que aún estén retenidos los 100 resultados. `verified_expired_runs` verifica indisponibilidad por la política de retención: con máximo 64 terminales, algunos resultados pueden eliminarse por cantidad antes del vencimiento. Las revisiones del harness y `result_observation_method` identifican la estrategia del consumidor; no mezclar timings de estrategias diferentes sin declararlo.

Conservar la serie de RSS, no solo el valor inicial/final. La expiración lógica y el comportamiento del allocator son observaciones distintas: no se exige volver al RSS del arranque, pero una tendencia sostenida o variación superior al 15% después del calentamiento requiere repetición/investigación antes de concluir estabilidad. `release --cycles N` permite ampliar esa investigación a 10–100 ciclos (100 muestras por ciclo, además de 100 calentamientos), manteniendo límites, concurrencia y expiración. Conservar también las observaciones iniciales; ampliar la ventana no convierte su crecimiento en un resultado estable. Tampoco se acredita liberación midiendo únicamente después de terminar el proceso.

Para investigar diferencias entre expiración y RSS en macOS, registrar además `malloc_zone_statistics(NULL, ...)` antes/después de expirar y tras calentamiento. La [API pública de Apple](https://github.com/apple-oss-distributions/libmalloc/blob/main/include/malloc/malloc.h) suma las zonas cuando recibe NULL y distingue bytes en uso, máximo y memoria reservada. El probe del ejemplo usa los tipos ABI de libc y lee contadores sin cambiar el allocator ni solicitar liberación forzada; queda fuera de los cronómetros de ejecución. En otras plataformas publica `null`. Estos contadores abarcan el heap del proceso, incluido el harness; no son una atribución exclusiva al engine ni equivalen a RSS.

El harness conserva IDs y vectores de latencia para verificar unicidad y calcular percentiles. Registrar también un probe final después de soltar esa metadata, manteniendo vivos el runtime, input reusable, plan e informe compacto. Así la acumulación del propio instrumento queda visible y no se atribuye automáticamente a retención del motor.

## 6. Sustituir referencias por casos reales

Esta etapa ocurre después del cierre técnico del engine, por decisión del usuario del 2026-09-27; no condiciona F-5 ni su commit. Por cada integración real registrar: origen y autenticación; ejemplo saneado de input/output; pasos y efectos; soporte de idempotencia/reconciliación del destino; tamaño/frecuencia/concurrencia; latencia esperada; retención; comportamiento ante fallo. Vincular sus variantes con C-* y V-* antes de declarar validada esa integración y su despliegue.

Los casos de referencia permiten implementar y probar contratos ahora. Su éxito no acredita adecuación a sistemas externos aún no identificados ni benchmarks de producción. La evidencia ejecutada se registra exclusivamente en [PROJECT](PROJECT.md).
