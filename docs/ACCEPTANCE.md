# Workflow Forge — casos de referencia y evidencia

Diseño de aceptación, 2026-09-26. Anexo del [PRD](PRD.md) y del [TDD](TDD.md). Son escenarios de referencia elegidos para probar generalidad, **no integraciones reales atribuidas al usuario**. P-01 conserva pendiente el contraste con sus sistemas. Las cifras y resultados de abajo son expectativas para pruebas futuras, no ejecuciones acreditadas.

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

Mediciones desde F-1, ampliadas en cada fase. Separar preparación fría, reutilización de plan y ejecución. Registrar versión de código, toolchain, sistema/CPU, perfil release, configuración, número de muestras y comando. Realizar 100 ejecuciones de calentamiento y al menos 1 000 observaciones por combinación; reportar p50/p95/p99, throughput y pico de memoria con el método de medición. No mezclar latencia simulada de red con overhead local.

| Carga de referencia | Qué verifica | Fase |
|---|---|---|
| Cadenas de 1/10/100 operaciones puras; JSON de 1/64/1024 KiB | Coste de preparar, coordinar, validar y retener datos; rechazos previstos por presupuesto. | F-1 |
| 1/8/32 runs concurrentes con operación sin I/O y lectura simulada de 10 ms | Uso de capacidad y separación de espera externa. | F-1/F-2 |
| C-02 con 100/10 000 filas y lotes de 100 | Memoria acotada y correlación; sin cargar todos los outputs indefinidamente. | F-2/F-3 |
| Store lento/fallido, señales duplicadas y reinicio | No perder aceptados ni confundir telemetría con commit. | F-3 |
| Misma definición por Rust y HTTP | Overhead del transporte separado de ejecución. | F-4 |

Cada combinación declara el presupuesto utilizado; para medir 100 outputs de 1 MiB se eleva explícitamente la retención del run o se espera rechazo, nunca se desactiva el límite sin registrarlo. La cola se llena hasta su capacidad y una solicitud adicional debe rechazarse antes de acreditar aceptación.

Criterios iniciales: resultados correctos bajo concurrencia, ninguna aceptación perdida, rechazo de exceso predecible, ausencia de crecimiento sostenido tras liberar runs/resultados y tiempos medidos reproducibles. La primera medición crea la línea de comparación; una regresión de más del 15% en p95 o memoria exige investigación con repeticiones en el mismo entorno, no rechazo automático por ruido. Las metas comerciales de latencia/carga siguen en P-07 hasta disponer de escenarios reales.

## 6. Sustituir referencias por casos reales

Por cada integración real registrar: origen y autenticación; ejemplo saneado de input/output; pasos y efectos; soporte de idempotencia/reconciliación del destino; tamaño/frecuencia/concurrencia; latencia esperada; retención; comportamiento ante fallo. Vincular sus variantes con C-* y V-* antes de declarar aceptación de producto.

Los casos de referencia permiten implementar y probar contratos ahora. Su éxito no acredita adecuación a sistemas externos aún no identificados ni benchmarks de producción. La evidencia ejecutada se registra exclusivamente en [PROJECT](PROJECT.md).
