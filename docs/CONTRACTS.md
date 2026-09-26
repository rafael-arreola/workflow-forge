# Workflow Forge — contratos de la primera entrega

Decisiones de diseño para implementar F-1, 2026-09-26. Este anexo del [TDD](TDD.md) establece el contrato que debe cumplir la implementación, dentro del alcance del [PRD](PRD.md). Define comportamiento y formas de datos; los fragmentos Rust siguen siendo parciales y no son una librería implementada. Los casos de [ACCEPTANCE](ACCEPTANCE.md) permiten comprobarlo sin conocer los internos del motor.

## 1. El recorrido que debe poder explicar un desarrollador

**Construir → arrancar → preparar → iniciar → consultar → apagar.**

El host construye recursos y registra operaciones. `build` comprueba la composición; `boot` la activa y devuelve el runtime. `prepare` comprueba una definición sin ejecutar sus operaciones. `start` crea una ejecución independiente. El host conserva el runtime y comparte su handle. `shutdown` cierra admisión y espera el cierre acordado. El orden completo está en [ARCHITECTURE](ARCHITECTURE.md#instancia-y-arranque).

Para ampliar el sistema, elegir una frontera:

| Necesidad | Implementar | Ejemplos |
|---|---|---|
| Transformar o ejecutar una actividad | `Operation` | Normalizar datos, llamar una API, generar un archivo, invocar un modelo. |
| Proveer infraestructura con garantías | Puerto correspondiente | Estado en memoria/SQL, secretos, artefactos, observación. |
| Recibir trabajo desde otro sistema | Adaptador de entrada que llama al handle | HTTP, mensaje de cola, cron, comando de otra aplicación. |

Una espera larga usa el control de esperas de F-3; un disparador no vive dentro de una operación infinita. Un nuevo proveedor de negocio se agrega como operación. Una nueva forma de coordinar requiere extender conscientemente el lenguaje del engine y su conformidad.

## 2. Alcance concreto de F-1

F-1 entrega una librería embebible, en memoria, con secuencias de operaciones puras o lecturas declaradas repetibles. Incluye catálogo, composición predeterminada, sustitución de proveedores, validación, mappings, diagnóstico, consulta/cancelación, lifecycle y una extensión externa de ejemplo. El paquete oficial inicial ofrece `forge.data.identity` y `forge.text.trim`; el kit de pruebas ofrece una lectura simulada. `forge.text.trim` recibe/devuelve string, elimina whitespace inicial/final según `str::trim` de la implementación fijada, es pura y admite únicamente config vacía. Otras normalizaciones se agregan como operaciones o configuraciones versionadas.

Se puede recibir JSON, normalizar campos, consultar una fuente mediante una extensión y construir una respuesta. Cada invocación tiene un solo intento por defecto en esta entrega. Una solicitud de efectos de escritura, retry automático, ramas, lotes, subworkflows, durabilidad o esperas devuelve `capability.unsupported` al preparar, nunca una aproximación silenciosa. F-2/F-3 añaden esas capacidades; F-4 añade el servicio. El objetivo completo del producto se conserva.

Aceptación: C-01A, C-03 y las verificaciones F-1 de ROADMAP. La extensión externa usa únicamente contratos públicos, sin privilegios frente al módulo oficial. La composición `standard()` registra operaciones locales y proveedores en memoria; no abre conexiones externas ni obtiene credenciales por sí sola.

## 3. Documento y revisiones

El formato nuevo se identifica como `forge.workflow/2`; no se procesa como spec 1.0. Su primera capacidad de control es `sequence`. El mismo formato podrá representar instrucciones adicionales con requisitos explícitos; un consumidor que no las soporte debe rechazarlas.

| Campo | Contrato |
|---|---|
| `format`, `id`, `revision` | Formato conocido, identidad del workflow y revisión exacta e inmutable. |
| `schema_dialect` | `https://json-schema.org/draft/2020-12/schema`. |
| `input_schema`, `output_schema` | Schema objeto o booleano para cada frontera. |
| `entry`, `nodes`, `edges` | Entrada, nodos con ID único y relaciones de control explícitas. |
| `output` | Mapping del resultado global, evaluado después de completar la secuencia. |
| `presentation` | Objeto opcional de autoría; se conserva, pero no participa en ejecución. |

En F-1 cada nodo tiene `id`, `kind: operation`, `operation`, `config` e `input`. La referencia `operation` contiene `id`, `contract` e `implementation`, todos exactos; no admite `latest` ni rangos. El editor puede ayudar a seleccionarlos consultando el catálogo.

La secuencia es una cadena no vacía: `entry` sin predecesor, un sucesor por nodo salvo el final, un predecesor por nodo salvo entrada; todos alcanzables y sin ciclos. Cada arista tiene `from` y `to`; duplicados se rechazan. El orden del array y las coordenadas visuales no ordenan ejecución. Campos desconocidos se rechazan, excepto dentro de `presentation`; capacidades futuras no se ignoran.

Las revisiones son strings opacos no vacíos asignados por quien publica. El registro conserva el contenido semántico normalizado de cada revisión y rechaza reutilizarla con otro contenido. El normalizador convierte `nodes` a un mapa por ID, compara `edges` como conjunto y excluye únicamente `presentation`; preserva arrays de datos, tipos y valores sin coerción. No depende de un hash sin especificar. Una revisión de implementación debe cambiar al cambiar el código o comportamiento de la operación. El catálogo y los perfiles fijados por el plan también conservan revisiones exactas.

Ejemplo CT-01 — definición completa del recorrido mínimo, para el formato objetivo:

```json
{
  "format": "forge.workflow/2",
  "id": "reference.echo",
  "revision": "r1",
  "schema_dialect": "https://json-schema.org/draft/2020-12/schema",
  "input_schema": {
    "type": "object",
    "required": ["name"],
    "properties": {"name": {"type": "string"}},
    "additionalProperties": false
  },
  "output_schema": {"type": "string"},
  "entry": "echo",
  "nodes": [{
    "id": "echo",
    "kind": "operation",
    "operation": {"id": "forge.data.identity", "contract": "1", "implementation": "r1"},
    "config": {},
    "input": {"select": {"source": "input", "pointer": "/name"}}
  }],
  "edges": [],
  "output": {"select": {"source": "node", "node": "echo", "pointer": ""}}
}
```

Entrada `{"name":"Ada"}` → resultado `"Ada"`. `forge.data.identity` acepta y devuelve cualquier JSON (`true` como ambos schemas); copia el valor sin interpretar strings. Se mantiene validación del resultado global aunque la operación tenga un contrato abierto.

## 4. Mappings pequeños y explícitos

El lenguaje inicial tiene cuatro constructores. Cada objeto de binding contiene exactamente uno; nunca se interpreta una string como código por comenzar con `$` o `@`.

| Constructor | Ejemplo | Resultado |
|---|---|---|
| Literal | `{"literal":"$.name"}` | La string literal `$.name`. |
| Selección | `{"select":{"source":"input","pointer":"/customer/name"}}` | Un valor de entrada. |
| Objeto | `{"object":{"name":{"select":{"source":"input","pointer":"/name"}}}}` | Un objeto construido campo a campo. |
| Array | `{"array":[{"literal":1},{"literal":2}]}` | `[1,2]`, en el orden declarado. |

`select.source` admite `input` y `node`; el segundo exige `node` con ID del productor y lee su output confirmado. `pointer` usa JSON Pointer, incluyendo `""` para el documento completo y escapes `~0`/`~1`. Seleccionar un array devuelve un valor array; no introduce fan-out. Es la sintaxis definida por [RFC 6901](https://www.rfc-editor.org/info/rfc6901/).

Un campo opcional `fallback` dentro de `select` contiene otro binding. Se usa solo cuando falta la ubicación del dato o el productor fue omitido bajo una instrucción que permite esa ausencia. `null` es un dato presente; un tipo incompatible no activa fallback. Una referencia a nodo inexistente o no disponible por control siempre es error de preparación. F-1 solo permite leer predecesores de la cadena y el input global.

La evaluación es pura: sin red, secretos, reloj, aleatoriedad ni ejecución de scripts. Conversiones, cálculos y parseo son operaciones con schemas propios. En F-2 las decisiones consumen booleanos producidos por mappings u operaciones; el compilador no incorpora un segundo lenguaje de scripts. Arrays grandes se trabajan por lotes y artefactos, no expandiendo esta gramática indefinidamente.

Esta decisión sustituye el uso implícito del DSL/JSONPath del prototipo para el formato nuevo. Un futuro importador deberá traducir solo casos equivalentes y reportar los demás; la compatibilidad automática no es requisito.

## 5. JSON Schema y validación

El dialecto base es [JSON Schema 2020-12](https://json-schema.org/draft/2020-12/json-schema-core). `schema_dialect` lo declara para el documento; los schemas objeto publicados por módulos también lo declaran con `$schema`. Un schema booleano hereda el dialecto del descriptor. La validación no inserta defaults ni convierte tipos; `format` es anotación en el perfil inicial. Si un contrato necesita validar un formato como condición de negocio, debe hacerlo explícito mediante restricciones soportadas u operación de validación.

Los recursos `$ref`/`$dynamicRef` se resuelven exclusivamente en el paquete de schemas registrado. Cada recurso tiene URI absoluta y revisión fijada; URIs desconocidas, dialectos/vocabularios requeridos no soportados y conflictos de contenido impiden preparar. La URI identifica un recurso y no autoriza descargarlo. Una referencia recursiva de schema no es por sí sola un error: el validador debe soportarla con límites o rechazar explícitamente la capacidad que no pueda cumplir.

Validar en orden: documento → grafo → referencias → mappings → capacidades. Acumular diagnósticos independientes; evitar derivados de premisas inválidas. En ejecución: input global antes de aceptar; input de operación antes de despachar; output de operación antes de publicarlo; output global antes de declarar éxito. Un error del output tras un efecto mantiene la evidencia del efecto.

## 6. Frontera de operación y errores

Se adopta la frontera `Operation: Send + Sync` de E-01: descriptor prestado inmutable y `execute` que devuelve un future boxed `Send`, ligado a la vida de `self`. Evita métodos genéricos o retornos opacos incompatibles con `dyn Operation`; la [referencia de Rust](https://doc.rust-lang.org/reference/items/traits.html#dyn-compatibility) describe esas restricciones. No obliga a usar dispatch dinámico dentro de cada transformación.

| Tipo público | Contenido mínimo |
|---|---|
| `OperationDescriptor` | ID; versión de contrato; revisión de implementación; dialecto; schemas de config/input/output; efectos; recursos requeridos; descripción y ejemplos. |
| `Invocation` | IDs de invocación/intento, revisión de operación, config validada, input y clave de efecto opcional. |
| `OperationContext` | Run y ámbito, deadline, cancelación y recursos autorizados. Nunca store de ejecuciones ni acceso libre al catálogo. |
| `WorkflowValue` | Valor JSON; los binarios se representan mediante referencias de artefacto bajo un schema explícito. |
| `OperationOutput` | Valor JSON de salida. Evidencia técnica adicional se entrega saneada mediante el mecanismo de diagnóstico, sin alterar el schema de negocio. |
| `OperationError` | Código, clase, certeza del efecto, mensaje seguro y detalles estructurados acotados. |

`BoundConfig` contiene JSON validado y referencias de recursos resueltas a permisos, nunca credenciales exportables. F-1 valida esa config al preparar; cambiarla requiere otra revisión de definición/perfil. Clientes/pools se inyectan al construir la operación.

El descriptor declara `pure`, `read` o `write`; garantía de repetición `safe`, `keyed` o `unsafe`; y reconciliación `available` o `unavailable`. `pure` exige `safe`; `keyed` requiere soporte comprobado del destino. No se interpreta `read` como resultado constante. Para F-1 solo se aceptan `pure/read` con repetición `safe`, aun cuando el engine no haga retry automático.

La clase de error distingue `invalid_input`, `rejected`, `transient`, `resource`, `internal` y `cancelled`. La certeza es `not_applied`, `applied` o `unknown`; ante duda se usa `unknown`. `transient` no autoriza retry por sí mismo. Panic/desconexión/timeout tras despacho conservan incertidumbre cuando hubo un posible efecto. Un panic con abort del proceso requiere recuperación durable; un trait no da aislamiento.

El diagnóstico público tiene `code`, `message`, `phase`, `location` y `retryable` cuando el motor puede decidirlo. `location` contiene nodo y JSON Pointer del campo, más ubicación del dato cuando aplica. Códigos iniciales: `definition.invalid`, `reference.missing`, `mapping.missing`, `mapping.invalid`, `schema.invalid`, `data.invalid`, `capability.unsupported`, `admission.full`, `runtime.unavailable`, `operation.failed`, `effect.unknown`, `state.conflict`, `access.denied`. Un código de operación queda bajo su namespace y se adjunta como causa, sin reemplazar la clasificación del motor.

Ejemplo CT-02 — forma de delegación pública, tipos auxiliares omitidos:

```rust
let runtime = EngineRuntime::boot(assembly, boot_options).await?;
let app = runtime.application();
let plan = app.prepare(access.clone(), definition).await?;
let request = StartRunRequest { plan, input, options: start_options };
let accepted = app.start(access.clone(), request).await?;
let state = app.status(access, accepted.run_id).await?;
runtime.shutdown(shutdown_options).await?;
```

La API pública usa argumentos explícitos de acceso también en embedding; el host puede construir un ámbito confiable único. Métodos del handle reciben primero `AccessContext`. `prepare` retorna un plan inmutable o diagnósticos, `start` un `RunId` y la garantía de aceptación, `status` estado/revisión y `result` resultado o `not_ready`/`expired`. Si ya expiró también la metadata, la consulta devuelve `not_found`; no promete recordar IDs indefinidamente. `cancel` retorna acuse de solicitud, no éxito remoto. Un plan pertenece a su composición: usarlo en otra instancia exige preparar su definición allí; nunca transferir punteros como contrato de red.

`StartRunRequest` agrupa plan, input y opciones. Las opciones eligen garantía requerida, deadline y clave de recepción opcional; omitirlas aplica el perfil de la composición. Solicitar durabilidad a F-1 se rechaza antes de aceptar. El acuse informa perfil y, cuando hay clave, vencimiento de su deduplicación. Dentro de esa ventana el store retiene la identidad y huella del request aun si expiró su resultado; no reutiliza la clave por presión de memoria. Si no puede reservar esa retención, rechaza admisión. Pasada la ventana, un reenvío puede crear otro run. La huella se obtiene del contenido semántico completo (plan/input/opciones), excluyendo únicamente la propia clave; dos requests distintos nunca se equiparan solo porque usen el mismo payload.

## 7. Confianza y presupuesto desde F-1

Primera composición: plugins compilados con el host, código confiable y un ámbito de ejecución por instancia. Sin ABI dinámica, instalación en caliente ni aislamiento de tenants hostiles. El contrato no impide otros cargadores futuros, pero requerirán su frontera de proceso y pruebas. Una extensión en proceso puede acceder al SO; los permisos de contexto organizan el acceso cooperativo, no constituyen un sandbox.

El host autoriza preparar, iniciar, consultar, cancelar, señalar y reconciliar. El engine conserva el ámbito en el run y comprueba que cada comando pertenezca a él. Las credenciales se resuelven por referencias autorizadas; no por expresiones de autoría. Los módulos oficiales de red/archivos usan perfiles configurados por el host: destinos y rutas permitidos, límites y credenciales. Un workflow no puede habilitar un destino nuevo; redirecciones y rutas resueltas se vuelven a comprobar.

Valores iniciales de protección, configurables y sujetos a medición; no son promesas de rendimiento:

| Recurso | Default inicial | Al exceder |
|---|---|---|
| Documento o paquete de schemas | 1 MiB cada uno; 64 recursos por paquete | Rechazo al registrar/preparar. |
| Nodos por definición | 256 | Rechazo al preparar. |
| Profundidad JSON / binding | 64 / 32 | Error localizado, sin evaluación parcial con efectos. |
| Pasos de evaluación de binding | 10 000 visitas a constructores/tokens por evaluación | `mapping.invalid` con motivo de límite. |
| Input/output JSON por frontera | 1 MiB | Rechazo antes del siguiente efecto; referencia de artefacto para mayor volumen. |
| Datos JSON retenidos por run | 16 MiB | Fallar el paso puro/lectura; si hubo efecto, conservar diagnóstico e identidad y bloquear su continuación cuando falte resultado confirmable. |
| Runs activos / aceptados pendientes | 32 / 128 por instancia | `admission.full` antes del acuse. |
| Intentos concurrentes | 32 por instancia; 1 por run en F-1 | Esperar capacidad sin perder aceptación. |
| Deadline de intento / run activo | 30 s / 5 min | Cancelación cooperativa y clasificación de resultado. |
| Resultados terminales en memoria | 1 000 o 1 h, lo que se alcance primero | Expirar resultados; no expulsar runs activos. |
| Recibos de deduplicación en memoria | Ventana 1 h; 10 000 recibos por instancia | Reservar al admitir; al llenarse rechazar nuevas recepciones con clave, sin borrar promesas vigentes. |

El presupuesto de datos retenidos cuenta también el input. Los validadores deben tener un presupuesto efectivo; si la biblioteca elegida no permite limitar una evaluación, restringir las construcciones problemáticas o aislar el trabajo antes de publicar el soporte. Un timeout async no interrumpe por sí solo código CPU bloqueante. Operaciones de CPU usan un executor acotado; no saturan el coordinador ni crean hilos sin límite.

F-2 concreta los límites de ramas/iteraciones/profundidad; F-3 agrega cuotas persistentes, esperas, evidencias y retención. Un límite global del host siempre prevalece sobre una petición del workflow. Todo rechazo de un resultado tras despacho pasa por la misma clasificación de efectos del TDD.

## 8. Qué se decide después sin invalidar F-1

El checkpoint y SQL se concretan antes de F-3; rutas y DTOs HTTP antes de F-4; cifras objetivo de producción con las cargas reales antes de F-5. La referencia durable será SQLite local para un coordinador, reemplazable por un proveedor conforme; embedding simple seguirá usando memoria por defecto. La elección aprovecha las [transacciones de SQLite](https://www.sqlite.org/transactional.html), pero su conformidad requiere las pruebas de caída del TDD: el nombre de una base de datos no demuestra por sí solo las garantías del adaptador.

El servicio inicial usará HTTP/JSON, acceso autenticado provisto por el host y aceptación consultable por `RunId`; desconectarse no cancela el run. El editor completo permanece como entrega separada. Estas decisiones se registran con sus límites y pendientes en P-01 a P-09; no existen defaults ocultos adicionales en los ejemplos.
