# Workflow Forge — contratos de la primera entrega

Contratos de F-1 y su ampliación F-2, 2026-09-26. Este anexo del [TDD](TDD.md) establece el comportamiento y las formas de datos, dentro del alcance del [PRD](PRD.md). La sección 2 conserva el perfil inicial; la sección 9 define el control y los efectos añadidos. La implementación usa temporalmente `workflow_forge::v2`; [PROJECT](PROJECT.md) acredita lo verificado. Los fragmentos de este documento son parciales; [v2_customer.rs](../crates/forge/examples/v2_customer.rs) contiene un host ejecutable. Los casos de [ACCEPTANCE](ACCEPTANCE.md) permiten comprobarlo sin conocer los internos del motor.

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

La secuencia es una cadena no vacía: `entry` sin predecesor, un sucesor por nodo salvo el final, un predecesor por nodo salvo entrada; todos alcanzables y sin ciclos. Cada arista tiene `from` y `to`; duplicados se rechazan. El orden del array y las coordenadas visuales no ordenan ejecución. Campos desconocidos se rechazan, excepto dentro de `presentation`; capacidades futuras no se ignoran. El [schema del documento](../schemas/2/workflow.schema.json) permite autoría estructural independiente; las relaciones, permisos y revisiones se verifican además mediante `prepare`.

Las revisiones son strings opacos no vacíos asignados por quien publica. El registro conserva el contenido semántico normalizado de cada revisión y rechaza reutilizarla con otro contenido. El normalizador ordena `nodes` por ID y `edges` por sus extremos después de rechazar duplicados y excluye únicamente `presentation`; preserva arrays de datos, tipos y valores sin coerción. No depende de un hash sin especificar. Una revisión de implementación debe cambiar al cambiar el código o comportamiento de la operación. El catálogo y los perfiles fijados por el plan también conservan revisiones exactas.

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

Los recursos `$ref` se resuelven exclusivamente en el paquete de schemas registrado. Cada recurso tiene URI absoluta y revisión fijada; URIs desconocidas, dialectos/vocabularios requeridos no soportados y conflictos de contenido impiden preparar. La URI identifica un recurso y no autoriza descargarlo. Una referencia recursiva de schema no es por sí sola un error: el perfil F-1 la rechaza explícitamente como capacidad no soportada.

El perfil inicial admite referencias locales por JSON Pointer y absolutas a recursos registrados. Rechaza `$dynamicRef`, anchors nombrados y `$id` anidados; el `$id` raíz de un recurso, si existe, debe coincidir con su URI registrada. La expansión está acotada a 10 000 visitas y profundidad 64. La evaluación comprueba un presupuesto conservador de 1 000 000 unidades sobre tamaño del dato y coste del schema; usa el motor regex lineal de la biblioteca con límite de compilación de 1 MiB. Estas restricciones forman parte del perfil publicado; no se anuncia soporte sin restricciones de todas las construcciones 2020-12.

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
| Revisiones de definición preparadas | 1 000 por instancia en F-1 | `admission.full`; se conservan para impedir reutilizar una revisión con otro contenido. |
| Artefactos del proveedor en memoria | 64 MiB de contenido total; 1 024 referencias; scope/media type de hasta 256 bytes | `resource.limit`; fragmentos de lectura de hasta 64 KiB. Se liberan al destruir el proveedor; retención durable en F-3. |

El store conserva metadatos inmutables, entrada y outputs confirmados, exige CAS y no permite sobrescribir resultados terminales. El kit público [workflow-forge-conformance](../crates/conformance/src/lib.rs) comprueba propiedad, deduplicación, CAS y retención sobre un proveedor aislado. Es una comprobación secuencial; las pruebas de concurrencia, pérdida de acuse y caídas complementan el kit, y las garantías durables se agregan en F-3.

El presupuesto de datos retenidos cuenta también el input. Los validadores deben tener un presupuesto efectivo; si la biblioteca elegida no permite limitar una evaluación, restringir las construcciones problemáticas o aislar el trabajo antes de publicar el soporte. Un timeout async no interrumpe por sí solo código CPU bloqueante. Operaciones de CPU usan un executor acotado; no saturan el coordinador ni crean hilos sin límite.

F-2 concreta los límites de ramas/iteraciones/profundidad; F-3 agrega cuotas persistentes, esperas, evidencias y retención. Un límite global del host siempre prevalece sobre una petición del workflow. Todo rechazo de un resultado tras despacho pasa por la misma clasificación de efectos del TDD.

## 8. Qué se decide después sin invalidar F-1

El checkpoint y SQL se concretan antes de F-3; rutas y DTOs HTTP antes de F-4; cifras objetivo de producción con las cargas reales antes de F-5. La referencia durable será SQLite local para un coordinador, reemplazable por un proveedor conforme; embedding simple seguirá usando memoria por defecto. La elección aprovecha las [transacciones de SQLite](https://www.sqlite.org/transactional.html), pero su conformidad requiere las pruebas de caída del TDD: el nombre de una base de datos no demuestra por sí solo las garantías del adaptador.

El servicio inicial usará HTTP/JSON, acceso autenticado provisto por el host y aceptación consultable por `RunId`; desconectarse no cancela el run. El editor completo permanece como entrega separada. Estas decisiones se registran con sus límites y pendientes en P-01 a P-09; no existen defaults ocultos adicionales en los ejemplos.

## 9. Contrato de ampliación F-2 — control estructurado y efectos

Esta sección concreta P-09 y las cuotas avanzadas antes de implementarlas. El cierre F-1 conserva su perfil secuencial; la presencia de esta especificación no habilita capacidades en runtime. PROJECT registra la transición a F-2.

### 9.1 Instrucciones y ámbitos

Cada cuerpo conserva `entry`, `nodes`, `edges` y `output`. Sus aristas forman una cadena; la concurrencia y los caminos alternativos se expresan mediante instrucciones con cuerpos anidados. Así un join siempre conoce las ramas que activó su fork. Se rechazan aristas entre cuerpos, ciclos arbitrarios y joins implícitos por contar predecesores. El editor puede representar cada cuerpo como un subgrafo, sin convertirlo en otra operación de negocio.

Los nodos comparten `id` e `input`. `kind` selecciona una variante cerrada; campos de otra variante se rechazan. Las operaciones siguen usando `operation`, `config` y schemas de su descriptor. Las instrucciones del lenguaje pertenecen al engine: una extensión agrega operaciones, no variantes al scheduler.

| `kind` | Campos específicos | Semántica y resultado |
|---|---|---|
| `operation` | `operation`, `config`, `retry` opcional | Invocación bajo revisión exacta; publica el JSON de la operación. |
| `decision` | `cases` ordenados con `id`, `when`, `body`; `fallback` opcional | Evalúa booleanos sobre el input del nodo; primera condición verdadera. Ejecuta solo ese cuerpo y devuelve `{"selected":"case-id","output":...}`. Ninguna coincidencia sin fallback produce error. |
| `parallel` | `branches` por ID, `concurrency`, `errors`, `join: "all"` | Activa todas sus ramas con el input del nodo; espera su conclusión y devuelve resultados por ID. Una rama no se confunde con otra por terminar antes. |
| `foreach` | `items`, `body`, `concurrency`, `errors` | `items` es un binding sobre el input del nodo y debe producir array. El cuerpo recibe `{"item":valor,"index":índice,"context":input_del_nodo}`. Devuelve resultados en orden de índice, independientemente del orden de efectos. |
| `loop` | `while`, `body`, `max_iterations`, `on_limit` | El input del nodo es el estado inicial. Condición y cuerpo reciben `{"state":estado,"iteration":índice}`. El output del cuerpo reemplaza el estado; condición falsa devuelve el estado actual. |
| `subworkflow` | `workflow: {id, revision}` | Resuelve una definición registrada antes de `build`; valida input/output del hijo y devuelve su output. Mantiene ámbito e identidad propios dentro del checkpoint del run raíz. |

`body` es un cuerpo inline, no un documento con revisión independiente. Una definición reutilizable se registra con `register_workflow(definition)` y se referencia por revisión exacta. `BootOptions.definitions` sigue siendo la lista de definiciones obligatorias que deben preparar antes de readiness; no permite cambiar catálogos después de construir la composición. Las dependencias de subworkflows se resuelven recursivamente; ciclos y revisiones ausentes se rechazan al preparar. Todo lo resuelto queda fijado en el plan.

Dentro de un cuerpo, `select.source: input` corresponde al input de ese ámbito y `source: node` solo permite predecesores locales confirmados. No existe acceso implícito a outputs de otra rama o a variables del padre. El host del cuerpo pasa esos datos explícitamente en `input`/`context`. La salida del nodo de control permite leer los resultados reunidos desde su sucesor. El `fallback` de selección mantiene las reglas de ausencia/null de F-1.

Un resultado de grupo usa `{"status":"succeeded","output":...}` o `{"status":"failed","error":diagnóstico}`. Foreach agrega `index` a cada elemento; paralelo usa la clave de rama. `errors` admite `fail_fast` y `collect`: ante un fallo conocido, el primero deja de admitir hijos y cancela cooperativamente los activos; el segundo reúne errores conocidos. Ambos esperan clasificar el trabajo activo. Ante incertidumbre, `fail_fast` pausa nuevas admisiones y deja concluir los hijos activos bajo sus deadlines; `collect` puede continuar hijos independientes. Un efecto incierto bloquea el grupo y el run; no se convierte en un error recolectable ni habilita éxito parcial. Tras resolverlo, se reutilizan los resultados confirmados. Un fallo conocido previo conserva su decisión de detener el grupo, incluso si después fue necesario resolver otro efecto.

En loop, `on_limit` admite `fail` y `return_last`. Se comprueba `while` antes de cada vuelta; si sigue verdadera al alcanzar `max_iterations`, se aplica esa política. La condición exige un booleano real, sin coerción ni expresiones ejecutables. Una operación puede calcular la siguiente condición dentro del estado.

### 9.2 Identidad, planificación y presupuesto

La identidad lógica incluye run raíz, camino de ámbitos, ID local e índice/iteración cuando corresponde. En `RunSnapshot.invocations` se usa una ruta de segmentos etiquetados: `/nodes/import/items/3/nodes/write`, o `/nodes/group/branches/left/nodes/call/workflows/lookup/revisions/r1/nodes/read`. Dentro de un segmento se escapan `~` como `~0` y `/` como `~1`; un ID que contiene separadores no puede suplantar otro ámbito. Estas rutas sustituyen las claves simples de F-1 durante el desarrollo previo a publicación. El intento añade una identidad nueva; nunca reemplaza la identidad lógica ni su clave de efecto. Dos elementos iguales de un array siguen teniendo invocaciones diferentes.

Un hijo de control se representa en el checkpoint del run raíz; no consume otro permiso de run activo mientras su padre retiene el suyo. Solo las llamadas a operaciones consumen permisos de intento. Los registros de control conservan ramas activadas, cursor/estado de iteración y resultados confirmados; no contienen futures. Antes de despachar, una decisión pura produce el trabajo listo y la transición que el coordinador debe confirmar. El ejecutor de un intento devuelve un resultado identificado; no elige sucesores.

Defaults adicionales: 32 ramas por paralelo, concurrencia de grupo hasta 32 y nunca mayor al límite del host; 10 000 elementos por foreach; 1 024 vueltas por loop; profundidad de cuerpos/subworkflows 8; hasta 10 000 activaciones totales por run, contando control y operaciones. El plan tiene un presupuesto de 16 MiB (`plan_bytes`) sobre tamaño serializado de definiciones expandidas y nodos preparados; incluye schemas y referencias repetidas. Es una cota de preparación, no una medición de RSS. El límite de 256 nodos del documento incluye sus cuerpos inline.

La instancia admite hasta 128 cuerpos hijos de grupos activos (`active_scopes`), compartidos entre runs y grupos anidados. El input de un hijo se construye al admitirlo; no se clona el contexto para todos los elementos por adelantado. Agotar esta cuota produce `resource.limit`, detiene nuevas admisiones y cancela/clasifica el trabajo activo. No se espera un permiso que pueda estar retenido por un padre, evitando interbloqueo por anidamiento. Si no se puede confirmar el progreso del grupo, falla la supervisión y la recuperación clasifica las intenciones pendientes antes de otro despacho. La activación se reserva antes del efecto. El host ajusta las cuotas según su carga; aumentar el máximo de filas también puede exigir aumentar activaciones y retención.

La política `retry` tiene `max_attempts` (incluye el primero, default 1, máximo del host 5), `initial_delay_ms` (default 100), `max_delay_ms` (default 30 000) y `jitter` (default true). Los deadlines siguen prevaleciendo. El backoff exponencial saturado y jitter solo eligen demora después de autorizar la repetición; una Strategy de demora no decide si es seguro repetir.

### 9.3 Clasificar antes de reintentar

| Resultado de intento | Acción |
|---|---|
| Output válido | Confirmar una sola vez y habilitar continuación. |
| Fallo de mapping/input antes del despacho | Fallo conocido; no se produjo efecto ni se ejecuta retry automático de validación. |
| Error transitorio/de recurso con `not_applied` definitivo | Retry si quedan intentos y deadline, incluso cuando el destino no es idempotente. |
| Error de negocio/input/interno con `not_applied` | Fallar; no aplicar backoff a rechazos permanentes. |
| Escritura `unknown` y repetición `safe/keyed` | Retry solo para errores elegibles, conservando la clave de efecto; sin presupuesto suficiente queda bloqueada. |
| Escritura `unknown` y repetición `unsafe` | Bloquear sin repetir. |
| Efecto `applied` sin output válido, incluido error de schema tras respuesta | Bloquear para obtener resultado conforme; no hacer retry automático por invalidar el output. |
| Cancelación/vencimiento/panic después de despachar escritura | Clasificar como potencial efecto desconocido; cancelar no demuestra ausencia de efecto. |

Para operaciones puras/lecturas, la certeza no introduce un efecto de escritura. La cancelación conocida termina como `Cancelled`; el deadline sin petición de cancelación termina como `Failed`. Un run con escritura incierta permanece `Blocked` aunque se haya pedido cancelar, hasta resolverla o cerrar seguimiento explícitamente.

La certeza se conserva a través de todos los intentos de una invocación: un intento nuevo declarado `not_applied` no borra un efecto incierto anterior. Un efecto ya confirmado como `applied` tampoco se convierte en no aplicado por una resolución contradictoria.

Los intentos obsoletos no pueden publicar outputs. Un resultado que llegue después de timeout/resolución puede conservarse como evidencia identificada y acotada; no sobrescribe el resultado autorizado. El runtime mantiene una ventana acotada de recogida de respuestas tardías (1 s por intento, hasta el límite de intentos concurrentes); después libera el future local y conserva la incertidumbre remota. Esto no constituye una garantía de cancelación del destino.

### 9.4 Inspección y resolución

`EffectInspector` es un puerto separado, asociado a la revisión de operación y registrado como contribución del mismo módulo. Si el descriptor anuncia reconciliación, la composición exige ese proveedor. Su consulta recibe input/config, identidad lógica/intento/clave y recursos autorizados; carece de acceso al store y no crea un efecto de negocio. `inspect_effect` retorna evidencia; aplicar una decisión requiere `reconcile`.

`ReconcileCommand` fija `command_id`, `run_id`, `invocation_id`, `expected_revision`, `observed_attempt` y decisión. Las decisiones son `confirm_applied`, `confirm_not_applied`, `record_inconclusive` y `stop_tracking`, según TDD-06. La evidencia identifica autoridad, referencia y afirmación; no basta una string «no encontrado». No aplicación exige además garantía explícita de que el intento ya no puede completarse. El host y el adaptador de confianza responden por esa afirmación; el motor no puede verificar por sí mismo el estado de un destino remoto.

Confirmar aplicación valida el output contra la revisión fijada. Si es inválido se registra la investigación y se mantiene el bloqueo. Confirmar no aplicación solo habilita otro intento si se solicitó, queda presupuesto y el run no está cancelado. `stop_tracking` requiere permiso separado `StopTracking`, clasifica el trabajo activo y finaliza sin éxito con `unresolved_effects` visibles. Nunca borra la incertidumbre histórica.

El commit CAS incluye decisión, auditoría y acuse. Un comando duplicado idéntico del mismo actor autenticado devuelve el acuse previo; el mismo ID con otro contenido o una revisión obsoleta produce conflicto. La búsqueda del acuse precede a la comprobación de revisión para poder recuperar un acuse perdido. Dos resoluciones distintas no pueden ganar sobre el mismo estado.

Por run: hasta 256 registros de investigación/comandos y 64 KiB por evidencia/comando JSON, además del presupuesto total de datos. Evidencia mayor usa referencia autorizada a artefacto. Llenar la auditoría rechaza nuevas resoluciones antes de aplicarlas; no elimina acuses vigentes para hacer espacio. Los resultados terminales no se reabren; observaciones tardías pueden agregar auditoría acotada conservando resultado, estado y outputs confirmados.

### 9.5 Recorrido C-02 en memoria

La extensión de referencia separa cuatro operaciones: `read_page` (leer/parsear una página), `apply_row` (adaptar el destino inyectado), `report_batch` (escribir un reporte parcial inmutable) y `publish_report` (reunir los reportes confirmados). El engine no interpreta CSV, SKU ni reglas de inventario. El workflow usa un loop con cursor y un foreach `collect` de concurrencia 4 dentro de cada lote; el host puede reducirla.

El lector acepta un `ArtifactRef` autorizado, UTF-8, cabecera exacta `sku,quantity`, hasta 4 MiB y 10 000 registros. Reabre el artefacto inmutable por página porque el puerto inicial no tiene seek; recorre el archivo con memoria de una página de hasta 100 filas y un buffer de entrada acotado. Cabecera, codificación, exceso de registros y errores estructurales se rechazan antes de llamar al destino. Cada fila conserva índice lógico y línea CSV; cantidad se convierte explícitamente a entero no negativo. SKU vacío/largo, número inválido o columnas incorrectas producen un error por fila; el schema de `apply_row` impide despacharla.

El cursor del loop conserva fuente, próxima fila, totales acumulados, indicador `more` y referencia al último reporte de lote. Los reportes de lote enlazan el anterior y siempre declaran `partial: true`; no se conserva un array creciente en el estado del loop. La publicación final recorre esas referencias y transmite JSON Lines en orden de origen, con un resumen `rows/succeeded/failed` y la referencia final. Un archivo vacío con cabecera válida produce cero filas; un archivo sin cabecera falla. El límite del fixture de 10 000 filas usa 12 000 activaciones del host, incluyendo controles y reporte, manteniendo las demás cuotas publicadas.

Si una fila deja un efecto incierto, el foreach y el run bloquean antes de confirmar ese lote o publicar un reporte final. El reporte del último lote confirmado sigue siendo parcial; los `Unknown` del snapshot identifican las filas pendientes de resolución. No se cuentan como fallos conocidos. El destino simulado deduplica por effect key y permite inspección autoritativa; sustituirlo requiere conservar esas garantías o declarar otras en su contrato.

Las operaciones de reporte declaran escritura repetible: crean artefactos inmutables, sin repetir ajustes de inventario. Una referencia no confirmada puede dejar un artefacto huérfano, sujeto al presupuesto del proveedor. F-3 implementa retención y limpieza de ese estado; F-2 no promete recuperación de artefactos tras caída del proceso. Error o respuesta perdida durante una escritura de artefacto conserva incertidumbre, sin atribuir ausencia de efecto a un error del proveedor.

### 9.6 Acceso al estado durante ejecución

La lectura de un snapshot completo sirve para diagnóstico, recuperación y resultado final. El camino frecuente usa una vista coherente de cabecera + invocación local + indicación de descendientes inciertos. Esa vista corresponde a una sola revisión; contiene conteos de activaciones y bytes retenidos para comprobar cuotas sin serializar todos los registros otra vez.

`ExecutionStore::view` consulta esa proyección; `unfinished_heads` enumera cabeceras pendientes; `commit_invocation` reemplaza un registro bajo propietario y revisión CAS. El reemplazo incrementa la revisión del run, conserva resultados confirmados y rechaza estados terminales. La creación de intención, resultado, cursor o fallo sigue siendo una transición observable; la proyección no omite confirmaciones ni autoriza despachar antes del commit. La transición de cabecera/auditoría y la recuperación completa conservan el puerto de snapshot existente.

Los nuevos métodos tienen una implementación predeterminada mediante `get`/`commit`/`unfinished`, para que un proveedor correcto pueda adoptarlos por etapas. Un backend puede optimizarlos manteniendo sus contadores e índices en la misma sección crítica/transacción que el registro. Conformidad compara proyección, snapshot y reemplazo, incluyendo CAS obsoleto y resultado confirmado inmutable. Las pruebas concurrentes enfrentan confirmación de nodo, cancelación y confirmación final; las cuotas se comprueban contra la misma revisión que el commit. [PROJECT](PROJECT.md) registra la mejora medida en memoria y los costes restantes; otros proveedores deben medir su implementación.
