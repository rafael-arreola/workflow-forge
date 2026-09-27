# Workflow Forge — requisitos de producto

Fecha: 2026-09-26. Especificación inicial de la refactorización; describe el producto objetivo, no capacidades ya entregadas. [Convenciones y autoridad](README.md). [Arquitectura](ARCHITECTURE.md). [Diseño técnico](TDD.md).

## 1. Problema y objetivo confirmado

Muchas integraciones repiten el mismo recorrido: recibir datos, interpretarlos, transformarlos, invocar operaciones en un orden y devolver un resultado. Programar cada recorrido nuevamente duplica coordinación y tratamiento de errores.

Workflow Forge permitirá implementar capacidades reutilizables una vez y componerlas mediante un documento de workflow que también pueda construirse gráficamente. El motor será agnóstico al dominio y estará escrito en Rust. Se utilizará como librería embebida y como servicio para consumidores de otros lenguajes.

El usuario confirmó tres familias: protocolos públicos, implementaciones predeterminadas y extensiones que cumplen esos protocolos. La experiencia inicial será integrada, con puntos explícitos de sustitución. La persistencia y recuperación podrán ser elegidas por el implementador mediante mecanismos previstos por el motor.

Se autoriza rediseñar API, formato y estructura: no hay una versión pública cuya compatibilidad deba conservarse. La referencia a n8n orienta la composición y ejecución; no exige importar sus workflows ni reproducir su plataforma.

## 2. Actores y conceptos

| Concepto | Significado |
|---|---|
| Integrador | Construye flujos y configura capacidades disponibles. |
| Implementador del motor | Embebe o despliega Workflow Forge y selecciona módulos, recursos y políticas. |
| Autor de extensión | Implementa una capacidad mediante los contratos públicos. |
| Consumidor | Invoca la librería/API, consulta resultados o construye una interfaz visual. |
| Operación | Capacidad ejecutable identificada y versionada, con contratos de datos. Equivale conceptualmente a la tarea actual. |
| Módulo | Paquete de implementaciones de uno o varios contratos. |
| Extensión | Módulo aportado por un tercero o por la aplicación anfitriona; usa la misma frontera pública que uno oficial. |
| Definición | Documento que describe nodos, relaciones, mappings y políticas. |
| Run | Una ejecución concreta de una revisión de la definición. |
| Invocación | Aplicación lógica de una operación dentro de un run, iteración o subworkflow. |
| Intento | Una ejecución técnica de la misma invocación; un reintento conserva su identidad lógica. |

Un protocolo es un contrato de interacción, no necesariamente un protocolo de red. Una definición válida tampoco prueba que un sistema externo esté disponible ni que todas las conexiones de datos sean compatibles estáticamente.

## 3. Alcance y estados de decisión

**Confirmado:** motor Rust agnóstico, librería y servicio, composición por contratos, módulos predeterminados sustituibles, extensiones, contratos utilizables desde una UI y mecanismos de recuperación opcionales para el anfitrión.

**Entrega base propuesta:** catálogo versionado, validación/compilación, ejecución con control de flujo, composición oficial, modos efímero y durable con implementaciones de referencia, señales/esperas, diagnóstico y kit de conformidad de extensiones.

**Decisiones de diseño adoptadas para avanzar:** extensiones Rust compiladas y confiables; mappings explícitos y JSON Schema 2020-12; memoria por defecto en embedding; SQLite local como referencia durable de un coordinador; HTTP/JSON para el primer servicio, durable por defecto y sin cancelación por desconexión. El editor completo será una entrega separada. Son elecciones técnicas de este plan, no capacidades implementadas ni requisitos originalmente expresados por el usuario. Sus límites y detalles pendientes están en la sección 7.

**Primera entrega usable, F-1:** librería en memoria que carga operaciones, valida/prepara una secuencia, recibe JSON, transforma/consulta y retorna un resultado consultable. Incluye diagnóstico, cancelación, arranque/apagado, presupuestos de recursos y extensión externa de ejemplo. Su contrato está en [CONTRACTS](CONTRACTS.md). Escrituras y coordinación avanzada llegan en F-2; recuperación/esperas en F-3; servicio en F-4. Esta división conserva la entrega base completa como objetivo.

**Futuro salvo decisión explícita:** ejecución distribuida entre hosts, WASM, marketplace, compatibilidad n8n/BPMN/Open Workflow, transacciones distribuidas y compensaciones automáticas. Una extensión puede integrar IA sin convertir el motor en un producto de agentes.

## 4. Recorridos de aceptación

Estos escenarios se concretan en [ACCEPTANCE](ACCEPTANCE.md), con datos, efectos y variantes de fallo. Son referencias para diseñar y probar; P-01 conserva el contraste pendiente con integraciones reales del usuario.

| Caso | Recorrido | Resultado observable |
|---|---|---|
| UC-01 Integración de petición | Entrada JSON → validar → normalizar → invocar sistema externo → mapear respuesta. | Respuesta conforme al contrato o error localizado; rechazo de datos inválidos antes del efecto correspondiente. |
| UC-02 Integración por archivo | Referencia de archivo → parsear → iterar con concurrencia limitada → entregar reporte. | Orden/identidad de resultados definidos y reporte de errores según política; blobs no copiados a JSON como base64. |
| UC-03 Espera recuperable | Iniciar trabajo → esperar señal → reiniciar proceso → recibir señal → continuar. | Se conserva avance confirmado y la señal duplicada no provoca una segunda continuación. |
| UC-04 Nueva capacidad | Declarar descriptor → implementar contrato → registrar módulo → descubrir y ejecutar. | Integración sin modificar internos del engine ni crear una excepción para módulos oficiales. |

## 5. Requisitos y aceptación

Los requisitos siguientes formalizan el objetivo confirmado y el diseño propuesto. Son obligaciones del producto objetivo cuando se adopte esta base; no son afirmaciones de implementación. Los detalles pendientes siguen sujetos a P-*.

| ID | Requisito | Criterio de aceptación |
|---|---|---|
| PRD-DEF-001 | Definir y conservar workflows declarativos con identidad/revisión, nodos, relaciones, mappings y políticas. | Serializar y volver a cargar preserva comportamiento; cambiar presentación no cambia el plan ejecutable. |
| PRD-CAT-001 | Descubrir operaciones mediante descriptores versionados y contratos JSON Schema. | Un consumidor descubre configuración, entrada/salida y requisitos sin inspeccionar código. Contratos abiertos se declaran explícitamente. |
| PRD-VAL-001 | Detectar errores de documento, estructura, referencias, capacidades y datos en su frontera correspondiente. | Diagnóstico con código y ubicación; validar/preparar no ejecuta operaciones de negocio. La compatibilidad desconocida no se comunica como demostrada. |
| PRD-EXT-001 | Usar los mismos protocolos para módulos oficiales, del anfitrión y de terceros. | Una extensión pasa conformidad y se ejecuta sin importar módulos internos del engine. |
| PRD-COMP-001 | Ofrecer composición integrada con sustituciones explícitas. | La configuración predeterminada válida funciona; el integrador puede reemplazar un proveedor de recursos sin reescribir el coordinador. |
| PRD-EXEC-001 | Coordinar secuencias, decisiones, paralelismo, joins, iteraciones y subworkflows con semántica definida. | Resultados independientes del orden accidental de finalización; ramas omitidas, fallos y límites producen estados definidos. |
| PRD-EFFECT-001 | Distinguir intento, invocación, errores reintentables y resultados inciertos de efectos. | Un timeout después de un efecto no conduce a una repetición automática insegura. No se promete exactamente una vez sin cooperación del destino. |
| PRD-DUR-001 | Permitir recuperación mediante un backend conforme y declarar las garantías del modo seleccionado. | Reiniciar conserva avance confirmado; un backend efímero rechaza una solicitud que exige recuperación entre procesos. |
| PRD-WAIT-001 | Permitir esperas, timers y señales correlacionadas sin mantener viva una llamada de operación durante toda la espera. | Se recupera una espera durable y cada señal aceptada se consume según su política de deduplicación. |
| PRD-API-001 | Exponer librería y servicio sobre el mismo motor. | Una misma definición/input/configuración produce resultados y errores semánticamente equivalentes en ambas superficies. Aceptación y finalización se distinguen. |
| PRD-VIS-001 | Proveer contratos para autoría gráfica independiente del núcleo. | Un consumidor descubre operaciones, conecta datos, valida y localiza errores usando superficies públicas; el engine funciona sin UI. |
| PRD-RES-001 | Manejar secretos y artefactos mediante proveedores sustituibles. | Exportar una definición no exporta credenciales; los artefactos requeridos por una reanudación no se eliminan prematuramente. |
| PRD-OBS-001 | Ofrecer estado, errores y correlación de runs, invocaciones e intentos. | Consultar una ejecución permite distinguir éxito, fallo, espera, cancelación y resultado incierto; no se registran secretos por defecto. |
| PRD-OPS-001 | Acotar recursos y demostrar rendimiento con cargas representativas. | Saturación no descarta runs confirmados; límites se comprueban; mediciones separan overhead del motor de latencia externa. |
| PRD-EVOL-001 | Fijar revisiones por run y mantener trazabilidad de decisiones y pruebas. | Cambiar catálogo/definición no cambia un run ya preparado; una revisión no disponible bloquea recuperación con diagnóstico explícito. |

## 6. Límites de garantía y cierre de producto

El motor coordina operaciones; un error o cancelación local no revierte efectos externos. Compensar es una operación de negocio adicional y no un rollback implícito. El esquema de datos no acredita permisos, idempotencia ni aislamiento de código.

Rust es una elección de implementación, no evidencia de rendimiento. Los presupuestos iniciales y mediciones tempranas están definidos en CONTRACTS/ACCEPTANCE; las metas de producción se fijarán con P-07. La primera composición admite código confiable en proceso, sin aislamiento para código no confiable; ampliar ese alcance reabre P-08.

El cierre de la refactorización exige los recorridos acordados, conformidad de módulos y backends, paridad de superficies y evidencia de recuperación. Las verificaciones se definen en [TDD](TDD.md#verificacion) y su orden en [ROADMAP](ROADMAP.md).

## 7. Registro canónico de decisiones pendientes

La tabla conserva los IDs históricos de pendientes y registra ahora también sus decisiones. **Adoptada** significa elección de diseño para esta refactorización; **parcial** conserva un cierre obligatorio antes de la fase indicada. Los hechos de negocio desconocidos no se inventan para cerrar una fila.

| ID | Estado / decisión | Diseño adoptado y detalle pendiente | Condición de cierre |
|---|---|---|---|
| P-01 | Parcial: casos y destinos. | C-01/C-02 definen petición y archivo con fixtures y efectos; falta contrastarlos con dos integraciones reales. | Referencias permiten F-1/F-2; aceptación de producto y cargas finales requieren casos reales en F-5. |
| P-02 | Adoptada: editor separado. | Contratos de autoría desde F-1; consumidor de conformidad en F-5. | Una UI completa necesita alcance propio; no bloquea el motor. |
| P-03 | Adoptada: plugins compilados. | Crates Rust registrados por el host, protocolo público y kit de conformidad; sin carga dinámica inicial. | V-04/V-17 en F-1; otro mecanismo requiere revisar aislamiento/versionado. |
| P-04 | Adoptada y verificada: perfiles y persistencia. | Memoria por defecto para embedding; SQLite local para referencia durable y servicio. Un coordinador propietario, sin filesystem compartido entre hosts. CONTRACTS §10–11 concreta paquete, codec, migraciones, artefactos y esperas; PROJECT registra caída, carreras y reconciliación durable. | V-08/V-18 pasan; el cierre de las mediciones F-3 sigue en P-07. |
| P-05 | Parcial: servicio. | HTTP/JSON; host autentica; scopes explícitos; aceptación por RunId; desconexión no cancela. Faltan rutas, DTOs, mecanismo concreto de autenticación y paginación. | Publicar contrato de transporte y probar V-10 antes de cerrar F-4. |
| P-06 | Adoptada para F-1: formato/datos. | `forge.workflow/2`, JSON Schema 2020-12, refs de paquete y bindings literal/select/object/array según CONTRACTS. | Implementar parser/schema y V-01/V-03/V-19; nuevas instrucciones se concretan por fase. |
| P-07 | Parcial: capacidad. | Presupuestos F-1/F-2, cuotas de SQLite/artefactos y reservas/señales en CONTRACTS. PROJECT registra mediciones F-1/F-2/F-3, ambos perfiles y análisis de regresiones. Faltan la campaña completa V-14 y objetivos del despliegue real. | Mediciones del perfil durable F-3 completadas; fijar y verificar metas de producción en F-5. |
| P-08 | Adoptada para primera topología: confianza. | Código confiable compilado, un ámbito por instancia; permisos del host y acceso acotado a recursos desde F-1. Sin multitenencia hostil ni sandbox. | V-19 desde F-1; autenticación del transporte en F-4 bajo P-05. |
| P-09 | Adoptada y verificada: control avanzado. | Fork/join estructurado sobre ramas activadas; collect/abort explícitos; señales sin reserva rechazadas, pre-registro antes de iniciar trabajo; resolución según TDD-06. | Matriz F-2 y persistencia de carreras F-3 probadas; otra instrucción debe preservar identidad, efectos y recuperación. |

Cada fase puede avanzar bajo las decisiones adoptadas y debe concretar los detalles de su alcance antes de programarlos. Nuevos datos pueden revisar una elección indicando motivo, consecuencias y contratos afectados. No se duplican registros activos en otros documentos.
