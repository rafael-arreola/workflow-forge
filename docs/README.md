# Workflow Forge — mapa documental

La documentación define la refactorización de Workflow Forge como motor agnóstico de integración en Rust. La planificación sigue la separación de producto, arquitectura y diseño técnico utilizada en `memory-forge`, adaptada a este proyecto.

**Estado al 2026-09-27:** F-0 a F-5 completadas. El motor en memoria, controles, efectos, persistencia SQLite, recuperación, esperas, servicio HTTP, módulos, CLI y consumidor de autoría tienen evidencia en PROJECT. El cierre usa pruebas esenciales y casos borde; las integraciones reales y metas del despliegue quedan para una etapa posterior, cuando el usuario decida integrar sus sistemas.

## Recorrido de lectura

| Documento | Pregunta que responde | Autoridad |
|---|---|---|
| [PRD](PRD.md) | ¿Qué construiremos, para quién y cómo sabremos que cumple? | Producto, alcance, requisitos y decisiones pendientes. |
| [ARCHITECTURE](ARCHITECTURE.md) | ¿Cómo se divide y comunica el sistema? | Límites, secuencias, propiedad, composición, patrones y fragmentos orientativos. |
| [PATTERNS](PATTERNS.md) | ¿Cómo extender el motor sin romper sus límites? | Desarrollo de los patrones de ARCHITECTURE: participantes, ejemplos y reglas de evolución. |
| [TDD](TDD.md) | ¿Cómo funcionan los contratos y mecanismos? | Diseño técnico, estados, persistencia y verificación. |
| [ROADMAP](ROADMAP.md) | ¿En qué orden lo implementaremos? | Fases, dependencias y criterios de salida. |
| [PROJECT](PROJECT.md) | ¿Qué existe y qué está verificado hoy? | Evidencia, brechas y punto de reanudación. |

Los anexos permiten avanzar sin leer todo el diseño a la vez:

| Anexo | Cuándo leerlo | Autoridad |
|---|---|---|
| [CONTRACTS](CONTRACTS.md) | Al implementar o integrar el motor. | Anexo normativo de TDD: formato, mappings, tipos, errores, confianza, persistencia, esperas y límites F-1/F-2/F-3. |
| [ACCEPTANCE](ACCEPTANCE.md) | Al comprobar generalidad y resultados. | Casos de referencia del PRD, fixtures y protocolo de mediciones; no resultados ejecutados. |
| [HTTP](HTTP.md) | Al implementar o integrar el servicio. | Anexo normativo de TDD-10/12: autenticación, rutas/DTOs, paginación, cuotas y lifecycle F-4. |
| [CLI](CLI.md) | Al usar el servicio desde una terminal o script. | Contrato del cliente HTTP: comandos, archivos/streams, límites y salida. |
| [INTEGRATIONS](INTEGRATIONS.md) | Al configurar o extender conectores oficiales. | Anexo normativo de TDD-02/10: perfiles, revisiones y límites de HTTP/JSON y archivos/CSV F-4. |
| [ADOPTION](ADOPTION.md) | Al integrar el motor o desarrollar un plugin/editor. | Recetas del producto, consumidor de autoría, versionado/retiro y migración del prototipo. |

Para empezar: leer el alcance F-1 en CONTRACTS y recorrer C-01A. Para crear un plugin: las tres fronteras de CONTRACTS y la selección rápida de PATTERNS. Para implementar el runtime: TDD y secuencias de ARCHITECTURE. Para conocer avances: PROJECT.

TDD significa *Technical Design Document*. No debe confundirse con la metodología de desarrollo guiado por pruebas.

Para ubicar un cambio, comenzar por la [matriz de comunicación](ARCHITECTURE.md#comunicacion), seguir los [recorridos de ejecución](ARCHITECTURE.md#recorridos) y aplicar la [receta de extensión](ARCHITECTURE.md#recetas) correspondiente. Los fragmentos E-* ilustran conexiones; no constituyen una implementación completa ni cierran decisiones pendientes.

Para integrar el motor al arrancar una aplicación, consultar [creación de instancia, bootstrap y apagado](ARCHITECTURE.md#instancia-y-arranque): quién construye el runtime, cuándo está listo y cómo compartirlo con los handlers.

## Convenciones y resolución de diferencias

- **Confirmado:** decisión expresada por el usuario; no equivale a implementado.
- **Diseño propuesto:** solución concreta para revisar e implementar después; no es una decisión atribuida al usuario.
- **Decisión adoptada:** elección técnica del plan que guía la implementación hasta revisarla explícitamente; no significa implementada ni requisito original del usuario.
- **Pendiente:** elección abierta identificada en el PRD. La fase afectada debe resolverla antes de implementar su comportamiento.
- **Existente:** localizado en el prototipo. **Verificado:** acompañado de comando, fecha y resultado en PROJECT.
- **Futuro:** capacidad fuera de la entrega base propuesta; no se introduce como requisito implícito.

El PRD gobierna comportamiento y alcance; ARCHITECTURE gobierna límites; TDD desarrolla los mecanismos que satisfacen ambos. Un conflicto entre estos documentos se corrige explícitamente: la conveniencia de un adaptador no cambia el producto. ROADMAP organiza trabajo, pero no añade requisitos.

Los IDs `PRD-*`, `ARC-*`, `TDD-*`, `V-*`, `F-*` y `P-*` enlazan requisitos, arquitectura, contratos, verificaciones, fases y pendientes. Se mantienen estables al reorganizar secciones. La matriz del TDD conecta requisito → contrato → verificación; ROADMAP vincula los contratos a sus fases.

`PAT-*` identifica patrones aplicados y `PX-*` sus fragmentos orientativos. PATTERNS desarrolla la arquitectura y remite las garantías a TDD; no mantiene otro registro de decisiones pendientes ni crea un sistema de plugins independiente.

`C-*` identifica casos de referencia y `CT-*` fragmentos de contrato. CONTRACTS desarrolla TDD y ACCEPTANCE desarrolla PRD/TDD; no agregan registros separados de pendientes. El alcance confirmado y las decisiones P-* siguen en PRD.

## Referencias complementarias vigentes

Los [recorridos ejecutables](../EXAMPLES.md), el [consumidor de autoría](../examples/authoring-client/src/lib.rs) y el [schema formato 2](../schemas/2/workflow.schema.json) corresponden al motor actual. Los schemas 1.0 se retiraron junto con su motor y consumidores; el [changelog](../CHANGELOG.md) conserva su historia, identificada como prototipo. [ADOPTION](ADOPTION.md#5-migrar-desde-el-prototipo) explica la migración incompatible.

El diseño anterior, el borrador de evolución y los manuales sustituidos se eliminaron tras consolidar la planificación. Las nuevas decisiones sustituyen las históricas incompatibles según la tabla de ARCHITECTURE. No se promete migración compatible de la spec 1.0: el proyecto aún no tiene una versión pública, según lo confirmado por el usuario.

## Cómo mantener el contexto

Antes de implementar una fase, leer PROJECT, sus contratos en TDD y sus pendientes en PRD. Después de trabajar, registrar evidencia en PROJECT y actualizar la matriz si cambió el alcance. Una decisión nueva debe indicar motivo, alternativas, consecuencias y los IDs afectados; puede documentarse inicialmente en ARCHITECTURE y extraerse a un ADR cuando su extensión lo amerite.

No marcar una capacidad terminada por existir su descripción, un trait o un ejemplo. El cierre exige el criterio de salida de ROADMAP y las verificaciones correspondientes.
