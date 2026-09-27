# Workflow Forge — guía de adopción y extensiones

Esta guía usa el formato `forge.workflow/2`. La fachada raíz, `workflow_forge::prelude` y `workflow_forge::v2` exponen la misma API. [CONTRACTS](CONTRACTS.md), [HTTP](HTTP.md) e [INTEGRATIONS](INTEGRATIONS.md) definen sus garantías; [PROJECT](PROJECT.md) registra qué está comprobado. Los ejemplos son locales y no acreditan destinos ni capacidad de producción.

## 1. Elegir y conservar la instancia

| Necesidad | Composición |
|---|---|
| Llamar desde un servicio Rust existente | `WorkflowBuilder` → `build` → `EngineRuntime::boot`; conservar el runtime y compartir `WorkflowApplication`. |
| Atender clientes HTTP | `ServiceRuntime` conserva el engine y el transporte. El ejecutable usa `HostConfig` y SQLite por defecto. |
| Consumir un host desde una terminal/script | `forge --server URL` usa HTTP y una credencial del entorno; [CLI](CLI.md) publica el contrato. |
| Recorridos efímeros locales | `WorkflowBuilder::standard()` usa memoria; una caída pierde su estado. |
| Recuperar runs y artefactos | Instalar el mismo `Arc<SqliteExecutionStore>` en los puertos de estado y artefactos; pedir `require_durable`. |

Una instancia corresponde a un scope y un propietario del store. Construirla una vez durante el arranque; clonar el handle por petición. La configuración, los módulos y los schemas se registran antes de `build`. El arranque valida la composición y recupera lo aceptado antes de habilitar admisión. El host espera `shutdown`; soltar el objeto no acredita drenado.

El [host mínimo](../crates/forge/examples/v2_customer.rs), el [host durable](../crates/forge/examples/v2_sqlite.rs) y el [bootstrap HTTP](HTTP.md#6-ejecutable-y-composición-del-host) son programas/configuraciones completos. Sus permisos confiables de demostración no sustituyen el autenticador de un servicio expuesto.

## 2. Crear una extensión mediante contratos públicos

Una extensión es un crate Rust compilado con el host. No es una librería cargada dinámicamente ni código aislado. Para conectarse al motor depende de `workflow-forge-protocol`; puede agregar su SDK o parser. El host usa la fachada para registrarla. El [ejemplo de texto](../examples/reference-module/src/text.rs) agrega `example.text.prefix`, sin cambios de negocio dentro del engine.

Separar cuatro decisiones:

1. Describir configuración, entrada y salida con JSON Schema, una revisión exacta y límites útiles. Declarar efecto, repetición y recursos desde el principio.
2. Implementar `Operation` sin almacenar input por invocación en el objeto compartido. El host puede inyectar un cliente/pool construido una vez.
3. Entregar un `OperationBundle` mediante una Factory Function. El bundle declara módulo, protocolo y todos sus exports; no instala proveedores por su cuenta.
4. Registrar en el Builder, preparar un workflow y comprobar resultados/errores usando la fachada pública. `build` y `prepare` no ejecutan el negocio.

Fragmento de registro en el host:

```rust
use workflow_forge::v2::*;
use workflow_forge_reference_module::text::text_operations;

let mut builder = WorkflowBuilder::standard();
builder.register_bundle(text_operations())?;
let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
let app = runtime.application();
// El host conserva runtime, comparte app y espera shutdown al cerrar.
```

El archivo enlazado contiene el contrato `Operation` con sus errores y schemas. `Prefix` es `Pure/Safe`; un conector HTTP que escribe no puede copiar esa clasificación. [INTEGRATIONS](INTEGRATIONS.md) muestra `Write/Unsafe`, incertidumbre después del despacho y uso de secretos por puerto. `Keyed` exige que el destino cumpla la deduplicación, no solo enviarle una cabecera.

| Patrón | Responsabilidad concreta | Límite |
|---|---|---|
| Builder | Componer módulos/proveedores antes del arranque. | No publicar un registro mutable a los workflows. |
| Factory Function | Construir y devolver contribuciones explícitas. | No iniciar runs ni reemplazar infraestructura del host. |
| Adapter | Traducir un SDK externo al contrato `Operation` o a un puerto. | No incorporar decisiones del negocio al coordinador. |
| Command | Transportar identidad, intento, configuración e input en `Invocation`. | No derivar identidad solo del contenido ni perder `effect_key`. |
| Decorator | Medir/observar una operación compartida preservando descriptor y respuesta. | No introducir retry ni alterar certeza de efectos. |
| Facade | Dar al host catálogo, preparación y ejecución coherentes. | No pedir imports de módulos internos para integrar una extensión. |

[PATTERNS](PATTERNS.md) contiene las definiciones completas. El [consumidor y sus pruebas](../examples/authoring-client/tests/consumer.rs) ejercitan el módulo externo mediante un Decorator que cuenta invocaciones: preparar y rechazar configuraciones no lo ejecutan.

## 3. Construir un consumidor de autoría

El [cliente de ejemplo](../examples/authoring-client/src/lib.rs) depende del protocolo y JSON. Su host de demostración usa la fachada como dependencia de desarrollo; el cliente no importa el engine. Ejecutar:

```sh
cargo run -p workflow-forge-authoring-example --example round_trip
```

El programa registra la extensión de texto, obtiene/serializa su catálogo, construye un documento, lo exporta/importa y prepara/ejecuta el resultado. stdout contiene el documento JSON; stderr informa el resultado `ID-42`. El código completo conserva el runtime y espera su cierre incluso ante error.

El recorrido para un editor es:

1. Obtener los descriptores mediante `catalog` o todas las páginas de `GET /v2/catalog`, bajo el acceso del usuario.
2. Seleccionar `{id, contract, implementation}` exactamente. Un catálogo obsoleto o ambiguo no autoriza elegir otra revisión. Usar los schemas para el formulario; el autor aporta los valores requeridos.
3. Crear nodos, bindings y relaciones de control explícitos. El cliente de referencia construye un solo nodo y copia los schemas de entrada/salida de la operación; el schema completo admite los controles del motor.
4. Conservar metadatos visuales en `presentation`. En el ejemplo, mover un nodo cambia `presentation.nodes[id].position` y preserva claves ajenas. Es una convención del cliente, no una instrucción del engine.
5. Exportar/importar el documento y pedir `prepare`. Conservar y mostrar diagnósticos por `location.node`, `location.field` y `location.data`, sin analizar el mensaje libre.
6. Iniciar solo una revisión preparada y consultar el RunId aceptado. Preparar no inicia trabajo.

Una configuración inválida de `prefix` señala el nodo `step`, campo `/config` y dato `/prefix`. Un literal inválido usa `/input/literal`; un error en la configuración de inicio de una espera usa `/start/config`. La referencia de un binding inválido se ubica en `/input`. El análisis estático puede devolver `compatibility.unknown`; el editor lo conserva y la validación en runtime sigue activa.

El consumidor no genera valores a partir de cualquier schema, no es un editor de controles anidados y no resuelve `$ref` por internet. Las referencias registradas pertenecen a la composición del host. Mover el layout conserva la revisión semántica; cambiar configuración/mappings de una revisión ya preparada exige otra revisión. El editor conserva su documento visual: un plan ejecutable no reemplaza su almacenamiento de autoría.

## 4. Versionar y retirar una extensión

| Cambio | Identidad que cambia |
|---|---|
| Corregir texto de ayuda o ejemplos sin cambiar ejecución | Metadata; no reutilizarla para ocultar cambios semánticos. |
| Cambiar comportamiento, dependencia fijada o perfil de un conector | Revisión de implementación nueva. |
| Cambiar incompatiblemente input/output/config, recursos o efectos | Contrato/revisión nuevos y migración explícita del consumidor. |
| Cambiar definición semántica de workflow | Revisión del workflow nueva. |
| Cambiar codec de persistencia | Versión de checkpoint y migración comprobada, independiente de las anteriores. |

El registro admite revisiones distintas, no reemplazo por orden de carga. Para convivir, un bundle enumera ambas operaciones/exports; la factoría del módulo conserva autoridad sobre su contenido y el host sobre su registro. Los perfiles oficiales incluyen su configuración pública en la revisión; el valor de un secreto no se publica en el descriptor.

Secuencia de retiro:

1. Publicar y registrar la nueva revisión junto con la anterior. Cambiar las definiciones destinadas a nuevas recepciones; conservar los binarios/revisiones anteriores para recuperación.
2. Dejar de ofrecer la revisión anterior para nueva autoría desde la política del host/editor. Ocultarla en el editor no cambia planes ni paquetes ya aceptados. Este perfil no expone hot reload ni una API de unloading.
3. Consultar los runs conocidos del host y sus dependencias fijadas; resolver esperas/incertidumbres pendientes bajo sus contratos. Un run bloqueado no deja de necesitar su implementación por cumplir una antigüedad arbitraria.
4. Retirar el código cuando no quede trabajo que deba reanudarse con esa revisión y se cumpla la política de retención del implementador. Un recibo todavía vigente mantiene su reserva y no autoriza ejecutar otra vez un trabajo cuyo resultado expiró.
5. Si falta accidentalmente una revisión, restaurar su implementación exacta y reiniciar el runtime. Un bloqueo de dependencia puede recuperarse; una escritura incierta sigue requiriendo inspección/reconciliación. No actualizar manualmente el checkpoint para fingir otra implementación.

[Recuperación de paquetes](../crates/forge/tests/v2_recovery_package.rs) y [recuperación SQLite](../crates/forge/tests/v2_sqlite.rs) comprueban revisión ausente/reinstalada, cambio de contrato bajo la misma revisión y conservación de paquetes aceptados. Su éxito no garantiza semántica de un SDK externo cambiado sin versionar. La lista de runs de un producto y la política de despliegue pertenecen al host; el servicio actual consulta RunIds conocidos, no ofrece un inventario global de deployments.

## 5. Migrar desde el prototipo

No hay conversión automática entre spec 1.0 y formato 2. Las [recetas actuales](../EXAMPLES.md) apuntan a programas ejecutables; los ejemplos anteriores permanecen recuperables en Git.

| Prototipo | API actual |
|---|---|
| `Task` y `TaskRegistry` mutable | `Operation` + `OperationBundle`, catálogo congelado antes de boot. |
| `WorkflowExecutor` por definición | Una instancia con `EngineRuntime` y handles de `WorkflowApplication`. |
| Strings JSONPath/expresiones implícitas | Binding `literal/select/object/array`, JSON Pointer y operaciones explícitas. |
| Convergencias por aristas arbitrarias | Controles estructurados con cuerpos, correlación y políticas de errores. |
| Retry por error/timeout | Clasificación de certeza/repetición antes de aplicar backoff. |
| Objetos binarios en memoria | `ArtifactRef`, puertos, cuotas y propiedad/retención declaradas. |

El motor `core`, los seis crates `extensions/*`, schemas 1.0, ejemplos y pruebas exclusivos del prototipo se retiraron después de disponer de consumidores Rust/HTTP/CLI sobre formato 2. La historia hasta `6806fa0` conserva sus fuentes y ejemplos. `forge` ahora requiere un servicio: validar prepara allí y ejecutar obtiene un recibo de ese host. Para embedding usar la fachada Rust.

La fachada mantiene `integrations` por defecto y permite `--no-default-features` para contratos, engine y proveedores de memoria sin conectores HTTP/archivos/CSV. `sqlite` agrega el proveedor durable; `full` habilita `sqlite` e `integrations`. Las features antiguas `util/data/http/tabular/sftp/compress/testing`, `Task`, `WorkflowExecutor`, `default_registry` y el DSL JSONPath se eliminaron.

HTTP/JSON y archivos/CSV tienen módulos formato 2, con perfiles explícitos y límites documentados. XLSX, SFTP, ZIP/GZIP, plantillas y conversiones genéricas del prototipo **no tienen reemplazo automático**: implementar una operación externa, describir sus schemas/efectos y registrar una revisión nueva antes de migrar un workflow que los necesite. Un delay corresponde al control `timer`; un mock se inyecta mediante los mismos puertos u operaciones, como en las pruebas públicas. La retirada de tests antiguos reduce el conteo total, sin acreditar capacidades retiradas.
