//! Catálogo de códigos de error del engine.
//!
//! Cada código es una constante documentada: este módulo es la fuente de
//! verdad de qué puede fallar y por qué. Los códigos son parte del contrato
//! público de la spec (viajan serializados en [`super::WorkflowError`] y en
//! los eventos de observabilidad), por lo que NUNCA cambian de valor; solo
//! se agregan nuevos.
//!
//! Convención de nombres: `FAMILIA_DETALLE` en SCREAMING_SNAKE_CASE. El
//! valor de la constante es idéntico a su nombre (verificado por test).

// ---------------------------------------------------------------------------
// Definición (spec / estructura del documento)
// ---------------------------------------------------------------------------

/// La versión de spec del documento no está soportada por este core.
pub const UNSUPPORTED_SPEC: &str = "UNSUPPORTED_SPEC";
/// Dos o más nodos declaran el mismo id.
pub const DUPLICATE_NODE_ID: &str = "DUPLICATE_NODE_ID";
/// Una arista referencia un nodo que no existe en el documento.
pub const UNKNOWN_NODE_REF: &str = "UNKNOWN_NODE_REF";
/// El workflow no declara ningún nodo `start`.
pub const NO_START_NODE: &str = "NO_START_NODE";
/// El workflow declara más de un nodo `start`; la spec 1.0 exige uno.
pub const MULTIPLE_START_NODES: &str = "MULTIPLE_START_NODES";
/// El workflow no declara ningún nodo `end`.
pub const NO_END_NODE: &str = "NO_END_NODE";
/// Un nodo `start` tiene aristas entrantes.
pub const START_HAS_INCOMING: &str = "START_HAS_INCOMING";
/// Un nodo `end` tiene aristas salientes.
pub const END_HAS_OUTGOING: &str = "END_HAS_OUTGOING";
/// Un nodo no es alcanzable desde el `start`.
pub const UNREACHABLE_NODE: &str = "UNREACHABLE_NODE";
/// El grafo contiene ciclos; la spec 1.0 exige un grafo acíclico.
pub const CYCLE_DETECTED: &str = "CYCLE_DETECTED";

// ---------------------------------------------------------------------------
// Definición (gateways)
// ---------------------------------------------------------------------------

/// Un gateway `exclusive` no declara ramas.
pub const GATEWAY_NO_BRANCHES: &str = "GATEWAY_NO_BRANCHES";
/// Un gateway declara más de una rama `else`.
pub const GATEWAY_MULTIPLE_ELSE: &str = "GATEWAY_MULTIPLE_ELSE";
/// Un gateway `exclusive` tiene dos aristas salientes (o dos ramas) con el
/// mismo label: la rama ganadora seguiría ambas aristas a la vez,
/// convirtiendo el exclusive en un fan-out accidental.
pub const GATEWAY_DUPLICATE_EDGE_LABEL: &str = "GATEWAY_DUPLICATE_EDGE_LABEL";
/// Una rama de gateway no tiene `when` ni es `else`.
pub const GATEWAY_BRANCH_WITHOUT_WHEN: &str = "GATEWAY_BRANCH_WITHOUT_WHEN";
/// Una rama de gateway no tiene arista saliente con su label.
pub const GATEWAY_BRANCH_WITHOUT_EDGE: &str = "GATEWAY_BRANCH_WITHOUT_EDGE";
/// Una arista saliente de un gateway no corresponde a ninguna rama.
pub const GATEWAY_EDGE_WITHOUT_BRANCH: &str = "GATEWAY_EDGE_WITHOUT_BRANCH";
/// Un gateway `parallel`/`join` declara branches, que no aceptan.
pub const GATEWAY_BRANCHES_IGNORED: &str = "GATEWAY_BRANCHES_IGNORED";
/// Un gateway `parallel` necesita al menos 2 aristas salientes.
pub const PARALLEL_TOO_FEW_OUTPUTS: &str = "PARALLEL_TOO_FEW_OUTPUTS";
/// Un gateway `join` necesita al menos 2 aristas entrantes del flujo normal.
pub const JOIN_TOO_FEW_INPUTS: &str = "JOIN_TOO_FEW_INPUTS";

// ---------------------------------------------------------------------------
// Definición (aristas y nodos especiales)
// ---------------------------------------------------------------------------

/// Una arista `on: error`/`on: panic` sale de un nodo que no ejecuta tareas.
pub const ERROR_EDGE_INVALID_SOURCE: &str = "ERROR_EDGE_INVALID_SOURCE";
/// Una arista `on: error`/`on: panic` apunta a un gateway `join`.
pub const ERROR_EDGE_TO_JOIN: &str = "ERROR_EDGE_TO_JOIN";
/// Un foreach declara `concurrency: 0`; debe ser >= 1.
pub const FOREACH_INVALID_CONCURRENCY: &str = "FOREACH_INVALID_CONCURRENCY";
/// Un loop declara `max_iterations: 0`; debe ser >= 1.
pub const LOOP_INVALID_MAX_ITERATIONS: &str = "LOOP_INVALID_MAX_ITERATIONS";
/// Un nodo subworkflow no declara el nombre del workflow hijo.
pub const SUBWORKFLOW_MISSING_NAME: &str = "SUBWORKFLOW_MISSING_NAME";
/// La sección `workflows` declara dos workflows con el mismo nombre.
pub const SUBWORKFLOW_DUPLICATE_NAME: &str = "SUBWORKFLOW_DUPLICATE_NAME";
/// Un nodo referencia una tarea que no está registrada.
pub const TASK_NOT_FOUND: &str = "TASK_NOT_FOUND";
/// Un schema declarado (nodo o tarea) no compila como JSON Schema.
pub const INVALID_SCHEMA: &str = "INVALID_SCHEMA";

// ---------------------------------------------------------------------------
// Construcción del executor (sub-workflows y registros)
// ---------------------------------------------------------------------------

/// Un nodo subworkflow referencia un workflow inexistente (ni inline ni en
/// el registro compartido).
pub const SUBWORKFLOW_NOT_FOUND: &str = "SUBWORKFLOW_NOT_FOUND";
/// Una cadena de sub-workflows se referencia a sí misma.
pub const SUBWORKFLOW_CYCLE: &str = "SUBWORKFLOW_CYCLE";
/// El anidamiento de sub-workflows supera la profundidad máxima.
pub const SUBWORKFLOW_DEPTH_EXCEEDED: &str = "SUBWORKFLOW_DEPTH_EXCEEDED";
/// Se intentó registrar un perfil con un id ya ocupado.
pub const PROFILE_ID_CONFLICT: &str = "PROFILE_ID_CONFLICT";
/// El `extends` de un perfil no corresponde a una tarea registrada.
pub const PROFILE_BASE_NOT_FOUND: &str = "PROFILE_BASE_NOT_FOUND";
/// Se intentó registrar un workflow con un nombre ya ocupado.
pub const WORKFLOW_NAME_CONFLICT: &str = "WORKFLOW_NAME_CONFLICT";

// ---------------------------------------------------------------------------
// Expresiones (mappings, shapes, condiciones)
// ---------------------------------------------------------------------------

/// Un path JSONPath no parsea (error de definición, no de datos).
pub const INVALID_JSONPATH: &str = "INVALID_JSONPATH";
/// Una expresión regular de un operador `matches` no compila.
pub const INVALID_REGEX: &str = "INVALID_REGEX";
/// Un path `$.` de un mapping no resuelve ningún valor del contexto.
pub const MAPPING_PATH_NOT_FOUND: &str = "MAPPING_PATH_NOT_FOUND";
/// Una condición usa un operador que no es de la spec ni está registrado
/// en el registro de operadores ([`crate::expr::operators`]).
pub const UNKNOWN_CONDITION_OPERATOR: &str = "UNKNOWN_CONDITION_OPERATOR";

// ---------------------------------------------------------------------------
// Ejecución
// ---------------------------------------------------------------------------

/// El trigger no cumple el schema declarado en el nodo `start`.
pub const SCHEMA_VALIDATION_FAILED: &str = "SCHEMA_VALIDATION_FAILED";
/// El resultado final no cumple el schema declarado en el nodo `end`.
pub const OUTPUT_SCHEMA_VALIDATION_FAILED: &str = "OUTPUT_SCHEMA_VALIDATION_FAILED";
/// El input resuelto de una tarea no cumple su `input_schema`.
pub const TASK_INPUT_INVALID: &str = "TASK_INPUT_INVALID";
/// El output de una tarea no cumple su `output_schema`.
pub const TASK_OUTPUT_INVALID: &str = "TASK_OUTPUT_INVALID";
/// Una tarea superó su `timeout_ms`. Reintenta si hay política de retry.
pub const TASK_TIMEOUT: &str = "TASK_TIMEOUT";
/// Una tarea panickeó (bug en la extensión). No reintenta y solo rutea por
/// aristas `on: panic`.
pub const TASK_PANIC: &str = "TASK_PANIC";
/// El mapping `items` de un foreach no resolvió a un array.
pub const FOREACH_ITEMS_NOT_ARRAY: &str = "FOREACH_ITEMS_NOT_ARRAY";
/// Un loop alcanzó `max_iterations` con su condición `while` aún verdadera
/// y `on_max: "fail"` (default). Con `on_max: "stop"` no es error.
pub const LOOP_MAX_ITERATIONS_EXCEEDED: &str = "LOOP_MAX_ITERATIONS_EXCEEDED";
/// Ninguna rama de un gateway `exclusive` se cumplió y no hay `else`.
pub const NO_BRANCH_MATCHED: &str = "NO_BRANCH_MATCHED";
/// La ejecución terminó con joins esperando ramas que nunca llegaron.
pub const JOIN_INCOMPLETE: &str = "JOIN_INCOMPLETE";
/// El workflow finalizó sin alcanzar ningún nodo `end`.
pub const NO_OUTPUT: &str = "NO_OUTPUT";
/// El bind de un perfil produce un input que la tarea base rechaza.
pub const PROFILE_BIND_INVALID: &str = "PROFILE_BIND_INVALID";
/// La ejecución superó el `deadline` indicado en las opciones de `run_with`.
/// (Por defecto las ejecuciones son ilimitadas; el host decide el límite.)
pub const EXECUTION_TIMEOUT: &str = "EXECUTION_TIMEOUT";
/// La ejecución se canceló de forma cooperativa vía el `CancellationToken`
/// de `run_with`.
pub const EXECUTION_CANCELLED: &str = "EXECUTION_CANCELLED";

// ---------------------------------------------------------------------------
// Recursos del host (blobs, secretos)
// ---------------------------------------------------------------------------

/// Un id de blob no tiene la forma generada por el store (referencia forjada).
pub const INVALID_BLOB_ID: &str = "INVALID_BLOB_ID";
/// El blob referenciado no existe en esta ejecución.
pub const BLOB_NOT_FOUND: &str = "BLOB_NOT_FOUND";
/// Error de I/O del almacenamiento de blobs.
pub const BLOB_IO_ERROR: &str = "BLOB_IO_ERROR";
/// Un `{"$secret": "X"}` no resuelve en el provider configurado.
pub const SECRET_NOT_FOUND: &str = "SECRET_NOT_FOUND";

/// Todos los códigos del catálogo, para tooling y tests.
pub const ALL: &[&str] = &[
    UNSUPPORTED_SPEC,
    DUPLICATE_NODE_ID,
    UNKNOWN_NODE_REF,
    NO_START_NODE,
    MULTIPLE_START_NODES,
    NO_END_NODE,
    START_HAS_INCOMING,
    END_HAS_OUTGOING,
    UNREACHABLE_NODE,
    CYCLE_DETECTED,
    GATEWAY_NO_BRANCHES,
    GATEWAY_MULTIPLE_ELSE,
    GATEWAY_DUPLICATE_EDGE_LABEL,
    GATEWAY_BRANCH_WITHOUT_WHEN,
    GATEWAY_BRANCH_WITHOUT_EDGE,
    GATEWAY_EDGE_WITHOUT_BRANCH,
    GATEWAY_BRANCHES_IGNORED,
    PARALLEL_TOO_FEW_OUTPUTS,
    JOIN_TOO_FEW_INPUTS,
    ERROR_EDGE_INVALID_SOURCE,
    ERROR_EDGE_TO_JOIN,
    FOREACH_INVALID_CONCURRENCY,
    LOOP_INVALID_MAX_ITERATIONS,
    SUBWORKFLOW_MISSING_NAME,
    SUBWORKFLOW_DUPLICATE_NAME,
    TASK_NOT_FOUND,
    INVALID_SCHEMA,
    SUBWORKFLOW_NOT_FOUND,
    SUBWORKFLOW_CYCLE,
    SUBWORKFLOW_DEPTH_EXCEEDED,
    PROFILE_ID_CONFLICT,
    PROFILE_BASE_NOT_FOUND,
    WORKFLOW_NAME_CONFLICT,
    INVALID_JSONPATH,
    INVALID_REGEX,
    MAPPING_PATH_NOT_FOUND,
    UNKNOWN_CONDITION_OPERATOR,
    SCHEMA_VALIDATION_FAILED,
    OUTPUT_SCHEMA_VALIDATION_FAILED,
    TASK_INPUT_INVALID,
    TASK_OUTPUT_INVALID,
    TASK_TIMEOUT,
    TASK_PANIC,
    FOREACH_ITEMS_NOT_ARRAY,
    LOOP_MAX_ITERATIONS_EXCEEDED,
    NO_BRANCH_MATCHED,
    JOIN_INCOMPLETE,
    NO_OUTPUT,
    PROFILE_BIND_INVALID,
    EXECUTION_TIMEOUT,
    EXECUTION_CANCELLED,
    INVALID_BLOB_ID,
    BLOB_NOT_FOUND,
    BLOB_IO_ERROR,
    SECRET_NOT_FOUND,
];

#[cfg(test)]
mod tests {
    use super::ALL;

    #[test]
    fn codigos_unicos() {
        let mut seen = std::collections::HashSet::new();
        for code in ALL {
            assert!(seen.insert(code), "código duplicado: {code}");
        }
    }
}
