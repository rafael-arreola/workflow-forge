//! Extensión `data` de workflow-forge: aquí viven todas las transformaciones
//! (decisión #14 de la spec: los mappings de nodos no transforman).
//!
//! Un módulo por tarea; para agregar una tarea nueva, crea su módulo y
//! súmala a [`register`]:
//!
//! | Tarea | Módulo | Contrato |
//! |-------|--------|----------|
//! | `data.transform` | [`transform`] | Reshape de `source` según `shape` (paths `@.` relativos al source) |
//! | `data.map` | [`map`] | Aplica `shape` a CADA elemento de `items` (paths `@.` relativos al elemento) |
//! | `data.merge` | [`merge`] | Merge profundo de `objects`; las llaves posteriores ganan |
//! | `data.template` | [`template`] | Interpola `{path.con.puntos}` de `values` en `template` |
//! | `data.cast` | [`cast`] | Conversiones declarativas por campo (fechas, números, strings) |

use serde_json::Value;

use workflow_forge_core::task::TaskRegistry;

pub mod cast;
pub mod map;
pub mod merge;
pub mod template;
pub mod transform;

pub use cast::CastTask;
pub use map::MapTask;
pub use merge::MergeTask;
pub use template::TemplateTask;
pub use transform::TransformTask;

/// Registra todas las tareas de la extensión en el registry
pub fn register(registry: &TaskRegistry) {
    registry.register(TransformTask::default());
    registry.register(MapTask::default());
    registry.register(MergeTask::default());
    registry.register(TemplateTask::default());
    registry.register(CastTask::default());
}

/// Códigos de error que esta extensión puede emitir. Mismo contrato que
/// [`workflow_forge_core::error::codes`]: constantes estables, nunca cambian
/// de valor. (`data.transform`, `data.map` y `data.merge` solo emiten los
/// códigos de expresión del core, como `INVALID_JSONPATH`.)
pub mod codes {
    /// Un placeholder de `data.template` quedó sin cerrar (`{abc` sin `}`).
    pub const TEMPLATE_INVALID: &str = "TEMPLATE_INVALID";
    /// Un placeholder de `data.template` no existe en `values`.
    pub const TEMPLATE_VALUE_MISSING: &str = "TEMPLATE_VALUE_MISSING";
    /// El input de `data.cast` no deserializa contra su contrato.
    pub const CAST_INPUT_INVALID: &str = "CAST_INPUT_INVALID";
    /// Un valor no se pudo convertir y `on_invalid` es `fail`.
    pub const CAST_FIELD_INVALID: &str = "CAST_FIELD_INVALID";
}

/// Helper compartido: parsea un JSON literal como `schemars::Schema` para
/// los manifiestos de las tareas.
pub(crate) fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("schema estático válido")
}
