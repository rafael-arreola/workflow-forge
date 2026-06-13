//! workflow-forge `data` extension: home to all transformations
//! (spec decision #14: node mappings do not transform).
//!
//! One module per task; to add a new task, create its module and
//! add it to [`register`]:
//!
//! | Task | Module | Contract |
//! |-------|--------|----------|
//! | `data.transform` | [`transform`] | Reshape `source` according to `shape` (paths `@.` relative to source) |
//! | `data.map` | [`map`] | Apply `shape` to EACH element of `items` (paths `@.` relative to the element) |
//! | `data.merge` | [`merge`] | Deep merge of `objects`; later keys win |
//! | `data.template` | [`template`] | Interpolate `{dot.separated.path}` from `values` into `template` |
//! | `data.cast` | [`cast`] | Declarative per-field conversions (dates, numbers, strings) |

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

/// Registers all extension tasks in the registry
pub fn register(registry: &TaskRegistry) {
    registry.register(TransformTask::default());
    registry.register(MapTask::default());
    registry.register(MergeTask::default());
    registry.register(TemplateTask::default());
    registry.register(CastTask::default());
}

/// Error codes this extension can emit. Same contract as
/// [`workflow_forge_core::error::codes`]: stable constants, never change
/// value. (`data.transform`, `data.map` and `data.merge` only emit the
/// core expression codes, such as `INVALID_JSONPATH`.)
pub mod codes {
    /// An unclosed `data.template` placeholder (`{abc` without `}`).
    pub const TEMPLATE_INVALID: &str = "TEMPLATE_INVALID";
    /// A `data.template` placeholder does not exist in `values`.
    pub const TEMPLATE_VALUE_MISSING: &str = "TEMPLATE_VALUE_MISSING";
    /// The input to `data.cast` does not deserialize against its contract.
    pub const CAST_INPUT_INVALID: &str = "CAST_INPUT_INVALID";
    /// A value could not be converted and `on_invalid` is `fail`.
    pub const CAST_FIELD_INVALID: &str = "CAST_FIELD_INVALID";
}

/// Shared helper: parses a JSON literal as a `schemars::Schema` for
/// task manifests.
pub(crate) fn schema(value: Value) -> workflow_forge_core::schemars::Schema {
    serde_json::from_value(value).expect("valid static schema")
}
