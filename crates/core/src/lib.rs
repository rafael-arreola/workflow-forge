//! # workflow-forge-core
//!
//! El engine de ejecución de workflows según la spec 1.0, organizado en
//! familias con responsabilidades cerradas:
//!
//! ```text
//! spec      el lenguaje: tipos serde del documento (nodos, aristas,
//!           condiciones, perfiles). Datos puros, sin lógica.
//!   ↓
//! validate  reglas estáticas sobre la definición (acumulan errores)
//!   ↓
//! runtime   ejecución: executor, contexto de estado, registro de
//!           sub-workflows, índices precompilados
//!
//! expr      resolución de expresiones ($.mapping, @.shape)
//! task      el contrato de extensión: trait Task, manifiestos, registry,
//!           perfiles
//! io        recursos del host: blobs ($blob) y secretos ($secret)
//! observe   eventos de ejecución, historia en memoria, reportes
//! error     WorkflowError + catálogo de códigos (error::codes)
//! ```
//!
//! ## ¿Dónde agrego…?
//!
//! | Quiero agregar | Dónde |
//! |---|---|
//! | una tarea nueva | implementa [`task::Task`] en tu crate de extensión y regístrala en el [`task::TaskRegistry`] |
//! | un perfil de tarea | [`spec::TaskProfile`] + `register_profile` (o sección `tasks` inline del documento) |
//! | un operador de condición | implementa [`expr::operators::ConditionOperator`] y regístralo en [`expr::operators::global`] |
//! | una regla de validación | implementa [`validate::ValidationRule`]; integrada → [`validate::rules`], del host → `builder().rule(...)` |
//! | otro almacenamiento de blobs | implementa [`io::BlobStore`] + [`io::BlobStoreFactory`] → `builder().blobs(...)` |
//! | otro provider de secretos | implementa [`io::SecretProvider`] → `builder().secrets(...)` |
//! | un kind de nodo (cambio de spec) | variante en [`spec::node::NodeKind`] + handler en `runtime::handlers` + reglas en [`validate::rules`] |
//! | un código de error | constante documentada en [`error::codes`] |
//! | un evento de observabilidad | variante en [`observe::EventKind`] |

#![warn(missing_docs)]

// TODO(refactor): los #[path] explícitos son temporales mientras existen los
// archivos legacy (error.rs, observe.rs, task.rs) junto a las carpetas
// nuevas; al eliminarlos, estos atributos sobran.
#[path = "error/mod.rs"]
pub mod error;
pub mod expr;
pub mod io;
#[path = "observe/mod.rs"]
pub mod observe;
pub mod runtime;
pub mod spec;
#[path = "task/mod.rs"]
pub mod task;
pub mod validate;

// Re-export para que las extensiones construyan schemas sin depender
// directamente de schemars
pub use schemars;

/// Lo necesario para escribir y ejecutar workflows: el contrato de tareas,
/// el executor y los tipos que cruzan la frontera del engine.
pub mod prelude {
    pub use crate::error::WorkflowError;
    pub use crate::observe::{ExecutionObserver, InMemoryHistory};
    pub use crate::runtime::{WorkflowContext, WorkflowExecutor, WorkflowRegistry};
    pub use crate::spec::{TaskProfile, WorkflowDefinition};
    pub use crate::task::{Task, TaskId, TaskManifest, TaskRegistry, WorkflowData, WorkflowResult};
}

/// Versión del crate workflow-forge-core
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
