//! La familia **runtime**: ejecución de workflows.
//!
//! - [`executor`]: el orquestador. Construye con validación + compilación
//!   (schemas, índice del grafo, sub-workflows) y recorre el grafo
//!   run-to-completion. La semántica por kind de nodo vive en `handlers`
//!   (un módulo por familia) y la política de retry/timeout/panic en
//!   `policy`.
//! - [`context`]: el documento de estado de una ejecución (`$.trigger`,
//!   `$.nodes.<id>.output`, `$.workflow`) sobre el que se resuelven
//!   mappings y condiciones.
//! - [`registry`]: workflows reusables como sub-workflows, por nombre.
//! - `graph` / `schemas` (privados): índices y validadores precompilados
//!   al construir el executor.

pub mod context;
pub mod executor;
pub(crate) mod graph;
pub(crate) mod handlers;
pub(crate) mod policy;
pub mod registry;
pub(crate) mod schemas;

pub use context::WorkflowContext;
pub use executor::{WorkflowExecutor, WorkflowExecutorBuilder};
pub use registry::WorkflowRegistry;
