//! Semántica de ejecución de cada familia de nodos, un módulo por kind.
//!
//! [`crate::spec::node::NodeKind`] es un enum cerrado (el vocabulario lo
//! define la spec), así que el dispatch vive en `executor::execute_from` y
//! cada familia implementa su semántica en un `impl WorkflowExecutor`
//! separado:
//!
//! | Módulo | Kinds | Responsabilidad |
//! |---|---|---|
//! | [`event`] | `start`, `end` | schema del trigger, defaults, output final |
//! | [`task`] | `task` | resolución de input y ejecución con política |
//! | [`foreach`] | `foreach` | iteración con concurrencia/throttle por elemento |
//! | [`gateway`] | `gateway` | exclusive (ramas), parallel (fan-out), join (fan-in) |
//! | [`subworkflow`] | `subworkflow` | ejecución del workflow hijo como tarea |
//!
//! La política de retry/timeout/panic compartida está en
//! [`crate::runtime::policy`]. Para agregar un kind nuevo (cambio de spec):
//! variante en `NodeKind`, módulo aquí, brazo en `execute_from`, reglas en
//! `validate/rules` y, si invoca tareas, reuso de `execute_with_policy`.

pub(crate) mod event;
pub(crate) mod foreach;
pub(crate) mod gateway;
pub(crate) mod subworkflow;
pub(crate) mod task;
