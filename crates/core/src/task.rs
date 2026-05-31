use async_trait::async_trait;

use crate::context::WorkflowContext;
use crate::types::{PortDef, WorkflowData, WorkflowResult};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TaskId(pub String);

impl From<String> for TaskId {
    fn from(s: String) -> Self {
        TaskId(s)
    }
}

impl From<&str> for TaskId {
    fn from(s: &str) -> Self {
        TaskId(s.to_string())
    }
}

impl From<TaskId> for String {
    fn from(task_id: TaskId) -> Self {
        task_id.0
    }
}

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl serde::Serialize for TaskId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for TaskId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(TaskId(s))
    }
}

/// Unidad mínima ejecutable dentro de un workflow.
/// Cada tarea declara sus puertos y define su lógica de transformación.
#[async_trait]
pub trait Task: Send + Sync + 'static {
    /// Identificador único de la tarea se usa para poder resolver la tarea en runtime.
    fn task_id(&self) -> &TaskId;

    /// Puertos de entrada que reciben datos para la ejecución
    fn input_ports(&self) -> &[PortDef];

    /// Puertos de salida que emiten el resultado de la ejecución
    fn output_ports(&self) -> &[PortDef];

    /// Transforma los datos de entrada en la salida esperada.
    /// Recibe el contexto inmutable del workflow y los datos de entrada.
    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult;
}
