use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::context::WorkflowContext;
use crate::error::WorkflowError;

/// Datos que circulan entre nodos durante la ejecución de un workflow.
/// Envoltorio sobre `serde_json::Value` con conversiones implícitas.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowData(pub serde_json::Value);

impl std::ops::Deref for WorkflowData {
    type Target = serde_json::Value;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<serde_json::Value> for WorkflowData {
    fn from(value: serde_json::Value) -> Self {
        Self(value)
    }
}

impl From<WorkflowData> for serde_json::Value {
    fn from(data: WorkflowData) -> Self {
        data.0
    }
}

impl From<&WorkflowData> for serde_json::Value {
    fn from(data: &WorkflowData) -> Self {
        data.0.clone()
    }
}

/// Resultado de la ejecución de una tarea: datos exitosos o error estructurado
pub type WorkflowResult = Result<WorkflowData, WorkflowError>;

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

/// Contrato público de una tarea: el "esquema de extensión" de la spec.
/// Es serializable, de modo que el catálogo completo de tareas disponibles
/// puede exportarse como JSON y validarse/documentarse sin el engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskManifest {
    /// Id namespaced único (ej. `"http.request"`)
    pub id: TaskId,
    /// Descripción legible del propósito de la tarea
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema que valida el input de la tarea
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_schema: Option<schemars::Schema>,
    /// JSON Schema que valida el output de la tarea
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_schema: Option<schemars::Schema>,
}

impl TaskManifest {
    /// Manifiesto mínimo: solo id, sin schemas
    pub fn new(id: impl Into<TaskId>) -> Self {
        Self {
            id: id.into(),
            description: None,
            input_schema: None,
            output_schema: None,
        }
    }
}

/// Unidad mínima ejecutable dentro de un workflow.
/// Cada tarea publica su manifiesto y define su lógica de transformación.
#[async_trait]
pub trait Task: Send + Sync + 'static {
    /// Contrato público de la tarea: id, descripción y schemas de input/output
    fn manifest(&self) -> &TaskManifest;

    /// Transforma los datos de entrada en la salida esperada.
    /// Recibe el contexto inmutable del workflow y los datos de entrada.
    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult;

    /// Id de la tarea, tomado del manifiesto
    fn task_id(&self) -> &TaskId {
        &self.manifest().id
    }
}
