use serde::{Deserialize, Serialize};

use crate::task::TaskId;

/// Nodo que invoca una tarea registrada en el `TaskRegistry`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskNode {
    /// Id namespaced de la tarea a ejecutar (ej. `"http.request"`)
    pub task: TaskId,
    /// Mapping de entrada. Strings que empiezan con `$.` se resuelven como
    /// JSONPath contra el contexto; `$$.` escapa a un literal `$.`;
    /// el resto son literales (objetos/arrays se recorren recursivamente).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
    /// Tiempo máximo de ejecución por intento, en milisegundos
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// Política de reintentos de un nodo task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Reintentos máximos después del intento inicial
    pub max: u32,
    #[serde(default)]
    pub backoff: Backoff,
    /// Espera base antes del primer reintento, en milisegundos
    #[serde(default = "default_initial_ms")]
    pub initial_ms: u64,
}

fn default_initial_ms() -> u64 {
    500
}

/// Estrategia de espera entre reintentos.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Backoff {
    /// initial_ms * 2^(intento-1)
    #[default]
    Exponential,
    /// initial_ms * intento
    Linear,
    /// initial_ms constante
    Fixed,
}
