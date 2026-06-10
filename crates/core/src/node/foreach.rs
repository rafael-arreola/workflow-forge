use serde::{Deserialize, Serialize};

use crate::node::task::RetryPolicy;
use crate::task::TaskId;

/// Nodo que itera un array invocando una tarea por elemento.
///
/// Cada elemento es el input directo de la tarea (el reshape por elemento se
/// hace antes con `data.map`). `retry`/`timeout_ms` aplican por elemento.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForeachNode {
    /// Id de la tarea (o perfil) a ejecutar por cada elemento
    pub task: TaskId,
    /// Mapping que debe resolver a un array (mismas reglas `$.` de los inputs)
    pub items: serde_json::Value,
    /// Elementos en vuelo simultáneamente; 1 = secuencial
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Espera entre arranques de elementos (throttle para no saturar destinos)
    #[serde(default)]
    pub throttle_ms: u64,
    /// Qué hacer cuando un elemento falla definitivamente
    #[serde(default)]
    pub on_item_error: OnItemError,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
    /// Tiempo máximo por intento de cada elemento, en milisegundos
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

fn default_concurrency() -> usize {
    1
}

/// Política de error por elemento de un foreach.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnItemError {
    /// El primer error cancela el resto y el nodo falla (fallo rápido)
    #[default]
    Fail,
    /// Se ejecutan todos; output `{ok: [...], failed: [{index, item, error}]}`
    /// y el nodo no falla por errores de elementos
    Collect,
}
