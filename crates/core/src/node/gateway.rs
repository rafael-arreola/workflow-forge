use serde::{Deserialize, Serialize};

use crate::condition::Condition;

/// Nodo de control de flujo estilo BPMN.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GatewayNode {
    /// Tipo de gateway
    pub gateway: GatewayKind,
    /// Ramas condicionales. Solo aplica a `exclusive`; se evalúan en orden
    /// y gana la primera cuyo `when` se cumple (o la rama `else`).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branches: Vec<Branch>,
}

/// Tipos de gateway de la spec 1.0.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GatewayKind {
    /// If/else: una sola rama saliente gana
    Exclusive,
    /// Fan-out: todas las aristas salientes se ejecutan concurrentemente
    Parallel,
    /// Fan-in: espera todas las ramas entrantes (wait_all, fallo rápido)
    Join,
}

/// Rama de un gateway exclusivo. `edge` referencia el `label` de una
/// arista saliente del mismo nodo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub when: Option<Condition>,
    /// Rama por defecto cuando ninguna otra se cumple
    #[serde(rename = "else", default, skip_serializing_if = "std::ops::Not::not")]
    pub is_else: bool,
    /// Label de la arista saliente que sigue el flujo si esta rama gana
    pub edge: String,
}
