use serde::{Deserialize, Serialize};

use crate::workflow::Condition;

/// Nodo que evalúa condiciones y deriva a la primera rama que cumpla.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalNode {
    /// Ramas condicionales evaluadas en orden
    pub branches: Vec<ConditionalBranch>,
    /// ID del nodo por defecto si ninguna rama cumple
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

/// Rama de un nodo condicional: si se cumple la condición, deriva al nodo indicado.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConditionalBranch {
    /// Condición a evaluar
    pub condition: Condition,
    /// ID del nodo destino si la condición se cumple
    pub target_node_id: String,
}
