use serde::{Deserialize, Serialize};

/// Nodo que evalúa una expresión y enruta según tabla de casos.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchNode {
    /// Expresión en jsonpath a evaluar contra los datos de entrada
    pub expression: String,
    /// Correspondencia valor → nodo destino
    pub cases: Vec<SwitchCase>,
    /// Nodo destino si ningún caso coincide
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

/// Caso de un nodo switch: si el valor de la expresión coincide, deriva al nodo indicado.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwitchCase {
    /// Valor esperado de la expresión
    pub value: serde_json::Value,
    /// ID del nodo destino si el valor coincide
    pub target_node_id: String,
}
