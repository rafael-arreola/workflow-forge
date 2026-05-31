use serde::{Deserialize, Serialize};

/// Punto de sincronización: espera resultados de ramas paralelas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinNode {
    /// Si es true, agrega los resultados en un array
    #[serde(default)]
    pub aggregate: bool,
}
