use serde::{Deserialize, Serialize};

/// Nodo que dispara múltiples ramas en paralelo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelNode {
    /// IDs de los nodos de inicio de cada rama paralela
    pub branches: Vec<String>,
}
