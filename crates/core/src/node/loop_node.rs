use serde::{Deserialize, Serialize};

use crate::workflow::Condition;

/// Nodo que ejecuta un subgrafo repetidamente mientras se cumpla una condición.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopNode {
    /// Condición que debe cumplirse para continuar iterando
    pub condition: Condition,
    /// Límite de seguridad de iteraciones
    pub max_iterations: u32,
    /// ID del primer nodo del cuerpo del bucle
    pub body_start: String,
    /// ID del último nodo del cuerpo del bucle
    pub body_end: String,
}
