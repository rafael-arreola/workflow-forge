use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::WorkflowData;

/// Punto de entrada del workflow. Define el contrato de los datos iniciales.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartNode {
    /// Esquema JSON que valida los datos iniciales del workflow.
    /// Si el input no cumple el esquema, el workflow falla antes de iniciar.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<schemars::Schema>,

    /// Valores por defecto que se fusionan con los datos entrantes.
    /// Útil para inyectar configuración o constantes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub defaults: Option<HashMap<String, WorkflowData>>,
}

/// Punto de salida del workflow. Representa un resultado terminal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndNode {
    /// Status del resultado final (útil para que el cliente distinga caminos)
    #[serde(default)]
    pub status: EndStatus,

    /// Transformación opcional de los datos antes de devolverlos como resultado final
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema: Option<schemars::Schema>,
}

/// Status del resultado terminal del workflow
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EndStatus {
    /// Ejecución completada exitosamente
    #[default]
    Success,
    /// Ejecución finalizó por un error controlado
    Error,
    /// Ejecución cancelada por el usuario o por lógica del workflow
    Cancelled,
    /// Status personalizado (ej: "timeout", "partial_success")
    Custom(String),
}
