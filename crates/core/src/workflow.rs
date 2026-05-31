use crate::node::Node;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Definición completa de un workflow lista para ser serializada/deserializada.
/// Contiene nodos (módulos + control de flujo), aristas y configuración global.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowDef {
    /// Identificador único del workflow (se asigna si no se provee)
    #[serde(default)]
    pub id: Option<String>,
    /// Nombre descriptivo del workflow
    pub name: String,
    /// Versión semántica
    #[serde(default = "default_version")]
    pub version: String,
    /// Nodos que componen el grafo (módulos, condicionales, bucles, etc.)
    pub nodes: Vec<Node>,
    /// Aristas dirigidas que definen el flujo de datos y control
    #[serde(default)]
    pub edges: Vec<FlowEdge>,
    /// Configuración global (concurrencia, timeouts, política de errores)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_config: Option<WorkflowConfig>,
}

fn default_version() -> String {
    "0.1.0".into()
}

/// Conexión dirigida entre dos puertos de nodos del workflow.
/// Puede incluir condición de activación, mapeo de datos y marca de paralelismo.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowEdge {
    /// Puerto de origen
    pub from: EdgeEndpoint,
    /// Puerto de destino
    pub to: EdgeEndpoint,
    /// Estado de salida requerido para activar esta arista
    #[serde(default)]
    pub on: OutputStatus,
    /// Condición adicional que debe cumplirse (jsonpath sobre los datos)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub condition: Option<Condition>,
    /// Transformación de datos entre puerto origen y destino
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_mapping: Option<DataMapping>,
    /// Indica si esta arista pertenece a una bifurcación paralela
    #[serde(default)]
    pub parallel: bool,
}

/// Referencia a un puerto concreto de un nodo del grafo
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EdgeEndpoint {
    /// ID del nodo al que pertenece el puerto
    pub module_id: String,
    /// Nombre del puerto dentro de ese nodo
    pub port_name: String,
}

/// Estado de salida de un nodo que determina qué aristas se activan.
/// Las aristas con `on: error` forman el flujo de manejo de errores.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum OutputStatus {
    /// La ejecución del nodo fue exitosa
    #[default]
    Success,
    /// La ejecución del nodo falló
    Error,
    /// Se activa siempre, sin importar el resultado
    Always,
}

/// Condición booleana evaluada en runtime mediante jsonpath.
/// Soporta operadores de comparación, existencia y pertenencia.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Condition {
    /// Expresión jsonpath que extrae un valor del documento de datos
    pub field: String,
    /// Operador de comparación
    pub op: ConditionOp,
    /// Valor esperado contra el que se compara
    pub value: serde_json::Value,
}

/// Operadores soportados para evaluación de condiciones
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ConditionOp {
    /// Igualdad estricta
    Eq,
    /// Desigualdad
    Neq,
    /// Mayor que
    Gt,
    /// Mayor o igual que
    Gte,
    /// Menor que
    Lt,
    /// Menor o igual que
    Lte,
    /// El valor contiene el substring esperado
    Contains,
    /// El campo existe en el documento
    Exists,
}

/// Transformación de datos entre puertos.
/// Cada entrada del mapa asocia un campo destino con una expresión jsonpath origen.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataMapping {
    /// Mapa: campo_destino → expresión_jsonpath_origen
    pub mappings: HashMap<String, String>,
}

/// Configuración global del workflow que controla el comportamiento del executor
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowConfig {
    /// Límite de tareas concurrentes (por defecto sin límite)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_concurrency: Option<usize>,

    /// Política de manejo de errores a nivel global
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_policy: Option<ErrorPolicy>,

    /// Timeout global en milisegundos para la ejecución completa
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// Estrategia de manejo de errores a nivel workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorPolicy {
    /// Detiene la ejecución inmediatamente ante el primer error
    Stop,
    /// Continúa con los nodos que no dependan del fallido
    Continue,
    /// Reintenta la ejecución del nodo fallido
    Retry {
        /// Número máximo de reintentos
        max_retries: u32,
        /// Retraso entre reintentos en milisegundos
        delay_ms: u64,
    },
}
