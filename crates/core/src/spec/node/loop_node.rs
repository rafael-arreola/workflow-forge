//! Nodo `loop`: iteración acotada de una tarea con condición de
//! continuación. (El archivo se llama `loop_node.rs` porque `loop` es
//! palabra reservada de Rust; el kind en la spec es `"loop"`.)

use serde::{Deserialize, Serialize};

use crate::spec::condition::Condition;
use crate::spec::node::task::RetryPolicy;
use crate::task::TaskId;

/// Nodo que invoca una tarea repetidamente hasta que su condición `while`
/// deje de cumplirse o se alcance `max_iterations`. Es la respuesta de la
/// spec al caso "pide páginas hasta que `next` sea null" que un grafo
/// acíclico no puede expresar: el ciclo vive **dentro** del nodo (como en
/// `foreach`), así que el grafo sigue siendo acíclico y la terminación queda
/// garantizada por el tope.
///
/// Semántica de cada iteración:
/// 1. La primera corre con `input` (mapping `$.` contra el contexto; sin
///    `input`, el token del predecesor). **La primera iteración siempre
///    corre**: `while` se evalúa después de cada iteración, nunca antes.
/// 2. Al terminar una iteración se construye su *documento de iteración*
///    `{ "input": <input usado>, "output": <output>, "index": <n> }`
///    (índice 0-based) y se evalúa `while` contra él (paths `$.output…`,
///    `$.input…`, `$.index`). Si es falsa, el nodo termina bien.
/// 3. Si es verdadera, el input de la siguiente iteración se construye
///    resolviendo el shape `next` (reglas `@.`) contra ese mismo documento;
///    sin `next`, la siguiente recibe el output anterior tal cual.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopNode {
    /// Id de la tarea (o perfil) a invocar en cada iteración
    pub task: TaskId,
    /// Mapping del input de la PRIMERA iteración (reglas `$.` contra el
    /// contexto); sin él, el token del predecesor
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    /// Shape (`@.`) que construye el input de cada iteración siguiente,
    /// resuelto contra el documento de iteración `{ input, output, index }`.
    /// Sin `next`, la siguiente iteración recibe el output anterior tal cual.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<serde_json::Value>,
    /// Condición de continuación, evaluada tras cada iteración contra el
    /// documento de iteración: `true` → otra iteración
    #[serde(rename = "while")]
    pub while_: Condition,
    /// Tope duro de iteraciones (obligatorio: garantiza terminación)
    pub max_iterations: u32,
    /// Qué pasa si se alcanza el tope con `while` aún verdadera
    #[serde(default)]
    pub on_max: OnMax,
    /// Forma del output del nodo
    #[serde(default)]
    pub collect: Collect,
    /// Política de reintentos por iteración
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
    /// Tiempo máximo por intento de cada iteración, en milisegundos
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

/// Comportamiento al alcanzar `max_iterations` con `while` aún verdadera.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnMax {
    /// El nodo falla con `LOOP_MAX_ITERATIONS_EXCEEDED` (default: truncar
    /// datos en silencio es peligroso; el fallo rutea por `on: error`)
    #[default]
    Fail,
    /// El nodo termina bien con lo acumulado hasta el tope
    Stop,
}

/// Forma del output de un nodo loop.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Collect {
    /// Solo el output de la última iteración (default: memoria acotada)
    #[default]
    Last,
    /// Array con los outputs de todas las iteraciones, en orden
    All,
}
