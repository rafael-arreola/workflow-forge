use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PrimitiveValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<PrimitiveValue>),
    Object(serde_json::Map<String, PrimitiveValue>),
}

/// Mapa de valores transmitidos entre puertos
/// key = nombre del puerto, value = dato
pub type PrimitiveMap = HashMap<String, PrimitiveValue>;
