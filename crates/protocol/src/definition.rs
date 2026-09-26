use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationRevision {
    pub id: String,
    pub contract: String,
    pub implementation: String,
}

impl OperationRevision {
    pub fn new(id: &str, contract: &str, implementation: &str) -> Self {
        Self {
            id: id.into(),
            contract: contract.into(),
            implementation: implementation.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Binding {
    Literal(Value),
    Select(Selection),
    Object(BTreeMap<String, Binding>),
    Array(Vec<Binding>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataSource {
    Input,
    Node,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Selection {
    pub source: DataSource,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub node: Option<String>,
    pub pointer: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub fallback: Option<Box<Binding>>,
}

fn present<'de, T: Deserialize<'de>, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<T>, D::Error> {
    T::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeDefinition {
    pub id: String,
    pub kind: String,
    pub operation: OperationRevision,
    pub config: Value,
    pub input: Binding,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub from: String,
    pub to: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowDefinition {
    pub format: String,
    pub id: String,
    pub revision: String,
    pub schema_dialect: String,
    pub input_schema: Value,
    pub output_schema: Value,
    pub entry: String,
    pub nodes: Vec<NodeDefinition>,
    pub edges: Vec<Edge>,
    pub output: Binding,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub presentation: BTreeMap<String, Value>,
}

impl WorkflowDefinition {
    /// Normalization is only valid after rejecting duplicate node/edge identities.
    pub fn semantic_value(&self) -> Value {
        let mut definition = self.clone();
        definition.presentation.clear();
        definition.nodes.sort_by(|a, b| a.id.cmp(&b.id));
        definition.edges.sort();
        // Serialization of these concrete JSON DTOs is infallible.
        serde_json::to_value(definition).expect("workflow DTO serializes")
    }
}

/// A schema package resource is resolved by identity, never by downloading its URI.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaResource {
    pub uri: String,
    pub revision: String,
    pub schema: Value,
}
