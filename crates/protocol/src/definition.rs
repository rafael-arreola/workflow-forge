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

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct NodeDefinition {
    pub id: String,
    pub input: Binding,
    #[serde(flatten)]
    pub instruction: Instruction,
}

// Parse common fields before the tagged instruction so unknown fields are still
// rejected. serde's flattened enums plus deny_unknown_fields do not guarantee this.
impl<'de> Deserialize<'de> for NodeDefinition {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;
        let mut object = serde_json::Map::<String, Value>::deserialize(deserializer)?;
        let id = serde_json::from_value(
            object
                .remove("id")
                .ok_or_else(|| D::Error::missing_field("id"))?,
        )
        .map_err(D::Error::custom)?;
        let input = serde_json::from_value(
            object
                .remove("input")
                .ok_or_else(|| D::Error::missing_field("input"))?,
        )
        .map_err(D::Error::custom)?;
        let instruction =
            serde_json::from_value(Value::Object(object)).map_err(D::Error::custom)?;
        Ok(Self {
            id,
            input,
            instruction,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Instruction {
    Operation {
        operation: OperationRevision,
        config: Value,
        #[serde(default, skip_serializing_if = "default_retry")]
        retry: crate::RetryPolicy,
    },
    Decision {
        cases: Vec<DecisionCase>,
        #[serde(
            default,
            deserialize_with = "present_fallback",
            skip_serializing_if = "Option::is_none"
        )]
        fallback: Option<FallbackCase>,
    },
    Parallel {
        branches: BTreeMap<String, BodyDefinition>,
        concurrency: usize,
        errors: GroupErrors,
        join: JoinPolicy,
    },
    Foreach {
        items: Binding,
        body: BodyDefinition,
        concurrency: usize,
        errors: GroupErrors,
    },
    Loop {
        r#while: Binding,
        body: BodyDefinition,
        max_iterations: usize,
        on_limit: LoopLimit,
    },
    Subworkflow {
        workflow: WorkflowRevision,
    },
}

fn default_retry(policy: &crate::RetryPolicy) -> bool {
    policy == &crate::RetryPolicy::default()
}

fn present_fallback<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<FallbackCase>, D::Error> {
    FallbackCase::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BodyDefinition {
    pub entry: String,
    pub nodes: Vec<NodeDefinition>,
    pub edges: Vec<Edge>,
    pub output: Binding,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DecisionCase {
    pub id: String,
    pub when: Binding,
    pub body: BodyDefinition,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FallbackCase {
    pub id: String,
    pub body: BodyDefinition,
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkflowRevision {
    pub id: String,
    pub revision: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupErrors {
    FailFast,
    Collect,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinPolicy {
    All,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoopLimit {
    Fail,
    ReturnLast,
}

impl Instruction {
    pub fn operation_revision(&self) -> Option<&OperationRevision> {
        match self {
            Self::Operation { operation, .. } => Some(operation),
            _ => None,
        }
    }
    pub fn operation_revision_mut(&mut self) -> Option<&mut OperationRevision> {
        match self {
            Self::Operation { operation, .. } => Some(operation),
            _ => None,
        }
    }
    fn normalize(&mut self) {
        match self {
            Self::Decision { cases, fallback } => {
                for case in cases {
                    case.body.normalize();
                }
                if let Some(fallback) = fallback {
                    fallback.body.normalize();
                }
            }
            Self::Parallel { branches, .. } => {
                for body in branches.values_mut() {
                    body.normalize();
                }
            }
            Self::Foreach { body, .. } | Self::Loop { body, .. } => body.normalize(),
            Self::Operation { .. } | Self::Subworkflow { .. } => (),
        }
    }
}
impl BodyDefinition {
    fn normalize(&mut self) {
        for node in &mut self.nodes {
            node.instruction.normalize();
        }
        self.nodes.sort_by(|a, b| a.id.cmp(&b.id));
        self.edges.sort();
    }
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
        for node in &mut definition.nodes {
            node.instruction.normalize();
        }
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
