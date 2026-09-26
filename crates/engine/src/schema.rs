use jsonschema::{Draft, PatternOptions, Retrieve, Uri};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use workflow_forge_protocol::*;

pub(crate) fn check_value(
    value: &Value,
    max_bytes: usize,
    max_depth: usize,
) -> Result<usize, ForgeError> {
    fn walk(v: &Value, depth: usize, max: usize, nodes: &mut usize) -> Result<(), ForgeError> {
        if depth > max {
            return Err(ForgeError::new(
                "data.invalid",
                "JSON exceeds its depth limit",
            ));
        }
        *nodes += 1;
        match v {
            Value::Array(a) => {
                for v in a {
                    walk(v, depth + 1, max, nodes)?;
                }
            }
            Value::Object(o) => {
                for v in o.values() {
                    walk(v, depth + 1, max, nodes)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
    let mut nodes = 0;
    walk(value, 1, max_depth, &mut nodes)?;
    if serde_json::to_vec(value).expect("JSON serializes").len() > max_bytes {
        return Err(ForgeError::new(
            "data.invalid",
            "JSON exceeds its byte limit",
        ));
    }
    Ok(nodes)
}

#[derive(Clone, Default)]
pub(crate) struct OfflineSchemas(pub Arc<BTreeMap<String, Value>>);
impl Retrieve for OfflineSchemas {
    fn retrieve(
        &self,
        uri: &Uri<String>,
    ) -> Result<Value, Box<dyn std::error::Error + Send + Sync>> {
        self.0.get(uri.as_str()).cloned().ok_or_else(|| {
            Box::new(ForgeError::new(
                "reference.missing",
                "Schema resource is not registered",
            )) as _
        })
    }
}

pub(crate) struct CompiledSchema {
    validator: jsonschema::Validator,
    cost: usize,
}
impl CompiledSchema {
    pub fn compile(
        schema: &Value,
        resources: &OfflineSchemas,
        limits: &Limits,
    ) -> Result<Self, ForgeError> {
        check_value(schema, limits.document_bytes, limits.json_depth)?;
        let mut active = BTreeSet::new();
        let mut cost = 0;
        inspect(schema, schema, resources, &mut active, &mut cost, 0)?;
        let validator = jsonschema::options()
            .with_draft(Draft::Draft202012)
            .should_validate_formats(false)
            .with_pattern_options(PatternOptions::regex().size_limit(1024 * 1024))
            .with_retriever(resources.clone())
            .build(schema)
            .map_err(|_| {
                ForgeError::new(
                    "schema.invalid",
                    "Schema or reference is invalid or unsupported by the validation profile",
                )
            })?;
        Ok(Self { validator, cost })
    }
    pub fn validate(&self, value: &Value, limits: &Limits) -> Result<(), ForgeError> {
        let nodes = check_value(value, limits.value_bytes, limits.json_depth)?;
        let work =
            nodes.saturating_add(serde_json::to_vec(value).expect("JSON serializes").len() / 64);
        if work.saturating_mul(self.cost) > 1_000_000 {
            return Err(ForgeError::new(
                "data.invalid",
                "Schema evaluation exceeds its work budget",
            ));
        }
        if let Err(error) = self.validator.validate(value) {
            let mut diagnostic =
                Diagnostic::new("data.invalid", "Data does not satisfy its schema");
            diagnostic.location.data = Some(error.instance_path().to_string());
            return Err(diagnostic.into());
        }
        Ok(())
    }
}

// Bound expansion before handing schemas to the validator. Recursive/dynamic
// schemas are an explicit unsupported capability in this first validation profile.
fn inspect(
    schema: &Value,
    root: &Value,
    resources: &OfflineSchemas,
    active: &mut BTreeSet<String>,
    cost: &mut usize,
    depth: usize,
) -> Result<(), ForgeError> {
    *cost += 1;
    if *cost > 10_000 || depth > 64 {
        return Err(ForgeError::new(
            "capability.unsupported",
            "Schema expansion exceeds the validation profile",
        ));
    }
    if schema.is_boolean() {
        return Ok(());
    }
    let obj = schema
        .as_object()
        .ok_or_else(|| ForgeError::new("schema.invalid", "Expected a schema object or boolean"))?;
    if depth > 0 && obj.contains_key("$id") {
        return Err(ForgeError::new(
            "capability.unsupported",
            "Nested schema identifiers are outside the initial validation profile",
        ));
    }
    if let Some(pattern) = obj.get("pattern").and_then(Value::as_str) {
        *cost = cost.saturating_add(pattern.len());
    }
    if obj
        .get("$schema")
        .is_some_and(|v| v.as_str() != Some(SCHEMA_DIALECT))
    {
        return Err(ForgeError::new(
            "capability.unsupported",
            "Unsupported schema dialect",
        ));
    }
    if obj.contains_key("$dynamicRef") || obj.contains_key("$dynamicAnchor") {
        return Err(ForgeError::new(
            "capability.unsupported",
            "Dynamic schema references are not in the initial validation profile",
        ));
    }
    if let Some(vocabularies) = obj.get("$vocabulary").and_then(Value::as_object) {
        for (uri, required) in vocabularies {
            if required == &Value::Bool(true)
                && ![
                    "core",
                    "applicator",
                    "unevaluated",
                    "validation",
                    "meta-data",
                    "format-annotation",
                    "content",
                ]
                .iter()
                .any(|name| uri == &format!("https://json-schema.org/draft/2020-12/vocab/{name}"))
            {
                return Err(ForgeError::new(
                    "capability.unsupported",
                    "Required schema vocabulary is unsupported",
                ));
            }
        }
    }
    if let Some(reference) = obj.get("$ref") {
        let reference = reference.as_str().ok_or_else(|| {
            ForgeError::new("schema.invalid", "Schema reference must be a string")
        })?;
        let (uri, fragment) = reference.split_once('#').unwrap_or((reference, ""));
        if !fragment.is_empty() && !fragment.starts_with('/') {
            return Err(ForgeError::new(
                "capability.unsupported",
                "Named schema anchors are outside the initial validation profile",
            ));
        }
        let document = if uri.is_empty() {
            root
        } else {
            resources.0.get(uri).ok_or_else(|| {
                ForgeError::new("reference.missing", "Schema resource is not registered")
            })?
        };
        let identity = format!("{:p}:{fragment}", document);
        if !active.insert(identity.clone()) {
            return Err(ForgeError::new(
                "capability.unsupported",
                "Recursive schemas require a different validation profile",
            ));
        }
        let target = document.pointer(fragment).ok_or_else(|| {
            ForgeError::new("reference.missing", "Schema fragment does not exist")
        })?;
        inspect(target, document, resources, active, cost, depth + 1)?;
        active.remove(&identity);
    }
    for key in ["properties", "patternProperties", "dependentSchemas"] {
        if let Some(map) = obj.get(key).and_then(Value::as_object) {
            for child in map.values() {
                inspect(child, root, resources, active, cost, depth + 1)?;
            }
        }
    }
    for key in [
        "items",
        "additionalProperties",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "unevaluatedProperties",
        "unevaluatedItems",
    ] {
        if let Some(child) = obj.get(key) {
            inspect(child, root, resources, active, cost, depth + 1)?;
        }
    }
    for key in ["prefixItems", "allOf", "anyOf", "oneOf"] {
        if let Some(items) = obj.get(key).and_then(Value::as_array) {
            for child in items {
                inspect(child, root, resources, active, cost, depth + 1)?;
            }
        }
    }
    Ok(())
}
