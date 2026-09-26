use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use workflow_forge_protocol::*;

fn invalid(message: &str) -> ForgeError {
    ForgeError::new("mapping.invalid", message)
}

fn tokens(pointer: &str) -> Result<Vec<String>, ForgeError> {
    if pointer.is_empty() {
        return Ok(vec![]);
    }
    if !pointer.starts_with('/') {
        return Err(invalid("JSON Pointer must be empty or start with /"));
    }
    pointer[1..]
        .split('/')
        .map(|token| {
            let mut chars = token.chars();
            let mut decoded = String::new();
            while let Some(c) = chars.next() {
                if c != '~' {
                    decoded.push(c);
                    continue;
                }
                decoded.push(match chars.next() {
                    Some('0') => '~',
                    Some('1') => '/',
                    _ => return Err(invalid("Invalid JSON Pointer escape")),
                });
            }
            Ok(decoded)
        })
        .collect()
}

pub(crate) fn check(
    binding: &Binding,
    available: &BTreeSet<String>,
    limits: &Limits,
) -> Result<(), ForgeError> {
    fn visit(
        b: &Binding,
        available: &BTreeSet<String>,
        limits: &Limits,
        depth: usize,
        steps: &mut usize,
    ) -> Result<(), ForgeError> {
        *steps += 1;
        if depth > limits.binding_depth || *steps > limits.binding_steps {
            return Err(invalid("Binding exceeds its evaluation budget"));
        }
        match b {
            Binding::Select(s) => {
                *steps += tokens(&s.pointer)?.len();
                match (&s.source, &s.node) {
                    (DataSource::Input, None) => (),
                    (DataSource::Node, Some(node)) if available.contains(node) => (),
                    _ => {
                        return Err(invalid(
                            "Producer is unknown or unavailable at this control point",
                        ));
                    }
                }
                if let Some(fallback) = &s.fallback {
                    visit(fallback, available, limits, depth + 1, steps)?;
                }
            }
            Binding::Object(fields) => {
                for b in fields.values() {
                    visit(b, available, limits, depth + 1, steps)?;
                }
            }
            Binding::Array(items) => {
                for b in items {
                    visit(b, available, limits, depth + 1, steps)?;
                }
            }
            Binding::Literal(v) => {
                crate::schema::check_value(v, limits.value_bytes, limits.json_depth)?;
            }
        }
        if *steps > limits.binding_steps {
            return Err(invalid("Binding exceeds its evaluation budget"));
        }
        Ok(())
    }
    visit(binding, available, limits, 1, &mut 0)
}

pub(crate) fn evaluate(
    binding: &Binding,
    input: &Value,
    outputs: &BTreeMap<String, Value>,
    limits: &Limits,
) -> Result<Value, ForgeError> {
    struct Budget {
        steps: usize,
        bytes: usize,
    }
    fn spend(b: &mut Budget, bytes: usize, limits: &Limits) -> Result<(), ForgeError> {
        b.bytes = b.bytes.saturating_add(bytes);
        if b.bytes > limits.value_bytes {
            Err(invalid("Mapping result exceeds its byte budget"))
        } else {
            Ok(())
        }
    }
    fn copy(v: &Value, b: &mut Budget, limits: &Limits) -> Result<Value, ForgeError> {
        spend(
            b,
            serde_json::to_vec(v).expect("JSON serializes").len(),
            limits,
        )?;
        Ok(v.clone())
    }
    fn walk(
        binding: &Binding,
        input: &Value,
        outputs: &BTreeMap<String, Value>,
        limits: &Limits,
        b: &mut Budget,
        depth: usize,
    ) -> Result<Value, ForgeError> {
        b.steps += 1;
        if depth > limits.binding_depth || b.steps > limits.binding_steps {
            return Err(invalid("Binding exceeds its evaluation budget"));
        }
        match binding {
            Binding::Literal(v) => copy(v, b, limits),
            Binding::Select(selection) => {
                let mut current = match selection.source {
                    DataSource::Input => Some(input),
                    DataSource::Node => selection.node.as_ref().and_then(|id| outputs.get(id)),
                };
                for token in tokens(&selection.pointer)? {
                    b.steps += 1;
                    if b.steps > limits.binding_steps {
                        return Err(invalid("JSON Pointer exceeds its evaluation budget"));
                    }
                    current = match current {
                        None => None,
                        Some(Value::Object(map)) => map.get(&token),
                        Some(Value::Array(items)) => {
                            if token == "-" {
                                None
                            } else {
                                if (token.len() > 1 && token.starts_with('0'))
                                    || token.is_empty()
                                    || !token.bytes().all(|c| c.is_ascii_digit())
                                {
                                    return Err(invalid("Invalid array index"));
                                }
                                token.parse::<usize>().ok().and_then(|i| items.get(i))
                            }
                        }
                        Some(_) => {
                            return Err(invalid(
                                "Selection traverses a scalar instead of a container",
                            ));
                        }
                    };
                }
                match current {
                    Some(value) => copy(value, b, limits),
                    None => match &selection.fallback {
                        Some(fallback) => walk(fallback, input, outputs, limits, b, depth + 1),
                        None => Err(ForgeError::new(
                            "mapping.missing",
                            "Selection has no value and no fallback",
                        )),
                    },
                }
            }
            Binding::Object(fields) => {
                spend(b, 2 + fields.len().saturating_sub(1), limits)?;
                let mut value = Map::new();
                for (key, inner) in fields {
                    spend(
                        b,
                        serde_json::to_vec(key).expect("string serializes").len() + 1,
                        limits,
                    )?;
                    value.insert(
                        key.clone(),
                        walk(inner, input, outputs, limits, b, depth + 1)?,
                    );
                }
                Ok(Value::Object(value))
            }
            Binding::Array(items) => {
                spend(b, 2 + items.len().saturating_sub(1), limits)?;
                items
                    .iter()
                    .map(|item| walk(item, input, outputs, limits, b, depth + 1))
                    .collect::<Result<Vec<_>, _>>()
                    .map(Value::Array)
            }
        }
    }
    let value = walk(
        binding,
        input,
        outputs,
        limits,
        &mut Budget { steps: 0, bytes: 0 },
        1,
    )?;
    crate::schema::check_value(&value, limits.value_bytes, limits.json_depth)?;
    Ok(value)
}
