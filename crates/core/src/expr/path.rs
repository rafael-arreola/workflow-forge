//! JSONPath engine shared by the three expression conventions
//! (mappings `$.`, shapes `@.`, and conditions) and global regex cache.
//!
//! Centralizing access to the JSONPath library here allows switching
//! implementations (or versions) in one place and guarantees the same
//! resolution semantics across the entire engine: singular paths, first
//! match wins.

use std::collections::HashMap;
use std::sync::OnceLock;

use jsonpath_rust::JsonPath;
use parking_lot::RwLock;
use serde_json::Value;

use crate::error::{WorkflowError, codes};

/// Resolves a JSONPath path against a document and returns the first
/// match, or `None` if the path does not resolve. `Err` only for paths that
/// do not parse (definition error, not data error): the message includes the
/// parser detail so the call site can contextualize it.
///
/// Uses a process-global validated path cache to eliminate path validation overhead.
pub fn query_first<'a>(doc: &'a Value, path: &str) -> Result<Option<&'a Value>, String> {
    static VALID_PATHS: OnceLock<RwLock<HashMap<String, bool>>> = OnceLock::new();
    let cache = VALID_PATHS.get_or_init(|| RwLock::new(HashMap::new()));

    let known_valid = {
        let read = cache.read();
        read.get(path).copied()
    };

    if known_valid == Some(false) {
        return Err(format!("Invalid JSONPath: {}", path));
    }

    let matches = match doc.query(path) {
        Ok(m) => {
            if known_valid.is_none() {
                cache.write().insert(path.to_string(), true);
            }
            m
        }
        Err(e) => {
            if known_valid.is_none() {
                cache.write().insert(path.to_string(), false);
            }
            return Err(e.to_string());
        }
    };

    Ok(matches.into_iter().next())
}

/// Compiles a regex with process-global cache: workflows evaluate
/// the same static patterns over and over (retries, executions).
pub fn cached_regex(pattern: &str) -> Result<regex::Regex, WorkflowError> {
    static CACHE: OnceLock<RwLock<HashMap<String, regex::Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| RwLock::new(HashMap::new()));

    if let Some(re) = cache.read().get(pattern) {
        return Ok(re.clone());
    }
    let re = regex::Regex::new(pattern).map_err(|e| {
        WorkflowError::new(
            codes::INVALID_REGEX,
            format!("Invalid regex '{}' in condition: {}", pattern, e),
        )
    })?;
    cache.write().insert(pattern.to_string(), re.clone());
    Ok(re)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn primer_match_y_ausentes() {
        let doc = json!({ "a": { "b": [1, 2] } });
        assert_eq!(query_first(&doc, "$.a.b[0]").unwrap(), Some(&json!(1)));
        assert_eq!(query_first(&doc, "$.no.existe").unwrap(), None);
        assert!(query_first(&doc, "$.[").is_err());
    }

    #[test]
    fn regex_invalida_es_error() {
        assert!(cached_regex("[a-z]+").is_ok());
        let err = cached_regex("(sin cerrar").unwrap_err();
        assert_eq!(err.code, "INVALID_REGEX");
    }
}
