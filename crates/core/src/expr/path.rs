//! Motor JSONPath compartido por las tres convenciones de expresión
//! (mappings `$.`, shapes `@.` y condiciones) y cache global de regex.
//!
//! Centralizar el acceso a la librería de JSONPath aquí permite cambiar de
//! implementación (o versión) en un solo lugar y garantiza la misma
//! semántica de resolución en todo el engine: paths singulares, primer
//! match gana.

use jsonpath_rust::JsonPath;
use serde_json::Value;

use crate::error::{WorkflowError, codes};

/// Resuelve un path JSONPath contra un documento y devuelve el primer
/// match, o `None` si el path no resuelve. `Err` solo por paths que no
/// parsean (error de definición, no de datos): el mensaje incluye el
/// detalle del parser para que el sitio de llamada lo contextualice.
pub fn query_first<'a>(doc: &'a Value, path: &str) -> Result<Option<&'a Value>, String> {
    let matches = doc.query(path).map_err(|e| e.to_string())?;
    Ok(matches.into_iter().next())
}

/// Compila una regex con cache global del proceso: los workflows evalúan
/// los mismos patrones estáticos una y otra vez (retries, ejecuciones).
pub fn cached_regex(pattern: &str) -> Result<regex::Regex, WorkflowError> {
    use std::collections::HashMap;
    use std::sync::{OnceLock, RwLock};

    static CACHE: OnceLock<RwLock<HashMap<String, regex::Regex>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| RwLock::new(HashMap::new()));

    if let Some(re) = cache.read().expect("regex cache poisoned").get(pattern) {
        return Ok(re.clone());
    }
    let re = regex::Regex::new(pattern).map_err(|e| {
        WorkflowError::new(
            codes::INVALID_REGEX,
            format!("Regex '{}' inválida en condición: {}", pattern, e),
        )
    })?;
    cache
        .write()
        .expect("regex cache poisoned")
        .insert(pattern.to_string(), re.clone());
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
