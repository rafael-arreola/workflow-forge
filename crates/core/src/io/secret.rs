//! Secret resolution in JSON documents: `{"$secret": "X"}` objects
//! are replaced by the value returned by a [`SecretProvider`]. Prevents
//! hardcoded credentials in definitions that are shared or versioned
//! (task profiles, primarily).

use serde_json::Value;

use crate::error::{WorkflowError, codes};

/// Secret source by name. The default implementation is
/// [`EnvSecrets`] (environment variables); a host can inject its own
/// (vault, KMS, etc.).
pub trait SecretProvider: Send + Sync {
    /// Value of the secret `name`, if the provider knows it.
    fn get(&self, name: &str) -> Option<String>;
}

/// Resolves secrets from the process environment variables.
#[derive(Debug, Clone, Copy, Default)]
pub struct EnvSecrets;

impl SecretProvider for EnvSecrets {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Recursively replaces every `{"$secret": "NAME"}` object (single key)
/// with the provider's value. An unavailable secret is a
/// `SECRET_NOT_FOUND` error.
pub fn resolve_secrets(
    value: &mut Value,
    provider: &dyn SecretProvider,
) -> Result<(), WorkflowError> {
    match value {
        Value::Object(map) => {
            if map.len() == 1
                && let Some(Value::String(name)) = map.get("$secret")
            {
                let secret = provider.get(name).ok_or_else(|| {
                    WorkflowError::new(
                        codes::SECRET_NOT_FOUND,
                        format!("Secret '{name}' is not available in the provider"),
                    )
                })?;
                *value = Value::String(secret);
                return Ok(());
            }
            for child in map.values_mut() {
                resolve_secrets(child, provider)?;
            }
            Ok(())
        }
        Value::Array(items) => {
            for item in items {
                resolve_secrets(item, provider)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    struct MapSecrets(HashMap<String, String>);

    impl SecretProvider for MapSecrets {
        fn get(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }

    #[test]
    fn reemplaza_secretos_anidados() {
        let provider = MapSecrets(HashMap::from([("TOKEN".into(), "abc123".into())]));
        let mut doc = json!({
            "auth": { "type": "bearer", "token": { "$secret": "TOKEN" } },
            "lista": [{ "$secret": "TOKEN" }],
            "literal": "sin cambios"
        });
        resolve_secrets(&mut doc, &provider).unwrap();
        assert_eq!(
            doc,
            json!({
                "auth": { "type": "bearer", "token": "abc123" },
                "lista": ["abc123"],
                "literal": "sin cambios"
            })
        );
    }

    #[test]
    fn secreto_ausente_es_error() {
        let provider = MapSecrets(HashMap::new());
        let mut doc = json!({ "token": { "$secret": "NOPE" } });
        let err = resolve_secrets(&mut doc, &provider).unwrap_err();
        assert_eq!(err.code, "SECRET_NOT_FOUND");
    }

    #[test]
    fn objeto_con_mas_llaves_no_es_secreto() {
        let provider = MapSecrets(HashMap::new());
        let mut doc = json!({ "$secret": "X", "otra": 1 });
        resolve_secrets(&mut doc, &provider).unwrap();
        assert_eq!(doc, json!({ "$secret": "X", "otra": 1 }));
    }
}
