//! Resolución de secretos en documentos JSON: los objetos `{"$secret": "X"}`
//! se reemplazan por el valor que entregue un [`SecretProvider`]. Evita
//! credenciales hardcodeadas en definiciones que se comparten o versionan
//! (perfiles de tarea, principalmente).

use serde_json::Value;

use crate::error::WorkflowError;

/// Fuente de secretos por nombre. La implementación por default es
/// [`EnvSecrets`] (variables de entorno); un host puede inyectar la suya
/// (vault, KMS, etc.).
pub trait SecretProvider: Send + Sync {
    fn get(&self, name: &str) -> Option<String>;
}

/// Resuelve secretos desde las variables de entorno del proceso.
#[derive(Debug, Clone, Copy, Default)]
pub struct EnvSecrets;

impl SecretProvider for EnvSecrets {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }
}

/// Reemplaza recursivamente todo objeto `{"$secret": "NOMBRE"}` (llave única)
/// por el valor del provider. Un secreto no disponible es error
/// `SECRET_NOT_FOUND`.
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
                        "SECRET_NOT_FOUND",
                        format!("El secreto '{name}' no está disponible en el provider"),
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
