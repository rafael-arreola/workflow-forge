//! Type-safe wrapper for sensitive values (passwords, tokens, keys).

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// Type-safe wrapper for sensitive values.
/// Automatically redacts content in `Debug`, `Display`, and Serde `Serialize`.
#[derive(Clone, PartialEq, Eq, Default, schemars::JsonSchema)]
#[schemars(transparent)]
pub struct Secure<T>(T);

impl<T> Secure<T> {
    /// Creates a new `Secure` wrapped value.
    pub fn new(val: T) -> Self {
        Self(val)
    }

    /// Explicitly accesses the underlying sensitive reference.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// Consumes the wrapper and yields the underlying sensitive value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for Secure<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Secure(\"[REDACTED]\")")
    }
}

impl<T> fmt::Display for Secure<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[REDACTED]")
    }
}

impl<T> Serialize for Secure<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str("[REDACTED]")
    }
}

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Secure<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Secure::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_secure_redaction_and_expose() {
        let sec = Secure::new("secret_api_key_123".to_string());
        assert_eq!(format!("{:?}", sec), "Secure(\"[REDACTED]\")");
        assert_eq!(format!("{}", sec), "[REDACTED]");
        assert_eq!(sec.expose(), "secret_api_key_123");

        let json = serde_json::to_string(&sec).unwrap();
        assert_eq!(json, "\"[REDACTED]\"");

        let deserialized: Secure<String> = serde_json::from_str("\"secret_api_key_123\"").unwrap();
        assert_eq!(deserialized.expose(), "secret_api_key_123");
    }
}
