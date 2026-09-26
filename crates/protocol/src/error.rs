use serde::{Deserialize, Serialize};

/// Locations contain paths and IDs, never the offending data or credentials.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    pub node: Option<String>,
    pub field: String,
    pub data: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub message: String,
    pub phase: String,
    pub location: Location,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_error: Option<crate::OperationError>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retryable: Option<bool>,
}

impl Diagnostic {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            phase: "execution".into(),
            location: Location::default(),
            operation_error: None,
            retryable: None,
        }
    }

    pub fn at(mut self, phase: &str, node: Option<&str>, field: &str) -> Self {
        self.phase = phase.into();
        self.location.node = node.map(str::to_owned);
        self.location.field = field.into();
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ForgeError {
    pub diagnostics: Vec<Diagnostic>,
}

impl ForgeError {
    pub fn new(code: &str, message: &str) -> Self {
        Diagnostic::new(code, message).into()
    }

    pub fn code(&self) -> &str {
        self.diagnostics
            .first()
            .map_or("internal", |d| d.code.as_str())
    }
}

impl From<Diagnostic> for ForgeError {
    fn from(value: Diagnostic) -> Self {
        Self {
            diagnostics: vec![value],
        }
    }
}

impl std::fmt::Display for ForgeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.diagnostics.first() {
            Some(d) => write!(f, "{}: {}", d.code, d.message),
            None => f.write_str("internal: unspecified failure"),
        }
    }
}
impl std::error::Error for ForgeError {}
