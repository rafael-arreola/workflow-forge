use workflow_forge_protocol::{Diagnostic, ForgeError};

pub struct Failure {
    pub error: ForgeError,
    pub exit: u8,
}
pub type Result<T> = std::result::Result<T, Failure>;

impl Failure {
    pub fn new(code: &str, message: &str) -> Self {
        Self {
            error: Diagnostic::new(code, message).at("client", None, "").into(),
            exit: 1,
        }
    }
    pub fn pending(code: &str, message: &str) -> Self {
        Self {
            exit: 3,
            ..Self::new(code, message)
        }
    }
}
impl From<ForgeError> for Failure {
    fn from(error: ForgeError) -> Self {
        Self { error, exit: 1 }
    }
}

pub fn transport(_: impl std::fmt::Display) -> Failure {
    // reqwest/IO parser messages may carry URLs or other caller-controlled text.
    Failure::new(
        "cli.transport",
        "HTTP exchange did not complete; accepted work is not cancelled",
    )
}
