use super::*;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    format: u32,
    payload: T,
}
#[derive(Deserialize)]
struct Version {
    format: u32,
}

pub(super) fn encode<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>, ForgeError> {
    let bytes = serde_json::to_vec(&Envelope {
        format: CHECKPOINT_FORMAT,
        payload: value,
    })
    .map_err(|_| corrupt())?;
    if bytes.len() > limit {
        return Err(ForgeError::new(
            "resource.limit",
            "Encoded checkpoint exceeds SQLite record budget",
        ));
    }
    Ok(bytes)
}
pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T, ForgeError> {
    decode_version(bytes, limit, CHECKPOINT_FORMAT)
}
pub(super) fn version(bytes: &[u8], limit: usize) -> Result<u32, ForgeError> {
    if bytes.len() > limit {
        return Err(ForgeError::new(
            "resource.limit",
            "Stored checkpoint exceeds configured record budget",
        ));
    }
    Ok(serde_json::from_slice::<Version>(bytes)
        .map_err(|_| corrupt())?
        .format)
}
pub(super) fn decode_version<T: DeserializeOwned>(
    bytes: &[u8],
    limit: usize,
    expected: u32,
) -> Result<T, ForgeError> {
    if version(bytes, limit)? != expected {
        return Err(unsupported());
    }
    let envelope: Envelope<T> = serde_json::from_slice(bytes).map_err(|_| corrupt())?;
    Ok(envelope.payload)
}
pub(super) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

pub(super) fn state(value: RunState) -> &'static str {
    match value {
        RunState::Accepted => "accepted",
        RunState::Running => "running",
        RunState::Waiting => "waiting",
        RunState::Blocked => "blocked",
        RunState::Cancelling => "cancelling",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Cancelled => "cancelled",
    }
}
pub(super) fn parse_state(value: &str) -> Result<RunState, ForgeError> {
    match value {
        "accepted" => Ok(RunState::Accepted),
        "running" => Ok(RunState::Running),
        "waiting" => Ok(RunState::Waiting),
        "blocked" => Ok(RunState::Blocked),
        "cancelling" => Ok(RunState::Cancelling),
        "succeeded" => Ok(RunState::Succeeded),
        "failed" => Ok(RunState::Failed),
        "cancelled" => Ok(RunState::Cancelled),
        _ => Err(corrupt()),
    }
}
