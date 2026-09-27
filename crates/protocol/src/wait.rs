use crate::{ArtifactRef, ControlFrame, ForgeError, InvocationState, RunId, RunSnapshot};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignalCommand {
    pub run_id: RunId,
    pub wait_id: String,
    pub message_id: String,
    pub correlation: String,
    pub payload: Value,
    #[serde(default)]
    pub artifacts: Vec<ArtifactRef>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalReceipt {
    pub run_id: RunId,
    pub wait_id: String,
    pub message_id: String,
    pub accepted_at_ms: u64,
    pub durable: bool,
    pub duplicate: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SignalDelivery {
    pub command: SignalCommand,
    pub actor: String,
    pub receipt: SignalReceipt,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitKind {
    Timer,
    Signal {
        correlation: String,
        payload_schema: Value,
        start_node: Option<String>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitState {
    Open,
    Consumed,
    Expired,
    Closed,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WaitRecord {
    pub id: String,
    pub node: String,
    pub kind: WaitKind,
    pub created_at_ms: u64,
    pub deadline_at_ms: u64,
    pub start_confirmed: bool,
    pub state: WaitState,
    pub delivery: Option<SignalDelivery>,
}

impl RunSnapshot {
    pub fn next_wakeup_at_ms(&self) -> Option<u64> {
        let pending = self
            .waits
            .values()
            .filter(|w| w.state == WaitState::Open)
            .filter_map(|w| {
                if w.delivery.is_some() {
                    w.start_confirmed.then_some(0)
                } else {
                    Some(w.deadline_at_ms)
                }
            })
            .min();
        if self.state == crate::RunState::Waiting {
            Some(
                pending
                    .unwrap_or(self.deadline_at_ms)
                    .min(self.deadline_at_ms),
            )
        } else {
            pending
        }
    }
    pub fn close_waits(&mut self) {
        for wait in self
            .waits
            .values_mut()
            .filter(|w| w.state == WaitState::Open)
        {
            wait.state = WaitState::Closed;
        }
    }
    /// Store-independent checks must share the atomic run CAS with the mutation.
    pub fn validate_waits(&self, previous: &Self) -> Result<(), ForgeError> {
        let invalid = || {
            ForgeError::new(
                "state.conflict",
                "Wait transition violates its accepted identity or receipt",
            )
        };
        if previous.waits.keys().any(|id| !self.waits.contains_key(id)) {
            return Err(invalid());
        }
        if self.state.is_terminal() && self.waits.values().any(|w| w.state == WaitState::Open) {
            return Err(invalid());
        }
        for (key, record) in &self.invocations {
            if let Some(ControlFrame::Wait { id }) = &record.control {
                if self.waits.get(id).is_none_or(|w| &w.node != key) {
                    return Err(invalid());
                }
            }
        }
        for (id, wait) in &self.waits {
            if &wait.id != id
                || wait.deadline_at_ms < wait.created_at_ms
                || wait.deadline_at_ms > self.deadline_at_ms
            {
                return Err(invalid());
            }
            let record = self.invocations.get(&wait.node).ok_or_else(invalid)?;
            if record.control != Some(ControlFrame::Wait { id: id.clone() }) {
                return Err(invalid());
            }
            if record.state == InvocationState::Succeeded && wait.state != WaitState::Consumed {
                return Err(invalid());
            }
            if let Some(old) = previous.waits.get(id) {
                if old.id != wait.id
                    || old.node != wait.node
                    || old.kind != wait.kind
                    || old.created_at_ms != wait.created_at_ms
                    || old.deadline_at_ms != wait.deadline_at_ms
                    || (old.start_confirmed && !wait.start_confirmed)
                    || (old.state != WaitState::Open && old != wait)
                    || (old.delivery.is_some() && old.delivery != wait.delivery)
                {
                    return Err(invalid());
                }
            } else if wait.state != WaitState::Open || wait.delivery.is_some() {
                return Err(invalid());
            }
            match &wait.kind {
                WaitKind::Timer => {
                    if wait.delivery.is_some()
                        || !wait.start_confirmed
                        || wait.state == WaitState::Expired
                    {
                        return Err(invalid());
                    }
                }
                WaitKind::Signal {
                    correlation,
                    start_node,
                    ..
                } => {
                    if correlation.is_empty() || correlation.len() > 256 {
                        return Err(invalid());
                    }
                    if start_node.is_none() && !wait.start_confirmed {
                        return Err(invalid());
                    }
                    if wait.start_confirmed
                        && start_node.as_ref().is_some_and(|key| {
                            self.invocations
                                .get(key)
                                .is_none_or(|r| r.state != InvocationState::Succeeded)
                        })
                    {
                        return Err(invalid());
                    }
                    if let Some(delivery) = &wait.delivery {
                        let cmd = &delivery.command;
                        let receipt = &delivery.receipt;
                        if cmd.run_id != self.id
                            || &cmd.wait_id != id
                            || &cmd.correlation != correlation
                            || cmd.message_id.is_empty()
                            || cmd.message_id.len() > 256
                            || receipt.run_id != self.id
                            || &receipt.wait_id != id
                            || receipt.message_id != cmd.message_id
                            || receipt.duplicate
                            || receipt.accepted_at_ms < wait.created_at_ms
                            || receipt.accepted_at_ms >= wait.deadline_at_ms
                            || wait.state == WaitState::Expired
                            || cmd.artifacts.len() > 1000
                            || cmd.artifacts.iter().any(|a| a.scope != self.scope)
                            || cmd.artifacts.windows(2).any(|p| p[0].id >= p[1].id)
                        {
                            return Err(invalid());
                        }
                    }
                    if wait.state == WaitState::Consumed
                        && (wait.delivery.is_none() || !wait.start_confirmed)
                    {
                        return Err(invalid());
                    }
                }
            }
            if wait.state == WaitState::Consumed && record.state != InvocationState::Succeeded {
                return Err(invalid());
            }
            if wait.state == WaitState::Consumed {
                match &wait.kind {
                    WaitKind::Signal { start_node, .. } => {
                        let started = start_node
                            .as_ref()
                            .and_then(|key| self.invocations.get(key))
                            .and_then(|r| r.output.as_ref())
                            .cloned()
                            .unwrap_or(Value::Null);
                        let output = serde_json::json!({"start":started,"signal":wait.delivery.as_ref().ok_or_else(invalid)?.command.payload});
                        if record.output.as_ref() != Some(&output) {
                            return Err(invalid());
                        }
                    }
                    WaitKind::Timer
                        if previous
                            .waits
                            .get(id)
                            .is_some_and(|w| w.state == WaitState::Open)
                            && record.output.as_ref()
                                != previous.invocations.get(&wait.node).map(|r| &r.input) =>
                    {
                        return Err(invalid());
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }
}
