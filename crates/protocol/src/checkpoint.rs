use crate::{CHECKPOINT_FORMAT, ForgeError, InvocationState, RunSnapshot, RunState};

fn conflict() -> ForgeError {
    ForgeError::new(
        "state.conflict",
        "Transition does not match the current run revision",
    )
}

impl RunSnapshot {
    /// Pure storage invariants, shared by providers before their atomic write.
    pub fn validate_initial(&self) -> Result<(), ForgeError> {
        if self.revision != 0
            || self.checkpoint_format != CHECKPOINT_FORMAT
            || self.state != RunState::Accepted
            || !self.invocations.is_empty()
            || !self.waits.is_empty()
            || self.output.is_some()
            || self.error.is_some()
            || self.finished_at_ms.is_some()
            || self.cancel_requested
            || !self.audit.is_empty()
            || !self.unresolved_effects.is_empty()
            || self.deadline_at_ms < self.created_at_ms
            || self.artifacts.len() > 1000
            || self.artifacts.iter().any(|r| r.scope != self.scope)
            || self
                .artifacts
                .iter()
                .map(|r| &r.id)
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.artifacts.len()
        {
            return Err(conflict());
        }
        Ok(())
    }

    /// Must run in the same transaction/critical section as the replacement.
    /// Terminal checkpoints permit append-only audit, never another result.
    pub fn validate_successor(&self, expected: u64, next: &Self) -> Result<(), ForgeError> {
        next.validate_waits(self)?;
        let terminal_changed = if self.state.is_terminal() {
            let mut stable = next.clone();
            stable.revision = self.revision;
            stable.audit = self.audit.clone();
            stable != *self
        } else {
            false
        };
        if self.revision != expected
            || Some(next.revision) != expected.checked_add(1)
            || self.id != next.id
            || self.scope != next.scope
            || self.actor != next.actor
            || self.resources != next.resources
            || self.definition != next.definition
            || self.package != next.package
            || self.input != next.input
            || self.artifacts != next.artifacts
            || self.created_at_ms != next.created_at_ms
            || self.deadline_at_ms != next.deadline_at_ms
            || self.checkpoint_format != next.checkpoint_format
            || (self.cancel_requested && !next.cancel_requested)
            || self.invocations.iter().any(|(id, invocation)| {
                invocation.state == InvocationState::Succeeded
                    && next.invocations.get(id) != Some(invocation)
            })
            || !next.audit.starts_with(&self.audit)
            || terminal_changed
        {
            return Err(conflict());
        }
        Ok(())
    }
}
