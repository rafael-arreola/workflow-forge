//! Pure control decisions. These functions never dispatch, persist or consult I/O.
use super::*;
use steps::StepError;

pub(super) enum NodeAction {
    Confirmed(Value),
    Execute,
}
pub(super) fn node(
    record: Option<&InvocationRecord>,
    cancelled: bool,
    expired: bool,
) -> Result<NodeAction, StepError> {
    if let Some(record) = record {
        match record.state {
            InvocationState::Succeeded => {
                return record
                    .output
                    .clone()
                    .map(NodeAction::Confirmed)
                    .ok_or_else(|| {
                        ForgeError::new("store.failed", "Confirmed node has no output").into()
                    });
            }
            InvocationState::Unknown => return Err(steps::expected(state::unknown_error())),
            InvocationState::Failed | InvocationState::Cancelled => {
                return Err(steps::expected(record.error.clone().unwrap_or_else(|| {
                    ForgeError::new("operation.failed", "Node did not succeed")
                })));
            }
            _ => (),
        }
    }
    if cancelled {
        return Err(steps::expected(ForgeError::new(
            "operation.cancelled",
            "Scope was cancelled",
        )));
    }
    if expired {
        return Err(steps::expected(ForgeError::new(
            "operation.timeout",
            "Run reached its deadline",
        )));
    }
    Ok(NodeAction::Execute)
}

pub(super) fn ready_range(
    next: usize,
    total: usize,
    active: usize,
    concurrency: usize,
    halted: bool,
) -> std::ops::Range<usize> {
    let slots = if halted {
        0
    } else {
        concurrency.saturating_sub(active)
    };
    next..next.saturating_add(slots).min(total)
}

pub(super) fn boolean(
    binding: &Binding,
    input: &Value,
    limits: &Limits,
) -> Result<bool, ForgeError> {
    binding::evaluate(binding, input, &BTreeMap::new(), limits)?
        .as_bool()
        .ok_or_else(|| ForgeError::new("data.invalid", "Control condition must be a boolean"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn group_planning_never_exceeds_capacity_or_admits_after_halt() {
        assert_eq!(ready_range(2, 10, 3, 4, false), 2..3);
        assert_eq!(ready_range(2, 10, 4, 4, false), 2..2);
        assert_eq!(ready_range(2, 10, 0, 4, true), 2..2);
        assert_eq!(ready_range(9, 10, 0, 4, false), 9..10);
    }
}
