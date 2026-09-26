use super::*;
use crate::compiler::PreparedBody;
use control::ScopeExecution;
use futures::{StreamExt, stream::FuturesUnordered};
use steps::{StepError, expected};

/// Inputs are built only when a child is admitted, not copied for an entire batch.
pub(super) enum GroupSource {
    Parallel {
        branches: Vec<(String, Arc<PreparedBody>)>,
        input: Value,
    },
    Foreach {
        items: Vec<Value>,
        context: Value,
        body: Arc<PreparedBody>,
    },
}
impl GroupSource {
    fn len(&self) -> usize {
        match self {
            Self::Parallel { branches, .. } => branches.len(),
            Self::Foreach { items, .. } => items.len(),
        }
    }
    fn child(&self, index: usize) -> (String, Value, Arc<PreparedBody>) {
        match self {
            Self::Parallel { branches, input } => (
                branches[index].0.clone(),
                input.clone(),
                branches[index].1.clone(),
            ),
            Self::Foreach {
                items,
                context,
                body,
            } => (
                index.to_string(),
                json!({"item":items[index],"index":index,"context":context}),
                body.clone(),
            ),
        }
    }
    fn label(&self, index: usize) -> String {
        match self {
            Self::Parallel { branches, .. } => branches[index].0.clone(),
            Self::Foreach { .. } => index.to_string(),
        }
    }
}
pub(super) struct GroupSpec {
    pub source: GroupSource,
    pub concurrency: usize,
    pub errors: GroupErrors,
    pub frame: ControlFrame,
}

fn progress(frame: &mut ControlFrame, next: usize, stop: Option<&ForgeError>) {
    let (cursor, stopped, error) = match frame {
        ControlFrame::Parallel {
            next_index,
            stopped,
            error,
            ..
        }
        | ControlFrame::Foreach {
            next_index,
            stopped,
            error,
            ..
        } => (next_index, stopped, error),
        _ => unreachable!("group frame is prepared"),
    };
    *cursor = (*cursor).max(next);
    if let Some(reason) = stop {
        *stopped = true;
        if error.is_none() {
            *error = Some(reason.clone());
        }
    }
}
fn saved(frame: &ControlFrame) -> (usize, bool, Option<ForgeError>) {
    match frame {
        ControlFrame::Parallel {
            next_index,
            stopped,
            error,
            ..
        }
        | ControlFrame::Foreach {
            next_index,
            stopped,
            error,
            ..
        } => (*next_index, *stopped, error.clone()),
        _ => unreachable!("group frame is prepared"),
    }
}

pub(super) async fn execute(
    scope: &ScopeExecution,
    node: &str,
    key: &str,
    mut spec: GroupSpec,
) -> Result<Vec<(String, Value)>, StepError> {
    let limits = &scope.shared.composition.limits;
    let foreach = matches!(spec.source, GroupSource::Foreach { .. });
    let (admitted, stopped, original_error) = saved(&spec.frame);
    let total = if stopped {
        admitted.min(spec.source.len())
    } else {
        spec.source.len()
    };
    let cancellation = scope.cancellation.child_token();
    if stopped {
        cancellation.cancel();
    }
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let mut active = FuturesUnordered::new();
    let mut results: Vec<Option<Value>> = (0..total).map(|_| None).collect();
    let mut next = 0;
    let mut halted = false;
    let mut failed = original_error.map(expected);
    let mut bytes = 2usize;
    loop {
        for index in planner::ready_range(next, total, active.len(), spec.concurrency, halted) {
            let permit = match scope.shared.scope_slots.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    let error = ForgeError::new(
                        "resource.limit",
                        "Concurrent child scope budget is exhausted",
                    );
                    progress(&mut spec.frame, next, Some(&error));
                    save_progress(scope, key, &spec.frame).await?;
                    failed = Some(expected(error));
                    halted = true;
                    cancellation.cancel();
                    break;
                }
            };
            next = index + 1;
            progress(&mut spec.frame, next, None);
            save_progress(scope, key, &spec.frame).await?;
            let (label, input, body) = spec.source.child(index);
            let kind = if foreach { "items" } else { "branches" };
            let child = scope.child(node, &[kind, &label], input, cancellation.child_token());
            active.push(async move {
                let result = control::execute_body(child, body).await;
                drop(permit);
                (index, result)
            });
        }
        let Some((index, result)) = active.next().await else {
            break;
        };
        let value = match result {
            Ok(output) => json!({"status":"succeeded","output":output}),
            Err(StepError::Infrastructure(error)) => return Err(error.into()),
            Err(StepError::Execution(error)) => {
                if error.code() == "effect.unknown" {
                    // Unknown is not a collectable failure. Independent active children
                    // may settle; a fail-fast group pauses admission until resolution.
                    if spec.errors == GroupErrors::FailFast {
                        halted = true;
                    }
                    failed = Some(expected(state::unknown_error()));
                } else if spec.errors == GroupErrors::FailFast || scope.cancellation.is_cancelled()
                {
                    progress(&mut spec.frame, next, Some(&error));
                    save_progress(scope, key, &spec.frame).await?;
                    if failed.is_none() {
                        failed = Some(expected(error.clone()));
                    }
                    halted = true;
                    cancellation.cancel();
                }
                json!({"status":"failed","error":error})
            }
        };
        let value = if foreach {
            let mut value = value;
            value["index"] = json!(index);
            value
        } else {
            value
        };
        bytes = bytes
            .saturating_add(state::json_bytes(&value))
            .saturating_add(spec.source.label(index).len() + 4);
        if bytes > limits.value_bytes {
            let error = ForgeError::new("resource.limit", "Group result exceeds its JSON budget");
            progress(&mut spec.frame, next, Some(&error));
            save_progress(scope, key, &spec.frame).await?;
            if failed.is_none() {
                failed = Some(expected(error));
            }
            halted = true;
            cancellation.cancel();
        } else {
            results[index] = Some(value);
        }
    }
    let view = state::view(&scope.shared, &scope.run_id, Some(key)).await?;
    let run = view.head;
    if view.unresolved_descendants {
        return Err(expected(state::unknown_error()));
    }
    if let Some(failure) = failed {
        return Err(failure);
    }
    if scope.cancellation.is_cancelled() || run.cancel_requested {
        return Err(expected(ForgeError::new(
            "operation.cancelled",
            "Group was cancelled",
        )));
    }
    results
        .into_iter()
        .enumerate()
        .map(|(index, value)| {
            value
                .map(|value| (spec.source.label(index), value))
                .ok_or_else(|| {
                    StepError::Infrastructure(ForgeError::new(
                        "state.conflict",
                        "Group has an unsettled activated child",
                    ))
                })
        })
        .collect()
}

// Losing group progress while children are live must fail supervision: dropping
// those futures cannot be reported as an ordinary settled execution failure.
// Recovery classifies every persisted unfinished operation before redispatch.
async fn save_progress(
    scope: &ScopeExecution,
    key: &str,
    frame: &ControlFrame,
) -> Result<(), StepError> {
    control::update_frame(scope, key, frame)
        .await
        .map_err(|error| match error {
            StepError::Execution(error) | StepError::Infrastructure(error) => {
                StepError::Infrastructure(error)
            }
        })
}
