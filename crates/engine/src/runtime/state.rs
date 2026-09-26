use super::*;

pub(super) async fn read(shared: &Shared, id: &RunId) -> Result<RunSnapshot, ForgeError> {
    shared
        .composition
        .store
        .get(id)
        .await?
        .filter(|r| r.scope == shared.composition.scope)
        .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))
}

pub(super) async fn view(
    shared: &Shared,
    id: &RunId,
    node: Option<&str>,
) -> Result<ExecutionView, ForgeError> {
    shared
        .composition
        .store
        .view(id, node)
        .await?
        .filter(|view| view.head.scope == shared.composition.scope)
        .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))
}

/// Only a node payload is cloned. The CAS includes counters used for admission.
pub(super) async fn update_node(
    shared: &Shared,
    id: &RunId,
    key: &str,
    check_budget: bool,
    edit: impl Fn(&RunHead, &mut Option<InvocationRecord>) -> Result<bool, ForgeError>,
) -> Result<ExecutionView, ForgeError> {
    loop {
        let mut current = view(shared, id, Some(key)).await?;
        if current.head.state.is_terminal() {
            return Ok(current);
        }
        let old_bytes = current
            .invocation
            .as_ref()
            .map_or(0, InvocationRecord::retained_data_bytes);
        let new_activation = current.invocation.is_none();
        if !edit(&current.head, &mut current.invocation)? {
            return Ok(current);
        }
        let record = current.invocation.take().ok_or_else(|| {
            ForgeError::new(
                "state.conflict",
                "A node transition cannot delete its record",
            )
        })?;
        let limits = &shared.composition.limits;
        if new_activation && current.head.invocation_count >= limits.activations {
            return Err(ForgeError::new(
                "resource.limit",
                "Run activation budget is exhausted",
            ));
        }
        if check_budget
            && current
                .head
                .retained_data_bytes
                .saturating_sub(old_bytes)
                .saturating_add(record.retained_data_bytes())
                > limits.run_bytes
        {
            return Err(ForgeError::new(
                "resource.limit",
                "Node transition exceeds run retention budget",
            ));
        }
        let expected = current.head.revision;
        match shared
            .composition
            .store
            .commit_invocation(&shared.composition.id, id, expected, key, record)
            .await
        {
            Ok(committed) => {
                let head = &committed.head;
                let _ = shared.events.try_send(ExecutionEvent {
                    run_id: head.id.clone(),
                    scope: head.scope.clone(),
                    revision: head.revision,
                    state: head.state,
                });
                shared.changed.notify_waiters();
                return Ok(committed);
            }
            Err(error) if error.code() == "state.conflict" => {
                if view(shared, id, None).await?.head.revision == expected {
                    return Err(error);
                }
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn publish(shared: &Shared, run: &RunSnapshot) {
    let _ = shared.events.try_send(ExecutionEvent {
        run_id: run.id.clone(),
        scope: run.scope.clone(),
        revision: run.revision,
        state: run.state,
    });
    shared.changed.notify_waiters();
}

/// The closure can decline a stale completion or reject a quota atomically.
pub(super) async fn update(
    shared: &Shared,
    id: &RunId,
    allow_terminal_audit: bool,
    edit: impl Fn(&mut RunSnapshot) -> Result<bool, ForgeError>,
) -> Result<RunSnapshot, ForgeError> {
    update_with_view(shared, id, allow_terminal_audit, |run, _| edit(run)).await
}

pub(super) async fn update_with_view(
    shared: &Shared,
    id: &RunId,
    allow_terminal_audit: bool,
    edit: impl Fn(&mut RunSnapshot, &RunHead) -> Result<bool, ForgeError>,
) -> Result<RunSnapshot, ForgeError> {
    loop {
        let mut run = read(shared, id).await?;
        if run.state.is_terminal() && !allow_terminal_audit {
            return Ok(run);
        }
        let expected = run.revision;
        let head = view(shared, id, None).await?.head;
        if head.revision != expected {
            tokio::task::yield_now().await;
            continue;
        }
        if !edit(&mut run, &head)? {
            return Ok(run);
        }
        run.revision = expected
            .checked_add(1)
            .ok_or_else(|| ForgeError::new("state.conflict", "Revision exhausted"))?;
        match shared
            .composition
            .store
            .commit(&shared.composition.id, expected, run.clone())
            .await
        {
            Ok(()) => {
                publish(shared, &run);
                return Ok(run);
            }
            Err(error) if error.code() == "state.conflict" => {
                if read(shared, id).await?.revision == expected {
                    return Err(error);
                }
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) fn json_bytes(value: &impl serde::Serialize) -> usize {
    serde_json::to_vec(value)
        .expect("public JSON contracts serialize")
        .len()
}
pub(super) fn retained_bytes(run: &RunSnapshot) -> usize {
    run.retained_data_bytes()
}

pub(super) fn unknown_error() -> ForgeError {
    ForgeError::new(
        "effect.unknown",
        "An effect needs authoritative resolution before continuation",
    )
}

pub(super) fn operation_failure(
    code: &str,
    class: ErrorClass,
    certainty: EffectCertainty,
    message: &str,
) -> ForgeError {
    let mut diagnostic = Diagnostic::new(code, message);
    diagnostic.operation_error = Some(OperationError {
        code: code.into(),
        class,
        certainty,
        message: message.into(),
    });
    diagnostic.into()
}
