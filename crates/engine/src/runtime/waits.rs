use super::*;
use crate::compiler::{PreparedInstruction, PreparedNode, PreparedOperation};
use control::ScopeExecution;
use steps::{StepError, expected};

fn expired() -> ForgeError {
    ForgeError::new("wait.expired", "Signal reservation reached its deadline")
}
fn cancelled() -> ForgeError {
    ForgeError::new("operation.cancelled", "Wait was cancelled")
}
fn budget() -> ForgeError {
    ForgeError::new(
        "resource.limit",
        "Wait exceeds the run retention or reservation budget",
    )
}
fn transition_error(error: ForgeError) -> StepError {
    if error.code() == "data.invalid" {
        expected(error)
    } else {
        steps::transition_error(error)
    }
}

pub(super) async fn execute(
    scope: &ScopeExecution,
    node: &PreparedNode,
    key: &str,
    input: Value,
) -> Result<Value, StepError> {
    let id = uuid::Uuid::new_v5(
        &uuid::Uuid::NAMESPACE_OID,
        &serde_json::to_vec(&(&scope.run_id, key, "wait")).expect("identity serializes"),
    )
    .to_string();
    let result = run(scope, node, key, &id, input).await;
    if let Err(StepError::Execution(error)) = &result {
        state::update(&scope.shared, &scope.run_id, false, |run| {
            let Some(wait) = run.waits.get_mut(&id) else {
                return Ok(false);
            };
            if wait.state == WaitState::Consumed {
                return Ok(false);
            }
            if error.code() != "effect.unknown" && wait.state == WaitState::Open {
                wait.state = WaitState::Closed;
            }
            let record = run
                .invocations
                .get_mut(key)
                .expect("wait activation is committed");
            record.state = if error.code() == "effect.unknown" {
                InvocationState::Pending
            } else if error.code() == "operation.cancelled" {
                InvocationState::Cancelled
            } else {
                InvocationState::Failed
            };
            record.error = Some(error.clone());
            Ok(true)
        })
        .await?;
    }
    result
}

async fn run(
    scope: &ScopeExecution,
    node: &PreparedNode,
    key: &str,
    id: &str,
    input: Value,
) -> Result<Value, StepError> {
    let limits = &scope.shared.composition.limits;
    let start_key = scope.path.child(&node.id, &["start"]).node("invoke");
    let (kind, duration) = match &node.instruction {
        PreparedInstruction::Timer { duration_ms } => (WaitKind::Timer, *duration_ms),
        PreparedInstruction::AwaitSignal {
            correlation,
            timeout_ms,
            payload_schema,
            start,
        } => {
            let value = binding::evaluate(correlation, &input, &BTreeMap::new(), limits)
                .map_err(expected)?;
            let correlation = value
                .as_str()
                .filter(|s| !s.is_empty() && s.len() <= 256)
                .ok_or_else(|| {
                    expected(ForgeError::new(
                        "data.invalid",
                        "Signal correlation must contain 1 to 256 bytes",
                    ))
                })?;
            (
                WaitKind::Signal {
                    correlation: correlation.into(),
                    payload_schema: payload_schema.clone(),
                    start_node: start.as_ref().map(|_| start_key.clone()),
                },
                *timeout_ms,
            )
        }
        _ => unreachable!("only wait instructions use this path"),
    };
    let snapshot = state::update(&scope.shared, &scope.run_id, false, |run| {
        if let Some(old) = run.waits.get(id) {
            if old.node != key || old.kind != kind {
                return Err(ForgeError::new(
                    "state.conflict",
                    "Saved wait differs from its prepared instruction",
                ));
            }
            return Ok(false);
        }
        if run.cancel_requested || scope.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        if run.waits.len() >= limits.waits_per_run || run.invocations.len() >= limits.activations {
            return Err(budget());
        }
        let now = now_ms();
        if now >= run.deadline_at_ms {
            return Err(ForgeError::new(
                "operation.timeout",
                "Run reached its deadline",
            ));
        }
        if run.invocations.contains_key(key) {
            return Err(ForgeError::new(
                "state.conflict",
                "Wait activation has no reservation",
            ));
        }
        run.waits.insert(
            id.into(),
            WaitRecord {
                id: id.into(),
                node: key.into(),
                kind: kind.clone(),
                created_at_ms: now,
                deadline_at_ms: now.saturating_add(duration).min(run.deadline_at_ms),
                start_confirmed: matches!(
                    &kind,
                    WaitKind::Timer
                        | WaitKind::Signal {
                            start_node: None,
                            ..
                        }
                ),
                state: WaitState::Open,
                delivery: None,
            },
        );
        run.invocations.insert(
            key.into(),
            InvocationRecord {
                id: uuid::Uuid::new_v5(
                    &uuid::Uuid::NAMESPACE_OID,
                    &serde_json::to_vec(&(&scope.run_id, key)).expect("identity serializes"),
                )
                .to_string(),
                attempt_id: String::new(),
                attempts: 0,
                state: InvocationState::Pending,
                input: input.clone(),
                output: None,
                error: None,
                operation: None,
                config: Value::Null,
                effect_key: None,
                retry: RetryPolicy::default(),
                next_attempt_at_ms: None,
                certainty: EffectCertainty::NotApplied,
                control: Some(ControlFrame::Wait { id: id.into() }),
            },
        );
        if run.retained_data_bytes() > limits.run_bytes {
            return Err(budget());
        }
        Ok(true)
    })
    .await
    .map_err(transition_error)?;
    if snapshot.state.is_terminal() {
        return Err(expected(cancelled()));
    }
    let wait = snapshot
        .waits
        .get(id)
        .ok_or_else(|| ForgeError::new("store.corrupt", "Wait reservation is missing"))?;
    if let PreparedInstruction::AwaitSignal {
        start: Some((binding, operation)),
        ..
    } = &node.instruction
        && !wait.start_confirmed
        && wait.state != WaitState::Closed
    {
        let value = json!({"input":input,"wait":{"id":id,"correlation":match &kind { WaitKind::Signal{correlation,..}=>correlation,_=>unreachable!() },"deadline_at_ms":wait.deadline_at_ms}});
        let input =
            binding::evaluate(binding, &value, &BTreeMap::new(), limits).map_err(expected)?;
        operation
            .operation
            .input
            .validate(&input, limits)
            .map_err(expected)?;
        start(scope, id, &start_key, operation, input, wait.deadline_at_ms).await?;
        state::update(&scope.shared, &scope.run_id, false, |run| {
            let wait = run.waits.get_mut(id).expect("reserved");
            if wait.state != WaitState::Open || wait.start_confirmed {
                return Ok(false);
            }
            wait.start_confirmed = true;
            Ok(true)
        })
        .await?;
    }
    let committed = state::update(&scope.shared, &scope.run_id, false, |run| {
        if run.cancel_requested || scope.cancellation.is_cancelled() {
            return Err(cancelled());
        }
        let now = now_ms();
        if now >= run.deadline_at_ms {
            return Err(ForgeError::new(
                "operation.timeout",
                "Run reached its deadline",
            ));
        }
        let wait = run.waits.get_mut(id).expect("reserved");
        if wait.state != WaitState::Open {
            return Ok(false);
        }
        let output = match &wait.kind {
            WaitKind::Timer if now >= wait.deadline_at_ms => Some(input.clone()),
            WaitKind::Timer => None,
            WaitKind::Signal { start_node, .. } => {
                if let Some(delivery) = &wait.delivery {
                    if !wait.start_confirmed {
                        return Ok(false);
                    }
                    let started = start_node
                        .as_ref()
                        .and_then(|key| run.invocations.get(key))
                        .and_then(|r| r.output.clone())
                        .unwrap_or(Value::Null);
                    Some(json!({"start":started,"signal":delivery.command.payload}))
                } else if now >= wait.deadline_at_ms {
                    wait.state = WaitState::Expired;
                    return Ok(true);
                } else {
                    None
                }
            }
        };
        let Some(output) = output else {
            return Ok(false);
        };
        crate::schema::check_value(&output, limits.value_bytes, limits.json_depth)?;
        wait.state = WaitState::Consumed;
        let record = run.invocations.get_mut(key).expect("reserved activation");
        record.state = InvocationState::Succeeded;
        record.output = Some(output);
        record.input = Value::Null;
        record.error = None;
        if run.retained_data_bytes() > limits.run_bytes {
            return Err(budget());
        }
        Ok(true)
    })
    .await
    .map_err(transition_error)?;
    let wait = &committed.waits[id];
    match wait.state {
        WaitState::Consumed => committed.invocations[key].output.clone().ok_or_else(|| {
            ForgeError::new("store.corrupt", "Consumed wait has no confirmed output").into()
        }),
        WaitState::Expired => Err(expected(expired())),
        WaitState::Closed => Err(expected(cancelled())),
        WaitState::Open if committed.cancel_requested || scope.cancellation.is_cancelled() => {
            Err(expected(cancelled()))
        }
        WaitState::Open => Err(StepError::Suspended),
    }
}

async fn expire(scope: &ScopeExecution, id: &str) -> Result<bool, ForgeError> {
    let run = state::update(&scope.shared, &scope.run_id, false, |run| {
        let wait = run.waits.get_mut(id).expect("reserved");
        if wait.state == WaitState::Open
            && wait.delivery.is_none()
            && now_ms() >= wait.deadline_at_ms
        {
            wait.state = WaitState::Expired;
            return Ok(true);
        }
        Ok(false)
    })
    .await?;
    Ok(run.waits[id].state == WaitState::Expired)
}

async fn start(
    scope: &ScopeExecution,
    id: &str,
    key: &str,
    operation: &PreparedOperation,
    input: Value,
    deadline: u64,
) -> Result<(), StepError> {
    let cancellation = scope.cancellation.child_token();
    let _cancel_on_drop = cancellation.clone().drop_guard();
    let mut timed_out = expire(scope, id).await?;
    if timed_out {
        cancellation.cancel();
    }
    let future = steps::execute_operation(
        &scope.shared,
        &scope.run_id,
        key,
        operation,
        input,
        &cancellation,
    );
    tokio::pin!(future);
    let mut checked = timed_out;
    let result = loop {
        tokio::select! {
            value=&mut future=>break value,
            _=tokio::time::sleep(Duration::from_millis(deadline.saturating_sub(now_ms()))),if !checked=>{
                timed_out=expire(scope,id).await?;
                // A wall-clock adjustment can make the monotonic timer early.
                checked=now_ms()>=deadline;
                if timed_out { cancellation.cancel(); }
            }
        }
    };
    match result {
        Err(StepError::Execution(error)) if timed_out && error.code() != "effect.unknown" => {
            Err(expected(expired()))
        }
        Err(error) => Err(error),
        Ok(_) if timed_out => Err(expected(expired())),
        Ok(_) => Ok(()),
    }
}
