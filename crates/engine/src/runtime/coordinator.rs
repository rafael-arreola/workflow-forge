use super::*;

pub(super) async fn transition(
    shared: &Shared,
    id: &RunId,
    edit: impl Fn(&mut RunSnapshot),
) -> Result<RunSnapshot, ForgeError> {
    let c = &shared.composition;
    loop {
        let mut run = c
            .store
            .get(id)
            .await?
            .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
        if run.scope != c.scope {
            return Err(ForgeError::new(
                "access.denied",
                "Run belongs to another scope",
            ));
        }
        if run.state.is_terminal() {
            return Ok(run);
        }
        let revision = run.revision;
        edit(&mut run);
        run.revision = revision
            .checked_add(1)
            .ok_or_else(|| ForgeError::new("state.conflict", "Revision exhausted"))?;
        match c.store.commit(&c.id, revision, run.clone()).await {
            Ok(()) => {
                let _ = shared.events.try_send(ExecutionEvent {
                    run_id: run.id.clone(),
                    scope: run.scope.clone(),
                    revision: run.revision,
                    state: run.state,
                });
                shared.changed.notify_waiters();
                return Ok(run);
            }
            Err(error) if error.code() == "state.conflict" => {
                if c.store
                    .get(id)
                    .await?
                    .is_none_or(|current| current.revision == revision)
                {
                    return Err(error);
                }
                tokio::task::yield_now().await;
            }
            Err(error) => return Err(error),
        }
    }
}

pub(super) async fn supervise(shared: Arc<Shared>) -> Result<(), ForgeError> {
    let mut jobs = JoinSet::new();
    let mut active = BTreeSet::new();
    loop {
        let c = &shared.composition;
        let pending = c.store.unfinished().await?;
        for run in pending.iter().filter(|r| {
            matches!(
                r.state,
                RunState::Accepted | RunState::Running | RunState::Cancelling
            )
        }) {
            if active.len() >= c.limits.active_runs {
                break;
            }
            if active.contains(&run.id) {
                continue;
            }
            let plan = shared.plans.lock().await.get(&run.id).cloned();
            let plan = match plan {
                Some(plan) => plan,
                None => match prepare_registered(&shared, run.definition.clone()).await {
                    Ok(plan) => plan,
                    Err(error) => {
                        transition(&shared, &run.id, |r| {
                            r.state = RunState::Blocked;
                            r.error = Some(error.clone());
                        })
                        .await?;
                        continue;
                    }
                },
            };
            let id = run.id.clone();
            let worker = shared.clone();
            let cancellation = shared.cancel.child_token();
            shared
                .cancellations
                .lock()
                .await
                .insert(id.clone(), cancellation.clone());
            active.insert(id.clone());
            jobs.spawn(async move {
                let result = execute(worker, plan, id.clone(), cancellation).await;
                (id, result)
            });
        }
        if shared.phase.load(Ordering::Acquire) == DRAINING
            && jobs.is_empty()
            && pending
                .iter()
                .all(|r| r.state == RunState::Blocked || r.state == RunState::Waiting)
        {
            return Ok(());
        }
        tokio::select! {
            Some(outcome)=jobs.join_next(), if !jobs.is_empty() => {
                let (id,result)=outcome.map_err(|_|ForgeError::new("runtime.failed","Execution supervisor lost a task"))?;
                active.remove(&id);shared.cancellations.lock().await.remove(&id);shared.plans.lock().await.remove(&id);
                result?;
            }
            _=shared.wake.notified()=>(),
            _=tokio::time::sleep(Duration::from_millis(50))=>{ c.store.collect(&c.id,now_ms(),&c.limits).await?; },
            _=shared.cancel.cancelled()=>{
                while let Some(outcome)=jobs.join_next().await {
                    let (_,result)=outcome.map_err(|_|unavailable())?;result?;
                }
                return Ok(());
            }
        }
    }
}

async fn finish_error(
    shared: &Shared,
    id: &RunId,
    node: Option<&str>,
    error: ForgeError,
    cancelled: bool,
) -> Result<(), ForgeError> {
    transition(shared, id, |run| {
        run.state = if cancelled {
            RunState::Cancelled
        } else {
            RunState::Failed
        };
        run.error = Some(error.clone());
        run.finished_at_ms = Some(now_ms());
        if let Some(record) = node.and_then(|n| run.invocations.get_mut(n)) {
            record.state = if cancelled {
                InvocationState::Cancelled
            } else {
                InvocationState::Failed
            };
            record.error = Some(error.clone());
        }
    })
    .await?;
    Ok(())
}

async fn execute(
    shared: Arc<Shared>,
    plan: PreparedWorkflow,
    id: RunId,
    cancellation: CancellationToken,
) -> Result<(), ForgeError> {
    let c = &shared.composition;
    let initial = transition(&shared, &id, |r| {
        if !r.cancel_requested {
            r.state = RunState::Running;
        }
    })
    .await?;
    let mut outputs: BTreeMap<_, _> = initial
        .invocations
        .iter()
        .filter_map(|(id, r)| r.output.clone().map(|v| (id.clone(), v)))
        .collect();
    for node in &plan.0.sequence {
        if outputs.contains_key(&node.definition.id) {
            continue;
        }
        let run = c
            .store
            .get(&id)
            .await?
            .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
        let node_id = &node.definition.id;
        if cancellation.is_cancelled() || run.cancel_requested || now_ms() >= run.deadline_at_ms {
            return finish_error(
                &shared,
                &id,
                None,
                ForgeError::new(
                    "operation.cancelled",
                    "Run was cancelled or reached its deadline",
                ),
                true,
            )
            .await;
        }
        let input = match binding::evaluate(&node.definition.input, &run.input, &outputs, &c.limits)
            .and_then(|v| {
                node.operation.input.validate(&v, &c.limits)?;
                Ok(v)
            }) {
            Ok(input) => input,
            Err(mut error) => {
                for d in &mut error.diagnostics {
                    d.location.node = Some(node_id.clone());
                    d.location.field = "/input".into();
                }
                return finish_error(&shared, &id, Some(node_id), error, false).await;
            }
        };
        let permit = tokio::select! {p=shared.attempts.clone().acquire_owned()=>p.map_err(|_|unavailable())?,_=cancellation.cancelled()=>return finish_error(&shared,&id,None,ForgeError::new("operation.cancelled","Run was cancelled"),true).await};
        if now_ms() >= run.deadline_at_ms {
            drop(permit);
            return finish_error(
                &shared,
                &id,
                None,
                ForgeError::new(
                    "operation.timeout",
                    "Run reached its deadline before dispatch",
                ),
                false,
            )
            .await;
        }
        if retained_bytes(&run).saturating_add(json_bytes(&input)) > c.limits.run_bytes {
            drop(permit);
            return finish_error(
                &shared,
                &id,
                None,
                ForgeError::new(
                    "resource.limit",
                    "Invocation input exceeds the run retention budget",
                ),
                false,
            )
            .await;
        }
        let attempt_id = uuid::Uuid::now_v7().to_string();
        let invocation_id = uuid::Uuid::new_v5(
            &uuid::Uuid::NAMESPACE_OID,
            format!("{}:{node_id}", id.0).as_bytes(),
        )
        .to_string();
        let record = InvocationRecord {
            id: invocation_id.clone(),
            attempt_id: attempt_id.clone(),
            attempts: run
                .invocations
                .get(node_id)
                .map_or(1, |r| r.attempts.saturating_add(1)),
            state: InvocationState::Running,
            input: input.clone(),
            output: None,
            error: None,
        };
        let intent = transition(&shared, &id, |r| {
            r.invocations.insert(node_id.clone(), record.clone());
        })
        .await?;
        if intent.cancel_requested {
            drop(permit);
            return finish_error(
                &shared,
                &id,
                Some(node_id),
                ForgeError::new("operation.cancelled", "Run was cancelled"),
                true,
            )
            .await;
        }
        let deadline = run
            .deadline_at_ms
            .min(now_ms().saturating_add(c.limits.attempt_timeout_ms));
        let attempt_cancel = cancellation.child_token();
        let context = OperationContext::new(
            id.clone(),
            deadline,
            attempt_cancel.clone(),
            run.scope.clone(),
            node.operation.descriptor.required_resources.clone(),
            c.secrets.clone(),
            c.artifacts.clone(),
        );
        let invocation = Invocation {
            id: invocation_id,
            attempt_id,
            operation: node.definition.operation.clone(),
            config: node.definition.config.clone(),
            input,
            effect_key: None,
        };
        let result =
            invocation::invoke(node.operation.operation.clone(), context, invocation).await;
        drop(permit);
        let value = match result.and_then(|v| {
            node.operation
                .output
                .validate(&v, &c.limits)
                .map_err(|mut error| {
                    for d in &mut error.diagnostics {
                        d.location.field = "/output".into();
                    }
                    error
                })?;
            Ok(v)
        }) {
            Ok(value) => value,
            Err(mut error) => {
                for d in &mut error.diagnostics {
                    d.location.node = Some(node_id.clone());
                }
                let cancelled = error.code() == "operation.cancelled";
                return finish_error(&shared, &id, Some(node_id), error, cancelled).await;
            }
        };
        outputs.insert(node_id.clone(), value.clone());
        if serde_json::to_vec(&outputs)
            .expect("JSON outputs serialize")
            .len()
            + serde_json::to_vec(&run.input)
                .expect("JSON input serializes")
                .len()
            > c.limits.run_bytes
        {
            return finish_error(
                &shared,
                &id,
                Some(node_id),
                ForgeError::new("resource.limit", "Run data exceeds its retention budget"),
                false,
            )
            .await;
        }
        transition(&shared, &id, |r| {
            if let Some(record) = r.invocations.get_mut(node_id) {
                record.state = InvocationState::Succeeded;
                record.output = Some(value.clone());
                record.input = Value::Null;
            }
        })
        .await?;
    }
    let current = c
        .store
        .get(&id)
        .await?
        .ok_or_else(|| ForgeError::new("not_found", "Run is unavailable"))?;
    if current.cancel_requested || cancellation.is_cancelled() {
        return finish_error(
            &shared,
            &id,
            None,
            ForgeError::new("operation.cancelled", "Run was cancelled"),
            true,
        )
        .await;
    }
    match binding::evaluate(
        &plan.definition().output,
        &current.input,
        &outputs,
        &c.limits,
    )
    .and_then(|v| {
        plan.0.output.validate(&v, &c.limits)?;
        Ok(v)
    }) {
        Ok(output) => {
            if retained_bytes(&current).saturating_add(json_bytes(&output)) > c.limits.run_bytes {
                return finish_error(
                    &shared,
                    &id,
                    None,
                    ForgeError::new(
                        "resource.limit",
                        "Final result exceeds the run retention budget",
                    ),
                    false,
                )
                .await;
            }
            transition(&shared, &id, |r| {
                if r.cancel_requested || cancellation.is_cancelled() {
                    r.state = RunState::Cancelled;
                    r.error = Some(ForgeError::new("operation.cancelled", "Run was cancelled"));
                } else if now_ms() >= r.deadline_at_ms {
                    r.state = RunState::Failed;
                    r.error = Some(ForgeError::new(
                        "operation.timeout",
                        "Run reached its deadline",
                    ));
                } else {
                    r.output = Some(output.clone());
                    r.state = RunState::Succeeded;
                }
                r.finished_at_ms = Some(now_ms());
            })
            .await?;
            Ok(())
        }
        Err(mut error) => {
            for d in &mut error.diagnostics {
                d.location.field = "/output".into();
            }
            finish_error(&shared, &id, None, error, false).await
        }
    }
}

fn json_bytes(value: &Value) -> usize {
    serde_json::to_vec(value).expect("JSON serializes").len()
}
fn retained_bytes(run: &RunSnapshot) -> usize {
    run.invocations
        .values()
        .fold(json_bytes(&run.input), |total, r| {
            total
                .saturating_add(json_bytes(&r.input))
                .saturating_add(r.output.as_ref().map_or(0, json_bytes))
        })
        .saturating_add(run.output.as_ref().map_or(0, json_bytes))
}
