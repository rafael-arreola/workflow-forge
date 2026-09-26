use super::*;

pub(super) async fn transition(
    shared: &Shared,
    id: &RunId,
    edit: impl Fn(&mut RunSnapshot),
) -> Result<RunSnapshot, ForgeError> {
    state::update(shared, id, false, |run| {
        edit(run);
        Ok(true)
    })
    .await
}

pub(super) async fn supervise(
    shared: Arc<Shared>,
    mut late_receiver: mpsc::Receiver<invocation::LateAttempt>,
) -> Result<(), ForgeError> {
    let mut late_jobs = JoinSet::new();
    let mut jobs = JoinSet::new();
    let mut active = BTreeSet::new();
    loop {
        let c = &shared.composition;
        let pending = c.store.unfinished_heads().await?;
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
                None => match prepare_registered(
                    &shared,
                    state::read(&shared, &run.id).await?.definition,
                    None,
                )
                .await
                {
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
            && late_jobs.is_empty()
            && late_receiver.is_empty()
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
            Some(late)=late_receiver.recv()=>{late_jobs.spawn(invocation::observe_late(shared.clone(),late));}
            Some(outcome)=late_jobs.join_next(),if !late_jobs.is_empty()=>{outcome.map_err(|_|unavailable())??;}
            _=shared.wake.notified()=>(),
            _=tokio::time::sleep(Duration::from_millis(50))=>{ c.store.collect(&c.id,now_ms(),&c.limits).await?; },
            _=shared.cancel.cancelled()=>{
                while let Some(outcome)=jobs.join_next().await {
                    let (_,result)=outcome.map_err(|_|unavailable())?;result?;
                }
                while let Ok(late)=late_receiver.try_recv(){drop(late);}
                late_jobs.abort_all();
                while late_jobs.join_next().await.is_some() {}
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
        for record in run.invocations.values_mut() {
            if record.state != InvocationState::Succeeded
                && record.certainty != EffectCertainty::NotApplied
            {
                record.state = InvocationState::Unknown;
            }
        }
        if run
            .invocations
            .values()
            .any(|r| r.state == InvocationState::Unknown)
        {
            run.state = RunState::Blocked;
            run.error = Some(state::unknown_error());
            run.finished_at_ms = None;
            return;
        }
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
    let scope = control::ScopeExecution {
        shared: shared.clone(),
        run_id: id.clone(),
        path: control::ScopePath::default(),
        input: initial.input.clone(),
        cancellation: cancellation.clone(),
    };
    let output = match control::execute_body(scope, plan.0.body.clone()).await {
        Ok(output) => output,
        Err(steps::StepError::Infrastructure(error)) => return Err(error),
        Err(steps::StepError::Execution(error)) => {
            let cancelled = error.code() == "operation.cancelled";
            return finish_error(&shared, &id, None, error, cancelled).await;
        }
    };

    let current = state::view(&shared, &id, None).await?.head;
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
    if current.unresolved_invocations != 0 {
        return finish_error(&shared, &id, None, state::unknown_error(), false).await;
    }
    match plan.0.output.validate(&output, &c.limits).map(|_| output) {
        Ok(output) => {
            let output_bytes = json_bytes(&output);
            state::update_with_view(&shared, &id, false, |r, head| {
                if r.cancel_requested || cancellation.is_cancelled() {
                    r.state = RunState::Cancelled;
                    r.error = Some(ForgeError::new("operation.cancelled", "Run was cancelled"));
                } else if now_ms() >= r.deadline_at_ms {
                    r.state = RunState::Failed;
                    r.error = Some(ForgeError::new(
                        "operation.timeout",
                        "Run reached its deadline",
                    ));
                } else if head.retained_data_bytes.saturating_add(output_bytes) > c.limits.run_bytes
                {
                    r.state = RunState::Failed;
                    r.error = Some(ForgeError::new(
                        "resource.limit",
                        "Final result exceeds the run retention budget",
                    ));
                } else {
                    r.output = Some(output.clone());
                    r.error = None;
                    r.state = RunState::Succeeded;
                }
                r.finished_at_ms = Some(now_ms());
                Ok(true)
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

use state::json_bytes;
