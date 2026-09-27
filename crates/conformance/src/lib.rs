//! Behavioral checks use only public ports. Run against a fresh, isolated store.
//! This sequential suite does not certify crash durability or atomicity under races.
use serde_json::json;
use workflow_forge_protocol::*;

fn require(condition: bool, message: &str) -> Result<(), ForgeError> {
    if condition {
        Ok(())
    } else {
        Err(ForgeError::new("conformance.failed", message))
    }
}

/// Consumes an isolated provider and a valid Accepted snapshot supplied by its host.
/// Releases ownership on both success and failure so a test can clean up its files.
pub async fn execution_store(
    store: &dyn ExecutionStore,
    initial: RunSnapshot,
) -> Result<(), ForgeError> {
    let owner = "conformance.owner";
    store.claim(owner).await?;
    let result = check_claimed(store, owner, initial).await;
    let release = store.release(owner).await;
    result.and(release)
}

async fn check_claimed(
    store: &dyn ExecutionStore,
    owner: &str,
    initial: RunSnapshot,
) -> Result<(), ForgeError> {
    let limits = Limits::default();
    require(
        store.claim("competing.owner").await.is_err(),
        "An owned store admitted a competing runtime",
    )?;
    require(
        store
            .create("competing.owner", initial.clone(), None, &limits)
            .await
            .is_err(),
        "A foreign owner created a run",
    )?;
    let reservation = ReceiptReservation {
        key: "conformance.receipt".into(),
        request: json!({"input":initial.input,"definition":initial.definition}),
        expires_at_ms: initial.created_at_ms.saturating_add(60_000),
    };
    require(
        matches!(
            store
                .create(owner, initial.clone(), Some(reservation.clone()), &limits)
                .await?,
            CreateOutcome::Created
        ),
        "First receipt did not create a run",
    )?;
    let mut duplicate = initial.clone();
    duplicate.id.0.push_str("-duplicate");
    require(
        matches!(store.create(owner,duplicate.clone(),Some(reservation.clone()),&limits).await?,CreateOutcome::Duplicate{run_id,..} if run_id==initial.id),
        "A repeated receipt was not deduplicated",
    )?;
    let mut conflicting = reservation.clone();
    conflicting.request = json!({"different":true});
    require(
        store
            .create(owner, duplicate, Some(conflicting), &limits)
            .await
            .is_err(),
        "Receipt identity was reused with different content",
    )?;
    let mut running = initial.clone();
    running.revision = 1;
    running.state = RunState::Running;
    let mut changed = running.clone();
    changed.input = json!({"changed":true});
    require(
        store.commit(owner, 0, changed).await.is_err(),
        "A commit changed immutable input",
    )?;
    let mut changed_package = running.clone();
    changed_package
        .package
        .definitions
        .push(initial.definition.clone());
    require(
        store.commit(owner, 0, changed_package).await.is_err(),
        "A commit changed the accepted dependency package",
    )?;
    let mut changed_artifacts = running.clone();
    changed_artifacts.artifacts.push(ArtifactRef {
        id: "undeclared".into(),
        scope: initial.scope.clone(),
        bytes: 0,
        media_type: "test/data".into(),
    });
    require(
        store.commit(owner, 0, changed_artifacts).await.is_err(),
        "A commit changed the accepted artifact declarations",
    )?;
    store.commit(owner, 0, running.clone()).await?;
    require(
        store.commit(owner, 0, running.clone()).await.is_err(),
        "A stale revision was accepted",
    )?;
    require(
        store
            .get(&initial.id)
            .await?
            .is_some_and(|r| r.revision == 1 && r.input == initial.input),
        "Committed revision or input was lost",
    )?;
    let running = check_projections(store, owner, running).await?;
    let running = check_wait_commit(store, owner, running).await?;
    let expected = running.revision;
    let mut finished = running;
    finished.revision = expected + 1;
    finished.state = RunState::Succeeded;
    finished.output = Some(json!("confirmed"));
    finished.finished_at_ms = Some(initial.created_at_ms);
    store.commit(owner, expected, finished.clone()).await?;
    require(
        store.view(&finished.id, None).await? == Some(finished.view(None)),
        "A head commit left stale payload counters",
    )?;
    let terminal_revision = finished.revision;
    let (key, record) = finished
        .invocations
        .iter()
        .next()
        .expect("projection fixture");
    require(
        store
            .commit_invocation(owner, &finished.id, terminal_revision, key, record.clone())
            .await
            .is_err(),
        "A node commit changed a terminal run",
    )?;
    finished.revision += 1;
    finished.output = Some(json!("overwritten"));
    require(
        store
            .commit(owner, terminal_revision, finished)
            .await
            .is_err(),
        "A terminal result was overwritten",
    )?;
    require(
        store.unfinished().await?.is_empty(),
        "A terminal run remained schedulable",
    )?;
    require(
        store.unfinished_heads().await?.is_empty(),
        "A terminal run remained in projected scheduling",
    )?;
    let retention = Limits {
        retention_ms: 1,
        ..limits
    };
    store
        .collect(owner, initial.created_at_ms + 2, &retention)
        .await?;
    require(
        store.get(&initial.id).await?.is_none(),
        "Expired result was retained",
    )?;
    require(
        store.view(&initial.id, None).await?.is_none(),
        "Expired projection was retained",
    )?;
    require(
        matches!(
            store
                .create(owner, initial, Some(reservation), &retention)
                .await?,
            CreateOutcome::Duplicate { .. }
        ),
        "Expiring a result erased its receipt guarantee",
    )?;
    Ok(())
}

async fn check_wait_commit(
    store: &dyn ExecutionStore,
    owner: &str,
    mut run: RunSnapshot,
) -> Result<RunSnapshot, ForgeError> {
    let key = "/nodes/timer";
    let id = "conformance.wait";
    let expected = run.revision;
    run.revision += 1;
    run.state = RunState::Waiting;
    run.waits.insert(
        id.into(),
        WaitRecord {
            id: id.into(),
            node: key.into(),
            kind: WaitKind::Timer,
            created_at_ms: run.created_at_ms,
            deadline_at_ms: run.created_at_ms,
            start_confirmed: true,
            state: WaitState::Open,
            delivery: None,
        },
    );
    run.invocations.insert(
        key.into(),
        InvocationRecord {
            id: "conformance.timer".into(),
            attempt_id: String::new(),
            attempts: 0,
            state: InvocationState::Pending,
            input: json!(9),
            output: None,
            error: None,
            operation: None,
            config: serde_json::Value::Null,
            effect_key: None,
            retry: RetryPolicy::default(),
            next_attempt_at_ms: None,
            certainty: EffectCertainty::NotApplied,
            control: Some(ControlFrame::Wait { id: id.into() }),
        },
    );
    store.commit(owner, expected, run.clone()).await?;
    require(
        store.view(&run.id, None).await? == Some(run.view(None)),
        "Wait projection differs from its committed deadline or counters",
    )?;
    let expected = run.revision;
    let mut record = run.invocations[key].clone();
    record.state = InvocationState::Succeeded;
    record.output = Some(json!(9));
    record.input = serde_json::Value::Null;
    require(
        store
            .commit_invocation(owner, &run.id, expected, key, record.clone())
            .await
            .is_err(),
        "A node-only commit confirmed a wait without consuming its reservation",
    )?;
    let mut separate = run.clone();
    separate.revision += 1;
    separate.invocations.insert(key.into(), record.clone());
    require(
        store.commit(owner, expected, separate).await.is_err(),
        "Wait result was confirmed before consumption",
    )?;
    let mut separate = run.clone();
    separate.revision += 1;
    separate.waits.get_mut(id).unwrap().state = WaitState::Consumed;
    require(
        store.commit(owner, expected, separate).await.is_err(),
        "Wait was consumed without confirming its result",
    )?;
    run.revision += 1;
    run.state = RunState::Running;
    run.invocations.insert(key.into(), record);
    run.waits.get_mut(id).unwrap().state = WaitState::Consumed;
    store.commit(owner, expected, run.clone()).await?;
    require(
        store.view(&run.id, None).await? == Some(run.view(None)),
        "Consumed wait retained a stale scheduling projection",
    )?;
    Ok(run)
}

async fn check_projections(
    store: &dyn ExecutionStore,
    owner: &str,
    run: RunSnapshot,
) -> Result<RunSnapshot, ForgeError> {
    let mut revision = run.revision;
    let mut record = InvocationRecord {
        id: "conformance.invocation".into(),
        attempt_id: "attempt-1".into(),
        attempts: 1,
        state: InvocationState::Unknown,
        input: json!({"value":1}),
        output: None,
        error: None,
        operation: Some(OperationRevision::new("conformance.operation", "1", "r1")),
        config: json!({}),
        effect_key: Some("effect-1".into()),
        retry: RetryPolicy::default(),
        next_attempt_at_ms: None,
        certainty: EffectCertainty::Unknown,
        control: None,
    };
    let other = "/nodes/batch_other/nodes/write";
    let target = "/nodes/batch/items/0/nodes/write";
    store
        .commit_invocation(owner, &run.id, revision, other, record.clone())
        .await?;
    revision += 1;
    require(
        !store
            .view(&run.id, Some("/nodes/batch"))
            .await?
            .unwrap()
            .unresolved_descendants,
        "A scope prefix included a different scope",
    )?;
    record.id = "conformance.other_invocation".into();
    record.effect_key = Some("effect-2".into());
    store
        .commit_invocation(owner, &run.id, revision, target, record.clone())
        .await?;
    revision += 1;
    require(
        store
            .view(&run.id, Some("/nodes/batch"))
            .await?
            .unwrap()
            .unresolved_descendants,
        "A scope lost its unresolved descendant",
    )?;
    require(
        store
            .commit_invocation(owner, &run.id, revision - 1, target, record.clone())
            .await
            .is_err(),
        "An invocation replacement ignored CAS",
    )?;
    require(
        store
            .commit_invocation("foreign", &run.id, revision, target, record.clone())
            .await
            .is_err(),
        "A foreign owner replaced an invocation",
    )?;
    for key in [target, other] {
        let mut view = store.view(&run.id, Some(key)).await?.unwrap();
        let mut confirmed = view.invocation.take().unwrap();
        confirmed.state = InvocationState::Succeeded;
        confirmed.certainty = EffectCertainty::Applied;
        confirmed.input = serde_json::Value::Null;
        confirmed.output = Some(json!({"accepted":true}));
        let committed = store
            .commit_invocation(owner, &run.id, revision, key, confirmed.clone())
            .await?;
        revision += 1;
        let snapshot = store.get(&run.id).await?.unwrap();
        require(
            committed == snapshot.view(Some(key)),
            "Invocation commit returned an incoherent projection",
        )?;
        require(
            store.view(&run.id, None).await? == Some(snapshot.view(None)),
            "Projection counters differ from the snapshot",
        )?;
        confirmed.output = Some(json!({"overwritten":true}));
        require(
            store
                .commit_invocation(owner, &run.id, revision, key, confirmed)
                .await
                .is_err(),
            "Invocation replacement overwrote a confirmed result",
        )?;
    }
    let snapshot = store.get(&run.id).await?.unwrap();
    require(
        snapshot.revision == revision && snapshot.head().unresolved_invocations == 0,
        "Projection did not remove resolved uncertainty",
    )?;
    require(
        store.unfinished_heads().await? == vec![snapshot.head()],
        "Pending head differs from its committed snapshot",
    )?;
    Ok(snapshot)
}
