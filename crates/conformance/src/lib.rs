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
    let mut finished = running;
    finished.revision = 2;
    finished.state = RunState::Succeeded;
    finished.output = Some(json!("confirmed"));
    finished.finished_at_ms = Some(initial.created_at_ms);
    store.commit(owner, 1, finished.clone()).await?;
    finished.revision = 3;
    finished.output = Some(json!("overwritten"));
    require(
        store.commit(owner, 2, finished).await.is_err(),
        "A terminal result was overwritten",
    )?;
    require(
        store.unfinished().await?.is_empty(),
        "A terminal run remained schedulable",
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
