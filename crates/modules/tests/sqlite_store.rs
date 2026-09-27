#![cfg(feature = "sqlite")]
use serde_json::{Value, json};
use std::sync::Arc;
use workflow_forge_modules::{SqliteExecutionStore, SqliteOptions};
use workflow_forge_protocol::*;

#[path = "support/sqlite.rs"]
mod support;
use support::{Directory, initial, reservation};

#[tokio::test]
async fn sqlite_passes_public_conformance_and_reopens_its_receipt() {
    let dir = Directory::new();
    let store = dir.store();
    assert!(
        !dir.path().exists(),
        "constructing the provider must stay inactive"
    );
    workflow_forge_conformance::execution_store(&store, initial("run"))
        .await
        .unwrap();
    let reopened = dir.store();
    reopened.claim("new").await.unwrap();
    let count = rusqlite::Connection::open(dir.path())
        .unwrap()
        .query_row("SELECT count(*) FROM wf_receipts", [], |r| {
            r.get::<_, usize>(0)
        })
        .unwrap();
    assert_eq!(
        count, 1,
        "receipt survives result expiration and connection close"
    );
    assert!(reopened.get(&RunId("run".into())).await.unwrap().is_none());
    reopened.release("new").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn competing_owners_and_revisions_cannot_overwrite_committed_state() {
    let dir = Directory::new();
    let store = Arc::new(dir.store());
    let competing = dir.store();
    store.claim("first").await.unwrap();
    assert_eq!(
        competing.claim("second").await.unwrap_err().code(),
        "state.conflict"
    );
    let run = initial("run");
    store
        .create("first", run.clone(), None, &Limits::default())
        .await
        .unwrap();
    let mut a = run.clone();
    a.revision = 1;
    a.state = RunState::Running;
    let mut b = a.clone();
    b.cancel_requested = true;
    b.state = RunState::Cancelling;
    let (a, b) = tokio::join!(store.commit("first", 0, a), store.commit("first", 0, b));
    assert_ne!(a.is_ok(), b.is_ok());
    let committed = store.get(&run.id).await.unwrap().unwrap();
    assert_eq!(committed.revision, 1);
    assert_eq!(
        store.view(&run.id, None).await.unwrap().unwrap(),
        committed.view(None)
    );
    store.release("first").await.unwrap();
    competing.claim("second").await.unwrap();
    assert_eq!(
        competing.get(&run.id).await.unwrap(),
        Some(committed.clone())
    );
    let mut stale = committed;
    stale.revision += 1;
    assert!(store.commit("first", 1, stale).await.is_err());
    competing.release("second").await.unwrap();
}

#[tokio::test]
async fn receipt_and_run_roll_back_together_when_disk_budget_is_exhausted() {
    let dir = Directory::new();
    let store = SqliteExecutionStore::open(
        dir.path(),
        SqliteOptions {
            max_database_bytes: 128 * 1024,
            ..Default::default()
        },
    )
    .unwrap();
    store.claim("owner").await.unwrap();
    let mut run = initial("large");
    run.input = Value::String("x".repeat(256 * 1024));
    assert_eq!(
        store
            .create(
                "owner",
                run.clone(),
                Some(reservation()),
                &Limits::default()
            )
            .await
            .err()
            .unwrap()
            .code(),
        "resource.limit"
    );
    assert!(store.get(&run.id).await.unwrap().is_none());
    run.id = RunId("small".into());
    run.input = json!(1);
    assert!(matches!(
        store
            .create(
                "owner",
                run.clone(),
                Some(reservation()),
                &Limits::default()
            )
            .await
            .unwrap(),
        CreateOutcome::Created
    ));
    store.release("owner").await.unwrap();
    let reopened = dir.store();
    reopened.claim("again").await.unwrap();
    assert_eq!(reopened.get(&run.id).await.unwrap(), Some(run));
    reopened.release("again").await.unwrap();
}

#[tokio::test]
async fn failed_migration_claim_releases_lock_and_future_codecs_are_not_interpreted() {
    let dir = Directory::new();
    let store = dir.store();
    store.claim("owner").await.unwrap();
    let run = initial("run");
    store
        .create("owner", run.clone(), None, &Limits::default())
        .await
        .unwrap();
    store.release("owner").await.unwrap();
    let conn = rusqlite::Connection::open(dir.path()).unwrap();
    conn.pragma_update(None, "user_version", 999).unwrap();
    assert_eq!(
        store.claim("bad-version").await.unwrap_err().code(),
        "capability.unsupported"
    );
    conn.pragma_update(None, "user_version", 3).unwrap();
    conn.execute(
        "UPDATE wf_runs SET body=?1 WHERE id='run'",
        [br#"{"format":999,"payload":{"unrecognized_future":true}}"#.as_slice()],
    )
    .unwrap();
    store.claim("fixed-schema").await.unwrap();
    assert_eq!(
        store.get(&run.id).await.unwrap_err().code(),
        "capability.unsupported"
    );
    conn.execute(
        "UPDATE wf_runs SET body=?1 WHERE id='run'",
        [b"{invalid".as_slice()],
    )
    .unwrap();
    assert_eq!(
        store.get(&run.id).await.unwrap_err().code(),
        "store.corrupt"
    );
    store.release("fixed-schema").await.unwrap();
}

#[tokio::test]
async fn foreign_databases_are_rejected_without_replacing_their_schema() {
    let dir = Directory::new();
    let conn = rusqlite::Connection::open(dir.path()).unwrap();
    conn.execute("CREATE TABLE unrelated(value TEXT)", [])
        .unwrap();
    assert_eq!(
        dir.store().claim("owner").await.unwrap_err().code(),
        "capability.unsupported"
    );
    let tables: usize = conn
        .query_row(
            "SELECT count(*) FROM sqlite_master WHERE type='table'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(tables, 1);
}

#[tokio::test]
async fn cancelling_a_claim_in_progress_does_not_strand_its_owner_lock() {
    use fs2::FileExt;
    use std::time::Duration;
    let dir = Directory::new();
    let store = Arc::new(dir.store());
    store.claim("initialize").await.unwrap();
    store.release("initialize").await.unwrap();
    let mut conn = rusqlite::Connection::open(dir.path()).unwrap();
    let transaction = conn
        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
        .unwrap();
    let task_store = store.clone();
    let task = tokio::spawn(async move { task_store.claim("cancelled").await });
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(dir.0.join("state.sqlite.owner.lock"))
        .unwrap();
    // The external write transaction holds claim before its owner commit. Observe
    // the OS lock rather than guessing that a sleep means the job has started.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match FileExt::try_lock_exclusive(&lock) {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Ok(()) => FileExt::unlock(&lock).unwrap(),
                Err(e) => panic!("lock probe failed: {e}"),
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    transaction.rollback().unwrap();
    // FIFO: the abandoned claim completes and releases before this next job.
    store.claim("replacement").await.unwrap();
    store.release("replacement").await.unwrap();
}

#[tokio::test]
async fn checkpoint_two_migration_preserves_confirmations_intentions_artifacts_and_receipts() {
    use futures::{StreamExt, stream};
    let dir = Directory::new();
    let store = dir.store();
    store.claim("old").await.unwrap();
    let source = store
        .write(
            "test",
            Box::pin(stream::once(async { Ok(vec![1, 2, 3]) })),
            "test/data",
        )
        .await
        .unwrap();
    let mut run = initial("run");
    run.artifacts = vec![source.clone()];
    store
        .create("old", run.clone(), Some(reservation()), &Limits::default())
        .await
        .unwrap();
    run.revision = 1;
    run.state = RunState::Running;
    let record = InvocationRecord {
        id: "invocation".into(),
        attempt_id: "attempt".into(),
        attempts: 1,
        state: InvocationState::Running,
        input: json!(7),
        output: None,
        error: None,
        operation: Some(OperationRevision::new("test.write", "1", "r1")),
        config: json!({}),
        effect_key: Some("effect".into()),
        retry: RetryPolicy::default(),
        next_attempt_at_ms: None,
        certainty: EffectCertainty::Unknown,
        control: None,
    };
    run.invocations
        .insert("/nodes/pending".into(), record.clone());
    let mut confirmed = record;
    confirmed.id = "confirmed".into();
    confirmed.attempt_id = "confirmed-attempt".into();
    confirmed.state = InvocationState::Succeeded;
    confirmed.input = Value::Null;
    confirmed.output = Some(json!({"confirmed":true}));
    confirmed.certainty = EffectCertainty::Applied;
    run.invocations.insert("/nodes/confirmed".into(), confirmed);
    store.commit("old", 0, run.clone()).await.unwrap();
    store.release("old").await.unwrap();
    support::downgrade_to_two(&dir.path());
    let migrated = dir.store();
    migrated.claim("new").await.unwrap();
    assert_eq!(migrated.get(&run.id).await.unwrap(), Some(run.clone()));
    assert_eq!(
        migrated.view(&run.id, None).await.unwrap(),
        Some(run.view(None))
    );
    let mut stream = migrated.read(&source).await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), vec![1, 2, 3]);
    assert!(stream.next().await.is_none());
    assert!(
        matches!(migrated.create("new",initial("another"),Some(reservation()),&Limits::default()).await.unwrap(),CreateOutcome::Duplicate { run_id,.. } if run_id==run.id)
    );
    migrated.release("new").await.unwrap();
    let reopened = dir.store();
    reopened.claim("again").await.unwrap();
    assert_eq!(reopened.get(&run.id).await.unwrap(), Some(run));
    reopened.release("again").await.unwrap();
}

#[tokio::test]
async fn migration_failure_rolls_back_envelopes_hashes_and_ddl_before_releasing_ownership() {
    let dir = Directory::new();
    let store = dir.store();
    store.claim("old").await.unwrap();
    let run = initial("run");
    store
        .create("old", run.clone(), Some(reservation()), &Limits::default())
        .await
        .unwrap();
    store.release("old").await.unwrap();
    support::downgrade_to_two(&dir.path());
    let conn = rusqlite::Connection::open(dir.path()).unwrap();
    let package: (String, Vec<u8>) = conn
        .query_row("SELECT id,body FROM wf_packages", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })
        .unwrap();
    let request: Vec<u8> = conn
        .query_row("SELECT request FROM wf_receipts", [], |r| r.get(0))
        .unwrap();
    conn.execute(
        "UPDATE wf_receipts SET request=?1",
        [br#"{"format":999,"payload":null}"#.as_slice()],
    )
    .unwrap();
    assert_eq!(
        store.claim("failed").await.unwrap_err().code(),
        "capability.unsupported"
    );
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))
            .unwrap(),
        2
    );
    assert_eq!(
        conn.query_row("SELECT id,body FROM wf_packages", [], |r| Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Vec<u8>>(1)?
        )))
        .unwrap(),
        package
    );
    assert!(
        conn.prepare("SELECT next_wakeup_at_ms FROM wf_runs")
            .is_err()
    );
    conn.execute("UPDATE wf_receipts SET request=?1", [request])
        .unwrap();
    store.claim("fixed").await.unwrap();
    assert_eq!(store.get(&run.id).await.unwrap(), Some(run));
    store.release("fixed").await.unwrap();
}
