#![cfg(feature = "sqlite")]
use futures::{StreamExt, stream};
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use workflow_forge_modules::{SqliteExecutionStore, SqliteOptions};
use workflow_forge_protocol::*;
#[path = "support/sqlite.rs"]
mod support;
use support::{Directory, initial, reservation};

fn bytes(data: Vec<u8>) -> ByteStream {
    Box::pin(stream::once(async { Ok(data) }))
}
fn access(owner: &str, id: &str) -> ArtifactAccess {
    ArtifactAccess {
        runtime_owner: owner.into(),
        run_id: RunId(id.into()),
    }
}
async fn read(store: &SqliteExecutionStore, reference: &ArtifactRef) -> Vec<u8> {
    let mut stream = store.read(reference).await.unwrap();
    let mut data = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert!(chunk.len() <= 65536);
        data.extend(chunk);
    }
    data
}
async fn finish(store: &SqliteExecutionStore, owner: &str, id: &str) {
    let mut run = store.get(&RunId(id.into())).await.unwrap().unwrap();
    run.revision += 1;
    run.state = RunState::Succeeded;
    run.finished_at_ms = Some(1000);
    run.output = Some(json!(1));
    store.commit(owner, run.revision - 1, run).await.unwrap();
}
fn count(dir: &Directory, table: &str) -> usize {
    // Identifiers are test constants, never input from users.
    rusqlite::Connection::open(dir.path())
        .unwrap()
        .query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

#[tokio::test]
async fn streaming_reopen_and_shared_run_retention_preserve_exact_bytes() {
    let dir = Directory::new();
    let store = dir.store();
    store.claim("first").await.unwrap();
    let data: Vec<u8> = (0..200_003).map(|n| (n % 251) as u8).collect();
    let source = store
        .write("test", bytes(data.clone()), "application/octet-stream")
        .await
        .unwrap();
    assert_eq!(read(&store, &source).await, data);
    for id in ["one", "two"] {
        let mut run = initial(id);
        run.artifacts = vec![source.clone()];
        store
            .create("first", run, None, &Limits::default())
            .await
            .unwrap();
    }
    // Checkpoint attachments are immutable, including when all metadata matches.
    let mut altered = store.get(&RunId("one".into())).await.unwrap().unwrap();
    altered.revision += 1;
    altered.artifacts.clear();
    assert_eq!(
        store.commit("first", 0, altered).await.unwrap_err().code(),
        "state.conflict"
    );
    finish(&store, "first", "one").await;
    store
        .collect(
            "first",
            1000,
            &Limits {
                terminal_runs: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(count(&dir, "wf_artifact_owners"), 1);
    store.release("first").await.unwrap();
    let reopened = dir.store();
    reopened.claim("second").await.unwrap();
    // Beyond the unowned-upload grace, a nonterminal owner still pins the file.
    reopened
        .collect("second", i64::MAX as u64, &Limits::default())
        .await
        .unwrap();
    assert_eq!(read(&reopened, &source).await, data);
    let snapshot = reopened.get(&RunId("two".into())).await.unwrap().unwrap();
    assert_eq!(snapshot.artifacts, vec![source.clone()]);
    assert_eq!(
        snapshot.head(),
        reopened
            .view(&snapshot.id, None)
            .await
            .unwrap()
            .unwrap()
            .head
    );
    finish(&reopened, "second", "two").await;
    reopened
        .collect(
            "second",
            1000,
            &Limits {
                terminal_runs: 0,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        reopened.read(&source).await.err().unwrap().code(),
        "not_found"
    );
    assert_eq!(count(&dir, "wf_artifact_chunks"), 0);
    reopened.release("second").await.unwrap();
}

#[tokio::test]
async fn acceptance_pins_atomically_and_checks_metadata_scope_and_declaration() {
    let dir = Directory::new();
    let store = dir.store();
    store.claim("owner").await.unwrap();
    let source = store
        .write("test", bytes(vec![1, 2, 3]), "test/data")
        .await
        .unwrap();
    let other = store
        .write("test", bytes(vec![4]), "test/data")
        .await
        .unwrap();
    let mut run = initial("run");
    let mut missing = source.clone();
    missing.id = "missing".into();
    // The first pin must roll back when the following reference fails.
    run.artifacts = vec![source.clone(), missing];
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
        "not_found"
    );
    assert_eq!(count(&dir, "wf_artifact_owners"), 0);
    assert_eq!(count(&dir, "wf_receipts"), 0);
    assert!(store.get(&run.id).await.unwrap().is_none());
    run.artifacts = vec![source.clone()];
    assert!(matches!(
        store
            .create("owner", run, Some(reservation()), &Limits::default())
            .await
            .unwrap(),
        CreateOutcome::Created
    ));
    let permit = access("owner", "run");
    assert!(store.read_for_run(&permit, &source).await.is_ok());
    assert_eq!(
        store
            .read_for_run(&permit, &other)
            .await
            .err()
            .unwrap()
            .code(),
        "access.denied"
    );
    let mut forged = source.clone();
    forged.bytes += 1;
    assert_eq!(store.read(&forged).await.err().unwrap().code(), "not_found");
    let mut forged = source.clone();
    forged.scope = "outside".into();
    assert_eq!(store.read(&forged).await.err().unwrap().code(), "not_found");
    let mut forged = source.clone();
    forged.media_type = "forged/type".into();
    assert_eq!(store.read(&forged).await.err().unwrap().code(), "not_found");
    let produced = store
        .write_for_run(&permit, "test", bytes(vec![9]), "test/output")
        .await
        .unwrap();
    assert!(store.read_for_run(&permit, &produced).await.is_ok());
    assert_eq!(
        store
            .write_for_run(&permit, "outside", bytes(vec![9]), "test/output")
            .await
            .unwrap_err()
            .code(),
        "access.denied"
    );
    store.release("owner").await.unwrap();
}

#[tokio::test]
async fn old_owner_streams_and_writers_cannot_follow_a_reclaimed_provider() {
    let dir = Directory::new();
    let store = dir.store();
    store.claim("old").await.unwrap();
    store
        .create("old", initial("run"), None, &Limits::default())
        .await
        .unwrap();
    let permit = access("old", "run");
    let artifact = store
        .write_for_run(&permit, "test", bytes(vec![7; 131_072]), "test/data")
        .await
        .unwrap();
    let mut opened = store.read_for_run(&permit, &artifact).await.unwrap();
    assert_eq!(opened.next().await.unwrap().unwrap().len(), 65536);
    store.release("old").await.unwrap();
    store.claim("new").await.unwrap();
    assert_eq!(
        opened.next().await.unwrap().unwrap_err().code(),
        "state.conflict"
    );
    assert_eq!(
        store
            .read_for_run(&permit, &artifact)
            .await
            .err()
            .unwrap()
            .code(),
        "state.conflict"
    );
    assert_eq!(
        store
            .write_for_run(&permit, "test", bytes(vec![1]), "test/data")
            .await
            .unwrap_err()
            .code(),
        "state.conflict"
    );
    assert!(
        store
            .read_for_run(&access("new", "run"), &artifact)
            .await
            .is_ok()
    );
    finish(&store, "new", "run").await;
    assert_eq!(
        store
            .write_for_run(&access("new", "run"), "test", bytes(vec![1]), "test/data")
            .await
            .unwrap_err()
            .code(),
        "state.conflict"
    );
    store.release("new").await.unwrap();
}

#[tokio::test]
async fn cancelled_streams_free_staging_and_quotas_reject_partial_publication() {
    let dir = Directory::new();
    let store = Arc::new(
        SqliteExecutionStore::open(
            dir.path(),
            SqliteOptions {
                max_artifacts: 2,
                max_artifact_bytes: 10,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    store.claim("owner").await.unwrap();
    let polled = Arc::new(tokio::sync::Notify::new());
    let notify = polled.clone();
    let content = stream::once(async { Ok(vec![1; 6]) }).chain(stream::once(async move {
        notify.notify_one();
        std::future::pending().await
    }));
    let writer = store.clone();
    let task =
        tokio::spawn(async move { writer.write("test", Box::pin(content), "test/data").await });
    tokio::time::timeout(Duration::from_secs(2), polled.notified())
        .await
        .unwrap();
    assert_eq!(count(&dir, "wf_artifact_chunks"), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while count(&dir, "wf_artifacts") != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        store
            .write("test", bytes(vec![1; 11]), "test/data")
            .await
            .unwrap_err()
            .code(),
        "resource.limit"
    );
    // Restart also clears unpublished rows if asynchronous cleanup was lost.
    store.release("owner").await.unwrap();
    store.claim("again").await.unwrap();
    let good = store
        .write("test", bytes(vec![8; 8]), "test/data")
        .await
        .unwrap();
    assert_eq!(read(&store, &good).await, vec![8; 8]);
    assert_eq!(
        store
            .write("test", bytes(vec![2; 3]), "test/data")
            .await
            .unwrap_err()
            .code(),
        "resource.limit"
    );
    store.release("again").await.unwrap();
    store.claim("third").await.unwrap();
    let empty = store
        .write("test", bytes(vec![]), "test/data")
        .await
        .unwrap();
    assert!(read(&store, &empty).await.is_empty());
    assert_eq!(
        store
            .write("test", bytes(vec![]), "test/data")
            .await
            .unwrap_err()
            .code(),
        "resource.limit"
    );
    // Unowned ready artifacts are collected after their documented grace.
    store
        .collect("third", i64::MAX as u64, &Limits::default())
        .await
        .unwrap();
    assert_eq!(count(&dir, "wf_artifacts"), 0);
    store.release("third").await.unwrap();
}

#[tokio::test]
async fn schema_one_preserves_values_and_receipts_while_upgrading_the_checkpoint() {
    let dir = Directory::new();
    let store = dir.store();
    store.claim("original").await.unwrap();
    let run = initial("run");
    store
        .create(
            "original",
            run.clone(),
            Some(reservation()),
            &Limits::default(),
        )
        .await
        .unwrap();
    store.release("original").await.unwrap();
    support::downgrade_to_two(&dir.path());
    // Recreate the schema-1 boundary, including its JSON encoding without the
    // new default fields. Logical data survives while envelopes advance to 3.
    let conn = rusqlite::Connection::open(dir.path()).unwrap();
    conn.execute_batch("DROP TABLE wf_artifact_owners; DROP TABLE wf_artifact_chunks; DROP TABLE wf_artifacts; PRAGMA user_version=1;").unwrap();
    let body: Vec<u8> = conn
        .query_row("SELECT body FROM wf_runs WHERE id='run'", [], |r| r.get(0))
        .unwrap();
    let mut value: Value = serde_json::from_slice(&body).unwrap();
    value["payload"]
        .as_object_mut()
        .unwrap()
        .remove("artifacts");
    let body = serde_json::to_vec(&value).unwrap();
    conn.execute("UPDATE wf_runs SET body=?1 WHERE id='run'", [&body])
        .unwrap();
    drop(conn);
    let migrated = dir.store();
    migrated.claim("migrated").await.unwrap();
    assert_eq!(migrated.get(&run.id).await.unwrap(), Some(run.clone()));
    assert!(matches!(
        migrated
            .create("migrated", run, Some(reservation()), &Limits::default())
            .await
            .unwrap(),
        CreateOutcome::Duplicate { .. }
    ));
    let conn = rusqlite::Connection::open(dir.path()).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, u32>(0))
            .unwrap(),
        3
    );
    let upgraded: Vec<u8> = conn
        .query_row("SELECT body FROM wf_runs WHERE id='run'", [], |r| r.get(0))
        .unwrap();
    let upgraded: Value = serde_json::from_slice(&upgraded).unwrap();
    assert_eq!(upgraded["format"], 3);
    assert_eq!(upgraded["payload"]["input"], value["payload"]["input"]);
    migrated
        .write("test", bytes(vec![1]), "test/data")
        .await
        .unwrap();
    migrated.release("migrated").await.unwrap();
}

#[tokio::test]
async fn expired_host_upload_cannot_be_read_or_pinned_before_garbage_collection() {
    let dir = Directory::new();
    let store = SqliteExecutionStore::open(
        dir.path(),
        SqliteOptions {
            artifact_grace_ms: 0,
            ..Default::default()
        },
    )
    .unwrap();
    store.claim("owner").await.unwrap();
    let reference = store
        .write("test", bytes(vec![1]), "test/data")
        .await
        .unwrap();
    assert_eq!(
        store.read(&reference).await.err().unwrap().code(),
        "not_found"
    );
    assert_eq!(
        count(&dir, "wf_artifacts"),
        1,
        "expiry is checked without requiring a GC pass"
    );
    let mut run = initial("run");
    run.artifacts = vec![reference];
    assert_eq!(
        store
            .create("owner", run, Some(reservation()), &Limits::default())
            .await
            .err()
            .unwrap()
            .code(),
        "not_found"
    );
    assert_eq!(count(&dir, "wf_receipts"), 0);
    store
        .collect("owner", i64::MAX as u64, &Limits::default())
        .await
        .unwrap();
    assert_eq!(count(&dir, "wf_artifacts"), 0);
    store.release("owner").await.unwrap();
}
