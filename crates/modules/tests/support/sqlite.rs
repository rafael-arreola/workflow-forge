use serde_json::json;
use std::path::PathBuf;
use workflow_forge_modules::{SqliteExecutionStore, SqliteOptions};
use workflow_forge_protocol::*;

pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("workflow-forge-sqlite-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub fn path(&self) -> PathBuf {
        self.0.join("state.sqlite")
    }
    pub fn store(&self) -> SqliteExecutionStore {
        SqliteExecutionStore::open(self.path(), SqliteOptions::default()).unwrap()
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn initial(id: &str) -> RunSnapshot {
    let definition:WorkflowDefinition=serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":"test","revision":"r1","entry":"one",
        "nodes":[{"id":"one","kind":"operation","operation":{"id":"forge.data.identity","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}}],
        "edges":[],"output":{"select":{"source":"node","node":"one","pointer":""}},"input_schema":true,"output_schema":true
    })).unwrap();
    RunSnapshot {
        waits: Default::default(),
        artifacts: vec![],
        checkpoint_format: CHECKPOINT_FORMAT,
        id: RunId(id.into()),
        scope: "test".into(),
        actor: "host".into(),
        resources: Default::default(),
        revision: 0,
        package: ResolvedPackage {
            definitions: vec![definition.clone()],
            ..Default::default()
        },
        definition,
        input: json!({"original":true}),
        state: RunState::Accepted,
        invocations: Default::default(),
        output: None,
        error: None,
        created_at_ms: 100,
        deadline_at_ms: 1000,
        finished_at_ms: None,
        cancel_requested: false,
        audit: vec![],
        unresolved_effects: vec![],
    }
}
pub fn reservation() -> ReceiptReservation {
    ReceiptReservation {
        key: "receipt".into(),
        request: json!({"input":1}),
        expires_at_ms: 60000,
    }
}

/// Build a genuine format-2 boundary from a closed provider, retaining every
/// logical value while removing only the new representation and SQL projection.
pub fn downgrade_to_two(path: &std::path::Path) {
    use sha2::{Digest, Sha256};
    let mut conn = rusqlite::Connection::open(path).unwrap();
    conn.pragma_update(None, "foreign_keys", true).unwrap();
    let tx = conn.transaction().unwrap();
    let rows: Vec<(String, Vec<u8>)> = tx
        .prepare("SELECT id,body FROM wf_packages")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    for (id, body) in rows {
        let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        value["format"] = json!(2);
        let bytes = serde_json::to_vec(&value).unwrap();
        let hash = format!("{:x}", Sha256::digest(&bytes));
        tx.execute(
            "INSERT INTO wf_packages(id,body) VALUES(?1,?2)",
            rusqlite::params![hash, bytes],
        )
        .unwrap();
        tx.execute(
            "UPDATE wf_runs SET package_id=?1 WHERE package_id=?2",
            rusqlite::params![hash, id],
        )
        .unwrap();
        tx.execute("DELETE FROM wf_packages WHERE id=?1", [id])
            .unwrap();
    }
    for (table, column) in [
        ("wf_runs", "body"),
        ("wf_invocations", "body"),
        ("wf_receipts", "request"),
    ] {
        let query = format!("SELECT rowid,{column} FROM {table}");
        let rows: Vec<(i64, Vec<u8>)> = tx
            .prepare(&query)
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        for (id, body) in rows {
            let mut value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            value["format"] = json!(2);
            if table == "wf_runs" {
                assert!(value["payload"]["waits"].as_object().unwrap().is_empty());
                value["payload"].as_object_mut().unwrap().remove("waits");
                value["payload"]["checkpoint_format"] = json!(2);
            }
            tx.execute(
                &format!("UPDATE {table} SET {column}=?1 WHERE rowid=?2"),
                rusqlite::params![serde_json::to_vec(&value).unwrap(), id],
            )
            .unwrap();
        }
    }
    tx.execute_batch("DROP INDEX wf_runs_wakeup; ALTER TABLE wf_runs DROP COLUMN next_wakeup_at_ms; PRAGMA user_version=2;").unwrap();
    tx.commit().unwrap();
}
