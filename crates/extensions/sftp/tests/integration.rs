//! Tests de integración contra un servidor SFTP real.
//!
//! Ignorados por default (no hay servidor en CI). Para correrlos:
//!
//! ```bash
//! export WF_SFTP_HOST=localhost WF_SFTP_PORT=2222 \
//!        WF_SFTP_USER=demo WF_SFTP_PASSWORD=demo WF_SFTP_DIR=/upload
//! cargo test -p workflow-forge-ext-sftp -- --ignored
//! ```
//!
//! Servidor desechable: `docker run -p 2222:22 atmoz/sftp demo:demo:::upload`

use std::sync::Arc;

use serde_json::{Value, json};
use workflow_forge_core::runtime::WorkflowExecutor;
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::task::TaskRegistry;
use workflow_forge_core::task::WorkflowData;

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_sftp::register(&registry);
    workflow_forge_ext_tabular_stub::register(&registry);
    registry
}

/// Mini-tarea local para producir un blob de prueba sin depender de otra extensión
mod workflow_forge_ext_tabular_stub {
    use async_trait::async_trait;
    use serde_json::{Value, json};
    use workflow_forge_core::runtime::WorkflowContext;
    use workflow_forge_core::task::TaskRegistry;
    use workflow_forge_core::task::{Task, TaskManifest};
    use workflow_forge_core::task::{WorkflowData, WorkflowResult};

    pub fn register(registry: &TaskRegistry) {
        registry.register(MakeBlobTask {
            manifest: TaskManifest::new("test.make_blob"),
        });
    }

    struct MakeBlobTask {
        manifest: TaskManifest,
    }

    #[async_trait]
    impl Task for MakeBlobTask {
        fn manifest(&self) -> &TaskManifest {
            &self.manifest
        }

        async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
            let content = input
                .get("content")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let name = input
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("test.txt");
            let blob = ctx
                .blobs()
                .put(content.as_bytes().to_vec(), Some(name.into()))
                .await?;
            Ok(WorkflowData(json!({ "file": blob })))
        }
    }
}

fn connection() -> Value {
    json!({
        "host": std::env::var("WF_SFTP_HOST").expect("WF_SFTP_HOST"),
        "port": std::env::var("WF_SFTP_PORT").map(|p| p.parse::<u16>().unwrap()).unwrap_or(22),
        "username": std::env::var("WF_SFTP_USER").expect("WF_SFTP_USER"),
        "auth": {
            "type": "password",
            "password": std::env::var("WF_SFTP_PASSWORD").expect("WF_SFTP_PASSWORD")
        }
    })
}

#[tokio::test]
#[ignore = "requiere servidor SFTP (variables WF_SFTP_*)"]
async fn put_list_get_roundtrip() {
    let dir = std::env::var("WF_SFTP_DIR").unwrap_or_else(|_| "/upload".into());
    let remote_path = format!("{dir}/wf-test.txt");

    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "sftp-roundtrip", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "crea", "kind": "task", "task": "test.make_blob",
              "input": { "content": "hola sftp", "name": "wf-test.txt" } },
            { "id": "sube", "kind": "task", "task": "sftp.put",
              "input": {
                  "connection": connection(),
                  "file": "$.nodes.crea.output.file",
                  "path": remote_path
              } },
            { "id": "lista", "kind": "task", "task": "sftp.list",
              "input": { "connection": connection(), "path": dir } },
            { "id": "baja", "kind": "task", "task": "sftp.get",
              "input": { "connection": connection(), "path": remote_path } },
            { "id": "end", "kind": "end",
              "output": {
                  "subido": "$.nodes.sube.output.size",
                  "entradas": "$.nodes.lista.output.entries",
                  "descargado": "$.nodes.baja.output.file"
              } }
        ],
        "edges": [
            { "from": "start", "to": "crea" },
            { "from": "crea", "to": "sube" },
            { "from": "sube", "to": "lista" },
            { "from": "lista", "to": "baja" },
            { "from": "baja", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    let result = executor.run(WorkflowData(json!({}))).await.unwrap();

    assert_eq!(result.0["subido"], 9);
    assert_eq!(result.0["descargado"]["name"], "wf-test.txt");
    assert_eq!(result.0["descargado"]["size"], 9);
    let entries = result.0["entradas"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e["name"] == "wf-test.txt" && e["kind"] == "file")
    );
}

#[test]
fn catalogo_con_schemas() {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_sftp::register(&registry);
    let catalog = registry.catalog();
    let ids: Vec<&str> = catalog.iter().map(|m| m.id.0.as_str()).collect();
    assert_eq!(ids, vec!["sftp.get", "sftp.list", "sftp.put"]);
    assert!(catalog.iter().all(|m| m.input_schema.is_some()));
}
