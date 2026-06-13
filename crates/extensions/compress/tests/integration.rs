//! Roundtrips de la extensión compress contra el executor y el BlobStore
//! reales: comprimir → descomprimir devuelve el contenido original, y el
//! catálogo expone las cuatro tareas.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::io::blob::BlobRef;
use workflow_forge_core::runtime::{WorkflowContext, WorkflowExecutor};
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::task::{Task, TaskManifest, TaskRegistry, WorkflowData, WorkflowResult};

/// `{ content: "texto"|[bytes], name? }` → `{ file: $blob }`
struct MakeBlobTask(TaskManifest);

#[async_trait]
impl Task for MakeBlobTask {
    fn manifest(&self) -> &TaskManifest {
        &self.0
    }
    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let bytes = match &input.0["content"] {
            Value::String(s) => s.clone().into_bytes(),
            Value::Array(nums) => nums.iter().map(|n| n.as_u64().unwrap() as u8).collect(),
            other => panic!("content inesperado: {other}"),
        };
        let name = input.0["name"].as_str().map(str::to_string);
        let blob = ctx.blobs().put(bytes, name).await?;
        Ok(WorkflowData(
            json!({ "file": serde_json::to_value(&blob).unwrap() }),
        ))
    }
}

/// `{ file: $blob }` → `{ bytes: [u8...], name }`
struct ReadBlobTask(TaskManifest);

#[async_trait]
impl Task for ReadBlobTask {
    fn manifest(&self) -> &TaskManifest {
        &self.0
    }
    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let blob = BlobRef::from_value(&input.0["file"]).expect("BlobRef en input.file");
        let bytes = ctx.blobs().get(&blob).await?;
        Ok(WorkflowData(json!({ "bytes": bytes, "name": blob.name })))
    }
}

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    workflow_forge_ext_compress::register(&registry);
    registry.register(MakeBlobTask(TaskManifest::new("test.make_blob")));
    registry.register(ReadBlobTask(TaskManifest::new("test.read_blob")));
    registry
}

async fn run(workflow: Value, trigger: Value) -> WorkflowData {
    let workflow: WorkflowDefinition = serde_json::from_value(workflow).unwrap();
    let executor = WorkflowExecutor::new(workflow, registry()).unwrap();
    executor.run(WorkflowData(trigger)).await.unwrap()
}

/// start → make_blob → <comprimir> → <descomprimir> → read_blob → end
fn roundtrip_workflow(compress_task: &str, decompress_task: &str) -> Value {
    json!({
        "name": "roundtrip", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "make", "kind": "task", "task": "test.make_blob",
              "input": { "content": "$.trigger.content", "name": "$.trigger.name" } },
            { "id": "comprimir", "kind": "task", "task": compress_task,
              "input": { "file": "$.nodes.make.output.file" } },
            { "id": "descomprimir", "kind": "task", "task": decompress_task,
              "input": { "file": "$.nodes.comprimir.output.file" } },
            { "id": "read", "kind": "task", "task": "test.read_blob",
              "input": { "file": "$.nodes.descomprimir.output.file" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "make" },
            { "from": "make", "to": "comprimir" },
            { "from": "comprimir", "to": "descomprimir" },
            { "from": "descomprimir", "to": "read" },
            { "from": "read", "to": "end" }
        ]
    })
}

fn bytes_to_string(out: &WorkflowData) -> String {
    let bytes: Vec<u8> = out.0["bytes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n.as_u64().unwrap() as u8)
        .collect();
    String::from_utf8(bytes).unwrap()
}

#[tokio::test]
async fn gzip_gunzip_roundtrip_conserva_el_contenido() {
    let original = "sku,cantidad\nABC-123,5\nÑÑÑ,9\n".repeat(50);
    let out = run(
        roundtrip_workflow("compress.gzip", "compress.gunzip"),
        json!({ "content": original, "name": "ventas.csv" }),
    )
    .await;
    assert_eq!(bytes_to_string(&out), original);
}

#[tokio::test]
async fn gunzip_nombra_quitando_la_extension_gz() {
    // make → gzip (ventas.csv → ventas.csv.gz) → gunzip (→ ventas.csv)
    let workflow = json!({
        "name": "nombre", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "make", "kind": "task", "task": "test.make_blob",
              "input": { "content": "hola", "name": "ventas.csv" } },
            { "id": "gz", "kind": "task", "task": "compress.gzip",
              "input": { "file": "$.nodes.make.output.file" } },
            { "id": "gunzip", "kind": "task", "task": "compress.gunzip",
              "input": { "file": "$.nodes.gz.output.file" } },
            { "id": "read", "kind": "task", "task": "test.read_blob",
              "input": { "file": "$.nodes.gunzip.output.file" } },
            { "id": "end", "kind": "end", "output": {
                "gz_name": "$.nodes.gz.output.file.name",
                "final_name": "$.nodes.read.output.name"
            } }
        ],
        "edges": [
            { "from": "start", "to": "make" },
            { "from": "make", "to": "gz" },
            { "from": "gz", "to": "gunzip" },
            { "from": "gunzip", "to": "read" },
            { "from": "read", "to": "end" }
        ]
    });
    let out = run(workflow, json!({})).await;
    assert_eq!(out.0["gz_name"], "ventas.csv.gz");
    assert_eq!(out.0["final_name"], "ventas.csv");
}

#[tokio::test]
async fn zip_unzip_roundtrip_con_varias_entradas() {
    // make dos blobs → zip [a.txt, b.txt] → unzip → leer ambas entradas
    let workflow = json!({
        "name": "zip", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "make_a", "kind": "task", "task": "test.make_blob",
              "input": { "content": "contenido A", "name": "a.txt" } },
            { "id": "make_b", "kind": "task", "task": "test.make_blob",
              "input": { "content": "contenido B", "name": "b.txt" } },
            { "id": "zip", "kind": "task", "task": "compress.zip",
              "input": { "name": "bundle.zip", "entries": [
                  { "name": "a.txt", "file": "$.nodes.make_a.output.file" },
                  { "name": "carpeta/b.txt", "file": "$.nodes.make_b.output.file" }
              ] } },
            { "id": "unzip", "kind": "task", "task": "compress.unzip",
              "input": { "file": "$.nodes.zip.output.file" } },
            { "id": "end", "kind": "end", "output": "$.nodes.unzip.output" }
        ],
        "edges": [
            { "from": "start", "to": "make_a" },
            { "from": "make_a", "to": "make_b" },
            { "from": "make_b", "to": "zip" },
            { "from": "zip", "to": "unzip" },
            { "from": "unzip", "to": "end" }
        ]
    });
    let out = run(workflow, json!({})).await;
    let entries = out.0["entries"].as_array().expect("entries es array");
    assert_eq!(entries.len(), 2);
    let names: Vec<&str> = entries
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"a.txt"), "nombres: {names:?}");
    assert!(names.contains(&"carpeta/b.txt"), "nombres: {names:?}");
    // cada entrada trae su referencia $blob
    assert!(entries[0]["file"]["$blob"].is_string());
}

#[tokio::test]
async fn input_invalido_da_codigo_claro() {
    let registry = registry();
    let task = registry.get(&"compress.gunzip".into()).unwrap();
    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "x", "version": "0.1.0", "nodes": [], "edges": []
    }))
    .unwrap();
    let ctx = WorkflowContext::new(&workflow, json!({}));
    // Falta `file`
    let err = task
        .execute(&ctx, WorkflowData(json!({ "nope": 1 })))
        .await
        .unwrap_err();
    assert_eq!(err.code, "COMPRESS_INPUT_INVALID");
}

#[test]
fn el_catalogo_expone_las_cuatro_tareas() {
    let ids: Vec<String> = registry().catalog().into_iter().map(|m| m.id.0).collect();
    for expected in [
        "compress.gzip",
        "compress.gunzip",
        "compress.zip",
        "compress.unzip",
    ] {
        assert!(ids.contains(&expected.to_string()), "falta {expected}");
    }
}
