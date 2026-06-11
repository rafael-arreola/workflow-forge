//! Nodos `kind: "subworkflow"`: el input es el trigger del hijo, el output
//! final del hijo es el output del nodo, las tres salidas rutean, los blobs
//! cruzan la frontera y los eventos llevan `parent_execution_id`.

use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::io::blob::BlobRef;
use workflow_forge_core::observe::{EventKind, InMemoryHistory, NodeStatus};
use workflow_forge_core::runtime::{WorkflowContext, WorkflowExecutor, WorkflowRegistry};
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::task::{Task, TaskManifest, TaskRegistry};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Devuelve su input con `marca: true`; falla si trae `fail` y panickea si
/// trae `boom`.
struct EchoTask(TaskManifest);

#[async_trait]
impl Task for EchoTask {
    fn manifest(&self) -> &TaskManifest {
        &self.0
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        if input.get("fail").and_then(Value::as_bool).unwrap_or(false) {
            return Err(WorkflowError::new("FALLO_HIJO", "el hijo falló"));
        }
        if input.get("boom").and_then(Value::as_bool).unwrap_or(false) {
            panic!("bug dentro del hijo");
        }
        let mut output = input.0;
        output["marca"] = json!(true);
        Ok(WorkflowData(output))
    }
}

/// Escribe `content` como blob y devuelve `{ file: BlobRef }`
struct PutBlobTask(TaskManifest);

#[async_trait]
impl Task for PutBlobTask {
    fn manifest(&self) -> &TaskManifest {
        &self.0
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let content = input.get("content").and_then(Value::as_str).unwrap_or("");
        let blob = ctx
            .blobs()
            .put(content.as_bytes().to_vec(), Some("datos.txt".into()))
            .await?;
        Ok(WorkflowData(
            json!({ "file": serde_json::to_value(&blob).unwrap() }),
        ))
    }
}

/// Lee un BlobRef y devuelve `{ text }`
struct ReadBlobTask(TaskManifest);

#[async_trait]
impl Task for ReadBlobTask {
    fn manifest(&self) -> &TaskManifest {
        &self.0
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let blob = BlobRef::from_value(input.get("file").unwrap_or(&Value::Null))
            .ok_or_else(|| WorkflowError::new("BLOB_REF_INVALID", "se esperaba un $blob"))?;
        let bytes = ctx.blobs().get(&blob).await?;
        Ok(WorkflowData(
            json!({ "text": String::from_utf8_lossy(&bytes) }),
        ))
    }
}

fn registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register(EchoTask(TaskManifest::new("test.echo")));
    registry.register(PutBlobTask(TaskManifest::new("test.put_blob")));
    registry.register(ReadBlobTask(TaskManifest::new("test.read_blob")));
    registry
}

/// Workflow hijo mínimo: start → test.echo → end
fn child_def(name: &str) -> Value {
    json!({
        "name": name, "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "trabajo", "kind": "task", "task": "test.echo" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "trabajo" },
            { "from": "trabajo", "to": "end" }
        ]
    })
}

fn parent_with_inline(
    child: Value,
    sub_node: Value,
    extra_nodes: Value,
    extra_edges: Value,
) -> Value {
    let mut nodes = vec![json!({ "id": "start", "kind": "start" })];
    nodes.push(sub_node);
    nodes.push(json!({ "id": "end", "kind": "end" }));
    nodes.extend(extra_nodes.as_array().unwrap().iter().cloned());
    let mut edges = vec![
        json!({ "from": "start", "to": "proceso" }),
        json!({ "from": "proceso", "to": "end" }),
    ];
    edges.extend(extra_edges.as_array().unwrap().iter().cloned());
    json!({
        "name": "padre", "version": "0.1.0",
        "workflows": [child],
        "nodes": nodes,
        "edges": edges
    })
}

fn executor(workflow: Value) -> Result<WorkflowExecutor, Vec<WorkflowError>> {
    WorkflowExecutor::new(serde_json::from_value(workflow).unwrap(), registry())
}

fn build_errors(workflow: Value) -> Vec<WorkflowError> {
    match executor(workflow) {
        Ok(_) => panic!("se esperaba un error de construcción"),
        Err(errors) => errors,
    }
}

// ---------------------------------------------------------------------------

#[tokio::test]
async fn subworkflow_inline_ejecuta_y_devuelve_el_output_del_hijo() {
    let workflow = parent_with_inline(
        child_def("hijo"),
        json!({ "id": "proceso", "kind": "subworkflow", "workflow": "hijo",
                "input": { "pedido": "$.trigger.pedido" } }),
        json!([]),
        json!([]),
    );
    let result = executor(workflow)
        .unwrap()
        .run(WorkflowData(json!({ "pedido": 42 })))
        .await
        .unwrap();

    assert_eq!(result.0, json!({ "pedido": 42, "marca": true }));
}

#[tokio::test]
async fn subworkflow_sin_input_recibe_el_token_del_predecesor() {
    let workflow = parent_with_inline(
        child_def("hijo"),
        json!({ "id": "proceso", "kind": "subworkflow", "workflow": "hijo" }),
        json!([]),
        json!([]),
    );
    let result = executor(workflow)
        .unwrap()
        .run(WorkflowData(json!({ "directo": true })))
        .await
        .unwrap();

    assert_eq!(result.0, json!({ "directo": true, "marca": true }));
}

#[tokio::test]
async fn subworkflow_desde_registro_compartido() {
    let workflows = Arc::new(WorkflowRegistry::new());
    let child: WorkflowDefinition = serde_json::from_value(child_def("comun")).unwrap();
    workflows.register(child).unwrap();

    let parent = json!({
        "name": "padre", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "proceso", "kind": "subworkflow", "workflow": "comun" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "proceso" },
            { "from": "proceso", "to": "end" }
        ]
    });
    let executor = WorkflowExecutor::builder(serde_json::from_value(parent).unwrap(), registry())
        .workflows(workflows)
        .build()
        .unwrap();

    let result = executor.run(WorkflowData(json!({ "x": 1 }))).await.unwrap();
    assert_eq!(result.0, json!({ "x": 1, "marca": true }));
}

#[tokio::test]
async fn nombre_inexistente_falla_al_construir() {
    let parent = json!({
        "name": "padre", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "proceso", "kind": "subworkflow", "workflow": "fantasma" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "proceso" },
            { "from": "proceso", "to": "end" }
        ]
    });
    let errors = build_errors(parent);
    assert!(errors.iter().any(|e| e.code == "SUBWORKFLOW_NOT_FOUND"));
}

#[tokio::test]
async fn ciclo_de_subworkflows_falla_al_construir() {
    // "padre" contiene un hijo inline que referencia a "padre"
    let recursive_child = json!({
        "name": "hijo", "version": "0.1.0",
        "workflows": [],
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "vuelta", "kind": "subworkflow", "workflow": "padre" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "vuelta" },
            { "from": "vuelta", "to": "end" }
        ]
    });
    let workflow = parent_with_inline(
        recursive_child,
        json!({ "id": "proceso", "kind": "subworkflow", "workflow": "hijo" }),
        json!([]),
        json!([]),
    );
    let errors = build_errors(workflow);
    assert!(
        errors.iter().any(|e| e.code == "SUBWORKFLOW_CYCLE"),
        "errores: {errors:?}"
    );
}

#[tokio::test]
async fn profundidad_excesiva_falla_al_construir() {
    // Cadena de 9 niveles: n0 → n1 → ... → n8
    let mut child = child_def("n8");
    for level in (0..8).rev() {
        child = json!({
            "name": format!("n{level}"), "version": "0.1.0",
            "workflows": [child],
            "nodes": [
                { "id": "start", "kind": "start" },
                { "id": "baja", "kind": "subworkflow", "workflow": format!("n{}", level + 1) },
                { "id": "end", "kind": "end" }
            ],
            "edges": [
                { "from": "start", "to": "baja" },
                { "from": "baja", "to": "end" }
            ]
        });
    }
    let errors = build_errors(child);
    assert!(
        errors
            .iter()
            .any(|e| e.code == "SUBWORKFLOW_DEPTH_EXCEEDED"),
        "errores: {errors:?}"
    );
}

#[tokio::test]
async fn fallo_del_hijo_rutea_por_on_error() {
    let workflow = parent_with_inline(
        child_def("hijo"),
        json!({ "id": "proceso", "kind": "subworkflow", "workflow": "hijo" }),
        json!([{ "id": "end-error", "kind": "end" }]),
        json!([{ "from": "proceso", "on": "error", "to": "end-error" }]),
    );
    let result = executor(workflow)
        .unwrap()
        .run(WorkflowData(json!({ "fail": true })))
        .await
        .unwrap();

    assert_eq!(result.0["code"], json!("FALLO_HIJO"));
}

#[tokio::test]
async fn panic_dentro_del_hijo_rutea_por_on_panic_del_nodo() {
    let workflow = parent_with_inline(
        child_def("hijo"),
        json!({ "id": "proceso", "kind": "subworkflow", "workflow": "hijo" }),
        json!([{ "id": "end-panico", "kind": "end" }]),
        json!([{ "from": "proceso", "on": "panic", "to": "end-panico" }]),
    );
    let history = Arc::new(InMemoryHistory::new());
    let executor = executor(workflow).unwrap().with_observer(
        Arc::clone(&history) as Arc<dyn workflow_forge_core::observe::ExecutionObserver>
    );

    let result = executor
        .run(WorkflowData(json!({ "boom": true })))
        .await
        .unwrap();

    assert_eq!(result.0["code"], json!("TASK_PANIC"));
    let report = history.report();
    let node = report
        .nodes
        .iter()
        .find(|n| n.node_id == "proceso")
        .unwrap();
    assert_eq!(node.status, NodeStatus::ErrorRouted);
}

#[tokio::test]
async fn los_blobs_cruzan_la_frontera_del_subworkflow() {
    // El hijo produce un blob; el padre lo lee después de que el hijo terminó
    let child = json!({
        "name": "productor", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "escribe", "kind": "task", "task": "test.put_blob",
              "input": { "content": "$.trigger.content" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "escribe" },
            { "from": "escribe", "to": "end" }
        ]
    });
    let parent = json!({
        "name": "padre", "version": "0.1.0",
        "workflows": [child],
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "produce", "kind": "subworkflow", "workflow": "productor",
              "input": { "content": "hola desde el hijo" } },
            { "id": "lee", "kind": "task", "task": "test.read_blob",
              "input": { "file": "$.nodes.produce.output.file" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "produce" },
            { "from": "produce", "to": "lee" },
            { "from": "lee", "to": "end" }
        ]
    });
    let result = executor(parent)
        .unwrap()
        .run(WorkflowData(json!({})))
        .await
        .unwrap();

    assert_eq!(result.0, json!({ "text": "hola desde el hijo" }));
}

#[tokio::test]
async fn los_eventos_del_hijo_llevan_parent_execution_id() {
    let workflow = parent_with_inline(
        child_def("hijo"),
        json!({ "id": "proceso", "kind": "subworkflow", "workflow": "hijo" }),
        json!([]),
        json!([]),
    );
    let history = Arc::new(InMemoryHistory::new());
    let executor = executor(workflow).unwrap().with_observer(
        Arc::clone(&history) as Arc<dyn workflow_forge_core::observe::ExecutionObserver>
    );
    executor.run(WorkflowData(json!({ "x": 1 }))).await.unwrap();

    let executions = history.executions();
    assert_eq!(executions.len(), 2, "raíz + hijo");
    let root = &executions[0];
    let child = &executions[1];

    for event in history.events() {
        if &event.execution_id == child {
            assert_eq!(event.parent_execution_id.as_deref(), Some(root.as_str()));
        } else {
            assert_eq!(event.parent_execution_id, None);
        }
    }

    // El reporte raíz resume el nodo subworkflow sin mezclar nodos del hijo
    let report = history.report();
    let ids: Vec<&str> = report.nodes.iter().map(|n| n.node_id.as_str()).collect();
    assert!(ids.contains(&"proceso"));
    assert!(!ids.contains(&"trabajo"));
    let proceso = report
        .nodes
        .iter()
        .find(|n| n.node_id == "proceso")
        .unwrap();
    assert_eq!(proceso.kind, "subworkflow");
    assert_eq!(proceso.status, NodeStatus::Completed);

    // Y el reporte del hijo existe por separado
    let child_report = history.report_for(child);
    let child_ids: Vec<&str> = child_report
        .nodes
        .iter()
        .map(|n| n.node_id.as_str())
        .collect();
    assert!(child_ids.contains(&"trabajo"));

    // Los WorkflowStarted son dos: raíz e hijo
    let started = history
        .events()
        .iter()
        .filter(|e| matches!(e.kind, EventKind::WorkflowStarted { .. }))
        .count();
    assert_eq!(started, 2);
}

#[tokio::test]
async fn registro_compartido_rechaza_nombres_duplicados() {
    let workflows = WorkflowRegistry::new();
    let child: WorkflowDefinition = serde_json::from_value(child_def("repetido")).unwrap();
    workflows.register(child.clone()).unwrap();
    let err = workflows.register(child).unwrap_err();
    assert_eq!(err.code, "WORKFLOW_NAME_CONFLICT");
}
