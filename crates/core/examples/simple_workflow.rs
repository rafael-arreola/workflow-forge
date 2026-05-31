use std::sync::Arc;

use async_trait::async_trait;
use serde_json::json;

use workflow_forge_core::context::WorkflowContext;
use workflow_forge_core::executor::WorkflowExecutor;
use workflow_forge_core::node::event::{EndNode, EndStatus, StartNode};
use workflow_forge_core::node::task::TaskNode;
use workflow_forge_core::node::{Node, NodeId, NodeKind};
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::task::{Task, TaskId};
use workflow_forge_core::types::{PortDef, WorkflowData, WorkflowResult};
use workflow_forge_core::workflow::{FlowEdge, WorkflowDefinition};

// ---------------------------------------------------------------------------
// 1. Tarea: transforma el campo "text" a mayúsculas
// ---------------------------------------------------------------------------

struct UpperCaseTask {
    task_id: TaskId,
}

static PORTS: &[PortDef] = &[];

#[async_trait]
impl Task for UpperCaseTask {
    fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    fn input_ports(&self) -> &[PortDef] {
        PORTS
    }

    fn output_ports(&self) -> &[PortDef] {
        PORTS
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let text = input
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_uppercase();

        Ok(WorkflowData(json!({ "text": text })))
    }
}

// ---------------------------------------------------------------------------
// 2. Tarea: agrega metadata al payload (workflow_id + elapsed_ms)
// ---------------------------------------------------------------------------

struct MetadataTask {
    task_id: TaskId,
}

#[async_trait]
impl Task for MetadataTask {
    fn task_id(&self) -> &TaskId {
        &self.task_id
    }

    fn input_ports(&self) -> &[PortDef] {
        PORTS
    }

    fn output_ports(&self) -> &[PortDef] {
        PORTS
    }

    async fn execute(&self, ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let mut map = match input.0 {
            serde_json::Value::Object(m) => m,
            other => {
                let mut m = serde_json::Map::new();
                m.insert("_input".to_string(), other);
                m
            }
        };

        map.insert("workflow_id".to_string(), json!(ctx.workflow_id()));
        map.insert("elapsed_ms".to_string(), json!(ctx.elapsed().as_millis()));

        Ok(WorkflowData(serde_json::Value::Object(map)))
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    // --- 1. Registrar tareas ---

    let registry = Arc::new(TaskRegistry::new());

    let upper = UpperCaseTask {
        task_id: TaskId::from("upper"),
    };
    let upper_id = upper.task_id().clone();

    let meta = MetadataTask {
        task_id: TaskId::from("metadata"),
    };
    let meta_id = meta.task_id().clone();

    registry.register(upper);
    registry.register(meta);

    println!("📋 Tareas registradas:");
    for id in registry.list() {
        println!("   - {id}");
    }

    // --- 2. Definir el workflow (programáticamente) ---

    let workflow = WorkflowDefinition {
        id: None,
        name: "text-pipeline".into(),
        version: "1.0.0".into(),
        nodes: vec![
            Node {
                id: NodeId::from("start"),
                kind: NodeKind::Start(StartNode {
                    schema: None,
                    defaults: None,
                }),
            },
            Node {
                id: NodeId::from("to_upper"),
                kind: NodeKind::Task(TaskNode { task_id: upper_id }),
            },
            Node {
                id: NodeId::from("add_meta"),
                kind: NodeKind::Task(TaskNode { task_id: meta_id }),
            },
            Node {
                id: NodeId::from("end"),
                kind: NodeKind::End(EndNode {
                    status: EndStatus::Success,
                    schema: None,
                }),
            },
        ],
        edges: vec![
            FlowEdge {
                from: NodeId::from("start"),
                to: NodeId::from("to_upper"),
            },
            FlowEdge {
                from: NodeId::from("to_upper"),
                to: NodeId::from("add_meta"),
            },
            FlowEdge {
                from: NodeId::from("add_meta"),
                to: NodeId::from("end"),
            },
        ],
    };

    // --- 3. Mostrar el workflow como JSON ---

    let workflow_json = serde_json::to_string_pretty(&workflow).unwrap();
    println!("\n📄 Workflow JSON:\n{workflow_json}\n");

    // --- 4. Ejecutar ---

    let executor = WorkflowExecutor::new(workflow, registry);

    let input = WorkflowData(json!({
        "text": "hello world from workflow-forge"
    }));

    println!(
        "➡️  Input:  {}",
        serde_json::to_string_pretty(&input).unwrap()
    );

    let result = executor.run(input).await;

    match result {
        Ok(output) => {
            println!(
                "✅ Output: {}",
                serde_json::to_string_pretty(&output).unwrap()
            );
        }
        Err(err) => {
            eprintln!("❌ Error: {err}");
            if let Some(payload) = &err.payload {
                eprintln!(
                    "   Payload: {}",
                    serde_json::to_string_pretty(payload).unwrap()
                );
            }
        }
    }
}
