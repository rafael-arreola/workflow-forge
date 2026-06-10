use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::context::WorkflowContext;
use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::executor::WorkflowExecutor;
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::types::{WorkflowData, WorkflowResult};
use workflow_forge_core::workflow::WorkflowDefinition;

// ---------------------------------------------------------------------------
// Tareas de prueba
// ---------------------------------------------------------------------------

/// Devuelve su input tal cual
struct EchoTask {
    manifest: TaskManifest,
}

#[async_trait]
impl Task for EchoTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        Ok(input)
    }
}

/// Falla las primeras `fail_times` invocaciones, luego devuelve su input
struct FlakyTask {
    manifest: TaskManifest,
    fail_times: u32,
    calls: Arc<AtomicU32>,
}

#[async_trait]
impl Task for FlakyTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call <= self.fail_times {
            Err(WorkflowError::new(
                "FLAKY",
                format!("fallo intencional #{call}"),
            ))
        } else {
            Ok(input)
        }
    }
}

/// Duerme `ms` milisegundos y devuelve su input
struct SleepyTask {
    manifest: TaskManifest,
    ms: u64,
}

#[async_trait]
impl Task for SleepyTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        tokio::time::sleep(Duration::from_millis(self.ms)).await;
        Ok(input)
    }
}

fn registry_with_echo() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register(EchoTask {
        manifest: TaskManifest::new("echo"),
    });
    registry
}

fn wf(value: Value) -> WorkflowDefinition {
    serde_json::from_value(value).expect("workflow deserializable")
}

async fn run(workflow: Value, registry: Arc<TaskRegistry>, trigger: Value) -> WorkflowResult {
    let executor = WorkflowExecutor::new(wf(workflow), registry)
        .map_err(|errors| errors.into_iter().next().expect("al menos un error"))?;
    executor.run(WorkflowData(trigger)).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flujo_secuencial_con_mappings() {
    let workflow = json!({
        "name": "seq", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "a", "kind": "task", "task": "echo",
              "input": { "msg": "$.trigger.text", "fixed": 1 } },
            { "id": "b", "kind": "task", "task": "echo",
              "input": { "prev": "$.nodes.a.output.msg", "lit": "$$.raw" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "a" },
            { "from": "a", "to": "b" },
            { "from": "b", "to": "end" }
        ]
    });

    let result = run(workflow, registry_with_echo(), json!({ "text": "hola" }))
        .await
        .unwrap();
    assert_eq!(result.0, json!({ "prev": "hola", "lit": "$.raw" }));
}

#[tokio::test]
async fn gateway_exclusive_elige_rama() {
    let workflow = json!({
        "name": "gw", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                { "when": { "path": "$.trigger.x", "eq": 1 }, "edge": "uno" },
                { "else": true, "edge": "otro" }
            ]},
            { "id": "t-uno", "kind": "task", "task": "echo", "input": { "ruta": "uno" } },
            { "id": "t-otro", "kind": "task", "task": "echo", "input": { "ruta": "otro" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "check" },
            { "from": "check", "to": "t-uno", "label": "uno" },
            { "from": "check", "to": "t-otro", "label": "otro" },
            { "from": "t-uno", "to": "end" },
            { "from": "t-otro", "to": "end" }
        ]
    });

    let cuando_uno = run(workflow.clone(), registry_with_echo(), json!({ "x": 1 }))
        .await
        .unwrap();
    assert_eq!(cuando_uno.0, json!({ "ruta": "uno" }));

    let cuando_otro = run(workflow, registry_with_echo(), json!({ "x": 99 }))
        .await
        .unwrap();
    assert_eq!(cuando_otro.0, json!({ "ruta": "otro" }));
}

#[tokio::test]
async fn parallel_join_combina_outputs() {
    let workflow = json!({
        "name": "pj", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "fan", "kind": "gateway", "gateway": "parallel" },
            { "id": "a", "kind": "task", "task": "echo", "input": { "r": "a" } },
            { "id": "b", "kind": "task", "task": "echo", "input": { "r": "b" } },
            { "id": "meet", "kind": "gateway", "gateway": "join" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "fan" },
            { "from": "fan", "to": "a" },
            { "from": "fan", "to": "b" },
            { "from": "a", "to": "meet" },
            { "from": "b", "to": "meet" },
            { "from": "meet", "to": "end" }
        ]
    });

    let result = run(workflow, registry_with_echo(), json!({}))
        .await
        .unwrap();
    assert_eq!(result.0, json!({ "a": { "r": "a" }, "b": { "r": "b" } }));
}

#[tokio::test]
async fn retry_agotado_sigue_ruta_de_error() {
    let registry = registry_with_echo();
    let calls = Arc::new(AtomicU32::new(0));
    registry.register(FlakyTask {
        manifest: TaskManifest::new("flaky"),
        fail_times: u32::MAX,
        calls: calls.clone(),
    });

    let workflow = json!({
        "name": "err", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "flaky", "kind": "task", "task": "flaky",
              "retry": { "max": 2, "backoff": "fixed", "initial_ms": 1 } },
            { "id": "handler", "kind": "task", "task": "echo",
              "input": { "err_code": "$.nodes.flaky.error.code" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "flaky" },
            { "from": "flaky", "on": "error", "to": "handler" },
            { "from": "flaky", "to": "end" },
            { "from": "handler", "to": "end" }
        ]
    });

    let result = run(workflow, registry, json!({})).await.unwrap();
    assert_eq!(result.0, json!({ "err_code": "FLAKY" }));
    assert_eq!(calls.load(Ordering::SeqCst), 3); // 1 intento + 2 reintentos
}

#[tokio::test]
async fn retry_se_recupera() {
    let registry = Arc::new(TaskRegistry::new());
    let calls = Arc::new(AtomicU32::new(0));
    registry.register(FlakyTask {
        manifest: TaskManifest::new("flaky"),
        fail_times: 2,
        calls: calls.clone(),
    });

    let workflow = json!({
        "name": "recover", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "flaky", "kind": "task", "task": "flaky",
              "input": { "ok": true },
              "retry": { "max": 3, "backoff": "exponential", "initial_ms": 1 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "flaky" },
            { "from": "flaky", "to": "end" }
        ]
    });

    let result = run(workflow, registry, json!({})).await.unwrap();
    assert_eq!(result.0, json!({ "ok": true }));
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn timeout_sin_ruta_de_error_falla() {
    let registry = Arc::new(TaskRegistry::new());
    registry.register(SleepyTask {
        manifest: TaskManifest::new("sleepy"),
        ms: 5000,
    });

    let workflow = json!({
        "name": "slow", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "lento", "kind": "task", "task": "sleepy", "timeout_ms": 20 },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "lento" },
            { "from": "lento", "to": "end" }
        ]
    });

    let err = run(workflow, registry, json!({})).await.unwrap_err();
    assert_eq!(err.code, "TASK_TIMEOUT");
    assert_eq!(err.source_task.as_deref(), Some("lento"));
}

#[tokio::test]
async fn end_con_output_mapping() {
    let workflow = json!({
        "name": "endmap", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "a", "kind": "task", "task": "echo", "input": { "msg": "$.trigger.text" } },
            { "id": "end", "kind": "end",
              "output": { "final": "$.nodes.a.output.msg", "exec": "$.workflow.name" } }
        ],
        "edges": [
            { "from": "start", "to": "a" },
            { "from": "a", "to": "end" }
        ]
    });

    let result = run(workflow, registry_with_echo(), json!({ "text": "fin" }))
        .await
        .unwrap();
    assert_eq!(result.0, json!({ "final": "fin", "exec": "endmap" }));
}

#[tokio::test]
async fn join_hambriento_reporta_diagnostico() {
    // El exclusive desvía el flujo hacia una sola rama del join:
    // la otra nunca llega y el join queda esperando para siempre.
    let workflow = json!({
        "name": "starved", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "desvio", "kind": "gateway", "gateway": "exclusive", "branches": [
                { "when": { "path": "$.trigger.x", "eq": 1 }, "edge": "a" },
                { "else": true, "edge": "b" }
            ]},
            { "id": "a", "kind": "task", "task": "echo", "input": { "r": "a" } },
            { "id": "b", "kind": "task", "task": "echo", "input": { "r": "b" } },
            { "id": "meet", "kind": "gateway", "gateway": "join" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "desvio" },
            { "from": "desvio", "to": "a", "label": "a" },
            { "from": "desvio", "to": "b", "label": "b" },
            { "from": "a", "to": "meet" },
            { "from": "b", "to": "meet" },
            { "from": "meet", "to": "end" }
        ]
    });

    let err = run(workflow, registry_with_echo(), json!({ "x": 1 }))
        .await
        .unwrap_err();
    assert_eq!(err.code, "JOIN_INCOMPLETE");
    assert!(err.message.contains("'meet'"), "mensaje: {}", err.message);
    assert!(err.message.contains("1/2"), "mensaje: {}", err.message);
}

#[tokio::test]
async fn workflow_invalido_no_construye() {
    let workflow = wf(json!({
        "name": "bad", "version": "0.1.0",
        "nodes": [ { "id": "start", "kind": "start" } ],
        "edges": []
    }));
    let errors = WorkflowExecutor::new(workflow, registry_with_echo())
        .err()
        .unwrap();
    assert!(errors.iter().any(|e| e.code == "NO_END_NODE"));
}

#[tokio::test]
async fn tarea_no_registrada_no_construye() {
    let workflow = wf(json!({
        "name": "missing", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "x", "kind": "task", "task": "inexistente" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "x" },
            { "from": "x", "to": "end" }
        ]
    }));
    let errors = WorkflowExecutor::new(workflow, registry_with_echo())
        .err()
        .unwrap();
    assert!(errors.iter().any(|e| e.code == "TASK_NOT_FOUND"));
}

#[tokio::test]
async fn schema_de_manifiesto_valida_input() {
    let registry = Arc::new(TaskRegistry::new());
    let mut manifest = TaskManifest::new("estricta");
    manifest.input_schema = Some(
        serde_json::from_value(json!({
            "type": "object",
            "required": ["msg"],
            "properties": { "msg": { "type": "string" } }
        }))
        .unwrap(),
    );
    registry.register(EchoTask { manifest });

    let workflow = json!({
        "name": "schema", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "t", "kind": "task", "task": "estricta", "input": { "otro_campo": 1 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "t" },
            { "from": "t", "to": "end" }
        ]
    });

    let err = run(workflow, registry, json!({})).await.unwrap_err();
    assert_eq!(err.code, "TASK_INPUT_INVALID");
    assert!(err.message.contains("msg"), "mensaje: {}", err.message);
}

#[test]
fn catalogo_exportable_como_json() {
    let registry = registry_with_echo();
    registry.register(SleepyTask {
        manifest: TaskManifest::new("util.sleep"),
        ms: 1,
    });

    let catalog = registry.catalog();
    let ids: Vec<&str> = catalog.iter().map(|m| m.id.0.as_str()).collect();
    assert_eq!(ids, vec!["echo", "util.sleep"]); // orden estable

    let as_json = serde_json::to_value(&catalog).unwrap();
    assert_eq!(as_json[0]["id"], "echo");
}
