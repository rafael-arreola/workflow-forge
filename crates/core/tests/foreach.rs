//! Nodo `foreach`: iteración de un array invocando una tarea por elemento,
//! con concurrencia, throttle, retry por elemento y políticas fail/collect.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::error::WorkflowError;
use workflow_forge_core::runtime::{WorkflowContext, WorkflowExecutor};
use workflow_forge_core::task::{Task, TaskManifest, TaskRegistry};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};

/// Duplica `{n}`; falla con DOUBLE_NEGATIVE si n < 0; duerme `sleep_ms`.
/// Si `fail_first_attempt`, la primera invocación por cada `n` falla.
struct DoubleTask {
    manifest: TaskManifest,
    sleep_ms: u64,
    fail_first_attempt: bool,
    calls: Mutex<HashMap<i64, u32>>,
}

impl DoubleTask {
    fn new(sleep_ms: u64, fail_first_attempt: bool) -> Self {
        let mut manifest = TaskManifest::new("test.double");
        manifest.input_schema = Some(
            serde_json::from_value(json!({
                "type": "object",
                "required": ["n"],
                "properties": { "n": { "type": "integer" } }
            }))
            .unwrap(),
        );
        Self {
            manifest,
            sleep_ms,
            fail_first_attempt,
            calls: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Task for DoubleTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        let n = input.get("n").and_then(Value::as_i64).unwrap_or(0);

        if self.fail_first_attempt {
            let mut calls = self.calls.lock().unwrap();
            let count = calls.entry(n).or_insert(0);
            *count += 1;
            if *count == 1 {
                return Err(WorkflowError::new(
                    "FLAKY",
                    format!("primer intento de {n}"),
                ));
            }
        }
        if self.sleep_ms > 0 {
            tokio::time::sleep(Duration::from_millis(self.sleep_ms)).await;
        }
        if n < 0 {
            return Err(WorkflowError::new(
                "DOUBLE_NEGATIVE",
                format!("no duplico negativos: {n}"),
            ));
        }
        Ok(WorkflowData(json!({ "n": n * 2 })))
    }
}

fn registry(task: DoubleTask) -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register(task);
    registry
}

/// Workflow lineal: start → foreach(test.double sobre $.trigger.items) → end
fn wf_foreach(extra: Value) -> Value {
    let mut foreach = json!({
        "id": "lote", "kind": "foreach",
        "task": "test.double",
        "items": "$.trigger.items"
    });
    foreach
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    json!({
        "name": "lote", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            foreach,
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "lote" },
            { "from": "lote", "to": "end" }
        ]
    })
}

async fn run(
    workflow: Value,
    registry: Arc<TaskRegistry>,
    trigger: Value,
) -> Result<WorkflowData, WorkflowError> {
    let executor = WorkflowExecutor::new(
        serde_json::from_value(workflow).expect("workflow deserializable"),
        registry,
    )
    .map_err(|errors| errors.into_iter().next().expect("al menos un error"))?;
    executor.run(WorkflowData(trigger)).await
}

// ---------------------------------------------------------------------------
// Comportamiento básico
// ---------------------------------------------------------------------------

#[tokio::test]
async fn secuencial_preserva_el_orden() {
    let result = run(
        wf_foreach(json!({})),
        registry(DoubleTask::new(0, false)),
        json!({ "items": [{ "n": 1 }, { "n": 2 }, { "n": 3 }] }),
    )
    .await
    .unwrap();
    assert_eq!(result.0, json!([{ "n": 2 }, { "n": 4 }, { "n": 6 }]));
}

#[tokio::test]
async fn items_que_no_son_array_es_error() {
    let err = run(
        wf_foreach(json!({})),
        registry(DoubleTask::new(0, false)),
        json!({ "items": { "no": "array" } }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "FOREACH_ITEMS_NOT_ARRAY");
}

#[tokio::test]
async fn valida_el_input_de_cada_elemento() {
    let err = run(
        wf_foreach(json!({})),
        registry(DoubleTask::new(0, false)),
        json!({ "items": [{ "n": 1 }, { "sin_n": true }] }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "TASK_INPUT_INVALID");
}

#[tokio::test]
async fn concurrency_cero_es_invalido() {
    let errors = WorkflowExecutor::new(
        serde_json::from_value(wf_foreach(json!({ "concurrency": 0 }))).unwrap(),
        registry(DoubleTask::new(0, false)),
    )
    .err()
    .expect("debe fallar la validación");
    assert!(
        errors
            .iter()
            .any(|e| e.code == "FOREACH_INVALID_CONCURRENCY")
    );
}

// ---------------------------------------------------------------------------
// Concurrencia y throttle
// ---------------------------------------------------------------------------

#[tokio::test]
async fn la_concurrencia_corre_elementos_en_paralelo() {
    let items = json!({ "items": [{ "n": 1 }, { "n": 2 }, { "n": 3 }, { "n": 4 }] });

    let started = Instant::now();
    run(
        wf_foreach(json!({ "concurrency": 4 })),
        registry(DoubleTask::new(60, false)),
        items.clone(),
    )
    .await
    .unwrap();
    let parallel = started.elapsed();

    let started = Instant::now();
    run(
        wf_foreach(json!({})), // secuencial
        registry(DoubleTask::new(60, false)),
        items,
    )
    .await
    .unwrap();
    let sequential = started.elapsed();

    assert!(
        parallel < Duration::from_millis(180),
        "4 elementos de 60ms con concurrency 4 tardaron {parallel:?}"
    );
    assert!(
        sequential >= Duration::from_millis(240),
        "secuencial debió tardar >= 240ms, tardó {sequential:?}"
    );
}

#[tokio::test]
async fn el_throttle_espacia_los_arranques() {
    let started = Instant::now();
    run(
        wf_foreach(json!({ "concurrency": 3, "throttle_ms": 50 })),
        registry(DoubleTask::new(0, false)),
        json!({ "items": [{ "n": 1 }, { "n": 2 }, { "n": 3 }] }),
    )
    .await
    .unwrap();
    // Arranques en 0, 50 y 100 ms aunque haya 3 slots libres
    assert!(
        started.elapsed() >= Duration::from_millis(100),
        "el throttle debió espaciar los arranques: {:?}",
        started.elapsed()
    );
}

// ---------------------------------------------------------------------------
// Políticas de error
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fail_aborta_y_puede_rutear_el_error() {
    let err = run(
        wf_foreach(json!({})),
        registry(DoubleTask::new(0, false)),
        json!({ "items": [{ "n": 1 }, { "n": -1 }, { "n": 3 }] }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "DOUBLE_NEGATIVE");

    // Con ruta on:error el workflow continúa por ella
    let workflow = json!({
        "name": "lote-con-ruta", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "lote", "kind": "foreach",
              "task": "test.double", "items": "$.trigger.items" },
            { "id": "end", "kind": "end" },
            { "id": "end-error", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "lote" },
            { "from": "lote", "to": "end" },
            { "from": "lote", "on": "error", "to": "end-error" }
        ]
    });
    let result = run(
        workflow,
        registry(DoubleTask::new(0, false)),
        json!({ "items": [{ "n": -5 }] }),
    )
    .await
    .unwrap();
    assert_eq!(result.0["code"], json!("DOUBLE_NEGATIVE"));
}

#[tokio::test]
async fn collect_separa_exitos_de_fallos_y_no_falla() {
    let result = run(
        wf_foreach(json!({ "on_item_error": "collect" })),
        registry(DoubleTask::new(0, false)),
        json!({ "items": [{ "n": 1 }, { "n": -1 }, { "n": 3 }] }),
    )
    .await
    .unwrap();

    assert_eq!(result.0["ok"], json!([{ "n": 2 }, { "n": 6 }]));
    let failed = result.0["failed"].as_array().unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0]["index"], json!(1));
    assert_eq!(failed[0]["item"], json!({ "n": -1 }));
    assert_eq!(failed[0]["error"]["code"], json!("DOUBLE_NEGATIVE"));
}

#[tokio::test]
async fn retry_aplica_por_elemento() {
    let result = run(
        wf_foreach(json!({ "retry": { "max": 1, "initial_ms": 1 } })),
        registry(DoubleTask::new(0, true)), // cada n falla su primer intento
        json!({ "items": [{ "n": 1 }, { "n": 2 }] }),
    )
    .await
    .unwrap();
    assert_eq!(result.0, json!([{ "n": 2 }, { "n": 4 }]));
}

// ---------------------------------------------------------------------------
// foreach + perfiles
// ---------------------------------------------------------------------------

#[tokio::test]
async fn foreach_puede_iterar_un_perfil() {
    let registry = registry(DoubleTask::new(0, false));
    registry
        .register_profile(
            serde_json::from_value(json!({
                "id": "lotes.doble",
                "extends": "test.double",
                "bind": { "n": "@.valor" }
            }))
            .unwrap(),
            &workflow_forge_core::io::secret::EnvSecrets,
        )
        .unwrap();

    let workflow = json!({
        "name": "lote-perfil", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "lote", "kind": "foreach",
              "task": "lotes.doble", "items": "$.trigger.items" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "lote" },
            { "from": "lote", "to": "end" }
        ]
    });
    let result = run(
        workflow,
        registry,
        json!({ "items": [{ "valor": 10 }, { "valor": 20 }] }),
    )
    .await
    .unwrap();
    assert_eq!(result.0, json!([{ "n": 20 }, { "n": 40 }]));
}
