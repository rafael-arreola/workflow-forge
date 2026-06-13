//! Nodo `loop`: iteración acotada con condición de continuación. Cubre el
//! caso canónico de paginación, `collect` last/all, `on_max` fail/stop,
//! retry por iteración, ruteo `on: error` y eventos de observabilidad.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value, json};

use workflow_forge_core::error::{WorkflowError, codes};
use workflow_forge_core::observe::InMemoryHistory;
use workflow_forge_core::runtime::WorkflowExecutor;
use workflow_forge_core::task::{TaskRegistry, WorkflowData};

/// Registry con `test.page`: simula una API paginada de 3 páginas
/// (`{page} → {page, items, next}`, donde `next` es null en la última).
fn paged_registry() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register_fn("test.page", |_ctx, input| async move {
        let page = input.get("page").and_then(Value::as_i64).unwrap_or(0);
        let next = if page < 3 {
            json!(page + 1)
        } else {
            Value::Null
        };
        Ok(WorkflowData(json!({
            "page": page,
            "items": [format!("item-{page}-a"), format!("item-{page}-b")],
            "next": next,
        })))
    });
    registry
}

fn pagination_workflow(extra_loop_fields: Value) -> Value {
    let mut node = json!({
        "id": "pages", "kind": "loop", "task": "test.page",
        "input": { "page": 1 },
        "next": { "page": "@.output.next" },
        "while": { "path": "$.output.next", "is_null": false },
        "max_iterations": 10
    });
    if let (Value::Object(node_map), Value::Object(extra)) = (&mut node, extra_loop_fields) {
        node_map.extend(extra);
    }
    json!({
        "name": "paginado", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            node,
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "pages" },
            { "from": "pages", "to": "end" }
        ]
    })
}

async fn run(workflow: Value, registry: Arc<TaskRegistry>) -> Result<Value, WorkflowError> {
    let executor =
        WorkflowExecutor::new(serde_json::from_value(workflow).unwrap(), registry).unwrap();
    executor.run(WorkflowData(json!({}))).await.map(|out| out.0)
}

#[tokio::test]
async fn pagina_hasta_que_next_es_null_y_colecta_todo() {
    let out = run(
        pagination_workflow(json!({ "collect": "all" })),
        paged_registry(),
    )
    .await
    .unwrap();

    let pages = out.as_array().expect("collect: all produce un array");
    assert_eq!(pages.len(), 3);
    assert_eq!(pages[0]["page"], 1);
    assert_eq!(pages[1]["page"], 2);
    assert_eq!(pages[2]["page"], 3);
    assert_eq!(pages[2]["next"], Value::Null);
}

#[tokio::test]
async fn collect_last_devuelve_solo_la_ultima_iteracion() {
    // Sin `collect` explícito: el default es last
    let out = run(pagination_workflow(json!({})), paged_registry())
        .await
        .unwrap();

    assert_eq!(out["page"], 3, "debe ser la última página, no un array");
    assert_eq!(out["next"], Value::Null);
}

#[tokio::test]
async fn while_falsa_tras_la_primera_iteracion_corre_exactamente_una() {
    let registry = Arc::new(TaskRegistry::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    registry.register_fn("test.once", move |_ctx, input| {
        let counter = Arc::clone(&counter);
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(input)
        }
    });

    let workflow = json!({
        "name": "una", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "solo", "kind": "loop", "task": "test.once",
              "input": { "valor": 7 },
              "while": { "path": "$.output.no_existe", "exists": true },
              "max_iterations": 100 },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "solo" },
            { "from": "solo", "to": "end" }
        ]
    });
    let out = run(workflow, registry).await.unwrap();

    assert_eq!(calls.load(Ordering::SeqCst), 1, "la primera siempre corre");
    assert_eq!(out, json!({ "valor": 7 }));
}

#[tokio::test]
async fn sin_next_la_siguiente_iteracion_recibe_el_output_anterior() {
    let registry = Arc::new(TaskRegistry::new());
    registry.register_fn("test.inc", |_ctx, input| async move {
        let n = input.get("n").and_then(Value::as_i64).unwrap_or(0);
        Ok(WorkflowData(json!({ "n": n + 1 })))
    });

    let workflow = json!({
        "name": "cadena", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "inc", "kind": "loop", "task": "test.inc",
              "input": { "n": 0 },
              "while": { "path": "$.output.n", "lt": 3 },
              "max_iterations": 10 },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "inc" },
            { "from": "inc", "to": "end" }
        ]
    });
    let out = run(workflow, registry).await.unwrap();
    assert_eq!(out, json!({ "n": 3 }));
}

#[tokio::test]
async fn on_max_fail_corta_con_error_explicito() {
    let out = run(
        // `while` siempre verdadera: el tope decide
        pagination_workflow(json!({
            "while": { "path": "$.index", "gte": 0 },
            "max_iterations": 2
        })),
        paged_registry(),
    )
    .await;

    let err = out.expect_err("debe fallar al alcanzar el tope");
    assert_eq!(err.code, codes::LOOP_MAX_ITERATIONS_EXCEEDED);
    assert_eq!(err.source_task.as_deref(), Some("pages"));
}

#[tokio::test]
async fn on_max_stop_termina_bien_con_lo_acumulado() {
    let out = run(
        pagination_workflow(json!({
            "while": { "path": "$.index", "gte": 0 },
            "max_iterations": 2,
            "on_max": "stop",
            "collect": "all"
        })),
        paged_registry(),
    )
    .await
    .unwrap();

    let pages = out.as_array().expect("collect: all produce un array");
    assert_eq!(pages.len(), 2, "exactamente el tope de iteraciones");
}

#[tokio::test]
async fn una_iteracion_que_falla_rutea_por_on_error() {
    let registry = Arc::new(TaskRegistry::new());
    registry.register_fn("test.boom", |_ctx, _input| async move {
        Err::<WorkflowData, _>(WorkflowError::new("BOOM", "fallo simulado"))
    });
    registry.register_fn("test.echo", |_ctx, input| async move { Ok(input) });

    let workflow = json!({
        "name": "ruta-error", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "roto", "kind": "loop", "task": "test.boom",
              "while": { "path": "$.output.x", "exists": true },
              "max_iterations": 5 },
            { "id": "rescate", "kind": "task", "task": "test.echo" },
            { "id": "end", "kind": "end" },
            { "id": "end-error", "kind": "end", "status": "error" }
        ],
        "edges": [
            { "from": "start", "to": "roto" },
            { "from": "roto", "to": "end" },
            { "from": "roto", "on": "error", "to": "rescate" },
            { "from": "rescate", "to": "end-error" }
        ]
    });
    let out = run(workflow, registry).await.unwrap();
    assert_eq!(
        out["code"],
        json!("BOOM"),
        "el error serializado es el token"
    );
}

#[tokio::test]
async fn retry_aplica_por_iteracion() {
    let registry = Arc::new(TaskRegistry::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&calls);
    registry.register_fn("test.flaky", move |_ctx, input| {
        let counter = Arc::clone(&counter);
        async move {
            // El primer intento de la ejecución falla; el reintento funciona
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(WorkflowError::new("FLAKY", "primer intento falla"));
            }
            Ok(input)
        }
    });

    let workflow = json!({
        "name": "flaky", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "inestable", "kind": "loop", "task": "test.flaky",
              "input": { "ok": true },
              "while": { "path": "$.output.no_existe", "exists": true },
              "max_iterations": 3,
              "retry": { "max": 1, "initial_ms": 1 } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "inestable" },
            { "from": "inestable", "to": "end" }
        ]
    });
    let out = run(workflow, registry).await.unwrap();

    assert_eq!(out, json!({ "ok": true }));
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "intento fallido + reintento"
    );
}

#[tokio::test]
async fn las_iteraciones_se_observan_y_pliegan_en_el_reporte() {
    let history = Arc::new(InMemoryHistory::new());
    let executor = WorkflowExecutor::new(
        serde_json::from_value(pagination_workflow(json!({ "collect": "all" }))).unwrap(),
        paged_registry(),
    )
    .unwrap()
    .with_observer(history.clone());

    executor.run(WorkflowData(json!({}))).await.unwrap();

    let report = history.report();
    let node = report
        .nodes
        .iter()
        .find(|n| n.node_id == "pages")
        .expect("el loop aparece en el reporte");
    assert_eq!(node.items_ok, Some(3));
    assert_eq!(node.items_failed, Some(0));

    let iteration_events = history
        .events()
        .iter()
        .filter(|e| {
            serde_json::to_value(&e.kind)
                .map(|v| v["type"] == "loop_iteration_completed")
                .unwrap_or(false)
        })
        .count();
    assert_eq!(iteration_events, 3);
}

#[tokio::test]
async fn max_iterations_cero_falla_en_validacion() {
    let workflow: workflow_forge_core::spec::WorkflowDefinition =
        serde_json::from_value(pagination_workflow(json!({ "max_iterations": 0 }))).unwrap();
    let errors = WorkflowExecutor::new(workflow, paged_registry())
        .err()
        .expect("debe fallar en la construcción");
    assert!(
        errors
            .iter()
            .any(|e| e.code == codes::LOOP_INVALID_MAX_ITERATIONS),
        "códigos: {:?}",
        errors.iter().map(|e| &e.code).collect::<Vec<_>>()
    );
}
