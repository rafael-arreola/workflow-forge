//! Tareas tipadas y por closure ejecutadas a través del executor real:
//! prueba que los schemas derivados de los tipos los aplica el engine, no solo
//! el wrapper, y que `register_typed`/`register_fn` participan del flujo
//! completo (mapping de input, publicación de output, validación).

use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use workflow_forge_core::error::codes;
use workflow_forge_core::runtime::WorkflowExecutor;
use workflow_forge_core::task::{TaskRegistry, WorkflowData, WorkflowResult};

#[derive(Deserialize, JsonSchema)]
struct SumIn {
    a: i64,
    b: i64,
}

#[derive(Serialize, JsonSchema)]
struct SumOut {
    total: i64,
}

fn wf() -> Value {
    json!({
        "name": "typed",
        "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "sum", "kind": "task", "task": "math.sum",
              "input": { "a": "$.trigger.a", "b": "$.trigger.b" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "sum" },
            { "from": "sum", "to": "end" }
        ]
    })
}

async fn run(registry: Arc<TaskRegistry>, trigger: Value) -> WorkflowResult {
    let executor = WorkflowExecutor::new(
        serde_json::from_value(wf()).expect("workflow válido"),
        registry,
    )
    .map_err(|e| panic!("executor inválido: {e:?}"))
    .unwrap();
    executor.run(WorkflowData(trigger)).await
}

#[tokio::test]
async fn typed_task_corre_en_el_executor() {
    let registry = Arc::new(TaskRegistry::new());
    registry.register_typed("math.sum", |_ctx, input: SumIn| async move {
        Ok(SumOut {
            total: input.a + input.b,
        })
    });

    let out = run(registry, json!({ "a": 40, "b": 2 })).await.unwrap();
    assert_eq!(out.0, json!({ "total": 42 }));
}

#[tokio::test]
async fn el_engine_aplica_el_schema_derivado_del_input() {
    let registry = Arc::new(TaskRegistry::new());
    registry.register_typed("math.sum", |_ctx, input: SumIn| async move {
        Ok(SumOut {
            total: input.a + input.b,
        })
    });

    // `b` falta: el schema derivado de SumIn lo marca como requerido, así que
    // el engine rechaza el input antes de invocar la closure.
    let err = run(registry, json!({ "a": 40, "b": null }))
        .await
        .expect_err("input incompleto debe fallar");
    assert_eq!(err.code, codes::TASK_INPUT_INVALID);
}

// Tipos ANIDADOS y enums: schemars genera el schema con `$defs` + `$ref`
// internos. Estos tests prueban que el validador del engine (jsonschema) los
// resuelve y los APLICA — el caso donde "siempre validado como se espera"
// podría romperse silenciosamente si los refs no se resolvieran.

// Los campos existen para deserialización/validación, no se leen en la closure.
#[allow(dead_code)]
#[derive(Deserialize, JsonSchema)]
struct Address {
    city: String,
    zip: u32,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
enum Priority {
    Low,
    High,
}

#[allow(dead_code)]
#[derive(Deserialize, JsonSchema)]
struct OrderIn {
    addr: Address,
    priority: Priority,
}

#[derive(Serialize, JsonSchema)]
struct OrderOut {
    ok: bool,
}

fn order_wf() -> Value {
    json!({
        "name": "order", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "place", "kind": "task", "task": "shop.place",
              "input": { "addr": "$.trigger.addr", "priority": "$.trigger.priority" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "place" },
            { "from": "place", "to": "end" }
        ]
    })
}

fn registry_with_place() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register_typed("shop.place", |_ctx, _input: OrderIn| async move {
        Ok(OrderOut { ok: true })
    });
    registry
}

async fn run_order(trigger: Value) -> WorkflowResult {
    let executor = WorkflowExecutor::new(
        serde_json::from_value(order_wf()).unwrap(),
        registry_with_place(),
    )
    .unwrap();
    executor.run(WorkflowData(trigger)).await
}

#[tokio::test]
async fn schema_anidado_valido_pasa() {
    let out = run_order(json!({
        "addr": { "city": "CDMX", "zip": 64000 },
        "priority": "high"
    }))
    .await
    .unwrap();
    assert_eq!(out.0, json!({ "ok": true }));
}

#[tokio::test]
async fn schema_anidado_el_engine_rechaza_subcampo_invalido() {
    // `zip` debe ser entero (u32); un string viola el `$ref` a Address.
    let err = run_order(json!({
        "addr": { "city": "CDMX", "zip": "no-es-entero" },
        "priority": "high"
    }))
    .await
    .expect_err("zip inválido debe ser rechazado por el engine");
    assert_eq!(err.code, codes::TASK_INPUT_INVALID);
}

#[tokio::test]
async fn schema_enum_el_engine_rechaza_variante_invalida() {
    let err = run_order(json!({
        "addr": { "city": "CDMX", "zip": 64000 },
        "priority": "urgente"
    }))
    .await
    .expect_err("variante de enum inexistente debe ser rechazada");
    assert_eq!(err.code, codes::TASK_INPUT_INVALID);
}

#[tokio::test]
async fn register_fn_corre_en_el_executor() {
    let registry = Arc::new(TaskRegistry::new());
    // Sin tipos: solo reenvía el JSON tal cual.
    registry.register_fn("math.sum", |_ctx, input| async move { Ok(input) });

    let out = run(registry, json!({ "a": 1, "b": 2 })).await.unwrap();
    assert_eq!(out.0, json!({ "a": 1, "b": 2 }));
}
