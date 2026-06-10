//! Anti-divergencia spec ↔ implementación: todo workflow que el schema
//! publicado acepta debe deserializar y pasar la validación estructural del
//! core, y los inválidos deben fallar en ambos lados.

use serde_json::{Value, json};
use workflow_forge_core::task::TaskManifest;
use workflow_forge_core::validation;
use workflow_forge_core::workflow::WorkflowDefinition;

fn workflow_validator() -> jsonschema::Validator {
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/1.0/workflow.schema.json"))
            .expect("schema parseable");
    jsonschema::validator_for(&schema).expect("schema compilable")
}

fn extension_validator() -> jsonschema::Validator {
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/1.0/extension.schema.json"))
            .expect("schema parseable");
    jsonschema::validator_for(&schema).expect("schema compilable")
}

/// Valida contra el schema publicado Y contra el core
fn assert_valid_everywhere(doc: Value) {
    let validator = workflow_validator();
    let errors: Vec<String> = validator.iter_errors(&doc).map(|e| e.to_string()).collect();
    assert!(errors.is_empty(), "el schema rechazó el doc: {errors:?}");

    let workflow: WorkflowDefinition =
        serde_json::from_value(doc).expect("el core debe deserializarlo");
    validation::validate(&workflow).expect("la validación estructural debe aceptarlo");
}

fn assert_schema_rejects(doc: Value, expectation: &str) {
    assert!(
        !workflow_validator().is_valid(&doc),
        "el schema debió rechazar: {expectation}"
    );
}

#[test]
fn workflow_minimo_valida() {
    assert_valid_everywhere(json!({
        "spec": "1.0",
        "name": "minimo", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [ { "from": "start", "to": "end" } ]
    }));
}

#[test]
fn workflow_completo_valida() {
    // Ejercita: defaults, task con retry/timeout, exclusive con condiciones
    // compuestas, parallel/join, ruta de error y end con output mapping
    assert_valid_everywhere(json!({
        "spec": "1.0",
        "name": "completo", "version": "1.2.3",
        "nodes": [
            { "id": "start", "kind": "start", "defaults": { "reintentos": 3 } },
            { "id": "fetch", "kind": "task", "task": "http.request",
              "input": { "url": "$.trigger.api", "method": "GET" },
              "retry": { "max": 3, "backoff": "exponential", "initial_ms": 500 },
              "timeout_ms": 10000 },
            { "id": "check", "kind": "gateway", "gateway": "exclusive", "branches": [
                { "when": { "and": [
                    { "path": "$.nodes.fetch.output.status", "eq": 200 },
                    { "or": [
                        { "path": "$.trigger.priority", "in": ["high", "urgent"] },
                        { "not": { "path": "$.trigger.draft", "exists": true } }
                    ]}
                ]}, "edge": "ok" },
                { "else": true, "edge": "fail" }
            ]},
            { "id": "fan", "kind": "gateway", "gateway": "parallel" },
            { "id": "a", "kind": "task", "task": "util.noop" },
            { "id": "b", "kind": "task", "task": "util.noop" },
            { "id": "meet", "kind": "gateway", "gateway": "join" },
            { "id": "alerta", "kind": "task", "task": "util.log",
              "input": { "message": "$.nodes.fetch.error" } },
            { "id": "end", "kind": "end", "status": "success",
              "output": { "resultado": "$.nodes.meet.output" } },
            { "id": "end-error", "kind": "end", "status": { "custom": "api_down" } }
        ],
        "edges": [
            { "from": "start", "to": "fetch" },
            { "from": "fetch", "to": "check" },
            { "from": "fetch", "on": "error", "to": "alerta" },
            { "from": "check", "label": "ok", "to": "fan" },
            { "from": "check", "label": "fail", "to": "end-error" },
            { "from": "fan", "to": "a" },
            { "from": "fan", "to": "b" },
            { "from": "a", "to": "meet" },
            { "from": "b", "to": "meet" },
            { "from": "meet", "to": "end" },
            { "from": "alerta", "to": "end-error" }
        ]
    }));
}

#[test]
fn el_schema_rechaza_documentos_invalidos() {
    assert_schema_rejects(
        json!({ "version": "1", "nodes": [{ "id": "s", "kind": "start" }] }),
        "falta name",
    );
    assert_schema_rejects(
        json!({ "name": "x", "version": "1",
            "nodes": [{ "id": "t", "kind": "task" }] }),
        "task sin campo task",
    );
    assert_schema_rejects(
        json!({ "name": "x", "version": "1",
            "nodes": [{ "id": "t", "kind": "task", "task": "SinNamespace" }] }),
        "task id sin namespace",
    );
    assert_schema_rejects(
        json!({ "name": "x", "version": "1",
            "nodes": [{ "id": "g", "kind": "gateway", "gateway": "exclusive" }] }),
        "exclusive sin branches",
    );
    assert_schema_rejects(
        json!({ "name": "x", "version": "1",
            "nodes": [{ "id": "g", "kind": "gateway", "gateway": "parallel",
                "branches": [{ "edge": "x", "else": true }] }] }),
        "parallel con branches",
    );
    assert_schema_rejects(
        json!({ "name": "x", "version": "1",
            "nodes": [{ "id": "g", "kind": "gateway", "gateway": "exclusive",
                "branches": [{ "edge": "x" }] }] }),
        "branch sin when ni else",
    );
    assert_schema_rejects(
        json!({ "name": "x", "version": "1",
            "nodes": [{ "id": "g", "kind": "gateway", "gateway": "exclusive",
                "branches": [{ "edge": "x",
                    "when": { "path": "$.a", "eq": 1, "gt": 0 } }] }] }),
        "comparación con dos operadores",
    );
    assert_schema_rejects(
        json!({ "name": "x", "version": "1",
            "nodes": [{ "id": "s", "kind": "start" }],
            "edges": [{ "from": "s", "to": "s", "on": "success" }] }),
        "trigger de arista desconocido",
    );
    assert_schema_rejects(
        json!({ "spec": "2.0", "name": "x", "version": "1",
            "nodes": [{ "id": "s", "kind": "start" }] }),
        "spec no soportada",
    );
}

#[test]
fn manifiestos_del_core_validan_contra_el_esquema_de_extension() {
    let validator = extension_validator();

    let mut manifest = TaskManifest::new("http.request");
    manifest.description = Some("ejemplo".into());
    manifest.input_schema = Some(serde_json::from_value(json!({ "type": "object" })).unwrap());

    let as_json = serde_json::to_value(&manifest).unwrap();
    let errors: Vec<String> = validator
        .iter_errors(&as_json)
        .map(|e| e.to_string())
        .collect();
    assert!(errors.is_empty(), "manifiesto rechazado: {errors:?}");

    // Un catálogo (array de manifiestos) también valida
    let catalog = json!([as_json]);
    assert!(validator.is_valid(&catalog));

    // Ids sin namespace son rechazados
    assert!(!validator.is_valid(&json!({ "id": "sinpunto" })));
}
