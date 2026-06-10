//! Perfiles de tarea: instancias nombradas reusables con config horneada
//! (`bind`), schemas propios y secrets. Cubre registro en registry,
//! definición inline en el workflow y los errores del contrato.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};

use workflow_forge_core::context::WorkflowContext;
use workflow_forge_core::executor::WorkflowExecutor;
use workflow_forge_core::profile::TaskProfile;
use workflow_forge_core::registry::TaskRegistry;
use workflow_forge_core::secret::SecretProvider;
use workflow_forge_core::task::{Task, TaskManifest};
use workflow_forge_core::task::{WorkflowData, WorkflowResult};
use workflow_forge_core::workflow::WorkflowDefinition;

/// Simula una API: exige {endpoint, payload} y devuelve {ok, endpoint, echo}.
struct FakeApiTask {
    manifest: TaskManifest,
}

impl Default for FakeApiTask {
    fn default() -> Self {
        let mut manifest = TaskManifest::new("fake.api");
        manifest.input_schema = Some(
            serde_json::from_value(json!({
                "type": "object",
                "required": ["endpoint", "payload"],
                "properties": {
                    "endpoint": { "type": "string" },
                    "payload": { "type": "object" }
                }
            }))
            .unwrap(),
        );
        Self { manifest }
    }
}

#[async_trait]
impl Task for FakeApiTask {
    fn manifest(&self) -> &TaskManifest {
        &self.manifest
    }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        Ok(WorkflowData(json!({
            "ok": true,
            "endpoint": input.get("endpoint").cloned().unwrap_or(Value::Null),
            "echo": input.get("payload").cloned().unwrap_or(Value::Null)
        })))
    }
}

struct MapSecrets(HashMap<String, String>);

impl SecretProvider for MapSecrets {
    fn get(&self, name: &str) -> Option<String> {
        self.0.get(name).cloned()
    }
}

fn no_secrets() -> MapSecrets {
    MapSecrets(HashMap::new())
}

fn registry_with_api() -> Arc<TaskRegistry> {
    let registry = Arc::new(TaskRegistry::new());
    registry.register(FakeApiTask::default());
    registry
}

fn perfil_crear_orden() -> TaskProfile {
    serde_json::from_value(json!({
        "id": "acme.crear_orden",
        "extends": "fake.api",
        "description": "Crea una orden en Acme",
        "input_schema": {
            "type": "object",
            "required": ["sku", "qty"],
            "properties": {
                "sku": { "type": "string" },
                "qty": { "type": "integer" }
            }
        },
        "output_schema": {
            "type": "object",
            "required": ["sku", "qty"]
        },
        "bind": { "endpoint": "/orders", "payload": "@" },
        "output": "@.echo"
    }))
    .expect("perfil deserializable")
}

fn wf_un_task(task: &str, input: Value) -> Value {
    json!({
        "name": "demo", "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "invoke", "kind": "task", "task": task, "input": input },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "invoke" },
            { "from": "invoke", "to": "end" }
        ]
    })
}

async fn run(workflow: Value, registry: Arc<TaskRegistry>, trigger: Value) -> WorkflowResult {
    let executor = WorkflowExecutor::new(
        serde_json::from_value(workflow).expect("workflow deserializable"),
        registry,
    )
    .map_err(|errors| errors.into_iter().next().expect("al menos un error"))?;
    executor.run(WorkflowData(trigger)).await
}

// ---------------------------------------------------------------------------
// Registro y ejecución
// ---------------------------------------------------------------------------

#[tokio::test]
async fn perfil_hornea_config_y_reshapea_el_output() {
    let registry = registry_with_api();
    registry
        .register_profile(perfil_crear_orden(), &no_secrets())
        .unwrap();

    let result = run(
        wf_un_task(
            "acme.crear_orden",
            json!({ "sku": "$.trigger.sku", "qty": 2 }),
        ),
        registry,
        json!({ "sku": "ABC-1" }),
    )
    .await
    .unwrap();

    // bind: endpoint horneado, payload = input completo; output: "@.echo"
    assert_eq!(result.0, json!({ "sku": "ABC-1", "qty": 2 }));
}

#[tokio::test]
async fn el_executor_valida_el_input_schema_del_perfil() {
    let registry = registry_with_api();
    registry
        .register_profile(perfil_crear_orden(), &no_secrets())
        .unwrap();

    let err = run(
        wf_un_task("acme.crear_orden", json!({ "sku": "ABC-1" })), // falta qty
        registry,
        json!({}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "TASK_INPUT_INVALID");
}

#[tokio::test]
async fn bind_que_la_base_rechaza_es_profile_bind_invalid() {
    let registry = registry_with_api();
    let perfil: TaskProfile = serde_json::from_value(json!({
        "id": "acme.rota",
        "extends": "fake.api",
        "bind": { "payload": "@" }
    }))
    .unwrap();
    registry.register_profile(perfil, &no_secrets()).unwrap();

    let err = run(
        wf_un_task("acme.rota", json!({ "x": 1 })),
        registry,
        json!({}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "PROFILE_BIND_INVALID");
}

#[tokio::test]
async fn perfil_puede_extender_a_otro_perfil() {
    let registry = registry_with_api();
    registry
        .register_profile(perfil_crear_orden(), &no_secrets())
        .unwrap();
    let especializado: TaskProfile = serde_json::from_value(json!({
        "id": "acme.crear_orden_unitaria",
        "extends": "acme.crear_orden",
        "bind": { "sku": "@.sku", "qty": 1 }
    }))
    .unwrap();
    registry
        .register_profile(especializado, &no_secrets())
        .unwrap();

    let result = run(
        wf_un_task(
            "acme.crear_orden_unitaria",
            json!({ "sku": "$.trigger.sku" }),
        ),
        registry,
        json!({ "sku": "XYZ" }),
    )
    .await
    .unwrap();
    assert_eq!(result.0, json!({ "sku": "XYZ", "qty": 1 }));
}

// ---------------------------------------------------------------------------
// Secrets
// ---------------------------------------------------------------------------

#[tokio::test]
async fn los_secrets_del_bind_se_resuelven_al_registrar() {
    let registry = registry_with_api();
    let perfil: TaskProfile = serde_json::from_value(json!({
        "id": "acme.con_secreto",
        "extends": "fake.api",
        "bind": { "endpoint": { "$secret": "ACME_ENDPOINT" }, "payload": "@" }
    }))
    .unwrap();
    let secrets = MapSecrets(HashMap::from([(
        "ACME_ENDPOINT".to_string(),
        "/secreto".to_string(),
    )]));
    registry.register_profile(perfil, &secrets).unwrap();

    let result = run(
        wf_un_task("acme.con_secreto", json!({ "a": 1 })),
        registry,
        json!({}),
    )
    .await
    .unwrap();
    assert_eq!(result.0["endpoint"], json!("/secreto"));
}

#[test]
fn secreto_ausente_falla_el_registro() {
    let registry = registry_with_api();
    let perfil: TaskProfile = serde_json::from_value(json!({
        "id": "acme.sin_secreto",
        "extends": "fake.api",
        "bind": { "endpoint": { "$secret": "NO_EXISTE" }, "payload": "@" }
    }))
    .unwrap();
    let err = registry
        .register_profile(perfil, &no_secrets())
        .unwrap_err();
    assert_eq!(err.code, "SECRET_NOT_FOUND");
    assert!(!registry.contains(&"acme.sin_secreto".into()));
}

// ---------------------------------------------------------------------------
// Errores de registro
// ---------------------------------------------------------------------------

#[test]
fn base_inexistente_y_conflicto_de_id() {
    let registry = registry_with_api();

    let huerfano: TaskProfile = serde_json::from_value(json!({
        "id": "acme.huerfano",
        "extends": "no.existe"
    }))
    .unwrap();
    let err = registry
        .register_profile(huerfano, &no_secrets())
        .unwrap_err();
    assert_eq!(err.code, "PROFILE_BASE_NOT_FOUND");

    registry
        .register_profile(perfil_crear_orden(), &no_secrets())
        .unwrap();
    let err = registry
        .register_profile(perfil_crear_orden(), &no_secrets())
        .unwrap_err();
    assert_eq!(err.code, "PROFILE_ID_CONFLICT");
}

// ---------------------------------------------------------------------------
// Perfiles inline en el workflow
// ---------------------------------------------------------------------------

#[tokio::test]
async fn perfiles_inline_ejecutan_sin_contaminar_el_registry() {
    let registry = registry_with_api();
    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "con-inline", "version": "0.1.0",
        "tasks": [{
            "id": "local.perfil",
            "extends": "fake.api",
            "bind": { "endpoint": "/local", "payload": "@" },
            "output": "@.echo"
        }],
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "invoke", "kind": "task", "task": "local.perfil" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "invoke" },
            { "from": "invoke", "to": "end" }
        ]
    }))
    .unwrap();

    let executor = WorkflowExecutor::new(workflow, Arc::clone(&registry))
        .map_err(|errors| format!("{errors:?}"))
        .unwrap();
    let result = executor.run(WorkflowData(json!({ "n": 7 }))).await.unwrap();
    assert_eq!(result.0, json!({ "n": 7 }));

    // El registry compartido no conoce el perfil local
    assert!(!registry.contains(&"local.perfil".into()));
}

#[tokio::test]
async fn perfil_inline_con_secret_usa_el_provider_inyectado() {
    let registry = registry_with_api();
    let workflow: WorkflowDefinition = serde_json::from_value(json!({
        "name": "inline-secret", "version": "0.1.0",
        "tasks": [{
            "id": "local.seguro",
            "extends": "fake.api",
            "bind": { "endpoint": { "$secret": "EP" }, "payload": "@" }
        }],
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "invoke", "kind": "task", "task": "local.seguro" },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "invoke" },
            { "from": "invoke", "to": "end" }
        ]
    }))
    .unwrap();

    let secrets = MapSecrets(HashMap::from([(
        "EP".to_string(),
        "/inyectado".to_string(),
    )]));
    let executor =
        WorkflowExecutor::new_with_secrets(workflow.clone(), Arc::clone(&registry), &secrets)
            .map_err(|errors| format!("{errors:?}"))
            .unwrap();
    let result = executor.run(WorkflowData(json!({}))).await.unwrap();
    assert_eq!(result.0["endpoint"], json!("/inyectado"));

    // Sin el secreto disponible, la construcción del executor falla
    let Err(errors) = WorkflowExecutor::new_with_secrets(workflow, registry, &no_secrets()) else {
        panic!("debió fallar por secreto ausente");
    };
    assert_eq!(errors[0].code, "SECRET_NOT_FOUND");
}

#[tokio::test]
async fn el_catalogo_incluye_los_perfiles_registrados() {
    let registry = registry_with_api();
    registry
        .register_profile(perfil_crear_orden(), &no_secrets())
        .unwrap();
    let catalog = registry.catalog();
    let perfil = catalog
        .iter()
        .find(|m| m.id.0 == "acme.crear_orden")
        .expect("el perfil está en el catálogo");
    assert!(perfil.input_schema.is_some());
    assert_eq!(
        perfil.description.as_deref(),
        Some("Crea una orden en Acme")
    );
}
