#![allow(dead_code)]
use reqwest::{Client, Response};
use serde_json::{Value, json};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};
use workflow_forge::v2::*;
use workflow_forge_reference_module::{StaticDirectory, customer_operations};
use workflow_forge_service::{BearerIdentity, ServiceOptions, ServiceRuntime, StaticBearerAuth};

pub const TOKEN: &str = "0123456789abcdef0123456789abcdef";
pub fn access() -> AccessContext {
    AccessContext::trusted("default")
}
pub fn auth() -> Arc<StaticBearerAuth> {
    identities(vec![(TOKEN, access())])
}
pub fn identities(entries: Vec<(&str, AccessContext)>) -> Arc<StaticBearerAuth> {
    Arc::new(
        StaticBearerAuth::new(
            entries
                .into_iter()
                .map(|(token, access)| BearerIdentity {
                    token: SecretValue::new(token.into()),
                    access,
                })
                .collect(),
        )
        .unwrap(),
    )
}
pub fn options(definitions: Vec<WorkflowDefinition>) -> ServiceOptions {
    ServiceOptions {
        listen: ([127, 0, 0, 1], 0).into(),
        definitions,
        request_timeout: Duration::from_secs(3),
        http_shutdown_timeout: Duration::from_secs(3),
        engine_shutdown_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}
pub fn client() -> Client {
    Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}
pub fn url(service: &ServiceRuntime, path: &str) -> String {
    format!("http://{}{}", service.local_addr(), path)
}
pub async fn value(response: Response, status: u16) -> Value {
    let actual = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(actual.as_u16(), status, "{text}");
    serde_json::from_str(&text).unwrap()
}
pub async fn get(service: &ServiceRuntime, path: &str, status: u16) -> Value {
    value(
        client()
            .get(url(service, path))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap(),
        status,
    )
    .await
}
pub async fn post(service: &ServiceRuntime, path: &str, body: Value, status: u16) -> Value {
    value(
        client()
            .post(url(service, path))
            .bearer_auth(TOKEN)
            .json(&body)
            .send()
            .await
            .unwrap(),
        status,
    )
    .await
}
pub async fn wait(service: &ServiceRuntime, id: RunId) -> RunSnapshot {
    tokio::time::timeout(
        Duration::from_secs(5),
        service.application().wait(access(), id),
    )
    .await
    .unwrap()
    .unwrap()
}
pub async fn state(service: &ServiceRuntime, id: &RunId, wanted: RunState) -> RunSnapshot {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let run = service
                .application()
                .status(access(), id.clone())
                .await
                .unwrap();
            if run.state == wanted {
                return run;
            }
            assert!(!run.state.is_terminal(), "Expected {wanted:?}, got {run:?}");
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap()
}
pub fn customer() -> WorkflowDefinition {
    serde_json::from_str(include_str!(
        "../../../../examples/workflows/customer_lookup.v2.json"
    ))
    .unwrap()
}
pub fn input() -> Value {
    json!({"request_id":"req-17","customer":" C-9 ","items":[{"sku":"A-1","quantity":2}]})
}
pub fn customer_builder() -> WorkflowBuilder {
    let mut builder = WorkflowBuilder::standard();
    builder
        .register_bundle(customer_operations(Arc::new(StaticDirectory(
            BTreeMap::from([("C-9".into(), true)]),
        ))))
        .unwrap();
    builder
}
pub fn single(id: &str, node: Value) -> WorkflowDefinition {
    serde_json::from_value(json!({
        "format":WORKFLOW_FORMAT,"schema_dialect":SCHEMA_DIALECT,"id":id,"revision":"r1",
        "input_schema":true,"output_schema":true,"entry":node["id"],"nodes":[node],"edges":[],
        "output":{"select":{"source":"node","node":node["id"],"pointer":""}}
    }))
    .unwrap()
}
pub fn echo() -> WorkflowDefinition {
    single(
        "test.echo",
        json!({"id":"echo","kind":"operation","operation":{"id":"forge.data.identity","contract":"1","implementation":"r1"},"config":{},"input":{"select":{"source":"input","pointer":""}}}),
    )
}
pub fn start_body(definition: &WorkflowDefinition, input: Value, durable: bool) -> Value {
    json!({"workflow":{"id":definition.id,"revision":definition.revision},"input":input,"options":{"require_durable":durable,"receipt_key":"request-1"}})
}
pub struct Directory(pub PathBuf);
impl Directory {
    pub fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("workflow-forge-service-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub fn store(&self, options: modules::SqliteOptions) -> Arc<modules::SqliteExecutionStore> {
        Arc::new(modules::SqliteExecutionStore::open(self.0.join("state.sqlite"), options).unwrap())
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
