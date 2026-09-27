mod support;
use serde_json::json;
use std::{collections::BTreeSet, time::Duration};
use support::*;
use workflow_forge::v2::*;
use workflow_forge_service::{ServiceRuntime, dto::*};

const READER: &str = "reader0123456789abcdef0123456789ab";
const OUTSIDER: &str = "outsider0123456789abcdef0123456789";

#[tokio::test]
async fn identities_permissions_resources_and_control_fields_cannot_be_forged() {
    let mut reader = access();
    reader.actor = "reader".into();
    reader.permissions = BTreeSet::from([Permission::Read]);
    reader.resources.clear();
    let authentication = identities(vec![
        (TOKEN, access()),
        (READER, reader),
        (OUTSIDER, AccessContext::trusted("another-scope")),
    ]);
    let definition = customer();
    let mut builder = customer_builder();
    builder
        .register_bundle(
            workflow_forge_reference_module::inventory::inventory_operations(std::sync::Arc::new(
                workflow_forge_reference_module::inventory::MemoryInventory::default(),
            )),
        )
        .unwrap();
    let service = ServiceRuntime::boot(
        builder.build().unwrap(),
        authentication,
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    for path in ["/v2/catalog", "/v2/not-a-route"] {
        let error = value(client().get(url(&service, path)).send().await.unwrap(), 401).await;
        assert_eq!(error["diagnostics"][0]["code"], "http.unauthenticated");
    }
    assert_eq!(
        value(
            client()
                .get(url(&service, "/health/live"))
                .send()
                .await
                .unwrap(),
            200
        )
        .await,
        json!({"live":true})
    );
    value(
        client()
            .get(url(&service, "/v2/catalog"))
            .bearer_auth(OUTSIDER)
            .send()
            .await
            .unwrap(),
        403,
    )
    .await;
    let catalog: CatalogResponse = serde_json::from_value(
        value(
            client()
                .get(url(&service, "/v2/catalog"))
                .bearer_auth(READER)
                .send()
                .await
                .unwrap(),
            200,
        )
        .await,
    )
    .unwrap();
    assert!(
        catalog
            .items
            .iter()
            .all(|d| d.required_resources.is_empty())
    );
    let full: CatalogResponse =
        serde_json::from_value(get(&service, "/v2/catalog", 200).await).unwrap();
    assert!(full.items.len() > catalog.items.len());
    value(
        client()
            .post(url(&service, "/v2/workflows/prepare"))
            .bearer_auth(READER)
            .json(&json!({"definition":definition}))
            .send()
            .await
            .unwrap(),
        403,
    )
    .await;
    value(
        client()
            .post(url(&service, "/v2/runs"))
            .bearer_auth(READER)
            .json(&start_body(&definition, input(), false))
            .send()
            .await
            .unwrap(),
        403,
    )
    .await;
    value(
        client()
            .post(url(&service, "/v2/artifacts"))
            .bearer_auth(READER)
            .body("data")
            .send()
            .await
            .unwrap(),
        403,
    )
    .await;
    let mut body = start_body(&definition, input(), false);
    body["actor"] = json!("host");
    post(&service, "/v2/runs", body, 400).await;
    let mut request = client()
        .get(url(&service, "/v2/catalog"))
        .bearer_auth(TOKEN)
        .build()
        .unwrap();
    request
        .headers_mut()
        .append("authorization", format!("Bearer {READER}").parse().unwrap());
    value(client().execute(request).await.unwrap(), 401).await;
    let response = client()
        .get(url(&service, "/v2/catalog"))
        .bearer_auth("invalid-secret-do-not-echo")
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["www-authenticate"], "Bearer");
    let text = response.text().await.unwrap();
    assert!(!text.contains("invalid-secret-do-not-echo"));
    let reference: ArtifactRef = serde_json::from_value(
        value(
            client()
                .post(url(&service, "/v2/artifacts"))
                .bearer_auth(TOKEN)
                .body("safe")
                .send()
                .await
                .unwrap(),
            201,
        )
        .await,
    )
    .unwrap();
    value(
        client()
            .post(url(&service, "/v2/artifacts/read"))
            .bearer_auth(READER)
            .json(&json!({"artifact":reference}))
            .send()
            .await
            .unwrap(),
        403,
    )
    .await;
    let mut mismatched = reference.clone();
    mismatched.bytes += 1;
    let missing = post(
        &service,
        "/v2/artifacts/read",
        json!({"artifact":mismatched}),
        404,
    )
    .await;
    assert_eq!(missing["diagnostics"][0]["code"], "resource.missing");
    let mut wrong = reference;
    wrong.scope = "another-scope".into();
    post(
        &service,
        "/v2/artifacts/read",
        json!({"artifact":wrong}),
        403,
    )
    .await;
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn cursors_are_bound_to_the_actor_collection_instance_and_run_revision() {
    let mut reader = access();
    reader.actor = "second-actor".into();
    let mut definition = echo();
    definition.nodes.push(
        serde_json::from_value(
            json!({"id":"pause","kind":"timer","duration_ms":60000,"input":{"literal":null}}),
        )
        .unwrap(),
    );
    definition
        .edges
        .push(serde_json::from_value(json!({"from":"echo","to":"pause"})).unwrap());
    let service = ServiceRuntime::boot(
        WorkflowBuilder::standard().build().unwrap(),
        identities(vec![(TOKEN, access()), (READER, reader)]),
        options(vec![definition.clone()]),
    )
    .await
    .unwrap();
    let first = get(&service, "/v2/catalog?limit=1", 200).await;
    let cursor = first["next_cursor"].as_str().unwrap();
    let path = format!("/v2/catalog?limit=100&cursor={cursor}");
    let next = get(&service, &path, 200).await;
    assert!(!next["items"].as_array().unwrap().is_empty());
    assert_ne!(first["items"][0], next["items"][0]);
    value(
        client()
            .get(url(&service, &path))
            .bearer_auth(READER)
            .send()
            .await
            .unwrap(),
        400,
    )
    .await;
    get(&service, "/v2/catalog?limit=0", 400).await;
    get(&service, "/v2/catalog?limit=101", 400).await;
    get(&service, "/v2/catalog?cursor=not-base64", 400).await;
    let receipt: StartReceipt = serde_json::from_value(
        post(
            &service,
            "/v2/runs",
            start_body(&definition, json!(1), false),
            202,
        )
        .await,
    )
    .unwrap();
    state(&service, &receipt.run_id, RunState::Waiting).await;
    let path = format!("/v2/runs/{}/invocations?limit=1", receipt.run_id.0);
    let page: Page<InvocationStatus> =
        serde_json::from_value(get(&service, &path, 200).await).unwrap();
    let cursor = page.next_cursor.unwrap();
    get(
        &service,
        &format!("/v2/runs/{}/waits?cursor={cursor}", receipt.run_id.0),
        400,
    )
    .await;
    value(
        client()
            .post(url(
                &service,
                &format!("/v2/runs/{}/cancel", receipt.run_id.0),
            ))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap(),
        202,
    )
    .await;
    assert_eq!(
        wait(&service, receipt.run_id.clone()).await.state,
        RunState::Cancelled
    );
    assert_eq!(
        get(&service, &format!("{path}&cursor={cursor}"), 409).await["diagnostics"][0]["code"],
        "state.conflict"
    );
    let old = first["next_cursor"].as_str().unwrap().to_owned();
    service.shutdown().await.unwrap();
    let replacement = ServiceRuntime::boot(
        WorkflowBuilder::standard().build().unwrap(),
        auth(),
        options(vec![]),
    )
    .await
    .unwrap();
    get(&replacement, &format!("/v2/catalog?cursor={old}"), 400).await;
    replacement.shutdown().await.unwrap();
}

#[tokio::test]
async fn parsers_byte_budgets_plan_capacity_and_response_budgets_are_enforced() {
    let definition = echo();
    let mut opts = options(vec![definition.clone()]);
    opts.max_json_bytes = 1024;
    opts.max_prepared = 1;
    opts.max_response_bytes = 1024;
    let service = ServiceRuntime::boot(WorkflowBuilder::standard().build().unwrap(), auth(), opts)
        .await
        .unwrap();
    value(
        client()
            .post(url(&service, "/v2/runs"))
            .bearer_auth(TOKEN)
            .body("private parser data")
            .send()
            .await
            .unwrap(),
        415,
    )
    .await;
    let malformed = value(
        client()
            .post(url(&service, "/v2/runs"))
            .bearer_auth(TOKEN)
            .header("content-type", "application/json")
            .body("{ private parser data")
            .send()
            .await
            .unwrap(),
        400,
    )
    .await;
    assert!(!malformed.to_string().contains("private parser data"));
    value(
        client()
            .post(url(&service, "/v2/runs"))
            .bearer_auth(TOKEN)
            .header("content-type", "application/json")
            .body("x".repeat(1025))
            .send()
            .await
            .unwrap(),
        413,
    )
    .await;
    let mut other = definition.clone();
    other.revision = "r2".into();
    post(
        &service,
        "/v2/workflows/prepare",
        json!({"definition":other}),
        422,
    )
    .await;
    let mut changed = definition.clone();
    changed.nodes[0].input = Binding::Literal(json!("changed"));
    post(
        &service,
        "/v2/workflows/prepare",
        json!({"definition":changed}),
        409,
    )
    .await;
    // A host can accept input through Rust larger than the HTTP response budget.
    let app = service.application();
    let plan = app.prepare(access(), definition).await.unwrap();
    let receipt = app
        .start(
            access(),
            StartRunRequest::new(plan, json!("x".repeat(2048))),
        )
        .await
        .unwrap();
    wait(&service, receipt.run_id.clone()).await;
    get(
        &service,
        &format!("/v2/runs/{}/result", receipt.run_id.0),
        413,
    )
    .await;
    get(&service, "/v2/catalog", 413).await;
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn authentication_is_subject_to_request_capacity_and_deadline() {
    struct Slow;
    impl workflow_forge_service::RequestAuthenticator for Slow {
        fn authenticate<'a>(
            &'a self,
            _: &'a workflow_forge_service::HeaderMap,
        ) -> PortFuture<'a, AccessContext> {
            Box::pin(std::future::pending())
        }
    }
    let mut opts = options(vec![]);
    opts.max_requests = 1;
    opts.request_timeout = Duration::from_millis(100);
    let service = ServiceRuntime::boot(
        WorkflowBuilder::standard().build().unwrap(),
        std::sync::Arc::new(Slow),
        opts,
    )
    .await
    .unwrap();
    let first = client().get(url(&service, "/v2/catalog")).send();
    let second = async {
        tokio::time::sleep(Duration::from_millis(20)).await;
        value(
            client()
                .get(url(&service, "/v2/catalog"))
                .send()
                .await
                .unwrap(),
            429,
        )
        .await
    };
    let (first, _) = tokio::join!(first, second);
    value(first.unwrap(), 503).await;
    service.shutdown().await.unwrap();
}
