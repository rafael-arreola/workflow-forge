use serde_json::{Value, json};
use workflow_forge_protocol::*;

fn operation(id: &str) -> Value {
    json!({"id":id,"kind":"operation","input":{"select":{"source":"input","pointer":""}},"operation":{"id":"forge.data.identity","contract":"1","implementation":"r1"},"config":{}})
}
fn body() -> Value {
    json!({"entry":"first","nodes":[operation("first"),operation("last")],"edges":[{"from":"first","to":"last"}],"output":{"select":{"source":"node","node":"last","pointer":""}}})
}
fn workflow(node: Value) -> WorkflowDefinition {
    serde_json::from_value(json!({"format":WORKFLOW_FORMAT,"id":"control.contract","revision":"1","schema_dialect":SCHEMA_DIALECT,"input_schema":true,"output_schema":true,"entry":"group","nodes":[node],"edges":[],"output":{"select":{"source":"node","node":"group","pointer":""}}})).unwrap()
}

#[test]
fn typed_control_roundtrips_and_rejects_fields_from_another_variant() {
    let node = json!({"id":"group","kind":"foreach","input":{"literal":{}},"items":{"literal":[1,2]},"body":body(),"concurrency":2,"errors":"collect"});
    let parsed: NodeDefinition = serde_json::from_value(node.clone()).unwrap();
    assert!(matches!(parsed.instruction, Instruction::Foreach { .. }));
    assert_eq!(serde_json::to_value(parsed).unwrap(), node);
    let mut malformed = node.clone();
    malformed["operation"] = json!({"id":"wrong"});
    assert!(serde_json::from_value::<NodeDefinition>(malformed).is_err());
    let mut malformed = node;
    malformed["body"]["ignored"] = json!(true);
    assert!(serde_json::from_value::<NodeDefinition>(malformed).is_err());
    let mut malformed = operation("first");
    malformed["retry"] = json!({"max_attempts":3,"unknown":true});
    assert!(serde_json::from_value::<NodeDefinition>(malformed).is_err());
    let malformed =
        json!({"id":"group","kind":"decision","input":{"literal":null},"cases":[],"fallback":null});
    assert!(serde_json::from_value::<NodeDefinition>(malformed).is_err());
}

#[test]
fn normalized_nested_bodies_ignore_layout_order_but_preserve_decision_priority() {
    let node = json!({"id":"group","kind":"decision","input":{"literal":true},"cases":[{"id":"a","when":{"literal":true},"body":body()},{"id":"b","when":{"literal":false},"body":body()}]});
    let original = workflow(node.clone());
    let mut reordered = node;
    reordered["cases"][0]["body"]["nodes"]
        .as_array_mut()
        .unwrap()
        .reverse();
    assert_eq!(
        original.semantic_value(),
        workflow(reordered.clone()).semantic_value()
    );
    reordered["cases"].as_array_mut().unwrap().reverse();
    assert_ne!(
        original.semantic_value(),
        workflow(reordered).semantic_value()
    );
}

#[test]
fn resolution_payload_cannot_supply_the_actor_or_skip_quiescence() {
    let command = json!({"command_id":"cmd-1","run_id":"run-1","invocation_id":"inv-1","expected_revision":3,"observed_attempt":"attempt-1","resolution":{"decision":"confirm_not_applied","evidence":{"authority":"adapter","reference":"receipt-1","note":"query"},"quiescent":false,"retry":true}});
    let parsed: ReconcileCommand = serde_json::from_value(command.clone()).unwrap();
    assert!(matches!(
        parsed.resolution,
        EffectResolution::ConfirmNotApplied {
            quiescent: false,
            ..
        }
    ));
    let mut malformed = command.clone();
    malformed["actor"] = json!("administrator");
    assert!(serde_json::from_value::<ReconcileCommand>(malformed).is_err());
    let mut malformed = command;
    malformed["resolution"]
        .as_object_mut()
        .unwrap()
        .remove("quiescent");
    assert!(serde_json::from_value::<ReconcileCommand>(malformed).is_err());
}
