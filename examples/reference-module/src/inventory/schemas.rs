use super::*;
use serde_json::json;

fn object(properties: Value, required: &[&str]) -> Value {
    json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
}
fn declared(mut schema: Value) -> Value {
    schema["$schema"] = json!(SCHEMA_DIALECT);
    schema
}
fn count() -> Value {
    json!({"type":"integer","minimum":0,"maximum":MAX_ROWS})
}
fn artifact() -> Value {
    object(
        json!({"id":{"type":"string","minLength":1},"scope":{"type":"string","minLength":1},"bytes":{"type":"integer","minimum":0},"media_type":{"type":"string","minLength":1}}),
        &["id", "scope", "bytes", "media_type"],
    )
}
fn summary() -> Value {
    object(
        json!({"rows":count(),"succeeded":count(),"failed":count()}),
        &["rows", "succeeded", "failed"],
    )
}
fn cursor() -> Value {
    object(
        json!({"source":artifact(),"cursor":count(),"more":{"type":"boolean"},"summary":summary(),"report":{"anyOf":[artifact(),{"type":"null"}]}}),
        &["source", "cursor", "more", "summary", "report"],
    )
}
fn update() -> Value {
    object(
        json!({"index":count(),"line":{"type":"integer","minimum":2},"sku":{"type":"string","minLength":1,"maxLength":64},"quantity":{"type":"integer","minimum":0}}),
        &["index", "line", "sku", "quantity"],
    )
}
fn row() -> Value {
    object(
        json!({"index":count(),"line":{"type":"integer","minimum":2},"sku":{"type":"string"},"quantity":{"type":["integer","null"],"minimum":0},"issue":{"type":"string"}}),
        &["index", "line", "sku", "quantity"],
    )
}
fn page() -> Value {
    object(
        json!({"cursor":count(),"next_cursor":count(),"total":count(),"rows":{"type":"array","maxItems":MAX_BATCH,"items":row()}}),
        &["cursor", "next_cursor", "total", "rows"],
    )
}
fn descriptor(
    id: &str,
    input: Value,
    output: Value,
    effect: EffectKind,
    artifacts: bool,
) -> OperationDescriptor {
    OperationDescriptor {
        revision:OperationRevision::new(&format!("reference.inventory.{id}"),"1","r1"),schema_dialect:SCHEMA_DIALECT.into(),
        config_schema:declared(object(json!({}),&[])),input_schema:declared(input),output_schema:declared(output),
        effect,repetition:Repetition::Safe,reconciliation:false,
        required_resources:if artifacts {["artifacts".into()].into()} else {Default::default()},
        description:match id {
            "read_page"=>"Read a bounded page of inventory CSV rows and retain source identities",
            "apply_row"=>"Apply one inventory row through a destination that deduplicates its logical effect key",
            "report_batch"=>"Publish an immutable partial batch report and advance its cursor",
            _=>"Stream confirmed batch reports into a final JSON Lines artifact",
        }.into(),examples:Vec::new(),
    }
}
pub(super) fn read_page() -> OperationDescriptor {
    let mut d = descriptor("read_page", cursor(), page(), EffectKind::Read, true);
    d.config_schema = declared(object(
        json!({"batch_size":{"type":"integer","minimum":1,"maximum":MAX_BATCH}}),
        &["batch_size"],
    ));
    d
}
pub(super) fn apply_row() -> OperationDescriptor {
    let mut d = descriptor("apply_row", update(), update(), EffectKind::Write, false);
    d.repetition = Repetition::Keyed;
    d.reconciliation = true;
    d
}
pub(super) fn report_batch() -> OperationDescriptor {
    descriptor(
        "report_batch",
        object(
            json!({"state":cursor(),"page":page(),"results":{"type":"array","maxItems":MAX_BATCH,"items":{"type":"object"}}}),
            &["state", "page", "results"],
        ),
        cursor(),
        EffectKind::Write,
        true,
    )
}
pub(super) fn publish_report() -> OperationDescriptor {
    let mut result = summary();
    result["properties"]["report"] = artifact();
    result["required"]
        .as_array_mut()
        .unwrap()
        .push(json!("report"));
    descriptor("publish_report", cursor(), result, EffectKind::Write, true)
}
