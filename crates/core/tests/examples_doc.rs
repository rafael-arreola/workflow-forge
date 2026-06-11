//! Mantiene honesto a EXAMPLES.md: cada bloque ```json que contenga un
//! workflow (tiene "nodes") debe validar contra el schema publicado y
//! contra la validación estructural del core.

use serde_json::Value;
use workflow_forge_core::spec::WorkflowDefinition;
use workflow_forge_core::validate as validation;

fn extract_json_blocks(markdown: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut rest = markdown;
    while let Some(start) = rest.find("```json") {
        let after = &rest[start + 7..];
        let Some(end) = after.find("```") else { break };
        blocks.push(after[..end].trim().to_string());
        rest = &after[end + 3..];
    }
    blocks
}

#[test]
fn todos_los_workflows_de_examples_md_validan() {
    let markdown = include_str!("../../../EXAMPLES.md");
    let schema: Value =
        serde_json::from_str(include_str!("../../../schemas/1.0/workflow.schema.json")).unwrap();
    let validator = jsonschema::validator_for(&schema).unwrap();

    let mut workflows_found = 0;
    for (i, block) in extract_json_blocks(markdown).iter().enumerate() {
        let doc: Value = serde_json::from_str(block)
            .unwrap_or_else(|e| panic!("bloque json #{i} no parsea: {e}\n---\n{block}"));
        if doc.get("nodes").is_none() {
            continue; // triggers y otros snippets
        }
        workflows_found += 1;

        let errors: Vec<String> = validator.iter_errors(&doc).map(|e| e.to_string()).collect();
        assert!(
            errors.is_empty(),
            "el workflow '{}' no cumple el schema: {errors:?}",
            doc["name"]
        );

        let workflow: WorkflowDefinition = serde_json::from_value(doc.clone())
            .unwrap_or_else(|e| panic!("el workflow '{}' no deserializa: {e}", doc["name"]));
        validation::validate(&workflow).unwrap_or_else(|errs| {
            panic!(
                "el workflow '{}' no pasa validación estructural: {errs:?}",
                doc["name"]
            )
        });
    }

    assert!(
        workflows_found >= 5,
        "se esperaban al menos 5 workflows en EXAMPLES.md, hay {workflows_found}"
    );
}
