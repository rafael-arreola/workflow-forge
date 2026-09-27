//! Print an authored document on stdout; validation/result metadata goes to stderr.
use serde_json::json;
use workflow_forge::v2::*;
use workflow_forge_authoring_example::{move_node, one_operation};
use workflow_forge_reference_module::text::text_operations;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = WorkflowBuilder::standard();
    builder.register_bundle(text_operations())?;
    let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
    let app = runtime.application();
    let access = AccessContext::trusted("default");
    let work = async {
        // Catalog serialization represents the editor/host boundary.
        let catalog_json = serde_json::to_vec(&app.catalog(&access)?)?;
        let catalog: Vec<OperationDescriptor> = serde_json::from_slice(&catalog_json)?;
        let selected = OperationRevision::new("example.text.prefix", "1", "r1");
        let mut draft = one_operation(
            &catalog,
            &selected,
            "example.authored",
            "r1",
            json!({"prefix":"ID-"}),
        )?;
        draft
            .presentation
            .insert("notes".into(), json!({"owner":"authoring example"}));
        move_node(&mut draft, "step", 80.0, 120.0)?;
        let exported = serde_json::to_vec_pretty(&draft)?;
        let imported: WorkflowDefinition = serde_json::from_slice(&exported)?;
        let prepared = app.prepare(access.clone(), imported).await?;
        let receipt = app
            .start(access.clone(), StartRunRequest::new(prepared, json!("42")))
            .await?;
        let completed = app.wait(access.clone(), receipt.run_id).await?;
        if completed.state != RunState::Succeeded || completed.output != Some(json!("ID-42")) {
            return Err(
                ForgeError::new("example.unexpected_result", "Authored workflow failed").into(),
            );
        }
        println!("{}", String::from_utf8(exported)?);
        eprintln!("Validated and executed: ID-42");
        Ok::<_, Box<dyn std::error::Error>>(())
    }
    .await;
    let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
    work?;
    shutdown?;
    Ok(())
}
