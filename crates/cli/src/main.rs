//! `forge` — the CLI runtime for workflow-forge.
//!
//! Runs, validates and inspects declarative JSON workflows using the bundled
//! official extensions (`util`, `data`, `http`, `tabular`, `sftp`).
//!
//! ```text
//! forge run workflow.json --input '{"who":"world"}'
//! echo '{"who":"world"}' | forge run workflow.json
//! forge validate workflow.json
//! forge catalog
//! ```

use std::io::{IsTerminal, Read};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use workflow_forge::prelude::*;

#[derive(Parser)]
#[command(
    name = "forge",
    about = "Run and inspect declarative JSON workflows",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a workflow to completion and print its output JSON to stdout.
    Run {
        /// Path to the workflow JSON document.
        workflow: PathBuf,
        /// Trigger input: inline JSON, or `@path` to a JSON file. If omitted,
        /// reads piped stdin, else defaults to `{}`.
        #[arg(short, long)]
        input: Option<String>,
        /// Abort the run after this many milliseconds (unlimited by default).
        #[arg(long)]
        timeout_ms: Option<u64>,
    },
    /// Validate a workflow document (structure, schemas, references) without
    /// running it. Exit code is non-zero if invalid.
    Validate {
        /// Path to the workflow JSON document.
        workflow: PathBuf,
    },
    /// Print the task catalog (all bundled extensions) as JSON.
    Catalog,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(message) => {
            eprintln!("error: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<ExitCode, String> {
    match Cli::parse().command {
        Command::Run {
            workflow,
            input,
            timeout_ms,
        } => cmd_run(&workflow, input, timeout_ms).await,
        Command::Validate { workflow } => cmd_validate(&workflow),
        Command::Catalog => cmd_catalog(),
    }
}

fn load_workflow(path: &Path) -> Result<WorkflowDefinition, String> {
    let text =
        std::fs::read_to_string(path).map_err(|e| format!("no se pudo leer {path:?}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("workflow JSON inválido: {e}"))
}

/// Resolve the trigger: `@file` → file contents, other text → inline JSON,
/// `None` → piped stdin if present, else `{}`.
fn resolve_trigger(input: Option<String>) -> Result<serde_json::Value, String> {
    let text = match input {
        Some(arg) => match arg.strip_prefix('@') {
            Some(path) => std::fs::read_to_string(path)
                .map_err(|e| format!("no se pudo leer el input {path:?}: {e}"))?,
            None => arg,
        },
        None => {
            let mut stdin = std::io::stdin();
            if stdin.is_terminal() {
                "{}".to_string()
            } else {
                let mut buf = String::new();
                stdin
                    .read_to_string(&mut buf)
                    .map_err(|e| format!("no se pudo leer stdin: {e}"))?;
                if buf.trim().is_empty() {
                    "{}".to_string()
                } else {
                    buf
                }
            }
        }
    };
    serde_json::from_str(&text).map_err(|e| format!("trigger JSON inválido: {e}"))
}

async fn cmd_run(
    path: &Path,
    input: Option<String>,
    timeout_ms: Option<u64>,
) -> Result<ExitCode, String> {
    let workflow = load_workflow(path)?;
    let trigger = resolve_trigger(input)?;

    let executor = WorkflowExecutor::new(workflow, workflow_forge::default_registry())
        .map_err(|errors| format_errors("workflow inválido", &errors))?;

    let options = match timeout_ms {
        Some(ms) => RunOptions::default().deadline(Duration::from_millis(ms)),
        None => RunOptions::default(),
    };

    match executor.run_with(WorkflowData(trigger), options).await {
        Ok(output) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&output.0).unwrap_or_default()
            );
            Ok(ExitCode::SUCCESS)
        }
        Err(error) => {
            eprintln!("la ejecución falló [{}]: {}", error.code, error.message);
            Ok(ExitCode::FAILURE)
        }
    }
}

fn cmd_validate(path: &Path) -> Result<ExitCode, String> {
    let workflow = load_workflow(path)?;
    match WorkflowExecutor::new(workflow, workflow_forge::default_registry()) {
        Ok(_) => {
            println!("ok: el workflow es válido");
            Ok(ExitCode::SUCCESS)
        }
        Err(errors) => {
            eprintln!("{}", format_errors("workflow inválido", &errors));
            Ok(ExitCode::FAILURE)
        }
    }
}

fn cmd_catalog() -> Result<ExitCode, String> {
    let catalog = workflow_forge::default_registry().catalog();
    println!(
        "{}",
        serde_json::to_string_pretty(&catalog).map_err(|e| e.to_string())?
    );
    Ok(ExitCode::SUCCESS)
}

fn format_errors(prefix: &str, errors: &[WorkflowError]) -> String {
    let mut out = format!("{prefix} ({} problema(s)):", errors.len());
    for e in errors {
        out.push_str(&format!("\n  - [{}] {}", e.code, e.message));
    }
    out
}
