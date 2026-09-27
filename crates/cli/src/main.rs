//! `forge` consumes the same authenticated HTTP contract as any external client.
mod client;
mod error;
mod input;

use clap::{Args, Parser, Subcommand};
use client::{Budgets, Client};
use error::{Failure, Result};
use reqwest::Method;
use serde_json::json;
use std::{path::PathBuf, process::ExitCode, time::Duration};
use workflow_forge_protocol::*;

#[derive(Parser)]
#[command(
    name = "forge",
    about = "HTTP client for Workflow Forge (format 2)",
    version
)]
struct Cli {
    #[arg(long, global = true, default_value = "http://127.0.0.1:7070")]
    server: String,
    #[arg(long, global = true, default_value = "WORKFLOW_FORGE_TOKEN")]
    token_env: String,
    #[arg(long,global=true,default_value_t=30000,value_parser=clap::value_parser!(u64).range(1..=3_600_000))]
    request_timeout_ms: u64,
    #[arg(long,global=true,default_value_t=2*1024*1024,value_parser=clap::value_parser!(u64).range(256..=64*1024*1024))]
    max_json_bytes: u64,
    #[arg(long,global=true,default_value_t=8*1024*1024,value_parser=clap::value_parser!(u64).range(256..=64*1024*1024))]
    max_response_bytes: u64,
    #[arg(long,global=true,default_value_t=8*1024*1024,value_parser=clap::value_parser!(u64).range(1..=1_099_511_627_776))]
    max_artifact_bytes: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Args)]
struct StartArgs {
    /// Inline JSON or @FILE. Otherwise reads piped stdin, or uses {}.
    #[arg(short, long)]
    input: Option<String>,
    /// Deadline for the remote run; independent of client waiting.
    #[arg(long,value_parser=clap::value_parser!(u64).range(1..))]
    timeout_ms: Option<u64>,
    #[arg(long)]
    receipt_key: Option<String>,
    /// Explicitly accept an ephemeral service profile.
    #[arg(long)]
    ephemeral: bool,
    /// JSON file containing one ArtifactRef; repeat to attach several references.
    #[arg(long = "artifact-ref")]
    artifacts: Vec<PathBuf>,
}
impl StartArgs {
    async fn resolve(self, limit: usize) -> Result<(serde_json::Value, StartOptions)> {
        if self.artifacts.len() > 1000 {
            return Err(Failure::new(
                "cli.body_too_large",
                "Too many input artifact references",
            ));
        }
        let input = input::trigger(self.input, limit).await?;
        let artifacts = self
            .artifacts
            .iter()
            .map(|p| input::read_json::<ArtifactRef>(p, limit))
            .collect::<Result<Vec<_>>>()?;
        Ok((
            input,
            StartOptions {
                require_durable: !self.ephemeral,
                timeout_ms: self.timeout_ms,
                receipt_key: self.receipt_key,
                artifacts,
            },
        ))
    }
}

#[derive(Args)]
struct WaitArgs {
    /// Closing or timing out this client does not cancel accepted work.
    #[arg(long,default_value_t=300000,value_parser=clap::value_parser!(u64).range(1..=86_400_000))]
    wait_timeout_ms: u64,
    #[arg(long,default_value_t=100,value_parser=clap::value_parser!(u64).range(1..=60_000))]
    poll_ms: u64,
}

#[derive(Args)]
struct PageArgs {
    run_id: String,
    #[arg(long, default_value_t=50, value_parser=clap::value_parser!(u16).range(1..=100))]
    limit: u16,
    #[arg(long)]
    cursor: Option<String>,
}

#[derive(Subcommand)]
enum Command {
    /// List all operations authorized for the credential, following pagination.
    Catalog,
    /// Prepare a format-2 workflow without invoking its operations.
    Validate {
        workflow: PathBuf,
    },
    /// Prepare, start and wait. The accepted receipt is printed to stderr.
    Run {
        workflow: PathBuf,
        #[command(flatten)]
        start: StartArgs,
        #[command(flatten)]
        wait: WaitArgs,
        /// Return the receipt immediately; the service continues execution.
        #[arg(long)]
        no_wait: bool,
    },
    /// Start an already prepared workflow revision and return its receipt.
    Start {
        id: String,
        revision: String,
        #[command(flatten)]
        start: StartArgs,
    },
    /// Wait for an accepted run without starting it again.
    Wait {
        run_id: String,
        #[command(flatten)]
        wait: WaitArgs,
    },
    Status {
        run_id: String,
    },
    /// List invocation metadata and identifiers; continue with next_cursor.
    Invocations(PageArgs),
    /// List wait identifiers and correlations needed to deliver a signal.
    Waits(PageArgs),
    /// List effect-resolution audit metadata.
    Audit(PageArgs),
    Result {
        run_id: String,
    },
    /// Request cancellation explicitly; remote effects may need reconciliation.
    Cancel {
        run_id: String,
    },
    /// Submit a SignalCommand JSON file; identity is taken from the credential.
    Signal {
        command: PathBuf,
    },
    Inspect {
        run_id: String,
        invocation_id: String,
    },
    /// Submit a ReconcileCommand JSON file without choosing a decision for you.
    Reconcile {
        command: PathBuf,
    },
    Upload {
        file: PathBuf,
        #[arg(long, default_value = "application/octet-stream")]
        media_type: String,
    },
    Download {
        reference: PathBuf,
        #[arg(long)]
        output: PathBuf,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    let arguments = Cli::parse();
    let result = tokio::select! {
        result = run(arguments) => result,
        _ = interruption() => Err(Failure::pending("cli.interrupted", "Client interrupted; accepted runs are not cancelled")),
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(failure) => {
            let _ = input::output(&failure.error, true);
            ExitCode::from(failure.exit)
        }
    }
}

async fn interruption() {
    #[cfg(unix)]
    {
        if let Ok(mut terminate) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            tokio::select! { _ = tokio::signal::ctrl_c() => (), _ = terminate.recv() => () }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

async fn run(arguments: Cli) -> Result<()> {
    if arguments.token_env.is_empty()
        || arguments.token_env.len() > 256
        || !arguments
            .token_env
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(Failure::new(
            "cli.credentials",
            "Invalid credential environment reference",
        ));
    }
    let token = std::env::var(&arguments.token_env).map_err(|_| {
        Failure::new(
            "cli.credentials",
            "Credential environment variable is unavailable",
        )
    })?;
    let client = Client::new(
        &arguments.server,
        token,
        Duration::from_millis(arguments.request_timeout_ms),
        Budgets {
            json: arguments.max_json_bytes as usize,
            response: arguments.max_response_bytes as usize,
            artifact: arguments.max_artifact_bytes,
        },
    )?;
    let limit = client.budgets.json;
    match arguments.command {
        Command::Catalog => input::output(&client.catalog().await?, false),
        Command::Validate { workflow } => input::output(
            &client.prepare(input::read_json(&workflow, limit)?).await?,
            false,
        ),
        Command::Run {
            workflow,
            start,
            wait,
            no_wait,
        } => {
            let definition: WorkflowDefinition = input::read_json(&workflow, limit)?;
            let revision = WorkflowRevision {
                id: definition.id.clone(),
                revision: definition.revision.clone(),
            };
            let (input, options) = start.resolve(limit).await?;
            client.prepare(definition).await?;
            let receipt = client.start(revision, input, options).await?;
            if no_wait {
                return input::output(&receipt, false);
            }
            input::output(&json!({"accepted":receipt}), true)?;
            input::output(
                &client
                    .wait(
                        &receipt.run_id,
                        Duration::from_millis(wait.wait_timeout_ms),
                        Duration::from_millis(wait.poll_ms),
                    )
                    .await?,
                false,
            )
        }
        Command::Start {
            id,
            revision,
            start,
        } => {
            let (input, options) = start.resolve(limit).await?;
            input::output(
                &client
                    .start(WorkflowRevision { id, revision }, input, options)
                    .await?,
                false,
            )
        }
        Command::Wait { run_id, wait } => input::output(
            &client
                .wait(
                    &RunId(run_id),
                    Duration::from_millis(wait.wait_timeout_ms),
                    Duration::from_millis(wait.poll_ms),
                )
                .await?,
            false,
        ),
        Command::Status { run_id } => input::output(
            &client
                .run_json(Method::GET, &RunId(run_id), &[], None)
                .await?,
            false,
        ),
        Command::Invocations(page) => page_output(&client, "invocations", page).await,
        Command::Waits(page) => page_output(&client, "waits", page).await,
        Command::Audit(page) => page_output(&client, "audit", page).await,
        Command::Result { run_id } => input::output(
            &client
                .run_json(Method::GET, &RunId(run_id), &["result"], None)
                .await?,
            false,
        ),
        Command::Cancel { run_id } => input::output(
            &client
                .run_json(Method::POST, &RunId(run_id), &["cancel"], None)
                .await?,
            false,
        ),
        Command::Signal { command } => {
            let command: SignalCommand = input::read_json(&command, limit)?;
            input::output(
                &client
                    .run_json(
                        Method::POST,
                        &command.run_id,
                        &["signals"],
                        Some(json!(command)),
                    )
                    .await?,
                false,
            )
        }
        Command::Inspect {
            run_id,
            invocation_id,
        } => input::output(
            &client
                .run_json(
                    Method::POST,
                    &RunId(run_id),
                    &["effects", "inspect"],
                    Some(json!({"invocation_id":invocation_id})),
                )
                .await?,
            false,
        ),
        Command::Reconcile { command } => {
            let command: ReconcileCommand = input::read_json(&command, limit)?;
            input::output(
                &client
                    .run_json(
                        Method::POST,
                        &command.run_id,
                        &["effects", "reconcile"],
                        Some(json!(command)),
                    )
                    .await?,
                false,
            )
        }
        Command::Upload { file, media_type } => {
            input::output(&client.upload(&file, &media_type).await?, false)
        }
        Command::Download { reference, output } => {
            let reference: ArtifactRef = input::read_json(&reference, limit)?;
            client.download(reference.clone(), &output).await?;
            input::output(&json!({"artifact":reference,"downloaded":true}), false)
        }
    }
}

async fn page_output(client: &Client, collection: &str, page: PageArgs) -> Result<()> {
    input::output(
        &client
            .page(
                &RunId(page.run_id),
                collection,
                page.limit,
                page.cursor.as_deref(),
            )
            .await?,
        false,
    )
}
