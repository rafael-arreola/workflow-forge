# Workflow Forge

A declarative workflow engine embedded in Rust applications. JSON definitions connect
operations through JSON Schema contracts, explicit data bindings and structured control.
The host owns the runtime, resources, authentication and incoming transports.

## Start here

- [Practical manual](manual/index.html): small calls through composed integrations.
- [Embedding guide](docs/EMBEDDING.md): startup, cancellation, shutdown and recovery.
- [Executable examples](EXAMPLES.md): local proofs of concept and expected results.
- [Contracts](docs/CONTRACTS.md) and [architecture](docs/ARCHITECTURE.md): extension boundaries.

The project is preparing its first public release. Until published, use a local dependency:

```toml
[dependencies]
workflow-forge = { path = "../workflow-forge/crates/forge", default-features = false }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust
// Fragment inside an async host; the host supplies a valid definition and input.
use workflow_forge::prelude::*;

let runtime = EngineRuntime::boot(
    WorkflowBuilder::standard().build()?, BootOptions::default(),
).await?;
let app = runtime.application();
let access = AccessContext::trusted("default"); // Explicit local trust decision.
let result = async {
    let plan = app.prepare(access.clone(), definition).await?;
    app.execute(access, StartRunRequest::new(plan, input), CancellationToken::new()).await
}.await;
let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
// Inspect both result and shutdown, including forced/pending work.
```

Build once, share the application handle and reuse prepared plans within the same runtime.
See the [complete customer example](examples/host/examples/v2_customer.rs).

## Capabilities

- JSON Schema validation, exact revisions and explicit JSON Pointer bindings.
- Decisions, error handlers, bounded parallelism, loops and subworkflows.
- Effect certainty, safe retry policies and explicit reconciliation of uncertain writes.
- Host cancellation, deadlines, admission limits and retention budgets.
- Memory providers, optional local SQLite persistence, timers and signals.
- Optional outbound HTTP/JSON, file reads, CSV batches and artifact streams.

Default features expose `integrations`; `sqlite` adds the durable provider and `full` enables
both. Disable default features for the memory/data-only dependency set. Features expose
constructors, not configured endpoints or implicit provider selection.

## Crates

| Package | Responsibility |
|---|---|
| `workflow-forge` | Public facade and standard composition. |
| `workflow-forge-protocol` | Extension traits, descriptors, commands and shared contracts. |
| `workflow-forge-engine` | Preparation, coordination, state transitions and lifecycle. |
| `workflow-forge-modules` | Official operations and infrastructure providers. |
| `workflow-forge-conformance` | Reusable provider contract checks. |

Extensions are compiled, trusted Rust. SQLite is single-owner local storage, not a distributed
worker system. Default boot rejects unfinished runs; the host must explicitly authorize recovery.
There is no execution server, remote client, dynamic plugin sandbox or graphical editor.
Fixtures do not establish production acceptance or a universal throughput/memory guarantee.

## Development and release

Use the pinned Rust toolchain. [Release checks](docs/RELEASING.md) document all verification,
proofs of concept, package contents and publication order. Repository-only tests and examples
live in `examples/host`; they are not dependencies of the published facade.

```sh
./scripts/check.sh
```

## License

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
