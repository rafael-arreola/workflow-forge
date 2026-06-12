# workflow-forge

[![CI](https://github.com/rafael-arreola/workflow-forge/actions/workflows/ci.yml/badge.svg)](https://github.com/rafael-arreola/workflow-forge/actions/workflows/ci.yml)
[![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](#license)

A declarative, JSON-based workflow engine for Rust. Workflows are **plain JSON
documents** — defined with [JSON Schema](schemas/1.0/), wired with JSONPath —
executed by an embeddable, async core.

> **Status: pre-1.0.** The spec and APIs may still change. Feedback and
> contributions are very welcome.

## Why

- **Workflows as data**: a workflow is a JSON document you can store, diff,
  validate and generate. No DSL to learn, no macros.
- **Validate without the engine**: the [published JSON Schemas](schemas/1.0/)
  let any standard validator (or editor) check a workflow definition.
- **Typed extensions**: every task declares a manifest (id + JSON Schemas for
  its input/output). The engine enforces the contracts at runtime and can
  export the full task catalog as JSON — the foundation for tooling and
  visual editors.
- **Real control flow**: exclusive/parallel/join gateways, per-node retries
  with backoff, timeouts, error routes (`on: "error"`), and a condition
  mini-DSL that is itself JSON.

## Quickstart

```toml
[dependencies]
workflow-forge = "0.1"   # features: util, data, http (default) + tabular, sftp
tokio = { version = "1", features = ["full"] }
```

```rust
use workflow_forge::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let workflow: WorkflowDefinition = serde_json::from_str(r#"{
        "spec": "1.0",
        "name": "greet",
        "version": "0.1.0",
        "nodes": [
            { "id": "start", "kind": "start" },
            { "id": "render", "kind": "task", "task": "data.template",
              "input": { "template": "Hello {who}!", "values": "$.trigger" } },
            { "id": "end", "kind": "end" }
        ],
        "edges": [
            { "from": "start", "to": "render" },
            { "from": "render", "to": "end" }
        ]
    }"#)?;

    let registry = workflow_forge::default_registry();
    let executor = WorkflowExecutor::new(workflow, registry)
        .map_err(|errors| format!("invalid workflow: {errors:?}"))?;

    let result = executor
        .run(WorkflowData(serde_json::json!({ "who": "world" })))
        .await?;

    println!("{}", result.0); // "Hello world!"
    Ok(())
}
```

## Anatomy of a workflow

Data flows through a per-execution **context document**. Every node reads its
input via JSONPath mappings and publishes its output:

```text
$.trigger              → the initial input of the execution
$.nodes.<id>.output    → the output of each executed node
$.nodes.<id>.error     → the error of a node, when routed via on: "error"
$.workflow             → metadata (name, version, execution_id)
```

A more complete example — branch on an HTTP status, retry with backoff, and
handle failures through an error route:

```json
{
  "spec": "1.0",
  "name": "sync-user",
  "version": "1.0.0",
  "nodes": [
    { "id": "start", "kind": "start" },
    {
      "id": "fetch",
      "kind": "task",
      "task": "http.request",
      "input": { "url": "$.trigger.url", "fail_on_error_status": true },
      "retry": { "max": 3, "backoff": "exponential", "initial_ms": 500 },
      "timeout_ms": 10000
    },
    {
      "id": "check",
      "kind": "gateway",
      "gateway": "exclusive",
      "branches": [
        {
          "when": { "path": "$.nodes.fetch.output.body.active", "eq": true },
          "edge": "active"
        },
        { "else": true, "edge": "inactive" }
      ]
    },
    {
      "id": "notify",
      "kind": "task",
      "task": "util.log",
      "input": {
        "message": "user is active",
        "value": "$.nodes.fetch.output.body"
      }
    },
    {
      "id": "alert",
      "kind": "task",
      "task": "util.log",
      "input": { "level": "error", "message": "$.nodes.fetch.error" }
    },
    { "id": "end", "kind": "end" },
    { "id": "end-error", "kind": "end", "status": "error" }
  ],
  "edges": [
    { "from": "start", "to": "fetch" },
    { "from": "fetch", "to": "check" },
    { "from": "fetch", "on": "error", "to": "alert" },
    { "from": "check", "label": "active", "to": "notify" },
    { "from": "check", "label": "inactive", "to": "end" },
    { "from": "notify", "to": "end" },
    { "from": "alert", "to": "end-error" }
  ]
}
```

Conditions are JSON too — composable with `and` / `or` / `not` and operators
like `eq`, `gt`, `in`, `contains`, `exists`, `starts_with`, `matches`:

```json
{
  "and": [
    { "path": "$.nodes.fetch.output.status", "eq": 200 },
    {
      "or": [
        { "path": "$.trigger.priority", "in": ["high", "urgent"] },
        { "path": "$.trigger.retry_count", "gt": 3 }
      ]
    }
  ]
}
```

## Official extensions

Extensions are crates that register tasks. Enable them via feature flags on
the `workflow-forge` facade (`util`, `data`, `http` are on by default; add
`tabular` and `sftp`, or use `full`).

| Namespace | Tasks                                           | Notes                                                 |
| --------- | ----------------------------------------------- | ----------------------------------------------------- |
| `util`    | `util.noop`, `util.log`, `util.delay`           | Debugging, testing, examples                          |
| `data`    | `data.transform`, `data.merge`, `data.template` | All data reshaping lives here                         |
| `http`    | `http.request`                                  | Methods, headers, query, JSON body, basic/bearer auth |
| `tabular` | `tabular.parse`, `tabular.write`                | CSV / XLSX ↔ JSON, via `$blob`                        |
| `sftp`    | `sftp.get`, `sftp.put`, `sftp.list`             | Streaming transfers, via `$blob`                      |

Large files never travel inline in the context: tasks exchange **blob
references** (`{ "$blob": "<id>", "name": "...", "size": ... }`) backed by a
per-execution `BlobStore` that is cleaned up when the run ends.

### Writing your own extension

A task is the unit of extension, and the fast path is a closure. With
`register_typed`, you write two Rust types and the engine **derives** their
JSON Schemas (via [`schemars`]) — input/output validation and the task catalog
come for free, no hand-written schema:

```rust
use serde::{Deserialize, Serialize};
use schemars::JsonSchema;

#[derive(Deserialize, JsonSchema)]
struct CreateShipmentIn { sku: String, qty: u32 }

#[derive(Serialize, JsonSchema)]
struct CreateShipmentOut { tracking: String }

registry.register_typed("acme.create_shipment", |ctx, input: CreateShipmentIn| async move {
    // `ctx` exposes per-execution resources (blobs, execution id)
    Ok(CreateShipmentOut { tracking: format!("{}-{}", input.sku, input.qty) })
});
```

For trivial JSON-in/JSON-out tasks, `register_fn` skips the types entirely:

```rust
registry.register_fn("util.echo", |_ctx, input| async move { Ok(input) });
```

When a task needs to hold state or dependencies (a DB pool, a configured HTTP
client) or read the full execution state, implement the `Task` trait directly —
the struct you register can carry whatever it needs:

```rust
use workflow_forge::prelude::*;
use async_trait::async_trait;

struct MyTask { manifest: TaskManifest /* + pools, clients, config… */ }

#[async_trait]
impl Task for MyTask {
    fn manifest(&self) -> &TaskManifest { &self.manifest }

    async fn execute(&self, _ctx: &WorkflowContext, input: WorkflowData) -> WorkflowResult {
        Ok(input) // your logic here
    }
}
```

Either way, `TaskRegistry::catalog()` exports every registered manifest as JSON
([extension schema](schemas/1.0/extension.schema.json)) — the foundation for
tooling and visual editors.

#### Validation: tolerant by default, strict on request

For `register_typed` tasks the engine validates the input against the derived
schema **before** the closure runs and the output **after**, and the typed
wrapper additionally deserializes/serializes your Rust types — two aligned
layers (`schemars` reads the same `serde` attributes the deserializer uses, so
the schema and the deserialization never disagree). Nested types and enums are
fully enforced: a derived schema's internal `$ref`/`$defs` are resolved and
applied, so an invalid sub-field is rejected with `TASK_INPUT_INVALID`.

By default validation is **tolerant**: extra, undeclared fields in the input
are accepted (neither `schemars` nor `serde` reject unknown keys). This is
usually what you want — forward-compatible inputs. When you need **strict**
validation that rejects unknown fields, add `#[serde(deny_unknown_fields)]` to
your input type; `schemars` honors it and emits `additionalProperties: false`,
keeping both layers strict and aligned:

```rust
#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)] // schema becomes additionalProperties: false
struct CreateShipmentIn { sku: String, qty: u32 }
```

[`schemars`]: https://docs.rs/schemars

## Spec & schemas

The execution contract is versioned independently from the crates:

- [`schemas/1.0/workflow.schema.json`](schemas/1.0/workflow.schema.json) —
  what a valid workflow definition looks like.
- [`schemas/1.0/extension.schema.json`](schemas/1.0/extension.schema.json) —
  what a task manifest / catalog looks like.

Design decisions and rationale live in [`DESIGN.md`](DESIGN.md).

## Workspace

```text
crates/
  core/                  workflow-forge-core  — spec types, executor, validation, BlobStore
  extensions/
    util/ data/ http/ tabular/ sftp/          — official extensions (one crate each)
  forge/                 workflow-forge       — facade with feature flags
```

Rules: extensions depend only on `core`, never on each other; every task ships
a manifest and integration tests against the real executor.

## Roadmap

- v1.x extensions: `compress`, `crypto`, `storage` (S3-compatible), `smtp`
- CLI runtime (`forge run workflow.json`)
- WASM extensions (installable without recompiling)
- Durable execution (event-sourced executor behind a storage trait)
- Visual editor (the graph model + schemas make it possible)

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
