# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The workflow **spec** is versioned independently (`"spec": "1.0"`, published
under `schemas/1.0/`); a crate version bump does not imply a spec bump.

## [Unreleased]

### Added

- Low-friction task authoring on top of the `Task` trait:
  - `TaskRegistry::register_typed(id, closure)` / `TypedTask<In, Out>` — a typed
    async closure whose input/output JSON Schemas are **derived** from the Rust
    types via `schemars`; the engine validates input (before) and output (after)
    as it does for any task, and nested types/enums (`$ref`/`$defs`) are enforced.
  - `TaskRegistry::register_fn(id, closure)` / `FnTask` — a raw `WorkflowData`
    closure for trivial tasks, no schemas.
  - `TaskCtx` — a by-value view of per-execution resources (`execution_id`,
    `parent_execution_id`, `blobs`) handed to closure-based tasks.
- `testing` feature (`workflow_forge::testing` / `workflow_forge_core::testing`):
  `MockTask` (returning / failing / closure behaviors) and a `CallLog` handle to
  dry-run a workflow against mocked tasks and assert what it would have called —
  no network, filesystem or secrets.
- Runnable examples under `crates/forge/examples/` (`greet`, `routing`,
  `custom_tasks`), checked by CI.
- Idempotency keys for side-effecting tasks: `idempotency::key_for(value)`
  (stable, content-addressed UUID v5), the `util.idempotency_key` task
  (`{ value } -> { key }`), and `TaskCtx::idempotency_key`.
- Durable observer adapters: `TracingObserver` (emits events via `tracing`)
  and `JsonlObserver` (append-only JSON-lines audit log, to any writer or file).
- Execution-level limits via `WorkflowExecutor::run_with(trigger, RunOptions)`:
  a total `deadline` and a cooperative `CancellationToken` — **unlimited by
  default** (`run` imposes nothing). New error codes `EXECUTION_TIMEOUT` and
  `EXECUTION_CANCELLED`.
- Connection reuse at volume:
  - `http`: `register_with_client(registry, client)` to inject a tuned
    `reqwest::Client` (pool size, timeouts, proxy, TLS). The default already
    pools connections per host via one shared client.
  - `sftp`: `register_pooled(registry, max_idle_per_conn)` + `SftpPool` —
    reuses authenticated SSH sessions across calls (keyed by connection, with a
    liveness check and reconnect), opening a cheap SFTP channel per call.
- `forge` CLI (new crate `workflow-forge-cli`): `forge run workflow.json`
  (`--input`/stdin, `--timeout-ms`), `forge validate`, `forge catalog`.
- Crate publishing metadata across the workspace (`description`, `rust-version`,
  `keywords`, `categories`, `homepage`); MSRV declared as 1.85.
- Every official extension now publishes its error codes as documented
  constants under `<crate>::codes` (e.g.
  `workflow_forge_ext_http::codes::HTTP_STATUS_ERROR`), mirroring the core
  catalog so hosts can match on them without magic strings. Code **values**
  are unchanged; the `http` extension's blob errors now reference
  `workflow_forge_core::error::codes::BLOB_IO_ERROR` directly.
- **`loop` node** (additive spec 1.0 extension): bounded iteration of a task —
  the canonical pagination primitive ("fetch pages until `next` is null").
  First iteration runs with `input`; `while` (a standard condition) is
  evaluated after each iteration against the local document
  `{ input, output, index }`, and the `next` shape builds the following input
  from it. `max_iterations` is mandatory (`on_max: fail` errors with
  `LOOP_MAX_ITERATIONS_EXCEEDED`, `stop` finishes with what was collected);
  `collect: last|all` shapes the output; `retry`/`timeout_ms` apply per
  iteration and failures route through `on: error`/`on: panic`. New events
  `loop_iteration_completed/failed` fold into the report as
  `items_ok`/`items_failed`. Published schema updated.
- Retry `jitter` (additive spec field, default `false`): the actual wait
  becomes uniform in `[0, computed delay]` (full jitter), de-synchronizing
  retry waves against the same target after an outage. Reported delays in
  `task_attempt_failed` events reflect the real (jittered) wait.
- **`compress` extension** (`workflow-forge-ext-compress`, `compress` feature,
  in `full`): `compress.gzip` / `compress.gunzip` and `compress.zip` /
  `compress.unzip` over the `$blob` convention — the `.csv.gz` / `.zip` files
  that move over SFTP. Everything is streamed through temp files (bounded
  memory) with pure-Rust deflate (flate2/miniz_oxide + zip, no C deps).
- HTTP resilience: `http.request` gains `retry_on_status: [int]` (only the
  listed statuses fail and thus retry — e.g. `[429, 503]` — sparing permanent
  errors like 400/404), and any status failure now carries the response's
  `Retry-After` header (delta-seconds or HTTP-date) so retries wait at least
  that long. Backed by a new additive `WorkflowError::retry_after_ms` field:
  a general "wait at least this before retrying" hint the engine honors as a
  floor over the computed backoff.
- New validation rule (`GATEWAY_DUPLICATE_EDGE_LABEL`): an `exclusive`
  gateway with two outgoing edges sharing a label — or two branches pointing
  to the same label — is now rejected at build time. Previously the winning
  branch silently followed **all** matching edges concurrently (accidental
  fan-out from an exclusive).

### Fixed

- Conditions now compare integers exactly: `gt`/`gte`/`lt`/`lte` went through
  `f64`, losing precision above 2^53 (e.g. large numeric IDs).

### Changed

- Readability pass, no behavior change: `workflow-forge-ext-data` reorganized
  into one module per task (`transform`, `map`, `merge`, `template`, `cast`;
  public re-exports preserved), executor construction split into named helpers
  (`registry_with_inline_profiles`, `build_subworkflows`), and all in-code
  documentation unified in Spanish (the project's code-doc convention).

## [0.1.0] - 2026-06-10

First release. Declarative workflow engine defined, validated and extended
with JSON Schema + JSONPath, embeddable as a Rust library.

### Added

**Spec 1.0** (`schemas/1.0/`: `workflow`, `extension`, `profile` — validable
with any standard JSON Schema validator, no engine required)

- Explicit graph: `nodes` + `edges`, single `start`, one or more `end`.
- Node kinds: `start` (trigger schema + defaults), `end` (output mapping +
  schema), `task`, `foreach`, `gateway` (`exclusive`/`parallel`/`join`),
  `subworkflow`.
- JSONPath data wiring: global per-run context (`$.trigger`,
  `$.nodes.<id>.output`, `$.workflow`), literal escapes (`$$.`), strict
  missing-path errors in mappings.
- Condition mini-DSL (`eq`/`ne`/`gt`/`gte`/`lt`/`lte`/`in`/`contains`/
  `exists`/`is_null`/`starts_with`/`ends_with`/`matches` + `and`/`or`/`not`);
  missing paths evaluate to `false`.
- Per-node resilience: `retry` (exponential/linear/fixed backoff),
  `timeout_ms`, and **three routable exits per node** — success (default
  edge), `on: "error"` (operational failure after retries) and `on: "panic"`
  (bug in extension code: captured, never retried, no fallback to error).
- `foreach`: one task call per array element, with `concurrency`,
  `throttle_ms` and `on_item_error: fail|collect`.
- Task profiles: named reusable task instances (`extends` + `bind` shapes +
  own schemas), registered shared or inline (`tasks` section); secrets via
  `{"$secret": "X"}` and the `SecretProvider` trait.
- Sub-workflows: `kind: "subworkflow"` runs another workflow as a task
  (input → child trigger, child output → node output), resolved by name from
  the inline `workflows` section or a shared `WorkflowRegistry`; cycles and
  nesting depth >8 rejected at build time; blobs shared across the boundary.
- Binary convention `$blob` + `BlobStore` trait (v1: per-run temp dir).

**Core** (`workflow-forge-core`)

- `WorkflowExecutor`: run-to-completion in-memory execution, parallel
  branches with fail-fast joins, precompiled JSON Schema validation for
  triggers/results/task inputs/outputs, structural graph validation
  (reachability, cycles, gateway coherence) with accumulated errors.
- `WorkflowExecutor::builder()` for optional dependencies (secrets,
  `WorkflowRegistry`).
- Observability: `ExecutionObserver` + serializable `ExecutionEvent`s (total
  `seq` order across the whole sub-workflow tree, `parent_execution_id` on
  child events), `InMemoryHistory` → `ExecutionReport` (plus `report_for` /
  `executions` for sub-workflow detail).
- `TaskRegistry` with exportable JSON catalog of task manifests.

**Official extensions** (each a `workflow-forge-ext-*` crate; facade
`workflow-forge` re-exports core and registers them via feature flags)

- `http.request`: all request bodies — `body` (JSON), `form` (urlencoded),
  `text` (raw), `body_blob` (streamed binary), `multipart` (text/json/blob
  parts, streamed) — mutually exclusive; `response_body: auto|text|blob`
  (streamed binary downloads into the blob store); auth basic/bearer,
  custom headers, `fail_on_error_status`.
- `data.transform` / `data.map` / `data.merge` / `data.template` /
  `data.cast` (per-field declarative conversions: dates, numbers, int, bool,
  string, trim, upper, lower, replace, defaults; `on_invalid:
  fail|null|collect`).
- `tabular.parse` / `tabular.write`: CSV and XLSX ↔ JSON via `$blob`.
- `sftp.get` / `sftp.put` / `sftp.list` (verified against a real SFTP server
  in CI).
- `util.delay` / `util.log` / `util.noop`.

**Project**

- Dual license MIT / Apache-2.0, CI (fmt + clippy + tests + SFTP service),
  README, DESIGN.md (decision log) and EXAMPLES.md (13 runnable examples,
  validated against the published schema by a test).

[Unreleased]: https://github.com/rafael-arreola/workflow-forge/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/rafael-arreola/workflow-forge/releases/tag/v0.1.0
