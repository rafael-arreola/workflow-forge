# Embedding Workflow Forge

Workflow Forge runs inside a Rust host. The host owns resources, incoming transports,
authentication and application lifetime. Outbound connectors are operations; the engine
does not start a server, install process signal handlers or run a standalone daemon.

## Startup and shutdown

```rust
// Fragment: builder, access, definition, input and cancellation belong to the host.
let runtime = EngineRuntime::boot(builder.build()?, BootOptions::default()).await?;
let app = runtime.application();
let result = async {
    let plan = app.prepare(access.clone(), definition).await?;
    app.execute(access, StartRunRequest::new(plan, input), cancellation).await
}.await;
let shutdown = runtime.shutdown(ShutdownOptions::default()).await;
// Inspect both results; always close, including after preparation/execution errors.
```

Build once, retain the runtime, share cloned application handles and reuse prepared plans
within that composition. Stop accepting host input before shutdown and inspect `forced` and
`pending` in its report. Do not create an engine per request or shut down Tokio before cleanup.

`execute` returns a final JSON value or structured error, including a blocked result requiring
intervention. It needs Start, Read and Cancel permissions and accepts a host `CancellationToken`.
Canceling the token or dropping the future requests cancellation. Acceptance and cleanup remain
supervised so dropping during admission does not abandon a store transaction halfway through.
Cancellation does not undo remote effects or forcibly stop arbitrary blocking Rust code.

`StartOptions.timeout_ms` is bounded by `Limits.run_timeout_ms`; attempts, retries, groups and
waits retain their own budgets. Use child tokens for independent calls under a host-wide token.
Use `start/wait/cancel` when the host needs explicit run identity, shared receipts or signal
correlation. Dropping a `start` caller does not cancel its accepted run. Check the state returned
by `wait`; blocked is not successful completion. `execute` intentionally rejects receipt keys.

## Recovery is explicit

Default boot uses `RecoveryPolicy::RejectUnfinished`: pending work produces `recovery.required`,
releases store ownership and executes no operations. It neither deletes nor silently resumes
that work. The host may choose `BootOptions { recovery: RecoveryPolicy::Resume, ..Default::default() }`.
Resume preserves pinned revisions, effect evidence, consumed attempts and original deadlines.
Persisting audit/results does not imply permission to resume business work automatically.

## Outcomes and error handlers

Operations return schema-conforming values or `OperationError { code, class, certainty, message }`.
Workflows choose business routes based on data and stable codes, not message text.
`forge.data.equals` compares `{left, right}` using structural JSON equality and returns a boolean.
A decision selects the first true case; no matching case without fallback is `control.no_match`.

A `try` protects a body, has optional exact-code `catches` and requires a named `fallback`.
Handlers receive `{"input": <try input>, "error": <ForgeError>}`. Matching uses the first
diagnostic's operation error code, or its engine code if no operation error exists. Handler IDs
and codes must be unique. Protect one operation for a local handler or the root body for a
workflow-wide handler. A handler failure propagates outward, never into the same handler again.
Selection and error are persisted before executing the handler so recovery does not repeat the
failed protected body.

The result is `{"outcome":"success","output":...}` or
`{"outcome":"handled","handler":"id","output":...}`. Select `/output` to expose a common
business result. See the [complete outcome example](../examples/host/examples/v2_outcomes.rs)
and [definition](../examples/workflows/response_routing.v2.json).

Infrastructure failures, suspension, cancellation, limits, uncertain effects and operation
classes Internal/Resource/Cancelled bypass handlers. Only operation errors with `NotApplied`
certainty are catchable. Global deadline and cancellation are checked before selecting/executing
a handler. Eligible operation retries resolve before the failure reaches `try`. Without a
handler, the library returns the structured error to the host.

## Outbound HTTP

`forge.http.<profile>` contract 2 returns `{status, body}` for valid JSON responses, including
4xx/5xx; 204 returns a null body. The workflow decides whether 404 means absence or failure,
what to do about 429, and how to report 500. Status does not enable hidden retries.
Transport, content type, invalid JSON and quota failures return structured errors. A write
failure after dispatch preserves unknown effect certainty. See [connectors](INTEGRATIONS.md).

## Determinism and scope

Pinned revisions and the same observed outcomes follow explicit control rules. This does not
promise constant external data, identical parallel completion order or identical timing.
Bounded execution is a separate guarantee enforced through budgets and cooperative modules.
Extensions are compiled Rust. A future editor can generate the same JSON definitions from
catalog/schema metadata; it does not require a different engine or an embedded code interpreter.
