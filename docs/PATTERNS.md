# Extension design patterns

Use a pattern when it addresses a real variation. A small factory function is preferable to
another trait hierarchy when only construction varies. Extensions depend on public protocol
contracts, not the engine's compiler, coordinator or persistence internals.

| Pattern | Application | Boundary to preserve |
|---|---|---|
| Builder | Register modules/providers before `build` | Setters do not execute business work. |
| Facade | Host shares `WorkflowApplication` and domain wrappers | Host retains and closes `EngineRuntime`. |
| Adapter | Wrap an existing SDK/function as `Operation` | Translate values/errors; do not select successors. |
| Strategy | Replace bounded backoff behavior | Cannot bypass repetition or deadline safety. |
| Command | Identified starts, signals and reconciliation | Preserve actor, identity, expected revision and duplicate semantics. |
| State | Explicit run/invocation/wait transitions | Unknown effects are not ordinary failure or success. |
| Composite | Nested bodies and pinned subworkflows | Preserve scope and root-run budgets. |
| Observer | Observe execution events | Never control scheduling or mutate results. |
| Decorator | Instrument a port/operation transparently | Preserve descriptor, context, identities, effect key and error certainty. |

## Compose once

```rust
// Fragment: bundle and provider are created by the host.
let mut builder = WorkflowBuilder::standard().execution_store(provider);
builder.register_bundle(bundle)?;
let assembly = builder.build()?;
let runtime = EngineRuntime::boot(assembly, BootOptions::default()).await?;
let app = runtime.application();
// Share cloned handles; retain runtime and call shutdown on every exit path.
```

The default builder uses the same ports available to third-party hosts. A factory can return an
`OperationBundle` with several related operations and inspectors. A namespace groups capabilities;
it does not create a process, a virtual machine, authorization or plugin isolation.

## Adapt existing code

An operation struct holds immutable configuration and reusable clients. Its descriptor declares
config/input/output schemas, exact revision, resources and real effect guarantees. Its execute
method maps `Invocation` data into the existing API and maps the result into `OperationOutput`
or `OperationError`. The [reference module](../examples/reference-module/src/lib.rs) and its
[text adapter](../examples/reference-module/src/text.rs) are complete implementations.

Do not mark an HTTP write Pure/Safe because the example you copied is a string transformation.
Use Keyed only when the destination provides the required deduplication guarantee. Declare
Unknown if a dispatched write may have happened. Cancellation cannot authorize a blind retry.

## Keep workflow policy declarative

Return facts from operations. Use decision for result data, try for eligible error codes and
parallel/foreach for explicitly bounded concurrency. A generic error handler wraps the root
body; local handlers wrap the relevant operation/body. Avoid one operation that secretly runs
an entire unbounded workflow. Use a subworkflow when a reusable body needs its own revision.

## Version and test extensions

1. Give operations stable names and immutable contract/implementation revisions.
2. Change implementation revision whenever behavior or configuration-derived identity changes.
3. Preserve revisions required by retained recovery packages.
4. Validate descriptor/config examples and error/output schemas.
5. Test cancellation, quotas, effect uncertainty and failure boundaries relevant to the adapter.
6. For providers, run conformance plus backend-specific concurrency and durability tests.

Avoid global service locators, runtime mutable catalogs, hot unloading, hidden spawned tasks and
string-based executable expressions. These introduce lifetimes and authority beyond the declared
contracts. Refer to [architecture](ARCHITECTURE.md) for ownership and communication boundaries.
