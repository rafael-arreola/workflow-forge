# Workflow and execution contracts

These contracts describe the current embedded engine. The public Rust types in
[protocol](../crates/protocol/src/lib.rs) and the
[workflow schema](../schemas/2/workflow.schema.json) define the serialized shapes.
The schema validates structure; preparation also validates graph relationships,
operation revisions, bindings, access and capabilities.

## Definitions and revisions

A workflow declares `format: "forge.workflow/2"`, `schema_dialect`, `id`, `revision`,
`input_schema`, `output_schema`, `entry`, `nodes`, `edges` and an `output` binding.
Optional `presentation` metadata survives authoring round trips but does not affect execution.
Operation references pin `id`, `contract` and `implementation`; there is no `latest` lookup.

Each body is a nonempty chain: one entry without a predecessor, one successor per
node except the last, and all nodes reachable without cycles. Array order and visual
coordinates do not establish execution order. Branching and concurrency use nested
control instructions, not ambiguous graph edges. Unknown fields and duplicate IDs/edges
are rejected. Cross-body edges are invalid.

A workflow revision is immutable within a composition. Semantic normalization sorts
nodes/edges and excludes presentation metadata; it preserves data arrays, types and values.
Changing operation behavior requires a new implementation revision. Plans belong to
one composition and must be prepared again in another instance.

Complete minimal definition:

```json
{
  "format": "forge.workflow/2",
  "id": "reference.echo",
  "revision": "r1",
  "schema_dialect": "https://json-schema.org/draft/2020-12/schema",
  "input_schema": {
    "type": "object",
    "required": ["name"],
    "properties": {"name": {"type": "string"}},
    "additionalProperties": false
  },
  "output_schema": {"type": "string"},
  "entry": "echo",
  "nodes": [{
    "id": "echo",
    "kind": "operation",
    "operation": {"id": "forge.data.identity", "contract": "1", "implementation": "r1"},
    "config": {},
    "input": {"select": {"source": "input", "pointer": "/name"}}
  }],
  "edges": [],
  "output": {"select": {"source": "node", "node": "echo", "pointer": ""}}
}
```

Input `{"name":"Ada"}` produces `"Ada"`. The identity operation accepts arbitrary JSON;
the workflow still validates its own input and output schemas.

## Bindings and schemas

A binding contains exactly one constructor:

| Constructor | Meaning |
|---|---|
| `{"literal": value}` | A literal JSON value, with no expression interpretation. |
| `{"select":{"source":"input","pointer":"/name"}}` | Read the current body's input. |
| `{"select":{"source":"node","node":"previous","pointer":""}}` | Read a confirmed local predecessor output. |
| `{"object":{"field": binding}}` | Construct an object field by field. |
| `{"array":[binding, binding]}` | Construct an array in declared order. |

Pointers use JSON Pointer escapes `~0` and `~1`; an empty pointer selects the whole value.
A `select.fallback` binding handles a missing value, not a present `null` or an incompatible
type. Invalid producer references fail preparation. A selected array remains one value;
only `foreach` introduces iteration. Nested scopes receive parent data explicitly.
Bindings perform no I/O, secret lookup, script evaluation, clock access or randomness.

The supported JSON Schema dialect is 2020-12 with a bounded profile. Validation does not
coerce types or insert defaults. `format` is an annotation. References resolve only against
registered, revisioned resources; an unknown URI never triggers a network download.
Recursive references, `$dynamicRef`, named anchors and nested `$id` are unsupported.
A resource's root `$id`, when present, must match its registered URI. Schema expansion,
regex compilation and evaluation have explicit budgets; this is not unrestricted support
for every construction in the dialect.

Validation occurs before admission for workflow input, before dispatch for operation input,
before confirmation for operation output and before success for workflow output. An invalid
output after a write does not erase evidence that the write may have happened.

## Structured control

| Instruction | Input, behavior and result |
|---|---|
| `operation` | Invokes a pinned operation with validated config/input and optional retry policy. |
| `decision` | Ordered boolean cases; first true wins, otherwise optional fallback. Returns `{selected, output}`; no match without fallback is `control.no_match`. |
| `try` | Protected body, exact error-code handlers and required fallback. See [embedding](EMBEDDING.md#outcomes-and-error-handlers). |
| `parallel` | Named branches, bounded concurrency, `join: "all"`. Results keyed by branch ID. |
| `foreach` | Array selected by `items`; body receives `{item, index, context}`. Results retain input index order. |
| `loop` | Condition/body receive `{state, iteration}`. Body output replaces the complete state. `max_iterations` and `on_limit: fail/return_last` bound repetition. |
| `subworkflow` | A registered workflow under an exact revision, with its own scope inside the root run. |
| `timer` | Confirms an absolute wake-up time and returns its input after expiry. |
| `await_signal` | Reserves an identified wait before an optional start operation; returns `{start, signal}`. |

Bodies contain `entry`, `nodes`, `edges` and `output`. Register reusable definitions before
`build`; `BootOptions.definitions` lists mandatory definitions to prepare before readiness.
Subworkflow cycles and missing revisions fail preparation. No scope reads a sibling's
internal node directly; it reads the enclosing control's confirmed output instead.

Group results are `{status: "succeeded", output}` or `{status: "failed", error}`;
foreach also includes `index`. `fail_fast` stops new admissions after a known failure and
cancels/classifies active children. `collect` gathers known failures. Uncertain effects block
the group/run and never become ordinary collected failures. Independent children may settle;
confirmed results are reused after resolution. Output order does not guarantee effect order.

## Operations, attempts and effects

`Operation: Send + Sync` exposes an immutable descriptor and a boxed `Send` execution future.
The descriptor defines schemas, revision, required resources, effect and repetition safety.
`Invocation` carries logical invocation identity, attempt identity, config, input and an
optional stable effect key. `OperationContext` supplies authorized resources, deadline and
cancellation; it does not expose the execution store or scheduler.

Effects are `Pure`, `Read` or `Write`; repetition is `Safe`, `Keyed` or `Unsafe`.
Pure operations must be safe. A keyed destination must actually guarantee deduplication.
Reads may observe different values on repetition. Errors report `code`, `class`, `certainty`
and a safe message. Certainty is `NotApplied`, `Applied` or `Unknown`.

Logical identity includes the root run, nested scope path, local node and iteration/index.
Path segments escape separators. Each retry gets a new attempt identity while preserving
the logical identity and effect key. Identical array elements are still separate invocations.

| Observed outcome | Required action |
|---|---|
| Valid output | Confirm once and enable continuation. |
| Invalid input or mapping before dispatch | Known failure; no effect or automatic validation retry. |
| Eligible transient/resource error with definite `NotApplied` | Retry only within attempt/deadline budgets. |
| Permanent rejection with `NotApplied` | Fail without retry. |
| Unknown write with safe/keyed repetition | Retry only when eligible, preserving the effect key; otherwise block. |
| Unknown write with unsafe repetition | Block without blindly repeating. |
| Applied write without valid output | Block for resolution; schema failure is not retry authorization. |
| Cancellation, timeout or panic after dispatching a write | Preserve possible remote effect. |

Certainty accumulates across attempts: a later `NotApplied` does not erase an earlier unknown
write. Late results cannot overwrite authorized outputs. Their collection window is bounded;
discarding a local future does not prove cancellation of the remote effect.

`EffectInspector` gathers evidence without modifying execution state. `reconcile` applies an
identified command with expected run revision and observed attempt. Decisions confirm applied,
confirm not applied, record inconclusive evidence, or stop tracking. Confirming no effect must
also assert that the attempt can no longer complete. Confirming application validates its output
against the pinned schema. Invalid evidence/output cannot silently unblock execution.

Resolution, audit and receipt commit atomically. Repeating the same command from the same actor
returns its receipt; conflicting content or stale revisions fail. `StopTracking` requires a
separate permission and preserves visible unresolved effects; it never means success.

## Ownership, admission and persistence

The host owns `EngineRuntime` and must call `shutdown`. Prefer owned `execute` calls; use
`start/wait/cancel` for explicit run tracking. Acceptance is not completion. `wait` may return
a blocked run: check `RunState` before reading output or reporting success.

`StartOptions` controls requested durability, timeout, receipt key and declared artifacts.
A receipt key deduplicates a complete normalized request within a retention window. Different
content under the same key conflicts. The receipt can outlive its result; capacity pressure
must not silently invalidate a promised deduplication window. After expiry, the same key may
create another run. `execute` rejects receipt keys because an exclusive caller cannot cancel
another caller's shared execution.

Memory providers are ephemeral. SQLite is local, with one exclusive coordinator and no
cross-host worker coordination. It commits state transitions using revision checks and retains
recovery packages with workflow/subworkflow definitions, schemas, operation revisions and
resource requirements. Recovery uses those pinned dependencies, not the current authoring
catalog. Missing or incompatible dependencies fail explicitly.

Default boot rejects unfinished work with `recovery.required`. Only explicit
`RecoveryPolicy::Resume` resumes it under its original deadlines. Recovery never grants unlimited
attempts or repeats confirmed effects. Unknown writes remain subject to reconciliation.

SQLite uses a sidecar ownership lock and WAL. The host must not rename/delete active database
or lock files, or bypass ownership through hardlinks. Unknown schema/checkpoint versions fail.
The current checkpoint format is 3; explicit migrations preserve compatible older persisted
state transactionally. These migrations are storage safety mechanisms, not legacy public APIs.

## Artifacts

Large bytes travel through `ArtifactRef`, not unbounded JSON values. The host declares existing
references in `StartOptions.artifacts`; arbitrary JSON resembling a reference grants no access.
Operations read declared inputs or artifacts created by their run. Passing a reference from
another run requires explicitly declaring it at admission.

Durable execution and artifact providers must share a nonempty coordination domain. With SQLite,
install the same provider instance behind both ports. Publication, run ownership and receipts
must coordinate atomically. Two independent providers reporting `durable: true` are insufficient.

Artifact streams recheck ownership. Writes publish a reference only after all bytes are stored;
partial staging data is not readable. A ready reference whose receipt was lost is not discarded
as an incomplete write. Linked artifacts remain while an owning run is retained; removing one
owner does not remove another owner's data. Host uploads without owners have bounded grace/GC.
Quotas cover bytes, references, metadata and stream chunks.

## Signals and waiting

`await_signal` declares correlation, timeout, payload schema and an optional start operation.
Reserve before dispatching external work so an immediate callback can be accepted. A signal can
arrive while start is running, but is consumed only after start is confirmed. A callback does not
resolve an unknown start effect. Without start, the output's `start` value is null.

`SignalCommand` identifies run, wait, message, correlation, payload and optional artifacts.
The host supplies authenticated scope/actor/permissions. Knowing a correlation alone grants no
access. An unknown reservation is `wait.not_found`; the engine does not invent an early inbox.
Each wait accepts one delivery. Identical delivery returns its receipt; changed actor/content
or a second message conflicts. Invalid delivery leaves the reservation available.

Delivery/receipt and attached artifact ownership commit before acknowledgment. Consumption and
confirmation of the wait node commit together. Signal, timeout and cancellation compete through
the same state transitions. A previously accepted delivery preserves its receipt; new delivery
cannot reopen an expired or terminal run. Timeout starts at reservation and does not extend the
run deadline. Open, consumed, expired and closed waits all count against retention limits.

Suspension is distinct from failure. It persists control progress and releases active capacity
when no work is runnable; no task needs to remain blocked on a callback for the entire wait.
Wake-up projections make accepted signals and due timers discoverable even after a lost in-memory
notification. Uncertain effects retain their block despite an available signal.

## Limits and trust

The public [Limits](../crates/protocol/src/execution.rs) type is the source for configured
budgets and defaults: definitions, schemas, values, retained run data, admission, attempts,
scopes, branches, iterations, activations, waits, artifacts and retention. Increasing one limit
does not increase all dependent budgets. Plan/value byte accounting is not a bound on process RSS.

Compiled Rust extensions are trusted in-process code, not a sandbox. They must cooperate with
cancellation and avoid blocking executor threads or leaving unowned tasks. The host authorizes
preparation, execution, reads, cancellation, signals and reconciliation. Secrets are resolved
through resource grants and never embedded in exported workflow definitions.

Use the [conformance crate](../crates/conformance/src/lib.rs) for reusable provider checks and
[host integration tests](../examples/host/tests) for concurrency, failure and recovery scenarios.
