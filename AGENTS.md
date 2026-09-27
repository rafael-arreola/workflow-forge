# Workflow Forge — Agent Instructions

## Purpose and Scope

Develop and extend an agnostic declarative workflow engine, embedded as a **library within a Rust application**. JSON definitions connect operations through JSON Schema contracts. The host selects implementations, resources, and transports.

- This product is a Rust-only embedded library. Do not add a server, HTTP execution API, CLI client, scheduler daemon, or support surface for other languages. The host owns all inbound transports. HTTP request operations are outbound capabilities, not services.
- Implement modules, graph validation, execution, and transformations using the existing public types and traits, without requiring step-by-step guidance.
- Keep the engine independent of providers, business rules, and any future graphical interface. Authoring metadata supports building a separate editor.
- The project has no public release yet. Breaking changes are allowed, but must explicitly update affected contracts, consumers, and documentation.
- Defer integrations with real systems until the user chooses to connect them. Fixtures do not establish production acceptance or capacity.

## Sources of Truth

Read only the documents relevant to the change:

| Document | Responsibility |
|---|---|
| [PRD](docs/PRD.md) | Product, scope, and user decisions. |
| [ARCHITECTURE](docs/ARCHITECTURE.md) | Boundaries, communication, composition, and lifecycle. |
| [TDD](docs/TDD.md) and [CONTRACTS](docs/CONTRACTS.md) | Technical design, format, guarantees, and invariants. |
| [PATTERNS](docs/PATTERNS.md) | Patterns, examples, and extension rules. |
| [ROADMAP](docs/ROADMAP.md) and [PROJECT](docs/PROJECT.md) | Phases, evidence, limitations, and pending work. |
| [Manual](manual/index.html) | Practical guidance for library integrators. |

Current user instructions take precedence over historical decisions. Distinguish implemented, verified, and proposed behavior. Do not reopen deferred work or repeat completed analysis without a concrete need.

<!-- codebase-memory-mcp:start -->
For structural codebase exploration, use the installed `codebase-memory` skill.
<!-- codebase-memory-mcp:end -->

Consult the index and verify coverage of files used as evidence. If coverage is incomplete or tools are unavailable, read the relevant source and state that limitation. Prefer `rg` and `rg --files` for text and file searches.

## Crate Boundaries

| Path | Responsibility |
|---|---|
| `crates/protocol` | Public types, traits, descriptors, and contracts. |
| `crates/engine` | Preparation, validation, coordination, and lifecycle; consumes ports. |
| `crates/modules` | Official operation and provider implementations. |
| `crates/forge` | The `workflow-forge` facade and default composition. |
| `crates/conformance` | Reusable public contract checks. |
| `examples/reference-module` | Reference extension using public contracts. |
| `examples/authoring-client` | Consumer of catalog, validation, and authoring capabilities. |

Extensions depend on the protocol and their own dependencies; they must not import engine internals. A logical or virtual module groups capabilities in an `OperationBundle`: compiled Rust within the process, not dynamic loading or plugin isolation.

## Composition and Patterns

- **Builder:** register modules and providers before `build()`. It produces an inactive `EngineAssembly`; setters must not start tasks or effects.
- **Facade:** the host retains `EngineRuntime`, obtains `WorkflowApplication` through `application()`, and shares cloned handles. Create the instance at startup and call `shutdown` when closing, including on error paths.
- **Adapter:** translate SDKs, inputs, outputs, and errors into public contracts. An operation does not choose which node runs next.
- **Strategy:** vary bounded policies, such as backoff, without bypassing repetition safety or persistence invariants.
- **Command and State:** preserve invocation identities and explicit transitions; distinguish a new attempt from the same logical effect.
- **Composite:** reuse bodies and subworkflows through engine contracts.
- **Observer and Decorator:** observe or instrument while preserving results, errors, and identities; do not transfer execution control to them.
- Prefer simple factory functions. Do not introduce additional traits, hierarchies, or patterns without a real variation that justifies them.

## Implementation Invariants

- Prefer `WorkflowApplication::execute` with a host cancellation token for owned calls. Dropping its future requests cancellation; the runtime supervises acceptance and cleanup. `start`/`wait` is an advanced host-owned API, not the default integration recipe.
- Boot must not resume old work implicitly. Default `RecoveryPolicy::RejectUnfinished` fails when pending work exists; only the host can explicitly choose `Resume`.
- Workflows choose business routes. Operations return schema-conforming values or stable structured errors. Use `try` for exact error handlers and a required fallback; wrap the root body for a general handler. Preserve cancellation, deadlines, quotas, and uncertain effects above error handlers.
- Keep preparation, acceptance, and completion separate. `wait` may return a blocked run; check its state before assuming success or requesting output.
- Pin operation and workflow revisions. Update schemas, validation, and examples when a public contract changes.
- Preserve the distinction between missing values, `null`, and literals. Edges determine order; bindings construct data.
- Declare effects, repetition safety, and error certainty. A write timeout does not prove that the effect did not occur; do not blindly retry uncertain effects.
- Preserve the deduplication, concurrency, recovery, and limits defined by the contracts. Do not present memory as durable storage or local SQLite as distributed coordination.
- Reserve the wait before requesting an external response. Authentication and transport adaptation belong to the host.
- Deterministic control and bounded execution are distinct guarantees. External I/O may vary. Rust extensions must cooperate with cancellation, avoid blocking executor threads, and never leave unowned tasks; timeouts cannot forcibly stop arbitrary in-process code.
- Bound data, artifacts, concurrency, and retention. Do not promise throughput or memory usage without evidence for an identified workload and environment.

## Working Practices and Verification

- Make focused changes and preserve others' work. Do not include refactors, new dependencies, commits, or publications outside the authorized scope.
- Use the Rust version pinned in [rust-toolchain.toml](rust-toolchain.toml) and the [workspace configuration](Cargo.toml).
- Run **essential tests and edge cases for the changed behavior**. Select existing tests for affected crates or integrations and add regressions when they provide useful evidence.
- For Rust changes, check formatting with `cargo fmt --all --check` and run Clippy and tests for affected packages, targets, and features. Expand to the workspace when shared contract or dependency changes justify it.
- For documentation or visual changes only, check links, affected snippets, and presentation. Do not rerun the engine suite or benchmarks for text or color changes.
- Do not repeat passing tests unless new changes, failures, or relevant uncertainty justify it. Do not resume extensive capacity matrices without a concrete need.
- Record commands actually executed, results, and limitations. A description, example, or successful compilation does not verify every guarantee.
- Communicate in Spanish, briefly explaining what changed, how it was verified, and what remains pending.

## Manual and Diagrams

- Keep the manual in `manual/index.html` and chapters in `manual/*.html`: Spanish, Tailwind via CDN, simple styling, and a progression from basic to advanced topics.
- Keep runnable examples in `manual/ejemplos`, which has an independent workspace. Verify only examples affected by API or behavior changes.
- Use Archify for diagrams and follow the [local maintenance guide](manual/diagramas/README.md). Diagram specifications are not executable engine workflows.
- **The manual palette overrides the skill's default colors:** white, black, and gray for nodes, arrows, highlights, and animations; light by default, with an optional neutral dark theme.
- Centralize customization in `manual/diagramas/manual-theme.css` and `apply-theme.py`. When regenerating: deliver with Archify, apply the palette, check the final HTML, and update affected SVG previews and evidence.
- Preserve provenance: `*.delivery.json` describes the original Archify output; `*.theme.json`, `*.check.json`, and browser evidence describe the customized HTML. Update `verificacion.json` without claiming reviews that were not performed.
- Documentation must teach the actual API through short snippets and complete references. Do not invent type names, methods, or guarantees.
