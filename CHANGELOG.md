# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
The workflow **spec** is versioned independently (`"spec": "1.0"`, published
under `schemas/1.0/`); a crate version bump does not imply a spec bump.

## [Unreleased]

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
