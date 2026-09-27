# Changelog

## Unreleased — 0.1.0

First public release preparation. No earlier prototype version is claimed as a published release.

- Rust-only embedded workflow engine with host-owned execution, cancellation and shutdown.
- JSON Schema contracts, explicit bindings and pinned workflow/operation revisions.
- Structured decisions, error handlers, parallel groups, loops, subworkflows, timers and signals.
- Memory providers and optional local SQLite persistence with explicit recovery.
- Effect certainty, safe retry policies, reconciliation and bounded artifact storage.
- Optional outbound HTTP/JSON, file reads and CSV batches.
- Public extension contracts, provider conformance checks and executable integration examples.

Crate versions, workflow format (`forge.workflow/2`) and checkpoint format (3) are separate contracts.
