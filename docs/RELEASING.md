# Release checks

The release consists of five library crates. `examples/host`, reference modules, authoring
consumers and the manual example workspace are repository-only validation tools (`publish = false`).
Package allowlists include source, self-contained provider tests, README and both licenses; runtime SQL migrations stay included.

## Verify the checkout

```sh
./scripts/check.sh
```

The script checks formatting, Clippy, all workspace tests/targets and doctests, manual tests,
API documentation and executable proofs of concept. It fails at the first unsuccessful command.
Proofs of concept use local fixtures and temporary databases; no real integrations are assumed.

For a focused rerun, use the individual commands in the script or run
`python3 scripts/run-examples.py`. Avoid rerunning performance matrices when a smoke-sized
measurement proves the example still works. Publishable source must also compile without
default features.

## Package and inspect

```sh
cargo package --workspace \
  --exclude workflow-forge-examples \
  --exclude workflow-forge-reference-module \
  --exclude workflow-forge-authoring-example \
  --all-features --locked
```

When verifying intentional uncommitted changes, add `--allow-dirty`; this is not permission to
publish an unreviewed tree. Cargo packages workspace dependencies into a temporary local registry
and verifies the extracted crates. This proves the archives compile independently of checkout
paths; it does not upload anything. Inspect `target/package/*.crate` and `cargo package --list`
to confirm contents. Do not use `--no-verify` as evidence of release readiness.

## Publish only after approval

Confirm package versions, ownership/name availability, README links and the intended release
commit immediately before upload. Release preparation does not reserve crate names or authorize
publication. The first planned version is 0.1.0; no prototype tag is treated as an existing release.

Publish in dependency order, waiting for each dependency to become available:

1. `workflow-forge-protocol`
2. `workflow-forge-conformance`
3. `workflow-forge-engine`
4. `workflow-forge-modules`
5. `workflow-forge`

Conformance precedes modules because the latter uses it in development checks. Engine and
conformance otherwise independently depend on protocol. Run `cargo publish -p PACKAGE --dry-run`
before each authorized upload, then `cargo publish -p PACKAGE`. Registry credentials stay outside
the repository. Tag and release notes should describe the actual published version, not historical
unreleased prototypes.

See the official [Cargo publishing guide](https://doc.rust-lang.org/cargo/reference/publishing.html)
for registry metadata, package checks and upload behavior.

## Verified preparation — 2026-09-27

Local verification on macOS with the pinned Rust toolchain:

- Workspace tests and doctests: 124 passed; example-target regression: 1 passed; none ignored.
- Manual workspace tests compiled; all ten executable manual examples passed their assertions.
- Customer, authoring, inventory, durable receipt and durable signal proofs of concept passed.
- Sequence, saturation and retention probes passed with memory and SQLite providers, using the
  probes' minimum sample/cycle requirements. These runs establish behavior, not production capacity.
- Formatting, Clippy with warnings denied, no-default-feature compilation and rustdoc with
  warnings denied passed. The documented workflow example passed schema validation.
- All five final archives packaged and compiled with no warnings. Archive inspection confirmed
  source equality, licenses, required SQL migrations and normalized dependencies without local paths.
- 108 local documentation references resolved; all seven manual diagrams rendered at desktop and
  mobile sizes without page overflow. Desktop/mobile captures were visually reviewed.

The original measurement smoke command requested fewer samples than the probe accepts. The
command and documentation were corrected to 1,000 samples, and the measurement group passed.
No library test was removed to obtain a passing result. Publication was not performed; registry
name availability must be checked again at upload time.
