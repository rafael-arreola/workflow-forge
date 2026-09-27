# Executable examples

Run commands from the repository root. All examples use public contracts and local fixtures;
no external account or deployed engine service is needed. Repository-only host examples live
in `workflow-forge-examples`, a non-published package separate from the release crates.

| Example | Command suffix for `cargo run -p workflow-forge-examples` | Demonstrates |
|---|---|---|
| Customer lookup | `--example v2_customer` | Normalize input, call an injected directory and return `{customer:"C-9",active:true}`. |
| Outcome routing | `--example v2_outcomes` | Classify response data and handle unexpected input with try/fallback. |
| Durable receipt | `--features sqlite --example v2_sqlite -- DATABASE` | Local SQLite acceptance and deduplication. |
| Inventory import | `--example v2_inventory -- 100 3` | Bounded CSV batches, business writes and correlated JSONL reports. |
| Durable signal | `--features sqlite --example v2_signal -- DATABASE start` then `... DATABASE signal` | Reserve, close, explicitly recover and deliver a signal within five minutes. |

Use a fresh database path for a new demonstration and retain it between the two signal commands.
For receipt deduplication, repeat the same command/input/key within its retention window.
The examples retain the `v2_` names to identify their workflow-format fixtures, not a second API.
All Rust imports use the root/prelude facade.

## Authoring and external modules

```sh
cargo run -p workflow-forge-authoring-example --example round_trip
```

The [authoring consumer](examples/authoring-client/src/lib.rs) builds a workflow from catalog
metadata, round-trips presentation data, prepares it through the public API and checks `ID-42`.
The [reference module](examples/reference-module/src/lib.rs) depends only on protocol contracts.
Its [inventory module](examples/reference-module/src/inventory/mod.rs) keeps domain rules out
of the engine. Workflow definitions live in [examples/workflows](examples/workflows).

## Progressive manual examples

```sh
cargo run --manifest-path manual/ejemplos/Cargo.toml --locked --bin inicio
```

Replace the binary name with:

| Binary | Scenario |
|---|---|
| `inicio` | One input → trim → output, with explicit shutdown. |
| `sistema` | Shared application facade, reused plan and bounded concurrent calls. |
| `cancelacion` | Host cancellation and run deadline, checking exact error codes. |
| `modulos` | Register a protocol-only extension and connect two operations. |
| `respuestas` | 200/404/429/other status, missing field and generic fallback. |
| `importacion` | Foreach + subworkflow + extension, empty batch and partial failure. |
| `controles` | Decision, parallel, foreach, loop, subworkflow and timer. |
| `artefactos` | Publish, declare and read bounded bytes by artifact reference. |
| `durable` | SQLite receipt; pass `-- DATABASE RECEIPT_KEY`. |
| `senales` | Reserve and deliver a signal inside one host. |

Existing fixture names and JSON field names remain stable; they are sample business data, not
engine keywords. The [manual](manual/index.html) explains input/output and host responsibilities.

## Measurement probes

These programs verify their results while reporting timings and retention. The sequence probe
requires at least 1,000 samples plus warmup; the retention probe uses at least ten cycles. Their output is
specific to the machine, workload and provider; it is not a production capacity claim.

```sh
cargo run --release -p workflow-forge-examples --example v2_measure -- 1 1024 1000 2 --terminal-runs 16
cargo run --release -p workflow-forge-examples --example v2_capacity -- saturation 2
cargo run --release -p workflow-forge-examples --example v2_capacity -- release 2 1024
```

Both probes support `--sqlite NEW_DATABASE` with the sqlite feature. Each measurement needs a
fresh database. Release retention probes query process RSS using `ps` on macOS/Linux. Artificial
latency used to exercise saturation is not the maximum throughput of the engine.

`python3 scripts/run-examples.py` executes the complete local proof-of-concept set with temporary
databases. Integration tests additionally cover process crashes, unknown effects, signal races,
provider behavior and recovery; see [release checks](docs/RELEASING.md).
