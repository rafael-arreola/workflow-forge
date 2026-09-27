#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --all --check
cargo fmt --manifest-path manual/ejemplos/Cargo.toml --all --check
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
cargo clippy --manifest-path manual/ejemplos/Cargo.toml --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --all-features --locked
cargo test --workspace --doc --all-features --locked
cargo test --manifest-path manual/ejemplos/Cargo.toml --workspace --locked
cargo check --workspace --all-targets --no-default-features --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features --locked
python3 scripts/run-examples.py
