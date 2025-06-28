#!/usr/bin/env bash
set -eux

# Ensure Rust toolchain is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Fast check before build
echo "Running cargo fmt check..."
cargo fmt --all -- --check

echo "Running cargo clippy..."
cargo clippy --workspace --all-targets -- -D warnings

echo "Running cargo check..."
cargo check --workspace --all-targets

echo "Running cargo build..."
cargo build --workspace --all-targets

echo "Running cargo test (build only)..."
cargo test --workspace --all-targets --no-run

echo "Running cargo test (run)..."
cargo test --workspace --all-targets

echo

echo "All checks passed!"