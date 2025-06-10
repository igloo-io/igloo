#!/usr/bin/env bash
set -e

echo "Running cargo fmt check..."
cargo fmt --all -- --force

echo "Running cargo clippy..."
cargo clippy --workspace --all-targets -- -D warnings

echo "Running cargo build..."
cargo build --workspace --all-targets

echo "Running cargo test..."
cargo test --workspace --all-targets

echo "\nAll checks passed!" 