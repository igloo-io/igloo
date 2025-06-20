#!/usr/bin/env bash
set -eux

# Fast check before build
echo "Running cargo fmt check..."
/home/jules/.cargo/bin/cargo fmt --all -- --check

echo "Running cargo clippy..."
/home/jules/.cargo/bin/cargo clippy --workspace --all-targets -- -D warnings

echo "Running cargo check..."
/home/jules/.cargo/bin/cargo check --workspace --all-targets

echo "Running cargo build..."
/home/jules/.cargo/bin/cargo build --workspace --all-targets

echo "Running cargo test (build only)..."
/home/jules/.cargo/bin/cargo test --workspace --all-targets --no-run

echo "Running cargo test (run)..."
/home/jules/.cargo/bin/cargo test --workspace --all-targets

echo

echo "All checks passed!"