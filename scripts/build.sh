#!/usr/bin/env bash
set -eux

# ===== Build Rust workspace and Python bindings =====
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
cargo build --workspace --all-targets
# Optionally build Python bindings if needed
# source pyigloo/.venv/bin/activate && maturin develop && deactivate

echo -e "\n✅ Build complete!"
