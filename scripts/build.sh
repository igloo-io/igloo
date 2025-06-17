#!/usr/bin/env bash
set -eux

# ===== Build Rust workspace and Python bindings =====
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

# Use --locked to avoid unnecessary dependency resolution if Cargo.lock is up to date
cargo build --workspace --all-targets --locked
# Optionally build Python bindings if needed
# Uncomment below if you want to always build Python bindings
# if [ -d pyigloo ]; then
#     source pyigloo/.venv/bin/activate && maturin develop --release && deactivate
# fi

echo -e "\n✅ Build complete!"
