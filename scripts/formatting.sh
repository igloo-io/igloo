#!/usr/bin/env bash
set -eux

# ===== Formatting and Linting for Rust and Python =====
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

# Rust formatting and linting
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings

# Python formatting (if desired, e.g. with black)
if [ -d pyigloo ]; then
    source pyigloo/.venv/bin/activate || true
    if command -v black &> /dev/null; then
        black pyigloo
    fi
    deactivate || true
fi

echo -e "\n✅ Formatting and linting complete!"
