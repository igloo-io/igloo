#!/usr/bin/env bash
set -eux

# ===== Formatting and Linting for Rust and Python =====
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

# Rust formatting and linting (run in sequence for clarity, but could be parallelized if needed)
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings

# Python formatting (only if there are Python files)
if [ -d pyigloo ]; then
    if find pyigloo -name '*.py' | grep -q .; then
        source pyigloo/.venv/bin/activate || true
        if command -v ruff &> /dev/null; then
            ruff format --line-length 120 pyigloo
        fi
        deactivate || true
    fi
fi

echo -e "\n✅ Formatting and linting complete!"
