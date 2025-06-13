#!/usr/bin/env bash
set -e

echo "===== Checking preinstalled tool versions ====="
node -v || echo "Node.js not found"
python3 --version || echo "Python not found"
rustc --version || echo "Rust not found"
protoc --version || echo "protoc not found"
cargo fmt --version || echo "cargo fmt not found"
cargo clippy --version || echo "cargo clippy not found"

# 1. Install Rust toolchain if needed
if ! command -v rustup &> /dev/null; then
    echo "Rustup not found. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
else
    echo "Rustup found. Skipping Rust installation."
fi

if [ -f rust-toolchain.toml ]; then
    echo "Setting Rust toolchain from rust-toolchain.toml..."
    rustup show
else
    echo "No rust-toolchain.toml found. Skipping toolchain setup."
fi

# 1b. Ensure clippy and rustfmt are installed
rustup component add clippy rustfmt

# 2. Install Protocol Buffers compiler (protoc)
if ! command -v protoc &> /dev/null; then
    echo "protoc not found. Installing..."
    sudo apt-get update && sudo apt-get install -y protobuf-compiler
else
    echo "protoc found. Skipping installation."
fi

# 3. Install Python dependencies (if pyproject.toml exists)
if [ -f python/pyigloo/pyproject.toml ]; then
    echo "Installing Python dependencies for pyigloo..."
    pip install --upgrade pip
    pip install maturin
    if [ -f python/pyigloo/requirements.txt ]; then
        pip install -r python/pyigloo/requirements.txt
    fi
    pip install -e python/pyigloo || true
else
    echo "No pyproject.toml found for Python bindings. Skipping Python deps."
fi

# 4. Install pre-commit and set up git hooks
pip install pre-commit
pre-commit install || true

# 5. Build the Rust workspace
echo "===== Building Rust workspace ====="
cargo build --workspace --all-targets

# 6. Run pre-commit checks
echo "===== Running pre-commit checks ====="
pre-commit run --all-files || true

# 7. Run additional project checks
echo "===== Running all checks ====="
./scripts/check.sh || true

echo -e "\n✅ Dev environment setup complete! You're ready to develop or run CI."