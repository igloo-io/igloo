#!/usr/bin/env bash
set -eux

# ===== Initial Setup: tools, dependencies, envs =====

# 1. Check tool versions (run in background for speed)
for cmd in "node -v" "python3 --version" "rustc --version" "protoc --version" "cargo fmt --version" "cargo clippy --version"; do
    eval $cmd || echo "$cmd not found" &
done
wait

# 2. Install Rust toolchain only if not already installed
REQUIRED_RUST_VERSION="1.87.0"
INSTALLED_RUST_VERSION=$(rustc --version 2>/dev/null | awk '{print $2}')
if [[ "$INSTALLED_RUST_VERSION" != "$REQUIRED_RUST_VERSION" ]]; then
    if [[ "$OSTYPE" == "darwin"* ]]; then
        echo "Installing Rust $REQUIRED_RUST_VERSION via rustup..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain $REQUIRED_RUST_VERSION --no-modify-path
        source "$HOME/.cargo/env"
        echo "Successfully installed Rust. Active version: $(rustc --version | awk '{print $2}')"
        echo "Installing clippy and rustfmt components..."
        rustup component add clippy rustfmt
    else
        echo "Removing system-installed Rust to prevent conflicts..."
        sudo apt-get remove -y rustc cargo || true
        sudo apt-get update
        sudo apt-get install -y curl python3-pip build-essential pkg-config libssl-dev protobuf-compiler
    fi
else
    echo "Rust $REQUIRED_RUST_VERSION already installed. Skipping."
fi

# 3. Install protoc (already handled above for Ubuntu)
if ! command -v protoc &> /dev/null; then
    echo "protoc not found. Installing..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if command -v brew &> /dev/null; then
            brew install protobuf
        else
            echo "Homebrew not found. Please install Homebrew and rerun the script."
            exit 1
        fi
    else
        sudo apt-get update && sudo apt-get install -y protobuf-compiler
    fi
else
    echo "protoc found. Skipping installation."
fi

# 4. Setup Python environment for pyigloo
if [ -f pyigloo/pyproject.toml ]; then
    echo "Setting up Python environment for pyigloo using uv..."
    PYTHON3_BIN="python3"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if [ -x "/opt/homebrew/bin/python3" ]; then
            PYTHON3_BIN="/opt/homebrew/bin/python3"
        elif [ -x "/usr/local/bin/python3" ]; then
            PYTHON3_BIN="/usr/local/bin/python3"
        fi
    fi
    echo "Using Python interpreter: $PYTHON3_BIN"
    if ! command -v uv &> /dev/null; then
        echo "Installing uv (Python package manager)..."
        curl -LsSf https://astral.sh/uv/install.sh | sh
    else
        echo "uv is already installed."
    fi
    PY_UV_ENV_DIR="pyigloo/.venv"
    if [ ! -d "$PY_UV_ENV_DIR" ]; then
        echo "Creating new Python environment with uv..."
        uv venv "$PY_UV_ENV_DIR" || { echo "Failed to create Python env with uv."; exit 1; }
    else
        echo "Python environment already exists at $PY_UV_ENV_DIR."
    fi
    source "$PY_UV_ENV_DIR/bin/activate"
    uv pip install --upgrade pip
    uv pip install maturin
    if [ -f pyigloo/requirements.txt ]; then
        uv pip install -r pyigloo/requirements.txt
    fi
    uv pip install -e pyigloo || true
    deactivate
else
    echo "pyproject.toml not found for Python bindings. Skipping Python deps."
fi

# 5. Install pre-commit only if not already installed
if ! command -v pre-commit &> /dev/null; then
    python3 -m pip install --user pre-commit
fi
pre-commit install || true

# 6. Cleanup (use -prune for speed)
find . -type d -name '__pycache__' -prune -exec rm -rf {} +
find . -type f -name '*.pyc' -delete
find pyigloo -type d -name '*.egg-info' -prune -exec rm -rf {} +
find . -type f -name '*.tmp' -delete
find . -type f -name '*.log' -delete
# Uncomment the next line to remove Rust build artifacts for a clean build
# rm -rf target/

echo -e "\n✅ Initial setup complete!"