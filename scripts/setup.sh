#!/usr/bin/env bash
set -e

# ===== Checking preinstalled tool versions =====
echo "===== Checking preinstalled tool versions ====="
node -v || echo "Node.js not found"
python3 --version || echo "Python not found"
rustc --version || echo "Rust not found"
protoc --version || echo "protoc not found"
cargo fmt --version || echo "cargo fmt not found"
cargo clippy --version || echo "cargo clippy not found"

# 1. Install or upgrade Rust toolchain if needed
REQUIRED_RUST_VERSION="1.82.0"

if command -v rustc &> /dev/null; then
    INSTALLED_RUST_VERSION=$(rustc --version | awk '{print $2}')
    if [ "$(printf '%s\n' "$REQUIRED_RUST_VERSION" "$INSTALLED_RUST_VERSION" | sort -V | head -n1)" != "$REQUIRED_RUST_VERSION" ]; then
        echo "Rust version $INSTALLED_RUST_VERSION is less than $REQUIRED_RUST_VERSION. Upgrading..."
        if command -v rustup &> /dev/null; then
            rustup install $REQUIRED_RUST_VERSION
            rustup default $REQUIRED_RUST_VERSION
        else
            echo "rustup not found. Installing rustup and Rust $REQUIRED_RUST_VERSION..."
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain $REQUIRED_RUST_VERSION
            source "$HOME/.cargo/env"
        fi
    else
        echo "Rust version $INSTALLED_RUST_VERSION meets requirement."
    fi
else
    echo "rustc not found. Installing Rust $REQUIRED_RUST_VERSION..."
    if ! command -v rustup &> /dev/null; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain $REQUIRED_RUST_VERSION
        source "$HOME/.cargo/env"
    else
        rustup install $REQUIRED_RUST_VERSION
        rustup default $REQUIRED_RUST_VERSION
    fi
fi

# 1b. Ensure clippy and rustfmt are installed
rustup component add clippy rustfmt

# 2. Install Protocol Buffers compiler (protoc)
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

# 3. Install Python dependencies (if pyproject.toml exists)
if [ -f pyigloo/pyproject.toml ]; then
    echo "Setting up Python environment for pyigloo using uv..."
    # Prefer Homebrew Python on macOS if available
    PYTHON3_BIN="python3"
    if [[ "$OSTYPE" == "darwin"* ]]; then
        if [ -x "/opt/homebrew/bin/python3" ]; then
            PYTHON3_BIN="/opt/homebrew/bin/python3"
        elif [ -x "/usr/local/bin/python3" ]; then
            PYTHON3_BIN="/usr/local/bin/python3"
        fi
    fi
    echo "Using Python interpreter: $PYTHON3_BIN"
    # Check if uv is installed, if not, install it using the official script
    if ! command -v uv &> /dev/null; then
        echo "Installing uv (Python package manager) using official script..."
        wget -qO- https://astral.sh/uv/install.sh | sh
        export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
    else
        echo "uv is already installed."
    fi
    # Create a new Python environment using uv if not already present
    PY_UV_ENV_DIR="pyigloo/.venv"
    if [ ! -d "$PY_UV_ENV_DIR" ]; then
        echo "Creating new Python environment with uv..."
        uv venv "$PY_UV_ENV_DIR" || { echo "Failed to create Python env with uv."; exit 1; }
    else
        echo "Python environment already exists at $PY_UV_ENV_DIR."
    fi
    source "$PY_UV_ENV_DIR/bin/activate"
    # Use uv to install dependencies
    uv pip install --upgrade pip
    uv pip install maturin
    if [ -f pyigloo/requirements.txt ]; then
        uv pip install -r pyigloo/requirements.txt
    fi
    uv pip install -e pyigloo || true
    deactivate
else
    echo "No pyproject.toml found for Python bindings. Skipping Python deps."
fi

# 4. Install pre-commit and set up git hooks
python3 -m pip install --user pre-commit
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
