#!/usr/bin/env bash
set -e

# ===== Helper Functions =====

check_tool_versions() {
    echo "===== Checking preinstalled tool versions ====="
    for cmd in "node -v" "python3 --version" "rustc --version" "protoc --version" "cargo fmt --version" "cargo clippy --version"; do
        eval $cmd || echo "$cmd not found"
    done
}

install_rust() {
    REQUIRED_RUST_VERSION="1.82.0"
    if command -v rustc &> /dev/null; then
        INSTALLED_RUST_VERSION=$(rustc --version | awk '{print $2}')
        if [[ "$(printf '%s\n' "$REQUIRED_RUST_VERSION" "$INSTALLED_RUST_VERSION" | sort -V | head -n1)" != "$REQUIRED_RUST_VERSION" ]]; then
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
    # Ensure additional components are installed
    rustup component add clippy rustfmt
}

install_protoc() {
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
}

setup_python_env() {
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
            wget -qO- https://astral.sh/uv/install.sh | sh
            export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
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
}

install_precommit() {
    python3 -m pip install --user pre-commit
    pre-commit install || true
}

# ===== Main Script Execution =====

check_tool_versions

# Run independent installation tasks in parallel
install_rust &
pid_rust=$!
install_protoc &
pid_protoc=$!
setup_python_env &
pid_python=$!
install_precommit &
pid_precommit=$!

# Wait for all parallel tasks to complete
wait $pid_rust $pid_protoc $pid_python $pid_precommit

echo "===== Building Rust workspace ====="
cargo build --workspace --all-targets

echo "===== Running pre-commit checks ====="
pre-commit run --all-files || true

echo "===== Running additional project checks ====="
./scripts/check.sh || true

echo -e "\n✅ Dev environment setup complete! You're ready to develop or run CI."
