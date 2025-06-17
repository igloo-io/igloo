#!/usr/bin/env bash
set -eux

# ===== Helper Functions =====

check_tool_versions() {
    echo "===== Checking preinstalled tool versions ====="
    for cmd in "node -v" "python3 --version" "rustc --version" "protoc --version" "cargo fmt --version" "cargo clippy --version"; do
        eval $cmd || echo "$cmd not found"
    done
}

install_rust() {
    # On Linux, remove system-installed rustc to avoid conflicts with rustup
    if [[ "$OSTYPE" != "darwin"* ]]; then
        echo "Removing system-installed Rust to prevent conflicts..."
        sudo apt-get remove -y rustc cargo || true
    fi

    REQUIRED_RUST_VERSION="1.87.0"
    # Always use rustup to manage the toolchain for consistency
    echo "Installing Rust $REQUIRED_RUST_VERSION via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain $REQUIRED_RUST_VERSION --no-modify-path
    
    # Source the environment directly to update the current shell
    source "$HOME/.cargo/env"

    # Verify the correct version is now active
    INSTALLED_RUST_VERSION=$(rustc --version | awk '{print $2}')
    echo "Successfully installed Rust. Active version: $INSTALLED_RUST_VERSION"

    # Ensure additional components are installed
    echo "Installing clippy and rustfmt components..."
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
            # Reverted to the correct official installation method for uv
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
}

install_precommit() {
    echo "Installing pre-commit..."
    python3 -m pip install --user pre-commit
    # The pre-commit executable is in ~/.local/bin, which is now in PATH
    pre-commit install || true
}

# ===== Main Script Execution =====

# Run initial version check
check_tool_versions

# --- Run installation tasks sequentially to ensure correct PATH propagation ---

# Install core dependencies first
install_rust
install_protoc

# Export the correct PATH to be used by all subsequent steps in this script
# This ensures binaries from rustup (~/.cargo/bin) and pip/uv (~/.local/bin) are found
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"

# Now install Python/pre-commit dependencies which rely on the new PATH
setup_python_env
install_precommit

# --- Run project commands ---

echo "===== Building Rust workspace ====="
# The -Z flag is a fallback; the updated rust version should handle the lockfile
cargo -Z next-lockfile-bump build --workspace --all-targets

echo "===== Running pre-commit checks ====="
pre-commit run --all-files || true

echo "===== Running additional project checks ====="
./scripts/check.sh || true

echo -e "\n✅ Dev environment setup complete! You're ready to develop or run CI."
