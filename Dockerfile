# syntax=docker/dockerfile:1
FROM ubuntu:22.04

# Set non-interactive mode for apt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl git python3 python3-pip python3-venv build-essential pkg-config libssl-dev \
        ca-certificates sudo lsb-release wget && \
    rm -rf /var/lib/apt/lists/*

# Install Rust (specific version)
ENV RUST_VERSION=1.87.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain ${RUST_VERSION} --no-modify-path
ENV PATH="/root/.cargo/bin:${PATH}"
RUN rustup component add clippy rustfmt

# Install protoc (protobuf compiler)
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*

# Install uv (Python package manager)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Copy project files
WORKDIR /workspace
COPY . .

# Set up Python environment for pyigloo
RUN if [ -f pyigloo/pyproject.toml ]; then \
    uv venv pyigloo/.venv && \
    . pyigloo/.venv/bin/activate && \
    uv pip install --upgrade pip && \
    uv pip install maturin && \
    if [ -f pyigloo/requirements.txt ]; then uv pip install -r pyigloo/requirements.txt; fi && \
    uv pip install -e pyigloo && \
    deactivate; \
    fi

# Install pre-commit
RUN python3 -m pip install --user pre-commit && pre-commit install || true

# Build Rust workspace
RUN cargo -Z next-lockfile-bump build --workspace --all-targets

# Run pre-commit and project checks
RUN pre-commit run --all-files || true
RUN ./scripts/check.sh || true

CMD ["bash"]
