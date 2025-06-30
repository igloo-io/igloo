# See https://containers.dev/ for more information
# This is a basic Rust + Python devcontainer for your project

FROM mcr.microsoft.com/devcontainers/rust:latest

# Install Python and pip
RUN apt-get update \
    && apt-get install -y python3 python3-pip python3-venv python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install maturin for building Python bindings from Rust
RUN pip3 install --break-system-packages maturin

# Install additional common tools
RUN apt-get update && apt-get install -y git curl pkg-config libssl-dev

# Set up a default non-root user (VS Code expects this)
USER vscode
WORKDIR /workspace
