# See https://containers.dev/ for more information
# This is a basic Rust + Python devcontainer for your project

FROM mcr.microsoft.com/devcontainers/rust:1.82-bullseye

# Install Python and pip
RUN apt-get update \
    && apt-get install -y python3 python3-pip python3-venv python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install maturin for building Python bindings from Rust
RUN pip3 install maturin

# Install additional common tools
RUN apt-get update && apt-get install -y git curl pkg-config libssl-dev

# Set up a default non-root user (VS Code expects this)
ARG USERNAME=vscode
ARG USER_UID=1000
ARG USER_GID=1000
RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER $USERNAME
WORKDIR /workspace
