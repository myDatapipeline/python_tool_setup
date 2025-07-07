# Base image
FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

# Set environment variable for non-interactive install
ENV DEBIAN_FRONTEND=noninteractive

# Install Python 3.12 and pip
RUN apt-get update && \
    python --version && \
    pip --version && \
    pip install pipx && \
    pipx install poetry && \
    # cull -sSL https://install.python-poetry.org | python3 - && \
    echo "export PATH=\"$HOME/.local/bin:\$PATH\"" >> ~/.bashrc &&
    poetry --version && \
    poetry new zen_pipeline && \



# Set default shell
SHELL ["/bin/bash", "-c"]
