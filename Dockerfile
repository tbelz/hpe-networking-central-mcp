# Stage 1: Builder
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /build

# Clone and install pycentral v2 from source
RUN git clone --branch 'v2(pre-release)' --depth 1 \
    https://github.com/aruba/pycentral.git /build/pycentral

# Clone Ansible collection from source
RUN git clone --branch v2-beta --depth 1 \
    https://github.com/aruba/aruba-central-ansible-collection.git /build/ansible-collection

# Copy MCP server source
COPY pyproject.toml uv.lock* README.md ./
COPY src/ ./src/

# Create venv and install all dependencies
RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install pycentral from local clone
RUN uv pip install /build/pycentral

# Install ansible-core
RUN uv pip install ansible-core

# Install MCP server
RUN uv pip install .

# Build and install Ansible collection
RUN cd /build/ansible-collection && \
    ansible-galaxy collection build --force --output-path /build/ && \
    ansible-galaxy collection install /build/arubanetworks-hpeanw_central-*.tar.gz --force

# Stage 2: Runtime
FROM python:3.12-slim

# Copy Python venv from builder
COPY --from=builder /opt/venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy Ansible collection
COPY --from=builder /root/.ansible /root/.ansible

# Copy documentation from repos
COPY --from=builder /build/pycentral/docs /docs/pycentral
COPY --from=builder /build/ansible-collection/docs /docs/ansible
COPY --from=builder /build/ansible-collection/examples /examples

# Copy seed scripts into default library
COPY seeds/ /scripts/library/

# Create required directories
RUN mkdir -p /config /scripts/library

# Environment
ENV PYTHONUNBUFFERED=1
ENV SCRIPT_LIBRARY_PATH=/scripts/library
ENV DOCS_PATH=/docs
ENV EXAMPLES_PATH=/examples
ENV INVENTORY_CONFIG_PATH=/config/central_inventory.yml

ENTRYPOINT ["hpe-networking-central-mcp"]
