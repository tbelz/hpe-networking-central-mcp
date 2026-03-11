# Stage 1: Builder
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /build

# Copy MCP server source
COPY pyproject.toml uv.lock* README.md ./
COPY src/ ./src/

# Create venv and install all dependencies
RUN uv venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install MCP server (pulls httpx, structlog, mcp, etc.)
RUN uv pip install .

# Stage 2: Runtime
FROM python:3.12-slim

# Copy Python venv from builder
COPY --from=builder /opt/venv /opt/venv
ENV VIRTUAL_ENV=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy seed scripts into default library
COPY seeds/ /scripts/library/

# Create required directories
RUN mkdir -p /scripts/library

# Environment
ENV PYTHONUNBUFFERED=1
ENV SCRIPT_LIBRARY_PATH=/scripts/library
ENV DOCS_PATH=/docs

ENTRYPOINT ["hpe-networking-central-mcp"]
