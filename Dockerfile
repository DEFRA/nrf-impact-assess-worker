# See: cdp-python-backend-template for production-ready Dockerfile patterns

ARG PARENT_VERSION=2.0.1-python3.13.9

FROM defradigital/python:${PARENT_VERSION} AS builder

# Install UV
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock ./

# Install dependencies (without dev dependencies)
RUN uv sync --frozen --no-dev

# ---

FROM defradigital/python:${PARENT_VERSION} AS production

USER root

# Install GDAL runtime libraries (no dev packages needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gdal-bin \
    libgdal36 \
    && rm -rf /var/lib/apt/lists/*

USER nonroot

WORKDIR /app

# Copy UV and virtual environment from builder
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv
COPY --from=builder /app/.venv .venv/

# Copy application code
COPY worker/ ./worker/

# Copy logging configuration for ECS/CDP
COPY logging.json ./

# Reference data stored on EFS (mounted at runtime by ECS)
# EFS will be mounted at /data by ECS task definition
# NOTE: EFS permissions must allow read access for the container user
ENV IAT_DATA_BASE_PATH=/data

# Set environment variables
ENV PATH="/app/.venv/bin:${PATH}"
ENV PYTHONUNBUFFERED=1

# Run worker (base image has ENTRYPOINT ["python"])
CMD ["-m", "worker.main"]
