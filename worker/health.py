"""Health check endpoint for CDP ECS monitoring.

This module provides a simple FastAPI application that serves the /health endpoint
required by the DEFRA Common Development Platform (CDP) for ECS health checks.

The health server runs in a separate process from the main SQS consumer to ensure
the health endpoint remains responsive even during CPU-intensive spatial calculations.
"""

from fastapi import FastAPI

app = FastAPI()


@app.get("/health")
def health():
    """Return health status for ECS health checks.

    Returns:
        JSON response with status "ok" and HTTP 200.
    """
    return {"status": "ok"}
