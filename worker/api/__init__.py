"""Local development API server with health check and test endpoints.

Follows the CDP APIRouter pattern: each feature has its own router module,
assembled here into a single FastAPI app.

NOT for production use - production uses health.py (GET /health only).

Endpoints:
    GET  /health       - Health check (same as health.py)
    POST /test/submit  - Upload geometry to LocalStack S3 + send SQS message
    POST /test/run     - Run assessment directly and return JSON results
"""

from fastapi import FastAPI

from worker.api.health_router import router as health_router
from worker.api.test_router import router as test_router

app = FastAPI(title="NRF Impact Assessment - Local Dev API")

app.include_router(health_router)
app.include_router(test_router)
