"""API server with health check and test endpoints.

Follows the CDP APIRouter pattern: each feature has its own router module,
assembled here into a single FastAPI app.

Endpoints:
    GET  /health       - Health check for CDP ECS monitoring
    POST /test/job     - Submit job via JSON (when API_TESTING_ENABLED=true)
    POST /test/submit  - Upload geometry to S3 + send SQS message (when API_TESTING_ENABLED=true)
    POST /test/run     - Run assessment directly and return JSON (when API_TESTING_ENABLED=true)
"""

from fastapi import FastAPI

from worker.api.health_router import router as health_router
from worker.config import ApiServerConfig

app = FastAPI(title="NRF Impact Assessment API")

app.include_router(health_router)

# Only include test endpoints when explicitly enabled
config = ApiServerConfig()
if config.testing_enabled:
    from worker.api.test_router import router as test_router

    app.include_router(test_router)
