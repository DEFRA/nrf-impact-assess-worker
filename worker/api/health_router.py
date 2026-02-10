"""Health check router."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
async def health():
    """Return health status for ECS health checks."""
    return {"status": "ok"}
