from logging import getLogger

from fastapi import APIRouter, Depends

from app.common.http_client import create_async_client
from app.config import config

router = APIRouter(prefix="/example")
logger = getLogger(__name__)


# basic endpoint example
@router.get("/test")
async def root():
    logger.info("TEST ENDPOINT")
    return {"ok": True}


# http client endpoint example
@router.get("/http")
async def http_query(client=Depends(create_async_client)):
    endpoint = config.aws_endpoint_url or "http://localstack:4566"
    resp = await client.get(f"{endpoint}/health")
    return {"ok": resp.status_code}
