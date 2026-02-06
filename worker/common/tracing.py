"""Distributed tracing support for CDP platform.

Inbound HTTP requests on the CDP platform include an `x-cdp-request-id` header.
This module provides middleware and context variables to propagate this trace ID
throughout the request lifecycle, enabling cross-service request correlation.
"""

import contextvars
from logging import getLogger

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = getLogger(__name__)

# CDP platform standard tracing header
CDP_TRACE_HEADER = "x-cdp-request-id"

# Context variables for request-scoped tracing data
ctx_trace_id: contextvars.ContextVar[str] = contextvars.ContextVar("trace_id", default="")
ctx_request: contextvars.ContextVar[dict | None] = contextvars.ContextVar("request", default=None)
ctx_response: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "response", default=None
)


class TraceIdMiddleware(BaseHTTPMiddleware):
    """Middleware to extract and propagate CDP trace IDs.

    Extracts the `x-cdp-request-id` header from inbound requests and stores it
    in a context variable for the duration of the request. This allows loggers
    and other components to include the trace ID for cross-service correlation.
    """

    async def dispatch(self, request: Request, call_next):
        req_trace_id = request.headers.get(CDP_TRACE_HEADER)
        if req_trace_id:
            ctx_trace_id.set(req_trace_id)

        ctx_request.set({"url": str(request.url), "method": request.method})

        response = await call_next(request)
        ctx_response.set({"status_code": response.status_code})
        return response
