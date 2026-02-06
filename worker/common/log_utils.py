"""Logging utilities for CDP ECS-compatible structured logging.

Provides filters that enhance log records with CDP-specific fields:
- Trace ID from the x-cdp-request-id header
- HTTP request/response details in ECS format
- Endpoint filtering to reduce noise from health checks
"""

import logging

from worker.common.tracing import ctx_request, ctx_response, ctx_trace_id


class ExtraFieldsFilter(logging.Filter):
    """Adds ECS-compatible fields to log records for CDP platform integration.

    Enhances log records with:
    - trace.id: CDP request trace ID for cross-service correlation
    - url.full: Full request URL
    - http.request.method: HTTP method
    - http.response.status_code: Response status code
    """

    def filter(self, record: logging.LogRecord) -> bool:
        trace_id = ctx_trace_id.get()
        req = ctx_request.get()
        resp = ctx_response.get()

        if trace_id:
            record.trace = {"id": trace_id}

        http = {}
        if req:
            record.url = {"full": req.get("url")}
            http["request"] = {"method": req.get("method")}
        if resp:
            http["response"] = resp
        if http:
            record.http = http

        return True


class EndpointFilter(logging.Filter):
    """Filters out log messages for specific endpoints.

    Useful for suppressing verbose health check logs in production.

    Args:
        path: The endpoint path to filter (e.g., "/health")
    """

    def __init__(self, path: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._path = path

    def filter(self, record: logging.LogRecord) -> bool:
        return record.getMessage().find(self._path) == -1
