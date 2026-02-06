# Deployment and operations guide

## Overview

The NRF Impact Assessment Worker is a **long-running ECS task**. It is not a request/response API service. Its primary functions are to:

-   Poll an SQS queue for job messages submitted by a frontend to a backend application and then onto this service.
-   Download user-submitted shapefiles from S3. (User submitted files might not be supported at this stage)
-   Run impact assessments using reference data from an PostgreSQL database with PostGIS.
-   Post result data to backend api that initiated assessment
-   Send email notifications via the GovUK Notify API.

## Infrastructure and platform requirements

### 1. ECS task

The service is deployed as a long-running ECS task.

-   **Launch Type**: Fargate
-   **Disk Space**: May require ephemeral storage for downloading and processing user-submitted shapefiles (typically <100MB per job).
-   **Runtime Characteristics**:
    -   Polls SQS continuously using 20-second long polling.
    -   Jobs typically complete in 30-120 seconds.
    -   Handles `SIGTERM` for graceful shutdown, allowing the current job to finish before the task exits.

#### CDP compliance: /health endpoint

To comply with the DEFRA Common Development Platform (CDP), the service must expose an HTTP `/health` endpoint for ECS health checks and platform monitoring.

**Architecture**: A **Flask web server runs in a separate background process** to serve the `/health` endpoint, while the main worker process continues to poll SQS.

**Rationale**:

| Approach | Pros | Cons | Verdict |
| --- | --- | --- | --- |
| **Multiprocessing** | Ensures health endpoint is always responsive, even during heavy CPU work in the main process (avoids GIL issues). Provides process isolation. | Slightly more complex than threading. | ✅ **Recommended** |
| **Threading** | Simple, lightweight. | Health endpoint can become unresponsive during CPU-bound tasks in the main worker due to the Python GIL. | ❌ **Not Recommended** |

This multiprocessing approach is chosen to guarantee the reliability of the health check, which is a critical requirement for the CDP platform to monitor the service's health accurately.

**Implementation**:

-   A `worker/health.py` module defines a simple Flask application.
-   The production-grade WSGI server `waitress` is used to serve the app.
-   The main worker entrypoint (`worker/main.py`) uses the `multiprocessing` module to spawn the health server as a separate child process before starting the main polling loop.
-   The `/health` endpoint returns a JSON response, either a minimal `{"status": "ok"}` or an enhanced status with metrics.

### 2. SQS queue access

The worker consumes jobs from a single SQS queue.

-   **Required Operations**: `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:ChangeMessageVisibility`.
-   **Queue Configuration**:
    -   **Visibility Timeout**: Recommended `300s` (5 minutes) to allow jobs to complete.
    -   **Long Polling**: Recommended `20s` to reduce empty receives.
    -   **Dead Letter Queue (DLQ)**: A DLQ is **required** to handle failed messages.

#### Dead letter queue (DLQ) and error handling

The worker is designed for simple and robust error handling, relying on the SQS redrive policy to manage failures.

-   **Worker Philosophy**: The worker **only deletes a message from the queue upon successful completion**. If any error occurs (validation error, processing failure, etc.), the worker logs the error but does **not** delete the message.
-   **Failure Workflow**:
    1.  An error occurs during processing.
    2.  The worker logs the exception.
    3.  The message is not deleted. Its visibility timeout expires, and it becomes visible on the queue again.
    4.  After a configured number of failed receives (`maxReceiveCount`, recommended **3**), SQS automatically moves the message to the DLQ.
-   **Benefits**: This approach simplifies the worker code (no custom DLQ routing logic) and ensures that no failed messages are lost.
-   **Monitoring**: A CloudWatch alarm should be configured on the DLQ's `ApproximateNumberOfMessagesVisible` metric to alert operators to failing jobs.

### 3. S3 bucket access

The worker requires read-only access to an S3 bucket containing user-uploaded input files.

-   **Required Operations**: `s3:GetObject`.
-   **Input Format**: The frontend is expected to upload either a ZIP archive containing shapefile components (.shp, .shx, .dbf, etc.) or a single GeoJSON file.
-   **Output**: The worker does not require an output bucket; results are handled via other means (e.g., email notifications).

### 4. PostGIS

The worker relies on a PostgreSQL database with the PostGIS extension for storing and querying all geospatial reference data.

### 5. Outbound internet access

The worker requires HTTPS egress to the public internet to call the GovUK Notify API (`api.notifications.service.gov.uk`). We understand in CDP that egress to Notify is already configured.

### 6. Secrets management

The following secrets must be managed via AWS Secrets Manager or Parameter Store:

1.  **GovUK Notify API Key**: For sending email notifications.

## Observability

### CloudWatch logs

-   **Log Format**: The worker outputs structured JSON logs, suitable for querying in CloudWatch Logs Insights.
-   **Log Group**: Should follow CDP naming conventions.

### Correlation ID tracing

To enable distributed tracing, the worker propagates a correlation ID from SQS to its logs.

-   **Mechanism**: The CDP platform is expected to place a `CorrelationId` in the SQS message attributes.
-   **Implementation**:
    1.  A `ContextVar` is used to store the correlation ID for the duration of a job's processing.
    2.  A custom `logging.Filter` is attached to the logger.
    3.  This filter automatically injects the `correlation_id` from the `ContextVar` into every log record.
-   **Benefit**: This allows for tracing a single user request from the frontend through SQS and across all logs generated by the worker, without needing to pass the ID through every function call.

## Environment variables

The following environment variables are used to configure the worker. See `.env.example` for a complete reference.

### Required variables

```bash
# GovUK Notify
GOVUK_NOTIFY_API_KEY=...
```

### Optional variables

```bash
# SQS Polling Configuration
SQS_WAIT_TIME_SECONDS=20         # Long polling wait time
SQS_VISIBILITY_TIMEOUT=300       # Time to process a job
SQS_MAX_MESSAGES=1               # Process one job at a time

# Worker Configuration
WORKER_GRACEFUL_SHUTDOWN_TIMEOUT=30  # Seconds to finish current job on SIGTERM
HEALTH_PORT=8085                     # Port for the /health endpoint
```
