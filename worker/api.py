"""Health check and job submission endpoints for CDP ECS deployment.

This module provides a FastAPI application that serves:
- /health - Health check endpoint required by CDP for ECS monitoring
- /job - HTTP job submission endpoint (when API_JOB_SUBMISSION_ENABLED=true)

The server runs in a separate process from the main SQS consumer to ensure
endpoints remain responsive even during CPU-intensive spatial calculations.
"""

import json
import logging
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, EmailStr, Field

from worker.config import ApiServerConfig, AWSConfig
from worker.models.enums import AssessmentType

logger = logging.getLogger(__name__)

app = FastAPI()


class JobSubmissionRequest(BaseModel):
    """Request body for job submission endpoint."""

    geometry: dict[str, Any] = Field(
        ...,
        description="GeoJSON Feature or FeatureCollection representing the Red Line Boundary",
    )
    developer_email: EmailStr = Field(
        ...,
        description="Email address for notifications",
    )
    dwelling_type: str = Field(
        ...,
        description="Type of dwelling (e.g., house, apartment)",
    )
    number_of_dwellings: int = Field(
        ...,
        ge=1,
        description="Number of dwellings",
    )
    development_name: str = Field(
        default="",
        description="Optional name for the development",
    )
    assessment_type: AssessmentType = Field(
        default=AssessmentType.NUTRIENT,
        description="Type of assessment to run (nutrient or gcn)",
    )


class JobSubmissionResponse(BaseModel):
    """Response from job submission."""

    job_id: str
    s3_key: str
    message: str


@app.get("/health")
def health():
    """Return health status for ECS health checks.

    Returns:
        JSON response with status "ok" and HTTP 200.
    """
    return {"status": "ok"}


@app.post("/job", response_model=JobSubmissionResponse)
def submit_job(request: JobSubmissionRequest):
    """Submit an impact assessment job via HTTP.

    This endpoint provides an alternative to SQS-based job submission,
    allowing direct HTTP submission of assessment jobs. It uploads the
    geometry to S3 and queues the job message to SQS for processing.

    Requires API_JOB_SUBMISSION_ENABLED=true in environment configuration.

    Args:
        request: JobSubmissionRequest containing geometry and job metadata

    Returns:
        JobSubmissionResponse with job_id and confirmation

    Raises:
        HTTPException 403: If job submission endpoint is not enabled
        HTTPException 500: If S3 upload or SQS send fails
    """
    config = ApiServerConfig()
    if not config.job_submission_enabled:
        raise HTTPException(
            status_code=403,
            detail="Job submission endpoint is disabled. Set API_JOB_SUBMISSION_ENABLED=true to enable.",
        )

    try:
        aws_config = AWSConfig()
    except Exception as e:
        logger.error(f"Failed to load AWS config: {e}")
        raise HTTPException(
            status_code=500,
            detail="AWS configuration not available",
        ) from e

    job_id = str(uuid4())
    s3_key = f"jobs/{job_id}/input.geojson"

    # Create boto3 clients
    client_kwargs: dict = {"region_name": aws_config.region}
    if aws_config.endpoint_url:
        client_kwargs["endpoint_url"] = aws_config.endpoint_url

    s3_client = boto3.client("s3", **client_kwargs)
    sqs_client = boto3.client("sqs", **client_kwargs)

    try:
        # Upload GeoJSON to S3
        geojson_bytes = json.dumps(request.geometry).encode("utf-8")
        s3_client.put_object(
            Bucket=aws_config.s3_input_bucket,
            Key=s3_key,
            Body=geojson_bytes,
            ContentType="application/geo+json",
        )
        logger.info(f"Uploaded job geometry to s3://{aws_config.s3_input_bucket}/{s3_key}")

    except ClientError as e:
        logger.error(f"Failed to upload to S3: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to upload geometry to S3: {e}",
        ) from e

    try:
        # Create and send SQS message
        job_message = {
            "job_id": job_id,
            "s3_input_key": s3_key,
            "developer_email": request.developer_email,
            "submitted_at": datetime.now(UTC).isoformat(),
            "development_name": request.development_name,
            "dwelling_type": request.dwelling_type,
            "number_of_dwellings": request.number_of_dwellings,
            "assessment_type": request.assessment_type.value,
        }

        sqs_client.send_message(
            QueueUrl=aws_config.sqs_queue_url,
            MessageBody=json.dumps(job_message),
        )
        logger.info(f"Queued job for processing: {job_id}")

    except ClientError as e:
        logger.error(f"Failed to send SQS message: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to queue job: {e}",
        ) from e

    return JobSubmissionResponse(
        job_id=job_id,
        s3_key=s3_key,
        message=f"Job {job_id} submitted successfully",
    )
