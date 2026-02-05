#!/usr/bin/env python

"""Submit test jobs to LocalStack for local development testing.

This script is for LOCAL DEVELOPMENT ONLY. It simulates job submission
by uploading geometry files to LocalStack S3 and sending messages to LocalStack SQS.

IMPORTANT: This script does NOT reflect the intended production integration pattern.

Current behaviour (this script):
    1. Upload RLB geometry file to S3
    2. Send SQS message containing job metadata + S3 key
    3. Worker fetches file from S3 and processes

Intended production pattern (not yet implemented):
    1. Source system stores quote/application data (including RLB geometry)
    2. SQS message contains only a reference ID (e.g. quote_id or application_id)
    3. Worker calls back to source system API to fetch full payload
    4. Payload includes RLB as GeoJSON points/polygon (not a file upload)
    5. Worker processes assessment
    6. Worker POSTs results back to source system
    7. Worker triggers email notification

The production pattern hasn't been implemented because the source systems
are not yet in place. This script exists purely to enable local development
and testing of the assessment logic in isolation.

Usage:
    uv run python scripts/submit_job.py <shapefile_or_geojson_path>
    uv run python scripts/submit_job.py \\
        tests/data/inputs/nutrients/BnW_small_under_1_hectare/BnW_small_under_1_hectare.shp
    uv run python scripts/submit_job.py --help
"""

import json
import logging
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import boto3
import typer
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = typer.Typer(help="Submit test jobs to LocalStack for local development")


def zip_shapefile(shapefile_path: Path, output_path: Path) -> None:
    """Zip shapefile and all required components (.shp, .shx, .dbf, .prj, .cpg).

    Args:
        shapefile_path: Path to .shp file
        output_path: Path to output .zip file
    """
    base_path = shapefile_path.parent
    base_name = shapefile_path.stem

    # Required shapefile components
    extensions = [".shp", ".shx", ".dbf", ".prj", ".cpg"]

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for ext in extensions:
            component = base_path / f"{base_name}{ext}"
            if component.exists():
                zipf.write(component, component.name)
                logger.debug(f"  Added {component.name} to zip")
            elif ext in [".shp", ".shx", ".dbf"]:
                # .shp, .shx, .dbf are required
                raise FileNotFoundError(f"Required component {component} not found")


@app.command()
def submit(
    geometry_file: Path = typer.Argument(
        ...,
        help="Path to shapefile (.shp) or GeoJSON (.geojson) file",
        exists=True,
    ),
    developer_email: str = typer.Option(
        "test@example.com",
        "--email",
        "-e",
        help="Developer email for job notifications",
    ),
    dwelling_type: str = typer.Option(
        "house",
        "--dwelling-type",
        "-d",
        help="Dwelling type (e.g., house, apartment, flat)",
    ),
    number_of_dwellings: int = typer.Option(
        1,
        "--dwellings",
        "-n",
        help="Number of dwellings",
        min=1,
    ),
    development_name: str = typer.Option(
        "",
        "--name",
        help="Optional development name",
    ),
    endpoint_url: str = typer.Option(
        "http://localhost:4566",
        "--endpoint",
        help="LocalStack endpoint URL",
    ),
    queue_url: str = typer.Option(
        "http://localhost:4566/000000000000/nrf-assessment-queue",
        "--queue",
        help="SQS queue URL",
    ),
    bucket: str = typer.Option(
        "nrf-inputs",
        "--bucket",
        help="S3 bucket name",
    ),
    region: str = typer.Option(
        "eu-west-2",
        "--region",
        help="AWS region",
    ),
):
    """Submit a test job to LocalStack.

    Uploads geometry file to LocalStack S3 and sends job message to SQS queue.
    """
    logger.info("=== NRF Impact Assessment - Submit Test Job ===")
    logger.info(f"Geometry file: {geometry_file}")
    logger.info(f"Developer email: {developer_email}")
    logger.info(f"Dwelling type: {dwelling_type}")
    logger.info(f"Number of dwellings: {number_of_dwellings}")
    logger.info(f"Development name: {development_name or '(none)'}")
    logger.info(f"Endpoint: {endpoint_url}")
    logger.info("")

    # Generate job ID
    job_id = str(uuid4())
    logger.info(f"Generated job ID: {job_id}")

    # Determine file format
    suffix = geometry_file.suffix.lower()
    if suffix == ".shp":
        geometry_format = "shapefile"
    elif suffix in [".geojson", ".json"]:
        geometry_format = "geojson"
    else:
        logger.error(f"Unsupported file format: {suffix}")
        logger.error("Supported formats: .shp (shapefile), .geojson, .json (GeoJSON)")
        raise typer.Exit(1)

    # Prepare upload file
    zip_path = None
    if geometry_format == "shapefile":
        logger.info("Zipping shapefile components...")
        zip_path = Path(tempfile.gettempdir()) / f"{job_id}_input.zip"
        try:
            zip_shapefile(geometry_file, zip_path)
            upload_file = zip_path
            s3_key = f"jobs/{job_id}/input.zip"
        except FileNotFoundError as e:
            logger.error(f"Failed to zip shapefile: {e}")
            raise typer.Exit(1)
    else:
        upload_file = geometry_file
        s3_key = f"jobs/{job_id}/input.geojson"

    # Initialize boto3 clients for LocalStack
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    sqs_client = boto3.client(
        "sqs",
        endpoint_url=endpoint_url,
        region_name=region,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )

    try:
        # Upload to S3
        logger.info(f"Uploading to S3: s3://{bucket}/{s3_key}")
        s3_client.upload_file(str(upload_file), bucket, s3_key)
        logger.info("✓ Upload successful")

        # Create job message
        job_message = {
            "job_id": job_id,
            "s3_input_key": s3_key,
            "developer_email": developer_email,
            "submitted_at": datetime.now(timezone.utc).isoformat(),
            "development_name": development_name,
            "dwelling_type": dwelling_type,
            "number_of_dwellings": number_of_dwellings,
            # TODO: temporarily hardcoding assessment_type for local testing
            "assessment_type": "nutrient",
        }

        # Send to SQS
        logger.info(f"Sending message to SQS: {queue_url}")
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(job_message),
        )
        logger.info(f"✓ Message sent (ID: {response['MessageId']})")

        logger.info("")
        logger.info("=== Job Submitted Successfully ===")
        logger.info(f"Job ID: {job_id}")
        logger.info(f"S3 Key: {s3_key}")
        logger.info(f"SQS Message ID: {response['MessageId']}")
        logger.info("")
        logger.info("Next steps:")
        logger.info("  1. Start worker: docker compose --profile worker up")
        logger.info("  2. Watch logs: docker compose logs -f worker")
        logger.info(f"  3. Look for: 'Processing job: {job_id}'")

    except ClientError as e:
        logger.error(f"AWS error: {e}")
        raise typer.Exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise typer.Exit(1)
    finally:
        # Cleanup temp zip file
        if zip_path is not None and zip_path.exists():
            zip_path.unlink()


if __name__ == "__main__":
    app()
