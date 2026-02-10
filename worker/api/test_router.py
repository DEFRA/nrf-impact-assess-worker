"""Test endpoints for local development.

Provides two ways to trigger assessments without the CLI workflow:
- POST /test/submit: Upload geometry to LocalStack S3 + send SQS message
- POST /test/run:    Run assessment directly and return JSON results
"""

import json
import logging
import tempfile
import time
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import boto3
import geopandas as gpd
from botocore.exceptions import ClientError
from fastapi import APIRouter, Form, HTTPException, UploadFile

from worker.config import AWSConfig, DatabaseSettings
from worker.repositories.engine import create_db_engine
from worker.repositories.repository import Repository
from worker.runner.runner import run_assessment

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/test")

# Lazy-initialised repository singleton (created on first /test/run call)
_repository: Repository | None = None


def _get_repository() -> Repository:
    """Get or create the module-level Repository singleton."""
    global _repository
    if _repository is None:
        logger.info("Initialising Repository for test API...")
        db_settings = DatabaseSettings()
        engine = create_db_engine(db_settings, pool_size=2, max_overflow=2)
        _repository = Repository(engine)
        logger.info("Repository initialised")
    return _repository


@router.post("/submit")
async def submit_job(
    geometry_file: UploadFile,
    assessment_type: str = Form("nutrient"),
    dwelling_type: str = Form("house"),
    dwellings: int = Form(1),
    name: str = Form("Test Development"),
    email: str = Form("test@example.com"),
):
    """Submit a test job via LocalStack S3 + SQS (replaces submit_job.py).

    Uploads the geometry file to LocalStack S3 and sends an SQS message
    with job metadata. The worker's SQS consumer will pick it up.
    """
    if assessment_type not in ("nutrient", "gcn"):
        raise HTTPException(status_code=400, detail=f"Invalid assessment_type: {assessment_type}")

    job_id = str(uuid4())

    try:
        aws_config = AWSConfig()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"AWS config error: {e}") from e

    endpoint_url = aws_config.endpoint_url
    region = aws_config.region
    bucket = aws_config.s3_input_bucket
    queue_url = aws_config.sqs_queue_url

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Save uploaded file
        filename = geometry_file.filename or "input"
        suffix = Path(filename).suffix.lower()
        saved_path = tmpdir_path / filename
        content = await geometry_file.read()
        saved_path.write_bytes(content)

        # Determine S3 key and prepare upload file
        if suffix == ".shp":
            zip_path = tmpdir_path / f"{job_id}_input.zip"
            _zip_shapefile(saved_path, zip_path)
            upload_path = zip_path
            s3_key = f"jobs/{job_id}/input.zip"
        elif suffix == ".zip":
            # Re-zip with flat structure — S3 client expects .shp at zip root
            upload_path = _flatten_zip(saved_path, tmpdir_path / f"{job_id}_input.zip")
            s3_key = f"jobs/{job_id}/input.zip"
        elif suffix in (".geojson", ".json"):
            upload_path = saved_path
            s3_key = f"jobs/{job_id}/input.geojson"
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported file format: {suffix}. Use .shp, .zip, .geojson, or .json",
            )

        # Upload to S3
        localstack_creds = {
            "aws_access_key_id": "test",
            "aws_secret_access_key": "test",  # noqa: S106
        }

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            region_name=region,
            **localstack_creds,
        )

        try:
            s3_client.upload_file(str(upload_path), bucket, s3_key)
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"S3 upload failed: {e}") from e

        # Send SQS message
        job_message = {
            "job_id": job_id,
            "s3_input_key": s3_key,
            "developer_email": email,
            "submitted_at": datetime.now(UTC).isoformat(),
            "development_name": name,
            "dwelling_type": dwelling_type,
            "number_of_dwellings": dwellings,
            "assessment_type": assessment_type,
        }

        sqs_client = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            region_name=region,
            **localstack_creds,
        )

        try:
            response = sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=json.dumps(job_message),
            )
        except ClientError as e:
            raise HTTPException(status_code=500, detail=f"SQS send failed: {e}") from e

    return {
        "job_id": job_id,
        "s3_key": s3_key,
        "message_id": response["MessageId"],
        "assessment_type": assessment_type,
    }


@router.post("/run")
async def run_job(
    geometry_file: UploadFile,
    assessment_type: str = Form("nutrient"),
    dwelling_type: str = Form("house"),
    dwellings: int = Form(1),
    name: str = Form("Test Development"),
):
    """Run an assessment directly and return results as JSON.

    Bypasses S3/SQS entirely - reads the geometry file, injects job data,
    runs the assessment, and returns results immediately.
    """
    if assessment_type not in ("nutrient", "gcn"):
        raise HTTPException(status_code=400, detail=f"Invalid assessment_type: {assessment_type}")

    job_id = str(uuid4())
    start_time = time.time()

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)

        # Save uploaded file
        filename = geometry_file.filename or "input.geojson"
        suffix = Path(filename).suffix.lower()
        saved_path = tmpdir_path / filename
        content = await geometry_file.read()
        saved_path.write_bytes(content)

        # For zip files, extract first then read
        if suffix == ".zip":
            extract_dir = tmpdir_path / "extracted"
            extract_dir.mkdir()
            with zipfile.ZipFile(saved_path, "r") as zf:
                zf.extractall(extract_dir)
            shp_files = list(extract_dir.glob("**/*.shp"))
            geojson_files = list(extract_dir.glob("**/*.geojson"))
            if shp_files:
                read_path = shp_files[0]
            elif geojson_files:
                read_path = geojson_files[0]
            else:
                raise HTTPException(
                    status_code=400,
                    detail="Zip file must contain a .shp or .geojson file",
                )
        else:
            read_path = saved_path

        # Read geometry
        try:
            gdf = gpd.read_file(read_path)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Failed to read geometry file: {e}") from e

        # Inject job data (same pattern as orchestrator._inject_job_data)
        gdf["id"] = job_id
        gdf["name"] = name
        gdf["dwelling_category"] = dwelling_type
        gdf["source"] = "test_api"
        gdf["dwellings"] = dwellings
        gdf["area_m2"] = gdf.geometry.area

        metadata = {"unique_ref": job_id}

        # Run assessment
        repository = _get_repository()
        try:
            dataframes = run_assessment(
                assessment_type=assessment_type,
                rlb_gdf=gdf,
                metadata=metadata,
                repository=repository,
            )
        except (KeyError, ValueError) as e:
            raise HTTPException(status_code=400, detail=str(e)) from e
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Assessment failed: {e}") from e

    elapsed = time.time() - start_time

    # Convert result DataFrames to JSON-serialisable dicts
    results = {}
    for key, df in dataframes.items():
        if hasattr(df, "geometry") and "geometry" in df.columns:
            results[key] = df.drop(columns=["geometry"]).to_dict(orient="records")
        else:
            results[key] = df.to_dict(orient="records")

    return {
        "job_id": job_id,
        "assessment_type": assessment_type,
        "results": results,
        "timing_s": round(elapsed, 2),
    }


def _flatten_zip(input_zip: Path, output_zip: Path) -> Path:
    """Re-zip with all files at the root (no subdirectories).

    The S3 download code expects .shp at the zip root, but uploaded zips
    often have files nested in a subdirectory. This flattens them.
    """
    with zipfile.ZipFile(input_zip, "r") as zin, zipfile.ZipFile(
        output_zip, "w", zipfile.ZIP_DEFLATED
    ) as zout:
        for info in zin.infolist():
            if info.is_dir():
                continue
            name = Path(info.filename).name
            # Skip macOS resource fork files
            if name.startswith("._") or name == ".DS_Store":
                continue
            # Read using original path, write with flattened name
            data = zin.read(info.filename)
            info.filename = name
            zout.writestr(info, data)
    return output_zip


def _zip_shapefile(shapefile_path: Path, output_path: Path) -> None:
    """Zip shapefile and all required components (.shp, .shx, .dbf, .prj, .cpg)."""
    base_path = shapefile_path.parent
    base_name = shapefile_path.stem
    extensions = [".shp", ".shx", ".dbf", ".prj", ".cpg"]

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for ext in extensions:
            component = base_path / f"{base_name}{ext}"
            if component.exists():
                zipf.write(component, component.name)
            elif ext in (".shp", ".shx", ".dbf"):
                msg = f"Required shapefile component {component} not found"
                raise FileNotFoundError(msg)
