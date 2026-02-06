# Local development guide

This guide covers local development and testing of the NRF Impact Assessment Worker using LocalStack to simulate AWS services and PostgreSQL/PostGIS for the spatial data backend.

## Prerequisites

-   Docker and Docker Compose
-   UV (Python package manager)
-   Reference data downloaded to the `iat_input/` directory. This directory acts as the source for the data loading script that populates the PostGIS database.

**Important: Download reference data**

Before running the local stack, you need the reference datasets. The `iat_input/` directory is git-ignored and must be populated manually from the NRF shared drive in the AD3 AWS account. See [Data Inventory](data-inventory.md#file-locations) for the expected directory structure and dataset details.

## Quickstart

### Step 1: Start the full stack

Start all services (LocalStack, PostgreSQL, worker) and, on the first run, load the reference data into the PostGIS database.

```bash
# First run: loads full reference data into PostGIS (~8 minutes)
./scripts/start-local-services.sh --load-data

# Subsequent runs: starts services without reloading data (much faster)
./scripts/start-local-services.sh
```

This script will:
1.  Start LocalStack (for S3 and SQS) and PostgreSQL/PostGIS containers.
2.  Run database migrations to prepare the schema.
3.  If `--load-data` is specified, run the `scripts/load_data.py` script to populate the database.
4.  Build and start the worker container.

All services run in the background.

### Step 2: Submit a test job

Submit a test job to verify the entire pipeline is working.

```bash
# Use the default test shapefile
./scripts/local-test.sh

# Or provide a path to your own shapefile
./scripts/local-test.sh path/to/your/shapefile.shp
```

This script uploads the shapefile to the local S3 and sends a job message to the local SQS queue.

### Step 3: Watch the job being processed

You can monitor the worker's logs to see the job being processed.

```bash
docker compose logs -f worker
```

Look for log messages like "Worker started, polling for jobs..." and "processing complete, message deleted from queue".

### Step 4: Stop services

When you are finished, stop all the Docker containers.

```bash
docker compose down
```

## Integrating with a web application

Once the local stack is running, a frontend or API can submit jobs by performing the following steps.

> **Note**: The integration pattern described here (S3 file upload + SQS message with full payload) is a **development convenience** and may not reflect the intended production integration.
>
> In final production implementation the SQS message could contain only a quote/reference ID, and the worker could call back to the source system API to fetch the payload (including RLB geometry as GeoJSON). See `scripts/submit_job.py` docstring for details.

### 1. Upload input file to S3

Upload a zipped shapefile or a GeoJSON file to the LocalStack S3 service.

-   **Endpoint**: `http://localhost:4566`
-   **Bucket**: `nrf-inputs`
-   **Region**: `eu-west-2`
-   **Credentials**: `test` / `test` (any value works with LocalStack)

### 2. Send message to SQS queue

Send a job message to the SQS queue. The message body must match the `ImpactAssessmentJob` schema.

-   **Queue URL**: `http://localhost:4566/000000000000/nrf-assessment-queue`

**Example message body (JSON):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "s3_input_key": "jobs/550e8400/input.zip",
  "developer_email": "developer@example.com",
  "submitted_at": "2025-10-15T14:30:00Z",
  "development_name": "Big homes",
  "dwelling_type": "apartment",
  "number_of_dwellings": 25,
  "assessment_type": "nutrient"
}
```
*Note: The fields in this model are subject to change.*

### 3. Monitor logs

Follow the worker logs to see the job being processed from start to finish.

```bash
docker compose logs -f worker
```

**What to look for:**
-   ✅ Job downloaded from S3
-   ✅ Input file validation passed
-   ✅ Assessment completed with results
-   ✅ Financial service stub called
-   ✅ Email service stub called
-   ✅ Job completed and message deleted from queue

## Architecture

The local stack consists of:
-   **LocalStack** (port 4566): Simulates AWS S3 and SQS.
-   **PostgreSQL/PostGIS** (port 5432): The spatial reference database.
-   **Worker**: The application container that processes jobs.

For a detailed explanation of the system design, see the [Architecture documentation](./architecture.md).

## Troubleshooting

### LocalStack not starting
-   **Symptom**: `docker compose up localstack` fails with a port conflict.
-   **Solution**: Check if port 4566 is already in use with `lsof -i :4566` and stop the conflicting process.

### Worker can't connect to services
-   **Symptom**: Worker logs show connection errors to LocalStack or PostgreSQL.
-   **Solution**: Use `docker ps` to ensure the `localstack` and `postgres` containers are running and healthy. Check the Docker network with `docker network inspect nrf-impact-assessment-worker_default`.

### Data loading fails
-   **Symptom**: The `./scripts/start-local-services.sh --load-data` command fails with "File not found" errors.
-   **Solution**: This indicates missing reference data files in the `iat_input/` directory. Download the data from the NRF shared drive and ensure the directory structure matches the [Data Inventory](data-inventory.md#file-locations).

### Job messages not received
-   **Symptom**: The worker is polling but never receives any jobs.
-   **Solution**: Use the AWS CLI (configured for LocalStack) to check the queue depth: `aws --endpoint-url=http://localhost:4566 sqs get-queue-attributes --queue-url http://localhost:4566/000000000000/nrf-assessment-queue --attribute-names ApproximateNumberOfMessages`. If there are no messages, the issue is with the job submission step.

## Environment variables

The local development stack is configured via `compose.yml` and `compose/aws.env`. The worker container uses these to connect to the other services.

-   `AWS_ENDPOINT_URL`: `http://localstack:4566` (for services within the Docker network)
-   `DB_URL`: `postgresql://postgres@postgres:5432/nrf_impact`
-   `IAT_DATA_BASE_PATH`: `/data` (path inside the data loading container, mapped to the local `iat_input` directory)
