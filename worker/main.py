"""Main SQS consumer process for job polling."""

import os

# Limit BLAS threads to avoid oversubscription with ProcessPoolExecutor.
# Each spatial worker process gets 1 BLAS thread; combined with the 80% CPU cap
# on worker processes this keeps total CPU usage at ~80%.
# Must be set before numpy is imported.
os.environ.setdefault("OMP_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("NUMEXPR_NUM_THREADS", "1")

import json
import logging
import logging.config
import multiprocessing
import signal
import sys
import time
from pathlib import Path

import uvicorn
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from worker.aws.sqs import SQSClient
from worker.config import AWSConfig, DatabaseSettings, HealthConfig, WorkerConfig
from worker.health import app as health_app
from worker.orchestrator import JobOrchestrator
from worker.repositories.repository import Repository
from worker.services.email import EmailService
from worker.services.financial import FinancialCalculationService


def is_running_in_ecs() -> bool:
    """Detect if running in AWS ECS (CDP environment).

    ECS automatically injects metadata URI environment variables into containers.
    These are always present in ECS and never present locally.
    """
    return bool(
        os.environ.get("ECS_CONTAINER_METADATA_URI_V4")
        or os.environ.get("ECS_CONTAINER_METADATA_URI")
    )


def configure_logging() -> None:
    """Configure logging based on environment.

    In ECS/CDP: Uses logging.json with ECS-compatible structured logging,
    trace ID injection, and health check filtering.

    Locally: Uses logging-dev.json with simple text format for readability.
    """
    config_file = "logging.json" if is_running_in_ecs() else "logging-dev.json"
    config_path = Path(__file__).parent.parent / config_file

    if config_path.exists():
        with open(config_path) as f:
            logging.config.dictConfig(json.load(f))
    else:
        # Fallback to basic config if file not found
        logging.basicConfig(
            level=logging.INFO,
            format=(
                '{"timestamp": "%(asctime)s", "level": "%(levelname)s", '
                '"logger": "%(name)s", "message": "%(message)s"}'
            ),
            datefmt="%Y-%m-%dT%H:%M:%S",
        )


configure_logging()
logger = logging.getLogger(__name__)


def run_health_server(port: int) -> None:
    """Run the health check server in a separate process.

    Uses uvicorn as an ASGI server to serve the FastAPI app.

    Args:
        port: The port to listen on for health check requests.
    """
    uvicorn.run(health_app, host="0.0.0.0", port=port, log_level="warning")


class SqsConsumer:
    """Long-running SQS consumer that polls for and processes jobs."""

    def __init__(self, sqs_client: SQSClient, orchestrator: JobOrchestrator):
        self.sqs_client = sqs_client
        self.orchestrator = orchestrator
        self.running = True

        signal.signal(signal.SIGTERM, self._handle_sigterm)
        signal.signal(signal.SIGINT, self._handle_sigint)

    def run(self) -> None:
        """Main polling loop."""
        logger.info("SQS consumer started, polling for jobs...")

        while self.running:
            try:
                results = self.sqs_client.receive_messages()

                if not results:
                    continue

                for job_message, receipt_handle in results:
                    logger.info(f"Processing job: {job_message.job_id}")
                    # Pass the assessment_type from the job message
                    self.orchestrator.process_job(job_message, job_message.assessment_type)

                    self.sqs_client.delete_message(receipt_handle)
                    logger.info(
                        f"Job {job_message.job_id} processing complete, message deleted from queue"
                    )

            except KeyboardInterrupt:
                logger.info("Received keyboard interrupt, shutting down...")
                break
            except Exception as e:
                logger.exception(f"Unexpected error in consumer loop: {e}")
                time.sleep(5)

        logger.info("SQS consumer stopped")

    def _handle_sigterm(self, _signum, _frame):
        """Handle SIGTERM for graceful ECS task shutdown."""
        logger.info("Received SIGTERM, initiating graceful shutdown...")
        self.running = False

    def _handle_sigint(self, _signum, _frame):
        """Handle SIGINT (Ctrl+C) for local testing."""
        logger.info("Received SIGINT, initiating graceful shutdown...")
        self.running = False


def check_database_connection(db_settings: DatabaseSettings) -> bool:
    """Check if the database is accessible.

    Attempts to connect and execute a simple query.
    Returns True if successful, False otherwise.
    Logs warnings on failure but does not raise exceptions.
    """
    try:
        engine = create_engine(str(db_settings.url), pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        logger.info("Database connection check: OK")
        return True
    except SQLAlchemyError as e:
        logger.warning(f"Database connection check failed: {e}")
        return False
    except Exception as e:
        logger.warning(f"Database connection check failed with unexpected error: {e}")
        return False


def main():
    """Main entry point for the SQS consumer worker."""
    health_process = None

    try:
        aws_config = AWSConfig()
        worker_config = WorkerConfig()
        health_config = HealthConfig()
        db_settings = DatabaseSettings()

        # Check database connectivity early
        check_database_connection(db_settings)

        logger.info("Initializing worker components...")

        # Start health server in separate process for CDP ECS health checks
        health_process = multiprocessing.Process(
            target=run_health_server,
            args=(health_config.port,),
            daemon=True,
        )
        health_process.start()
        logger.info(f"Health server started on port {health_config.port}")

        # Initialize PostGIS repository (ONCE - reused across jobs)
        engine = create_engine(str(db_settings.url), pool_pre_ping=True)
        repository = Repository(engine)

        financial_service = FinancialCalculationService()
        email_service = EmailService()

        sqs_client = SQSClient(
            queue_url=aws_config.sqs_queue_url,
            region=aws_config.region,
            wait_time_seconds=worker_config.wait_time_seconds,
            visibility_timeout=worker_config.visibility_timeout,
            max_messages=worker_config.max_messages,
        )

        orchestrator = JobOrchestrator(
            aws_config=aws_config,
            repository=repository,
            financial_service=financial_service,
            email_service=email_service,
        )

        consumer = SqsConsumer(sqs_client=sqs_client, orchestrator=orchestrator)
        consumer.run()

    except Exception as e:
        logger.exception(f"Worker failed to start: {e}")
        sys.exit(1)

    finally:
        # Explicit cleanup of health server process
        if health_process is not None and health_process.is_alive():
            logger.info("Terminating health server...")
            health_process.terminate()
            health_process.join(timeout=5)


if __name__ == "__main__":
    main()
