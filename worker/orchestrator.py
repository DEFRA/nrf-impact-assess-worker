"""Assessment Job Processor - coordinates S3 download, assessment, financial calc, and email."""

import logging
import tempfile
import time
from pathlib import Path

import geopandas as gpd

from worker.assessments.adapters import gcn_adapter, nutrient_adapter
from worker.aws.s3 import S3Client
from worker.config import AWSConfig
from worker.models.enums import AssessmentType
from worker.models.geometry import GeometryFormat
from worker.models.job import ImpactAssessmentJob
from worker.repositories.repository import Repository
from worker.runner.runner import run_assessment
from worker.services.email import EmailService
from worker.services.financial import FinancialCalculationService
from worker.validation.geometry import GeometryValidator

logger = logging.getLogger(__name__)


class JobOrchestrator:
    """Orchestrates complete job lifecycle: download → validate → assess → financial → email."""

    def __init__(
        self,
        aws_config: AWSConfig,
        repository: Repository,
        financial_service: FinancialCalculationService,
        email_service: EmailService,
    ):
        self.aws_config = aws_config
        self.repository = repository
        self.financial_service = financial_service
        self.email_service = email_service
        # self.assessment_type removed from here

        # Initialize S3 client (input only)
        self.s3_input = S3Client(
            bucket_name=aws_config.s3_input_bucket,
            region=aws_config.region,
            endpoint_url=aws_config.endpoint_url,
        )

    def process_job(self, job: ImpactAssessmentJob, assessment_type: AssessmentType) -> list:
        """Process a single job end-to-end.

        Pipeline:
        1. Download shapefile from S3
        2. Validate shapefile
        3. Run impact assessment → List[ImpactAssessmentResult]
        4. Apply financial calculation for estimate (stub)
        5. Send email with estimate (stub)

        Args:
            job: Job message from SQS
            assessment_type: The type of assessment to run for this job.

        Returns:
            List of assessment results, or empty list if validation fails or unsupported type.

        Note:
            Errors are logged but not raised. Error handling strategy TBD.
        """
        start_time = time.time()
        logger.info(f"Processing job {job.job_id} for assessment type: {assessment_type.value}")

        # Send job started notification
        self.email_service.send_job_started(job)

        try:
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                logger.info("Step 1: Downloading geometry file from S3")
                geometry_path, geometry_format = self.s3_input.download_geometry_file(
                    s3_key=job.s3_input_key, local_dir=tmpdir_path
                )

                if not geometry_path.exists():
                    msg = f"S3Client returned non-existent path: {geometry_path}"
                    raise RuntimeError(msg)

                # Process geometry file (steps 2-4)
                assessment_results = self._process_geometry_file(
                    job,
                    geometry_path,
                    geometry_format,
                    assessment_type,  # Pass assessment_type
                )

                if not assessment_results:
                    logger.error(f"Assessment produced no results for job {job.job_id}")
                    return []  # Return empty list if no results

                logger.info("Step 5: Applying financial calculations (stub)")
                try:
                    financial_result = self.financial_service.calculate(assessment_results)
                except NotImplementedError:
                    financial_result = None
                    logger.info("FinancialCalculationService.calculate() is not yet implemented.")

                logger.info("Step 6: Sending completion email")
                self.email_service.send_job_completed(
                    job_id=job.job_id,
                    developer_email=job.developer_email,
                    assessment_type=assessment_type.value,
                    development_name=job.development_name,
                    assessment_results=assessment_results,
                    financial_data=financial_result,
                )

                processing_time = time.time() - start_time
                logger.info(f"Job {job.job_id} completed successfully in {processing_time:.2f}s")
                logger.info(f"Processed {len(assessment_results)} developments")
                return assessment_results  # Return results

        except Exception as e:
            logger.exception(f"Job {job.job_id} failed with exception: {e}")
            return []  # Return empty list on exception

    def _process_geometry_file(
        self,
        job: ImpactAssessmentJob,
        geometry_path: Path,
        geometry_format: GeometryFormat,
        assessment_type: AssessmentType,  # Added assessment_type parameter
    ) -> list:
        """Process geometry file: validate, inject job data, and run assessment.

        This method encapsulates the core processing logic (steps 2-4) and can be
        used for testing without requiring S3 download.

        Args:
            job: ImpactAssessmentJob with development data
            geometry_path: Path to local geometry file
            geometry_format: GeometryFormat (SHAPEFILE or GEOJSON)
            assessment_type: The type of assessment to run.

        Returns:
            List of assessment results, or empty list if validation fails or unsupported type.
        """
        logger.info("Step 2: Validating geometry (geometry only - no embedded attributes)")
        geometry_validator = GeometryValidator()
        validation_errors = geometry_validator.validate(geometry_path, geometry_format)

        if validation_errors:
            error_msg = "; ".join([e.message for e in validation_errors])
            logger.error(f"Geometry validation failed for job {job.job_id}: {error_msg}")
            logger.error(f"  Validation errors: {[e.message for e in validation_errors]}")
            return []  # Return empty list on validation failure

        logger.info("Step 3: Loading geometry and injecting job data")
        gdf = gpd.read_file(geometry_path)
        gdf = self._inject_job_data(gdf, job)

        logger.info(f"Step 4: Running {assessment_type.value} assessment via new runner")
        # Build metadata for assessment
        metadata = {"unique_ref": job.job_id}

        dataframes = run_assessment(
            assessment_type=assessment_type.value,  # Use parameter
            rlb_gdf=gdf,
            metadata=metadata,
            repository=self.repository,
        )

        if assessment_type == AssessmentType.NUTRIENT:
            domain_models = nutrient_adapter.to_domain_models(dataframes)
            assessment_results = domain_models["assessment_results"]
        elif assessment_type == AssessmentType.GCN:
            domain_models = gcn_adapter.to_domain_models(dataframes)
            assessment_results = domain_models["assessment_results"]
        else:
            # This case should ideally not be reached if AssessmentType enum is exhaustive
            logger.error(f"Unsupported assessment type: {assessment_type.value}")
            return []  # Return empty list for unsupported types

        return assessment_results

    def _inject_job_data(self, gdf: gpd.GeoDataFrame, job: ImpactAssessmentJob) -> gpd.GeoDataFrame:
        """Inject job data from SQS message into GeoDataFrame.

        In production, geometry files contain only geometry. All development data
        comes from the frontend form via the SQS job message. This method injects
        that data into the GeoDataFrame so the assessment service can use it.

        Args:
            gdf: GeoDataFrame loaded from geometry file (contains only geometry)
            job: Job message from SQS with development data

        Returns:
            GeoDataFrame with job data injected into columns
        """

        gdf["id"] = job.job_id
        gdf["name"] = job.development_name  # Renamed from "Name" to "name" for consistency
        gdf["dwelling_category"] = job.dwelling_type  # Renamed from "Dwel_Cat"
        gdf["source"] = "web_submission"  # Renamed from "Source"
        gdf["dwellings"] = job.number_of_dwellings  # Renamed from "Dwellings"
        gdf["area_m2"] = gdf.geometry.area  # Renamed from "Shape_Area"
        logger.info(
            f"Injected job data: id: {job.job_id} {job.number_of_dwellings} {job.dwelling_type}"
        )

        return gdf
