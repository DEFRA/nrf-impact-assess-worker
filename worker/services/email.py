"""Email notification service using GOV.UK Notify."""

import logging

from notifications_python_client import NotificationsAPIClient
from notifications_python_client.errors import HTTPError

from worker.config import NotifyConfig
from worker.models.job import ImpactAssessmentJob

logger = logging.getLogger(__name__)


class EmailService:
    """Email notification service using GOV.UK Notify.

    Sends email notifications at key points in the job lifecycle:
    - Job started: When processing begins
    - Job completed: When processing finishes successfully

    Each email includes a link to view the job status/results.
    """

    def __init__(self, config: NotifyConfig):
        """Initialize the email service.

        Args:
            config: GOV.UK Notify configuration with API key and template IDs
        """
        self.config = config
        self._client: NotificationsAPIClient | None = None

        if config.is_configured and config.enabled:
            self._client = NotificationsAPIClient(config.api_key)
            logger.info("EmailService initialized with GOV.UK Notify client")
        elif not config.enabled:
            logger.info("EmailService disabled via configuration")
        else:
            logger.warning(
                "EmailService not fully configured - emails will not be sent. "
                "Set GOVUK_NOTIFY_API_KEY, GOVUK_NOTIFY_TEMPLATE_JOB_STARTED, "
                "GOVUK_NOTIFY_TEMPLATE_JOB_COMPLETED, and GOVUK_NOTIFY_RESULTS_BASE_URL"
            )

    def _build_status_link(self, job_id: str) -> str:
        """Build the URL for viewing job status/results.

        Args:
            job_id: The job identifier

        Returns:
            Full URL to the job status page
        """
        base_url = self.config.results_base_url.rstrip("/")
        return f"{base_url}/{job_id}"

    def send_job_started(self, job: ImpactAssessmentJob) -> bool:
        """Send notification that job processing has started.

        Args:
            job: The job that has started processing

        Returns:
            True if email was sent successfully, False otherwise
        """
        if not self._client:
            logger.debug(f"Skipping job started email for {job.job_id} - service not configured")
            return False

        try:
            personalisation = {
                "job_id": job.job_id,
                "development_name": job.development_name or "Unnamed development",
                "assessment_type": job.assessment_type.value,
                "status_link": self._build_status_link(job.job_id),
            }

            self._client.send_email_notification(
                email_address=job.developer_email,
                template_id=self.config.template_job_started,
                personalisation=personalisation,
            )

            logger.info(f"Job started email sent for job {job.job_id} to {job.developer_email}")
            return True

        except HTTPError as e:
            logger.error(
                f"Failed to send job started email for {job.job_id}: "
                f"status={e.status_code}, message={e.message}"
            )
            return False
        except Exception as e:
            logger.exception(f"Unexpected error sending job started email for {job.job_id}: {e}")
            return False

    def send_job_completed(
        self,
        job_id: str,
        developer_email: str,
        assessment_type: str,
        development_name: str = "",
        assessment_results: list | None = None,  # noqa: ARG002 - reserved for future template use
        financial_data: dict | None = None,  # noqa: ARG002 - reserved for future template use
    ) -> bool:
        """Send notification that job processing has completed.

        Args:
            job_id: Unique job identifier
            developer_email: Email address to send notification to
            assessment_type: Type of assessment (e.g., "nutrient", "gcn")
            development_name: Name of the development
            assessment_results: List of assessment results (for future template use)
            financial_data: Financial calculation results (for future template use)

        Returns:
            True if email was sent successfully, False otherwise
        """
        if not self._client:
            logger.debug(f"Skipping job completed email for {job_id} - service not configured")
            return False

        try:
            personalisation = {
                "job_id": job_id,
                "development_name": development_name or "Unnamed development",
                "assessment_type": assessment_type,
                "results_link": self._build_status_link(job_id),
            }

            self._client.send_email_notification(
                email_address=developer_email,
                template_id=self.config.template_job_completed,
                personalisation=personalisation,
            )

            logger.info(f"Job completed email sent for job {job_id} to {developer_email}")
            return True

        except HTTPError as e:
            logger.error(
                f"Failed to send job completed email for {job_id}: "
                f"status={e.status_code}, message={e.message}"
            )
            return False
        except Exception as e:
            logger.exception(f"Unexpected error sending job completed email for {job_id}: {e}")
            return False

    def send_email(
        self,
        job_id: str,
        developer_email: str,
        assessment_results: list,
        financial_data: dict | None,
    ) -> None:
        """Legacy method for backwards compatibility with orchestrator.

        This method is called by the orchestrator at job completion.
        It delegates to send_job_completed with default values.

        Args:
            job_id: Unique job identifier
            developer_email: Email address to send notification to
            assessment_results: List of assessment results
            financial_data: Financial calculation results
        """
        # Extract assessment type from results if available
        assessment_type = "assessment"
        development_name = ""

        if assessment_results:
            first_result = assessment_results[0]
            # Try to get development name from result
            if hasattr(first_result, "development") and hasattr(first_result.development, "name"):
                development_name = first_result.development.name or ""

        self.send_job_completed(
            job_id=job_id,
            developer_email=developer_email,
            assessment_type=assessment_type,
            development_name=development_name,
            assessment_results=assessment_results,
            financial_data=financial_data,
        )
