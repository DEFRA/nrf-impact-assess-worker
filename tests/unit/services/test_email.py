"""Unit tests for email notification service."""

from unittest.mock import MagicMock, patch

import pytest

from worker.config import NotifyConfig
from worker.models.enums import AssessmentType
from worker.models.job import ImpactAssessmentJob
from worker.services.email import EmailService


@pytest.fixture
def notify_config():
    """Create a fully configured NotifyConfig."""
    return NotifyConfig(
        api_key="test-api-key-12345678-1234-1234-1234-123456789012-12345678-1234-1234-1234-123456789012",
        template_job_started="template-started-id",
        template_job_completed="template-completed-id",
        results_base_url="https://example.gov.uk/results",
        enabled=True,
    )


@pytest.fixture
def unconfigured_notify_config():
    """Create an unconfigured NotifyConfig."""
    return NotifyConfig(
        api_key="",
        template_job_started="",
        template_job_completed="",
        results_base_url="",
        enabled=True,
    )


@pytest.fixture
def disabled_notify_config():
    """Create a disabled NotifyConfig."""
    return NotifyConfig(
        api_key="test-api-key",
        template_job_started="template-id",
        template_job_completed="template-id",
        results_base_url="https://example.gov.uk/results",
        enabled=False,
    )


@pytest.fixture
def sample_job():
    """Create a sample job for testing."""
    return ImpactAssessmentJob(
        job_id="test-job-123",
        s3_input_key="jobs/test/input.zip",
        developer_email="developer@example.com",
        development_name="Test Development",
        dwelling_type="apartment",
        number_of_dwellings=25,
        assessment_type=AssessmentType.NUTRIENT,
    )


class TestEmailServiceInitialization:
    """Tests for EmailService initialization."""

    @patch("worker.services.email.NotificationsAPIClient")
    def test_initializes_client_when_configured(self, mock_client_class, notify_config):
        """Service initializes NotificationsAPIClient when fully configured."""
        service = EmailService(notify_config)

        mock_client_class.assert_called_once_with(notify_config.api_key)
        assert service._client is not None

    def test_no_client_when_unconfigured(self, unconfigured_notify_config):
        """Service does not initialize client when not fully configured."""
        service = EmailService(unconfigured_notify_config)

        assert service._client is None

    def test_no_client_when_disabled(self, disabled_notify_config):
        """Service does not initialize client when disabled."""
        service = EmailService(disabled_notify_config)

        assert service._client is None


class TestBuildStatusLink:
    """Tests for _build_status_link method."""

    @patch("worker.services.email.NotificationsAPIClient")
    def test_builds_correct_link(self, mock_client_class, notify_config):
        """Builds correct URL from base URL and job ID."""
        service = EmailService(notify_config)

        link = service._build_status_link("job-123")

        assert link == "https://example.gov.uk/results/job-123"

    @patch("worker.services.email.NotificationsAPIClient")
    def test_handles_trailing_slash(self, mock_client_class):
        """Handles base URL with trailing slash."""
        config = NotifyConfig(
            api_key="test-key",
            template_job_started="t1",
            template_job_completed="t2",
            results_base_url="https://example.gov.uk/results/",
            enabled=True,
        )
        service = EmailService(config)

        link = service._build_status_link("job-123")

        assert link == "https://example.gov.uk/results/job-123"


class TestSendJobStarted:
    """Tests for send_job_started method."""

    @patch("worker.services.email.NotificationsAPIClient")
    def test_sends_email_successfully(self, mock_client_class, notify_config, sample_job):
        """Sends job started email with correct parameters."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        service = EmailService(notify_config)
        result = service.send_job_started(sample_job)

        assert result is True
        mock_client.send_email_notification.assert_called_once_with(
            email_address="developer@example.com",
            template_id="template-started-id",
            personalisation={
                "job_id": "test-job-123",
                "development_name": "Test Development",
                "assessment_type": "nutrient",
                "status_link": "https://example.gov.uk/results/test-job-123",
            },
        )

    def test_returns_false_when_not_configured(self, unconfigured_notify_config, sample_job):
        """Returns False when service is not configured."""
        service = EmailService(unconfigured_notify_config)

        result = service.send_job_started(sample_job)

        assert result is False

    @patch("worker.services.email.NotificationsAPIClient")
    def test_handles_empty_development_name(self, mock_client_class, notify_config):
        """Uses default name when development_name is empty."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        job = ImpactAssessmentJob(
            job_id="test-job-123",
            s3_input_key="jobs/test/input.zip",
            developer_email="developer@example.com",
            development_name="",
            dwelling_type="apartment",
            number_of_dwellings=25,
            assessment_type=AssessmentType.NUTRIENT,
        )

        service = EmailService(notify_config)
        service.send_job_started(job)

        call_args = mock_client.send_email_notification.call_args
        assert call_args[1]["personalisation"]["development_name"] == "Unnamed development"


class TestSendJobCompleted:
    """Tests for send_job_completed method."""

    @patch("worker.services.email.NotificationsAPIClient")
    def test_sends_email_successfully(self, mock_client_class, notify_config):
        """Sends job completed email with correct parameters."""
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client

        service = EmailService(notify_config)
        result = service.send_job_completed(
            job_id="test-job-123",
            developer_email="developer@example.com",
            assessment_type="nutrient",
            development_name="Test Development",
        )

        assert result is True
        mock_client.send_email_notification.assert_called_once_with(
            email_address="developer@example.com",
            template_id="template-completed-id",
            personalisation={
                "job_id": "test-job-123",
                "development_name": "Test Development",
                "assessment_type": "nutrient",
                "results_link": "https://example.gov.uk/results/test-job-123",
            },
        )

    def test_returns_false_when_not_configured(self, unconfigured_notify_config):
        """Returns False when service is not configured."""
        service = EmailService(unconfigured_notify_config)

        result = service.send_job_completed(
            job_id="test-job-123",
            developer_email="developer@example.com",
            assessment_type="nutrient",
        )

        assert result is False


class TestNotifyConfigIsConfigured:
    """Tests for NotifyConfig.is_configured property."""

    def test_is_configured_when_all_set(self, notify_config):
        """Returns True when all required fields are set."""
        assert notify_config.is_configured is True

    def test_not_configured_when_missing_api_key(self):
        """Returns False when API key is missing."""
        config = NotifyConfig(
            api_key="",
            template_job_started="t1",
            template_job_completed="t2",
            results_base_url="https://example.com",
        )
        assert config.is_configured is False

    def test_not_configured_when_missing_templates(self):
        """Returns False when template IDs are missing."""
        config = NotifyConfig(
            api_key="key",
            template_job_started="",
            template_job_completed="",
            results_base_url="https://example.com",
        )
        assert config.is_configured is False

    def test_not_configured_when_missing_base_url(self):
        """Returns False when results base URL is missing."""
        config = NotifyConfig(
            api_key="key",
            template_job_started="t1",
            template_job_completed="t2",
            results_base_url="",
        )
        assert config.is_configured is False
