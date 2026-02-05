"""Unit tests for email service stub."""

import pytest

from worker.services.email import EmailService


def test_email_service_stub():
    """Test email service stub raises NotImplementedError."""
    service = EmailService()

    with pytest.raises(NotImplementedError, match="EmailService.send_email"):
        service.send_email()
