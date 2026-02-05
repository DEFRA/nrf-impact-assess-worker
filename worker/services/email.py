"""Email notification service (stub implementation for Phase 7)."""

import logging

logger = logging.getLogger(__name__)


class EmailService:
    """Stub for future email notification service.

    This service will eventually send NRF levy estimates to users
    via GovUK Notify. For Phase 7 stub, it only logs what would be sent.
    """

    def send_email(self, *args, **kwargs) -> None:
        """Send job completion email to developer.

        Note:
            Arguments TBD - will be determined by GovUK Notify template requirements
            in future implementation.

        Raises:
            NotImplementedError: This service is not yet implemented (Phase 7 stub)
        """
        msg = (
            "EmailService.send_email() is not yet implemented. "
            "This stub will be replaced with GovUK Notify integration in a future phase."
        )
        raise NotImplementedError(
            msg
        )
