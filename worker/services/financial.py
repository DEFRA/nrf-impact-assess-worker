"""Financial calculation service (stub implementation for Phase 7)."""

import logging

from worker.models.domain import ImpactAssessmentResult

logger = logging.getLogger(__name__)


class FinancialCalculationService:
    """Stub for future financial calculation service.

    This service will eventually apply financial calculations to impact
    assessment results. For Phase 7, it's a no-op pass-through that logs
    execution.

    Future implementation will:
    - Calculate costs based on nutrient impacts
    - Apply regional pricing models
    - Compute mitigation/offset requirements
    """

    def calculate(self, assessment_results: list[ImpactAssessmentResult]) -> dict:
        """Apply financial calculations to assessment results.

        Args:
            assessment_results: List of impact assessment results

        Returns:
            Dictionary containing assessment results and financial data

        Raises:
            NotImplementedError: This service is not yet implemented (Phase 7 stub)
        """
        raise NotImplementedError(
            "FinancialCalculationService.calculate() is not yet implemented. "
            "This stub will be replaced with actual financial calculations in a future phase."
        )
