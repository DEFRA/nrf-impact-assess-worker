"""Unit tests for financial calculation service stub."""

from unittest.mock import Mock

import pytest

from worker.services.financial import FinancialCalculationService


def test_financial_service_stub():
    """Test financial service stub raises NotImplementedError."""
    service = FinancialCalculationService()

    # Create mock assessment results
    mock_result = Mock()

    # Test calculation raises NotImplementedError
    with pytest.raises(NotImplementedError, match="FinancialCalculationService.calculate"):
        service.calculate([mock_result])
