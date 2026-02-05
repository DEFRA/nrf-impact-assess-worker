"""Unit tests for assessment runner."""

from unittest.mock import MagicMock, Mock

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import Point

from worker.runner import runner
from worker.runner.runner import run_assessment


@pytest.fixture
def sample_rlb():
    """Create a sample RLB GeoDataFrame."""
    return gpd.GeoDataFrame(
        {
            "id": [1],
            "name": ["Site A"],
            "geometry": [Point(450000, 100000)],
        },
        crs="EPSG:27700",
    )


@pytest.fixture
def sample_metadata():
    """Create sample metadata."""
    return {"unique_ref": "20250115123456"}


@pytest.fixture
def mock_repository():
    """Create a mock repository."""
    return MagicMock()


def test_run_assessment_successful_execution(sample_rlb, sample_metadata, mock_repository):
    """Test successful assessment class instantiation and execution."""
    # Create mock assessment class
    mock_instance = Mock()
    mock_instance.run = Mock(
        return_value={
            "results": pd.DataFrame({"col1": [1, 2, 3]}),
            "summary": pd.DataFrame({"total": [6]}),
        }
    )

    MockAssessmentClass = Mock(return_value=mock_instance, __name__="TestAssessment")

    # Patch the registry to include our mock
    original_registry = runner.ASSESSMENT_TYPES.copy()
    runner.ASSESSMENT_TYPES["test_assessment"] = MockAssessmentClass

    try:
        result = run_assessment("test_assessment", sample_rlb, sample_metadata, mock_repository)

        # Verify class was instantiated correctly
        assert MockAssessmentClass.called
        assert MockAssessmentClass.call_count == 1

        # Verify arguments passed to constructor
        call_args = MockAssessmentClass.call_args
        assert call_args[0][0] is sample_rlb
        assert call_args[0][1] == sample_metadata
        assert call_args[0][2] is mock_repository

        # Verify run() was called
        assert mock_instance.run.called
        assert mock_instance.run.call_count == 1

        # Verify return value
        assert isinstance(result, dict)
        assert "results" in result
        assert "summary" in result
        assert isinstance(result["results"], pd.DataFrame)
        assert isinstance(result["summary"], pd.DataFrame)
    finally:
        # Restore original registry
        runner.ASSESSMENT_TYPES = original_registry


def test_run_assessment_not_in_registry(sample_rlb, sample_metadata, mock_repository):
    """Test error when assessment type is not registered."""
    with pytest.raises(KeyError) as exc_info:
        run_assessment("nonexistent", sample_rlb, sample_metadata, mock_repository)

    assert "Assessment type nonexistent not supported" in str(exc_info.value)


def test_run_assessment_returns_non_dict(sample_rlb, sample_metadata, mock_repository):
    """Test error when assessment.run() returns non-dict."""
    mock_instance = Mock()
    mock_instance.run = Mock(return_value="not a dict")
    MockAssessmentClass = Mock(return_value=mock_instance, __name__="BadReturnAssessment")

    original_registry = runner.ASSESSMENT_TYPES.copy()
    runner.ASSESSMENT_TYPES["bad_return"] = MockAssessmentClass

    try:
        with pytest.raises(ValueError) as exc_info:
            run_assessment("bad_return", sample_rlb, sample_metadata, mock_repository)

        assert "must return a dict" in str(exc_info.value)
    finally:
        runner.ASSESSMENT_TYPES = original_registry


def test_run_assessment_returns_invalid_dataframe_type(
    sample_rlb, sample_metadata, mock_repository
):
    """Test error when assessment.run() returns dict with non-DataFrame values."""
    mock_instance = Mock()
    mock_instance.run = Mock(
        return_value={
            "results": "not a dataframe",
            "summary": pd.DataFrame({"total": [6]}),
        }
    )
    MockAssessmentClass = Mock(return_value=mock_instance, __name__="BadValueAssessment")

    original_registry = runner.ASSESSMENT_TYPES.copy()
    runner.ASSESSMENT_TYPES["bad_values"] = MockAssessmentClass

    try:
        with pytest.raises(ValueError) as exc_info:
            run_assessment("bad_values", sample_rlb, sample_metadata, mock_repository)

        assert "expected DataFrame or GeoDataFrame" in str(exc_info.value)
        assert "results" in str(exc_info.value)
    finally:
        runner.ASSESSMENT_TYPES = original_registry


def test_run_assessment_execution_raises_exception(sample_rlb, sample_metadata, mock_repository):
    """Test error handling when assessment.run() raises exception."""
    mock_instance = Mock()
    mock_instance.run = Mock(side_effect=ValueError("Calculation error"))
    MockAssessmentClass = Mock(return_value=mock_instance, __name__="FailingAssessment")

    original_registry = runner.ASSESSMENT_TYPES.copy()
    runner.ASSESSMENT_TYPES["failing"] = MockAssessmentClass

    try:
        with pytest.raises(ValueError) as exc_info:
            run_assessment("failing", sample_rlb, sample_metadata, mock_repository)

        assert "execution failed" in str(exc_info.value)
    finally:
        runner.ASSESSMENT_TYPES = original_registry


def test_run_assessment_with_geodataframe_results(sample_rlb, sample_metadata, mock_repository):
    """Test assessment that returns GeoDataFrames."""
    mock_instance = Mock()
    mock_instance.run = Mock(
        return_value={
            "spatial_results": gpd.GeoDataFrame(
                {"id": [1], "geometry": [Point(0, 0)]}, crs="EPSG:27700"
            ),
            "tabular_results": pd.DataFrame({"count": [1]}),
        }
    )
    MockAssessmentClass = Mock(return_value=mock_instance, __name__="SpatialAssessment")

    original_registry = runner.ASSESSMENT_TYPES.copy()
    runner.ASSESSMENT_TYPES["spatial"] = MockAssessmentClass

    try:
        result = run_assessment("spatial", sample_rlb, sample_metadata, mock_repository)

        assert isinstance(result["spatial_results"], gpd.GeoDataFrame)
        assert isinstance(result["tabular_results"], pd.DataFrame)
    finally:
        runner.ASSESSMENT_TYPES = original_registry


def test_run_assessment_empty_results_dict(sample_rlb, sample_metadata, mock_repository):
    """Test assessment that returns empty dict (valid but unusual)."""
    mock_instance = Mock()
    mock_instance.run = Mock(return_value={})
    MockAssessmentClass = Mock(return_value=mock_instance, __name__="EmptyAssessment")

    original_registry = runner.ASSESSMENT_TYPES.copy()
    runner.ASSESSMENT_TYPES["empty"] = MockAssessmentClass

    try:
        result = run_assessment("empty", sample_rlb, sample_metadata, mock_repository)

        assert isinstance(result, dict)
        assert len(result) == 0
    finally:
        runner.ASSESSMENT_TYPES = original_registry
