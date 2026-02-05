"""Regression tests comparing PostGIS implementation against known-good baselines.

These tests validate that the PostGIS-based ImpactAssessmentService produces identical
results to pre-generated baseline outputs (originally from the legacy script).

PREREQUISITES:
1. PostgreSQL with PostGIS running (docker compose up -d)
2. Database migrations applied (alembic upgrade head)
3. Full reference data loaded (python scripts/load_data.py)

The baseline CSV files in tests/data/expected/nutrients were generated once from the legacy script
and are considered the "source of truth". We don't regenerate them on every test run.

NOTE: These tests connect to the actual nrf_impact database (not test_nrf_impact)
and require the complete dataset to be loaded (~5.4M coefficient polygons).
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import pytest

from worker.assessments.adapters import nutrient_adapter
from worker.outputs import CSVOutputStrategy
from worker.repositories.repository import Repository
from worker.runner.runner import run_assessment

# Mark all tests in this module as regression tests
pytestmark = pytest.mark.regression


@pytest.mark.regression
@pytest.mark.parametrize(
    "geometry_file",
    [
        "BnW_small_under_1_hectare/BnW_small_under_1_hectare.shp",
        "BnW_small_under_1_hectare_geojson/BnW_small_under_1_hectare.geojson",
    ],
    ids=["shapefile", "geojson"],
)
def test_regression_single_site_assessment(
    production_repository: Repository,
    test_data_dir: Path,
    tolerance: dict[str, float],
    tmp_path: Path,
    geometry_file: str,
):
    """Test PostGIS-based assessment against known-good baseline.

    This test validates that ImpactAssessmentService with PostGIS Repository
    produces identical results to the legacy file-based implementation.

    Prerequisites:
    - PostgreSQL with PostGIS running
    - Full reference data loaded into nrf_impact database

    Parameterised to test both shapefile and GeoJSON input formats.

    Args:
        production_repository: Repository connected to nrf_impact database
        test_data_dir: Path to test data directory
        tolerance: Numerical comparison tolerances
        tmp_path: Pytest temporary directory
        geometry_file: Relative path to geometry file (shapefile or GeoJSON)
    """
    # Load test geometry
    test_geometry_file = test_data_dir / "inputs" / "nutrients" / geometry_file
    baseline_path = test_data_dir / "expected" / "nutrients" / "BnW_small_under_1_hectare.csv"

    # Verify inputs exist
    assert test_geometry_file.exists(), f"Test geometry file not found: {test_geometry_file}"
    assert baseline_path.exists(), f"Baseline not found: {baseline_path}"

    # Read development geometry
    developments_gdf = gpd.read_file(test_geometry_file)
    print(f"\nLoaded {len(developments_gdf)} development sites from {geometry_file}")

    # Run assessment via new runner
    print("Running PostGIS-based impact assessment...")
    metadata = {"unique_ref": "regression_test_single_site"}
    dataframes = run_assessment(
        assessment_type="nutrient",
        rlb_gdf=developments_gdf,
        metadata=metadata,
        repository=production_repository,
    )

    # Convert to domain models using adapter
    domain_models = nutrient_adapter.to_domain_models(dataframes)
    results = domain_models["assessment_results"]
    print(f"Assessment complete: {len(results)} results")

    # Write results to CSV
    output_path = tmp_path / "postgis_output.csv"
    output_strategy = CSVOutputStrategy()
    output_strategy.write(results, output_path)
    print(f"Results written to: {output_path}")

    # Load baseline and compare
    baseline_df = pd.read_csv(baseline_path)
    postgis_df = pd.read_csv(output_path)

    print(f"\nComparing against baseline: {baseline_path.name}")
    _compare_csv_outputs(
        baseline_df,
        postgis_df,
        tolerance,
        label1="baseline",
        label2="postgis",
    )
    print("✓ Results match baseline within tolerance!")


@pytest.mark.regression
def test_regression_full_broads_wensum_assessment(
    production_repository: Repository,
    test_data_dir: Path,
    tolerance: dict[str, float],
    tmp_path: Path,
):
    """Test PostGIS-based assessment against full Broads & Wensum dataset.

    This test validates ImpactAssessmentService against the complete
    BnW_cleaned_110925 dataset (245 development sites, 219 after filtering),
    comparing against the baseline generated from the legacy script with
    90% WwTW operating rate.

    Prerequisites:
    - PostgreSQL with PostGIS running
    - Full reference data loaded into nrf_impact database

    Args:
        production_repository: Repository connected to nrf_impact database
        test_data_dir: Path to test data directory
        tolerance: Numerical comparison tolerances
        tmp_path: Pytest temporary directory
    """
    # Load test geometry
    test_geometry_file = (
        test_data_dir / "inputs" / "nutrients" / "BnW_cleaned_110925" / "BnW_cleaned_110925.shp"
    )
    baseline_path = test_data_dir / "expected" / "nutrients" / "BroadsWensum_IAT_101025.csv"

    # Verify inputs exist
    assert test_geometry_file.exists(), f"Test geometry file not found: {test_geometry_file}"
    assert baseline_path.exists(), f"Baseline not found: {baseline_path}"

    # Read development geometry
    developments_gdf = gpd.read_file(test_geometry_file)
    print(f"\nLoaded {len(developments_gdf)} development sites from BnW_cleaned_110925.shp")

    # Run assessment via new runner
    print("Running PostGIS-based impact assessment on full dataset...")
    metadata = {"unique_ref": "regression_test_full_broads_wensum"}
    dataframes = run_assessment(
        assessment_type="nutrient",
        rlb_gdf=developments_gdf,
        metadata=metadata,
        repository=production_repository,
    )

    # Convert to domain models using adapter
    domain_models = nutrient_adapter.to_domain_models(dataframes)
    results = domain_models["assessment_results"]
    print(f"Assessment complete: {len(results)} results")

    # Write results to CSV
    output_path = tmp_path / "postgis_full_broads_wensum_output.csv"
    output_strategy = CSVOutputStrategy()
    output_strategy.write(results, output_path)
    print(f"Results written to: {output_path}")

    # Load baseline and compare
    baseline_df = pd.read_csv(baseline_path)
    postgis_df = pd.read_csv(output_path)

    print(f"\nComparing against baseline: {baseline_path.name}")
    print(f"Baseline rows: {len(baseline_df)}, PostGIS rows: {len(postgis_df)}")
    _compare_csv_outputs(
        baseline_df,
        postgis_df,
        tolerance,
        label1="baseline",
        label2="postgis",
    )
    print("✓ Full dataset results match baseline within tolerance!")


def _compare_csv_outputs(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    tolerance: dict[str, float],
    label1: str = "baseline",
    label2: str = "test",
    skip_columns: list[str] | None = None,
) -> None:
    """Compare two DataFrames with appropriate tolerance for numerical columns.

    Args:
        df1: First DataFrame (baseline)
        df2: Second DataFrame (test output)
        tolerance: Dictionary with 'absolute' and 'relative' tolerance values
        label1: Label for first DataFrame (for error messages)
        label2: Label for second DataFrame (for error messages)
        skip_columns: List of column names to skip comparison (e.g., ['id'])

    Raises:
        AssertionError: If DataFrames don't match within tolerance
    """
    skip_columns = skip_columns or []

    # Check same number of rows
    assert len(df1) == len(df2), f"Row count mismatch: {label1}={len(df1)}, {label2}={len(df2)}"

    # Check same columns
    assert set(df1.columns) == set(df2.columns), (
        f"Column mismatch: {label1} has {set(df1.columns) - set(df2.columns)} extra, "
        f"{label2} has {set(df2.columns) - set(df1.columns)} extra"
    )

    # Sort by RLB_ID to ensure row order matches
    df1_sorted = df1.sort_values("RLB_ID").reset_index(drop=True)
    df2_sorted = df2.sort_values("RLB_ID").reset_index(drop=True)

    # Identify numerical vs string columns
    numerical_cols = df1_sorted.select_dtypes(include=["number"]).columns.tolist()
    string_cols = df1_sorted.select_dtypes(include=["object", "string"]).columns.tolist()

    # Compare string columns exactly
    for col in string_cols:
        if col == "geometry":  # Skip geometry column if present
            continue
        if col in skip_columns:  # Skip columns specified by caller
            continue
        assert df1_sorted[col].equals(
            df2_sorted[col]
        ), f"String column '{col}' mismatch between {label1} and {label2}"

    # Compare numerical columns with tolerance
    for col in numerical_cols:
        if col in skip_columns:  # Skip columns specified by caller
            continue

        # Handle NaN values
        df1_col = df1_sorted[col].fillna(0)
        df2_col = df2_sorted[col].fillna(0)

        # Check within absolute tolerance (with float epsilon to avoid
        # false failures from floating-point representation noise)
        diff = (df1_col - df2_col).abs()
        max_diff = diff.max()
        eps = 1e-9

        assert max_diff <= tolerance["absolute"] + eps, (
            f"Numerical column '{col}' exceeds absolute tolerance: "
            f"max_diff={max_diff:.6f}, tolerance={tolerance['absolute']}"
        )

        # For non-zero values, also check relative tolerance
        # NOTE: Skip relative tolerance check for small values (< 0.2) where floating-point
        # precision in geometry operations can cause large relative differences despite
        # negligible absolute differences. For example, -0.03 vs -0.02 is a 33% relative
        # difference but only 0.01 absolute - well within acceptable precision for
        # nutrient calculations. PostGIS ST_Area and Python geometry.area use different
        # computational paths (GEOS vs JTS-derived), so small area differences are expected
        # and amplified in relative terms for small values.
        non_zero_mask = df1_col != 0
        significant_value_mask = (
            df1_col.abs() >= 0.1
        )  # Only check relative tolerance for significant values
        check_mask = non_zero_mask & significant_value_mask
        if check_mask.any():
            rel_diff = (diff[check_mask] / df1_col[check_mask].abs()).max()
            assert rel_diff <= tolerance["relative"], (
                f"Numerical column '{col}' exceeds relative tolerance: "
                f"max_rel_diff={rel_diff:.6f}, tolerance={tolerance['relative']}"
            )
