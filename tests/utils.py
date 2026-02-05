import pandas as pd


def compare_dataframes(
    legacy_df: pd.DataFrame, refactored_df: pd.DataFrame, tolerance: dict[str, float]
) -> None:
    """Compare two result dataframes with appropriate tolerances.

    Args:
        legacy_df: Legacy script output
        refactored_df: Refactored code output
        tolerance: Numerical comparison tolerances

    Raises:
        AssertionError: If dataframes don't match within tolerance
    """
    # Check same number of rows
    assert len(legacy_df) == len(
        refactored_df
    ), f"Row count mismatch: {len(legacy_df)} vs {len(refactored_df)}"

    # Check columns match
    assert set(legacy_df.columns) == set(
        refactored_df.columns
    ), f"Column mismatch: {set(legacy_df.columns) ^ set(refactored_df.columns)}"

    # Sort both by RLB_ID for comparison
    legacy_sorted = legacy_df.sort_values("RLB_ID").reset_index(drop=True)
    refactored_sorted = refactored_df.sort_values("RLB_ID").reset_index(drop=True)

    # Identify numerical vs categorical columns
    numerical_cols = legacy_sorted.select_dtypes(include=["number"]).columns
    categorical_cols = legacy_sorted.select_dtypes(exclude=["number"]).columns

    # Compare categorical columns (exact match)
    for col in categorical_cols:
        pd.testing.assert_series_equal(
            legacy_sorted[col],
            refactored_sorted[col],
            check_names=True,
            obj=f"Column '{col}'",
        )

    # Compare numerical columns (with tolerance)
    for col in numerical_cols:
        legacy_values = legacy_sorted[col]
        refactored_values = refactored_sorted[col]

        # Use absolute tolerance for values near zero, relative for larger values
        abs_diff = abs(legacy_values - refactored_values)
        rel_diff = abs_diff / abs(legacy_values).replace(0, 1)  # Avoid division by zero

        # Allow either absolute OR relative tolerance to pass
        within_tolerance = (abs_diff <= tolerance["absolute"]) | (rel_diff <= tolerance["relative"])

        failures = ~within_tolerance
        if failures.any():
            failure_rows = legacy_sorted[failures][[col]].copy()
            failure_rows["Legacy"] = legacy_values[failures]
            failure_rows["Refactored"] = refactored_values[failures]
            failure_rows["AbsDiff"] = abs_diff[failures]
            failure_rows["RelDiff"] = rel_diff[failures]

            raise AssertionError(f"Column '{col}' values differ beyond tolerance:\n{failure_rows}")
