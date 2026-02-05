"""Sustainable Drainage Systems (SuDS) mitigation calculations.

Applies SuDS nutrient removal to land use change impacts.
"""

import numpy as np

from worker.config import SuDsConfig


def apply_suds_mitigation(
    nitrogen_uplift: float,
    phosphorus_uplift: float,
    dwelling_count: int,  # noqa: ARG001 - Reserved for future threshold enforcement
    suds_config: SuDsConfig,
) -> tuple[float, float]:
    """Apply SuDS mitigation to land use nutrient uplifts.

    SuDS (Sustainable Drainage Systems) can remove nutrients from surface runoff.
    The reduction is applied to the absolute value of the uplift to correctly
    handle both positive and negative nutrient changes.

    This code recreates the functionality from lines 273-283 of the original
    FullDFMScript250925.py.

    Renaming of variables from legacy script:
    N_LU_Uplift -> nitrogen_uplift
    P_LU_Uplift -> phosphorus_uplift
    total_reduction -> suds_config.total_reduction_factor (pre-computed)

    Formula:
        reduction_factor = (flow_capture% / 100) * (removal_rate% / 100)
        N_post_suds = N_uplift - (abs(N_uplift) * reduction_factor)
        P_post_suds = P_uplift - (abs(P_uplift) * reduction_factor)

    Note: Currently applies to ALL developments in the legacy script, regardless of
    the dwelling threshold. The threshold exists in config but is not enforced.

    Args:
        nitrogen_uplift: Land use N uplift (kg/year)
        phosphorus_uplift: Land use P uplift (kg/year)
        dwelling_count: Number of dwellings in development
        suds_config: SuDS configuration (threshold, capture%, removal%)

    Returns:
        Tuple of (nitrogen_post_suds, phosphorus_post_suds) in kg/year.
    """
    # Note: Legacy script applies SuDS to ALL developments, ignoring threshold
    # Keeping this behavior for now to match regression tests
    total_reduction = suds_config.total_reduction_factor

    # Apply reduction to absolute value to correctly handle negative uplifts
    nitrogen_post_suds = nitrogen_uplift - (abs(nitrogen_uplift) * total_reduction)
    phosphorus_post_suds = phosphorus_uplift - (abs(phosphorus_uplift) * total_reduction)

    # Round to 2 decimal places to match legacy script (using np.round for pandas compatibility)
    nitrogen_post_suds = np.round(nitrogen_post_suds, 2)
    phosphorus_post_suds = np.round(phosphorus_post_suds, 2)

    return nitrogen_post_suds, phosphorus_post_suds
