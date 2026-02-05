"""Precautionary buffer application for nutrient impact assessment.

Aggregates land use change and wastewater impacts, applying a precautionary buffer.
"""


def apply_buffer(
    nitrogen_land_use_post_suds: float,
    phosphorus_land_use_post_suds: float,
    nitrogen_wastewater: float,
    phosphorus_wastewater: float,
    precautionary_buffer_percent: float,
) -> tuple[float, float]:
    """Apply precautionary buffer to combined nutrient impacts.

    Sums land use change (post-SuDS) and wastewater impacts, then applies
    a precautionary buffer to the absolute value of the combined impact.

    The buffer is applied to the absolute value to correctly handle:
    - Positive impacts (nutrient increases)
    - Negative impacts (nutrient decreases from land use change)
    - Mixed scenarios (land use decreases partially offset by wastewater)

    This code recreates the functionality from lines 401-411 of the original
    FullDFMScript250925.py.

    Renaming of variables from legacy script:
    N_WwTW_Perm -> nitrogen_wastewater
    P_WwTW_Perm -> phosphorus_wastewater
    N_LU_postSuDS -> nitrogen_land_use_post_suds
    P_LU_postSuDS -> phosphorus_land_use_post_suds
    prec_buff -> precautionary_buffer_percent
    N_Total -> nitrogen_total
    P_Total -> phosphorus_total

    Note: Legacy uses .fillna(0) for NaN handling; our refactored version
    expects the orchestration layer to pass 0.0 for components that don't apply.

    Formula:
        base_impact = land_use_post_suds + wastewater
        buffer_amount = abs(base_impact) * (precautionary_buffer_percent / 100)
        total_impact = base_impact + buffer_amount

    Note: Caller should pass 0.0 for components that don't apply (e.g., if outside
    NN catchment, pass 0.0 for land use; if outside WwTW catchment, pass 0.0 for wastewater).

    Args:
        nitrogen_land_use_post_suds: N from land use after SuDS (kg/year), use 0.0 if outside NN
        phosphorus_land_use_post_suds: P from land use after SuDS (kg/year), use 0.0 if outside NN
        nitrogen_wastewater: N from wastewater (kg/year), use 0.0 if outside WwTW
        phosphorus_wastewater: P from wastewater (kg/year), use 0.0 if outside WwTW
        precautionary_buffer_percent: Additional buffer percentage (e.g., 20 for 20%)

    Returns:
        Tuple of (nitrogen_total_kg_per_year, phosphorus_total_kg_per_year).
    """
    n_base = nitrogen_land_use_post_suds + nitrogen_wastewater
    p_base = phosphorus_land_use_post_suds + phosphorus_wastewater

    # Apply precautionary buffer to absolute value
    # This ensures buffer is always added, even for negative base impacts
    buffer_factor = precautionary_buffer_percent / 100
    n_buffer = abs(n_base) * buffer_factor
    p_buffer = abs(p_base) * buffer_factor

    nitrogen_total = n_base + n_buffer
    phosphorus_total = p_base + p_buffer

    # Note: Legacy script does NOT round total impact here - it is rounded
    # at the end in batch with wastewater loads (see legacy line 443)

    return nitrogen_total, phosphorus_total
