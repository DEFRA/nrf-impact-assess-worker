"""Land use change nutrient uplift calculations.

Calculates nutrient impacts from converting existing land use to residential development.
"""

import numpy as np


def calculate_land_use_uplift(
    area_hectares: float,
    current_nitrogen_coeff: float,
    residential_nitrogen_coeff: float,
    current_phosphorus_coeff: float,
    residential_phosphorus_coeff: float,
) -> tuple[float, float]:
    """Calculate nutrient uplift from land use change.

    Computes the difference between residential and current land use coefficients,
    multiplied by the development area within the Nutrient Neutrality catchment.

    This code recreates the functionality from lines 218-223 of the original
    FullDFMScript250925.py.

    Renaming of variables from legacy script:
    N_ResiCoeff -> residential_nitrogen_coeff
    LU_CurrNcoeff -> current_nitrogen_coeff
    AreaInNNCatchment -> area_hectares

    Formula:
        N_uplift = (N_residential - N_current) * area_hectares
        P_uplift = (P_residential - P_current) * area_hectares

    Args:
        area_hectares: Development area within NN catchment (hectares)
        current_nitrogen_coeff: Current land use N coefficient (kg/ha/year)
        residential_nitrogen_coeff: Residential land use N coefficient (kg/ha/year)
        current_phosphorus_coeff: Current land use P coefficient (kg/ha/year)
        residential_phosphorus_coeff: Residential land use P coefficient (kg/ha/year)

    Returns:
        Tuple of (nitrogen_kg_per_year, phosphorus_kg_per_year).
    """
    nitrogen_uplift = (residential_nitrogen_coeff - current_nitrogen_coeff) * area_hectares
    phosphorus_uplift = (residential_phosphorus_coeff - current_phosphorus_coeff) * area_hectares

    # Round to 2 decimal places to match legacy script (using np.round for pandas compatibility)
    nitrogen_uplift = np.round(nitrogen_uplift, 2)
    phosphorus_uplift = np.round(phosphorus_uplift, 2)

    return nitrogen_uplift, phosphorus_uplift
