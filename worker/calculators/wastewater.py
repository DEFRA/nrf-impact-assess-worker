"""Wastewater treatment nutrient load calculations.

Calculates nutrient loads from residential wastewater based on water usage,
occupancy rates, and treatment works permit concentrations.
"""

from worker.config import CONSTANTS


def calculate_wastewater_load(
    dwellings: int,
    occupancy_rate: float,
    water_usage_litres_per_person_per_day: float,
    nitrogen_conc_mg_per_litre: float,
    phosphorus_conc_mg_per_litre: float,
) -> tuple[float, float, float]:
    """Calculate nutrient load from wastewater treatment.

    Computes annual nutrient loads based on:
    - Water usage (dwellings * occupancy * water usage per person)
    - WwTW permit concentrations (mg/L)
    - 90% permit limit operating rate assumption
    - Unit conversions (litres/day -> litres/year, mg -> kg)

    This code recreates the functionality from line 287 (daily water) and lines 377-391
    (nutrient loads) of the original FullDFMScript250925.py. Note that lines 294-364
    (WwTW lookup and merge) are handled by the orchestration layer, not this function.

    Renaming of variables from legacy script:
    Dwellings -> dwellings
    Occ_Rate -> occupancy_rate
    Water_Usage_L_Day -> water_usage_litres_per_person_per_day
    Litres_used -> daily_water_litres
    Nitrogen_2025_2030 / Nitrogen_2030_onwards -> nitrogen_conc_mg_per_litre (caller selects)
    Phosphorus_2025_2030 / Phosphorus_2030_onwards -> phosphorus_conc_mg_per_litre (caller selects)

    Formula:
        daily_water_litres = dwellings * occupancy_rate * water_usage_per_person
        annual_water_litres = daily_water_litres * days_per_year
        N_load_kg = annual_water_litres * ((N_conc_mg/L / 1,000,000) * 0.9)
        P_load_kg = annual_water_litres * ((P_conc_mg/L / 1,000,000) * 0.9)

    Args:
        dwellings: Number of residential units
        occupancy_rate: People per dwelling (e.g., 2.4)
        water_usage_litres_per_person_per_day: Water consumption per person per day
        nitrogen_conc_mg_per_litre: N concentration at WwTW permit (mg/L)
        phosphorus_conc_mg_per_litre: P concentration at WwTW permit (mg/L)

    Returns:
        Tuple of (daily_water_litres, nitrogen_kg_per_year, phosphorus_kg_per_year).

    Note:
        Physical constants (DAYS_PER_YEAR, MILLIGRAMS_PER_KILOGRAM) are imported
        from worker.config.CONSTANTS.

        Assumes treatment works operate at 90% of permit limit concentrations,
        not 100%. This is a precautionary assumption for nutrient loading.
    """
    daily_water_litres = dwellings * (occupancy_rate * water_usage_litres_per_person_per_day)
    annual_water_litres = daily_water_litres * CONSTANTS.DAYS_PER_YEAR

    # Assume 90% permit limit operating rate (legacy lines 377-391)
    nitrogen_kg_per_year = annual_water_litres * (
        (nitrogen_conc_mg_per_litre / CONSTANTS.MILLIGRAMS_PER_KILOGRAM) * 0.9
    )
    phosphorus_kg_per_year = annual_water_litres * (
        (phosphorus_conc_mg_per_litre / CONSTANTS.MILLIGRAMS_PER_KILOGRAM) * 0.9
    )

    # Note: Legacy script does NOT round wastewater loads here - they are rounded
    # at the end in batch with N_Total/P_Total (see legacy line 443)

    return daily_water_litres, nitrogen_kg_per_year, phosphorus_kg_per_year
