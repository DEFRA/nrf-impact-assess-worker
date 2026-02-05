"""Unit tests for business logic calculators.

Tests all calculator functions with known inputs/outputs from legacy script.
"""

import pytest

from worker.calculators import (
    apply_buffer,
    apply_suds_mitigation,
    calculate_land_use_uplift,
    calculate_wastewater_load,
)
from worker.config import SuDsConfig


class TestLandUseCalculator:
    """Tests for land use change uplift calculations."""

    def test_positive_uplift(self):
        """Test land use change with positive nutrient uplift."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=1.5,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
        )

        assert n_uplift == 22.5  # (25 - 10) * 1.5
        assert p_uplift == 4.5  # (5 - 2) * 1.5

    def test_negative_uplift(self):
        """Test land use change with negative nutrient uplift (improvement)."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=2.0,
            current_nitrogen_coeff=30.0,
            residential_nitrogen_coeff=15.0,
            current_phosphorus_coeff=8.0,
            residential_phosphorus_coeff=3.0,
        )

        assert n_uplift == -30.0  # (15 - 30) * 2.0
        assert p_uplift == -10.0  # (3 - 8) * 2.0

    def test_zero_area(self):
        """Test with zero development area."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=0.0,
            current_nitrogen_coeff=10.0,
            residential_nitrogen_coeff=25.0,
            current_phosphorus_coeff=2.0,
            residential_phosphorus_coeff=5.0,
        )

        assert n_uplift == 0.0
        assert p_uplift == 0.0

    def test_rounding(self):
        """Test that results are rounded to 2 decimal places."""
        n_uplift, p_uplift = calculate_land_use_uplift(
            area_hectares=1.333,
            current_nitrogen_coeff=10.777,
            residential_nitrogen_coeff=25.888,
            current_phosphorus_coeff=2.111,
            residential_phosphorus_coeff=5.999,
        )

        # Verify rounding to 2 decimal places
        assert n_uplift == round((25.888 - 10.777) * 1.333, 2)
        assert p_uplift == round((5.999 - 2.111) * 1.333, 2)


class TestSuDsMitigationCalculator:
    """Tests for SuDS mitigation calculations."""

    @pytest.fixture
    def default_suds_config(self):
        """Default SuDS configuration from legacy script."""
        return SuDsConfig(
            threshold_dwellings=50,
            flow_capture_percent=100.0,
            removal_rate_percent=25.0,
        )

    def test_positive_uplift_with_suds(self, default_suds_config):
        """Test SuDS applied to positive nutrient uplift."""
        n_post_suds, p_post_suds = apply_suds_mitigation(
            nitrogen_uplift=22.5,
            phosphorus_uplift=4.5,
            dwelling_count=100,
            suds_config=default_suds_config,
        )

        # 25% reduction: 22.5 - (22.5 * 0.25) = 16.875 ≈ 16.88
        assert n_post_suds == 16.88
        # 4.5 - (4.5 * 0.25) = 3.375 ≈ 3.38
        assert p_post_suds == 3.38

    def test_negative_uplift_with_suds(self, default_suds_config):
        """Test SuDS applied to negative uplift (uses absolute value)."""
        n_post_suds, p_post_suds = apply_suds_mitigation(
            nitrogen_uplift=-30.0,
            phosphorus_uplift=-10.0,
            dwelling_count=100,
            suds_config=default_suds_config,
        )

        # -30.0 - (|-30.0| * 0.25) = -30.0 - 7.5 = -37.5
        assert n_post_suds == -37.5
        # -10.0 - (|-10.0| * 0.25) = -10.0 - 2.5 = -12.5
        assert p_post_suds == -12.5

    def test_zero_uplift_with_suds(self, default_suds_config):
        """Test SuDS with zero uplift."""
        n_post_suds, p_post_suds = apply_suds_mitigation(
            nitrogen_uplift=0.0,
            phosphorus_uplift=0.0,
            dwelling_count=100,
            suds_config=default_suds_config,
        )

        assert n_post_suds == 0.0
        assert p_post_suds == 0.0

    def test_below_threshold_still_applies_suds(self, default_suds_config):
        """Test that SuDS applies even below threshold (legacy behavior)."""
        # Note: Legacy script applies SuDS to ALL developments, ignoring threshold
        n_post_suds, p_post_suds = apply_suds_mitigation(
            nitrogen_uplift=10.0,
            phosphorus_uplift=2.0,
            dwelling_count=10,  # Below 50 dwelling threshold
            suds_config=default_suds_config,
        )

        # SuDS still applied
        assert n_post_suds == 7.5  # 10 - (10 * 0.25)
        assert p_post_suds == 1.5  # 2 - (2 * 0.25)

    def test_custom_suds_config(self):
        """Test with custom SuDS configuration."""
        custom_config = SuDsConfig(
            threshold_dwellings=100,
            flow_capture_percent=75.0,  # Only 75% captured
            removal_rate_percent=50.0,  # 50% removal rate
        )

        n_post_suds, p_post_suds = apply_suds_mitigation(
            nitrogen_uplift=20.0,
            phosphorus_uplift=4.0,
            dwelling_count=100,
            suds_config=custom_config,
        )

        # Reduction = 0.75 * 0.50 = 0.375
        assert n_post_suds == 12.5  # 20 - (20 * 0.375)
        assert p_post_suds == 2.5  # 4 - (4 * 0.375)


class TestWastewaterLoadCalculator:
    """Tests for wastewater nutrient load calculations."""

    def test_basic_wastewater_load(self):
        """Test basic wastewater load calculation."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=100,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=10.0,
            phosphorus_conc_mg_per_litre=1.0,
        )

        # Daily water: 100 * (2.4 * 110) = 26,400 L
        assert daily_water == 26400.0

        # Annual water: 26,400 * 365.25 = 9,642,600 L
        # N load: 9,642,600 * ((10 / 1,000,000) * 0.9) = 86.7834 kg
        # (assumes 90% permit limit operating rate, not rounded - as rounding done elsewhere)
        assert n_load == 86.7834

        # P load: 9,642,600 * ((1 / 1,000,000) * 0.9) = 8.67834 kg
        # (assumes 90% permit limit operating rate, not rounded - as rounding done elsewhere)
        assert p_load == 8.67834

    def test_single_dwelling(self):
        """Test with single dwelling."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=1,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=10.0,
            phosphorus_conc_mg_per_litre=1.0,
        )

        # Daily water: 1 * (2.4 * 110) = 264 L
        assert daily_water == 264.0

        # Annual: 264 * 365.25 = 96,426 L
        # N: 96,426 * ((10 / 1M) * 0.9) = 0.867834 kg
        # (assumes 90% permit limit operating rate, not rounded)
        assert n_load == pytest.approx(0.867834)
        # P: 96,426 * ((1 / 1M) * 0.9) = 0.0867834 kg
        # (assumes 90% permit limit operating rate, not rounded)
        assert p_load == pytest.approx(0.0867834)

    def test_zero_concentration(self):
        """Test with zero WwTW permit concentration."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=50,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=0.0,  # Perfect treatment
            phosphorus_conc_mg_per_litre=0.0,
        )

        assert daily_water == 13200.0
        assert n_load == 0.0
        assert p_load == 0.0

    def test_high_concentration(self):
        """Test with high nutrient concentrations."""
        daily_water, n_load, p_load = calculate_wastewater_load(
            dwellings=10,
            occupancy_rate=2.4,
            water_usage_litres_per_person_per_day=110.0,
            nitrogen_conc_mg_per_litre=50.0,  # High N
            phosphorus_conc_mg_per_litre=10.0,  # High P
        )

        assert daily_water == 2640.0

        # Annual: 2640 * 365.25 = 964,260 L
        # N: 964,260 * ((50 / 1M) * 0.9) = 43.3917 kg
        # (assumes 90% permit limit operating rate, not rounded)
        assert n_load == pytest.approx(43.3917)

        # P: 964,260 * ((10 / 1M) * 0.9) = 8.67834 kg
        # (assumes 90% permit limit operating rate, not rounded)
        assert p_load == pytest.approx(8.67834)


class TestTotalImpactCalculator:
    """Tests for total nutrient impact with precautionary buffer."""

    def test_both_components_positive(self):
        """Test with positive land use and wastewater impacts."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=16.88,
            phosphorus_land_use_post_suds=3.38,
            nitrogen_wastewater=96.53,
            phosphorus_wastewater=9.65,
            precautionary_buffer_percent=20.0,
        )

        # N base: 16.88 + 96.53 = 113.41
        # N buffer: 113.41 * 0.20 = 22.682
        # N total: 113.41 + 22.682 = 136.092 (not rounded - as rounding done elsewhere)
        assert n_total == pytest.approx(136.092)

        # P base: 3.38 + 9.65 = 13.03
        # P buffer: 13.03 * 0.20 = 2.606
        # P total: 13.03 + 2.606 = 15.636 (not rounded - as rounding done elsewhere)
        assert p_total == pytest.approx(15.636)

    def test_negative_land_use_positive_wastewater(self):
        """Test with negative land use (improvement) and positive wastewater."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=-37.5,
            phosphorus_land_use_post_suds=-12.5,
            nitrogen_wastewater=96.53,
            phosphorus_wastewater=9.65,
            precautionary_buffer_percent=20.0,
        )

        # N base: -37.5 + 96.53 = 59.03
        # N buffer: |59.03| * 0.20 = 11.806
        # N total: 59.03 + 11.806 = 70.836 (not rounded - as rounding done elsewhere)
        assert n_total == 70.836

        # P base: -12.5 + 9.65 = -2.85
        # P buffer: |-2.85| * 0.20 = 0.57
        # P total: -2.85 + 0.57 = -2.28
        assert p_total == -2.28

    def test_land_use_only(self):
        """Test with only land use impact (no wastewater)."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=16.88,
            phosphorus_land_use_post_suds=3.38,
            nitrogen_wastewater=0.0,
            phosphorus_wastewater=0.0,
            precautionary_buffer_percent=20.0,
        )

        # N base: 16.88 + 0 = 16.88
        # N buffer: 16.88 * 0.20 = 3.376
        # N total: 16.88 + 3.376 = 20.256 (not rounded - as rounding done elsewhereg)
        assert n_total == 20.256

        # P base: 3.38 + 0 = 3.38
        # P buffer: 3.38 * 0.20 = 0.676
        # P total: 3.38 + 0.676 = 4.056 (not rounded - as rounding done elsewhere)
        assert p_total == 4.056

    def test_wastewater_only(self):
        """Test with only wastewater impact (no land use in NN catchment)."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=0.0,
            phosphorus_land_use_post_suds=0.0,
            nitrogen_wastewater=96.53,
            phosphorus_wastewater=9.65,
            precautionary_buffer_percent=20.0,
        )

        # N base: 0 + 96.53 = 96.53
        # N buffer: 96.53 * 0.20 = 19.306
        # N total: 96.53 + 19.306 = 115.836 (not rounded - as rounding done elsewhere)
        assert n_total == 115.836

        # P base: 0 + 9.65 = 9.65
        # P buffer: 9.65 * 0.20 = 1.93
        # P total: 9.65 + 1.93 = 11.58
        assert p_total == 11.58

    def test_all_zero(self):
        """Test with zero impacts."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=0.0,
            phosphorus_land_use_post_suds=0.0,
            nitrogen_wastewater=0.0,
            phosphorus_wastewater=0.0,
            precautionary_buffer_percent=20.0,
        )

        assert n_total == 0.0
        assert p_total == 0.0

    def test_different_buffer_percent(self):
        """Test with different precautionary buffer percentage."""
        n_total, p_total = apply_buffer(
            nitrogen_land_use_post_suds=10.0,
            phosphorus_land_use_post_suds=2.0,
            nitrogen_wastewater=90.0,
            phosphorus_wastewater=8.0,
            precautionary_buffer_percent=10.0,  # 10% buffer instead of 20%
        )

        # N base: 10 + 90 = 100
        # N buffer: 100 * 0.10 = 10
        # N total: 100 + 10 = 110
        assert n_total == 110.0

        # P base: 2 + 8 = 10
        # P buffer: 10 * 0.10 = 1
        # P total: 10 + 1 = 11
        assert p_total == 11.0
