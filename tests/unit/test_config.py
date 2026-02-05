"""Unit tests for configuration."""


def test_default_config_values():
    """Test default configuration values."""
    from worker.config import DEFAULT_CONFIG

    assert DEFAULT_CONFIG.precautionary_buffer_percent == 20.0
    assert DEFAULT_CONFIG.suds.threshold_dwellings == 50
    assert DEFAULT_CONFIG.suds.removal_rate_percent == 25.0
    assert DEFAULT_CONFIG.fallback_wwtw_id == 141


def test_suds_reduction_calculation():
    """Test SuDS total reduction factor calculation."""
    from worker.config import SuDsConfig

    suds = SuDsConfig(
        threshold_dwellings=50,
        flow_capture_percent=100.0,
        removal_rate_percent=25.0,
    )

    # 100% capture * 25% removal = 0.25 total reduction
    assert suds.total_reduction_factor == 0.25


def test_precautionary_buffer_calculation():
    """Test precautionary buffer factor calculation."""
    from worker.config import AssessmentConfig

    config = AssessmentConfig(precautionary_buffer_percent=20.0)

    # 20% = 0.20 factor
    assert config.precautionary_buffer_factor == 0.20
