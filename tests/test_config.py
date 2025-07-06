"""Tests for configuration module."""

from pathlib import Path

from src.bq_sql_antipattern_checker.config import AntipatternConfig, Config


class TestAntipatternConfig:
    """Test AntipatternConfig dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        config = AntipatternConfig()
        assert config.enabled is True
        assert config.description is None

    def test_custom_values(self):
        """Test custom values are set correctly."""
        config = AntipatternConfig(enabled=False, description="Test description")
        assert config.enabled is False
        assert config.description == "Test description"


class TestConfig:
    """Test Config class."""

    def test_from_yaml(self, sample_config_file: Path):
        """Test loading configuration from YAML file."""
        config = Config.from_yaml(sample_config_file)

        assert config.bigquery_job_project == "test-project"
        assert config.bigquery_dataset_project == "test-dataset-project"
        assert config.bigquery_dataset == "test_dataset"
        assert config.bigquery_region == "US"
        assert config.results_table_name == "test_antipattern_results"
        assert config.large_table_row_count == 1000
        assert config.distinct_function_row_count == 10000
        assert config.days_back == 1

    def test_antipattern_configuration(self, sample_config_file: Path):
        """Test antipattern configuration loading."""
        config = Config.from_yaml(sample_config_file)

        assert config.is_antipattern_enabled("select_star") is True
        assert config.is_antipattern_enabled("partition_used") is False

        enabled_antipatterns = config.get_enabled_antipatterns()
        assert "select_star" in enabled_antipatterns
        assert "partition_used" not in enabled_antipatterns

    def test_from_env(self, monkeypatch):
        """Test loading configuration from environment variables."""
        monkeypatch.setenv("BIGQUERY_JOB_PROJECT", "env-project")
        monkeypatch.setenv("DESTINATION_DATASET_PROJECT", "env-dataset-project")

        config = Config.from_env()
        assert config.bigquery_job_project == "env-project"
        assert config.bigquery_dataset_project == "env-dataset-project"

    def test_post_init_calculations(self, sample_config: Config):
        """Test that post-init calculations work correctly."""
        assert hasattr(sample_config, "job_run_date")
        assert hasattr(sample_config, "date_values")
        assert hasattr(sample_config, "table_names")

        assert "partition_date" in sample_config.date_values
        assert "query_run_date" in sample_config.date_values
        assert "query_run_date_str" in sample_config.date_values

        assert sample_config.table_names["results"] == "test_antipattern_results"

    def test_default_antipatterns(self, sample_config: Config):
        """Test that default antipatterns are created."""
        expected_antipatterns = [
            "select_star",
            "semi_join_without_aggregation",
            "order_without_limit",
            "regexp_in_where",
            "like_before_more_selective",
            "multiple_cte_reference",
            "partition_used",
            "big_date_range",
            "big_table_no_date",
            "unpartitioned_tables",
            "distinct_on_big_table",
            "count_distinct_on_big_table",
        ]

        for antipattern in expected_antipatterns:
            assert antipattern in sample_config.antipatterns
            assert isinstance(sample_config.antipatterns[antipattern], AntipatternConfig)
            assert sample_config.antipatterns[antipattern].enabled is True

    def test_is_antipattern_enabled_missing(self, sample_config: Config):
        """Test behavior when antipattern doesn't exist."""
        # Should return False for non-existent antipatterns due to default AntipatternConfig()
        assert sample_config.is_antipattern_enabled("non_existent_antipattern") is True
