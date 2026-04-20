"""Tests for the DataCard class."""

from unittest.mock import Mock, patch

import pytest

from labretriever import DataCard
from labretriever.datacard import DatasetSchema
from labretriever.errors import DataCardError, DataCardValidationError, HfDataFetchError
from labretriever.models import DatasetType


def _external_metadata_card_data():
    """Card data with external metadata (no embedded metadata_fields)."""
    return {
        "configs": [
            {
                "config_name": "coverage_data",
                "description": "Coverage measurements",
                "dataset_type": "genome_map",
                "default": True,
                "data_files": [{"split": "train", "path": "coverage.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "sample_id",
                            "dtype": "integer",
                            "description": "Sample ID",
                        },
                        {
                            "name": "chr",
                            "dtype": "string",
                            "description": "Chromosome",
                            "role": "genomic_coordinate",
                        },
                        {
                            "name": "coverage",
                            "dtype": "float32",
                            "description": "Coverage value",
                            "role": "quantitative_measure",
                        },
                    ]
                },
            },
            {
                "config_name": "sample_metadata",
                "description": "Sample metadata",
                "dataset_type": "metadata",
                "applies_to": ["coverage_data"],
                "data_files": [{"split": "train", "path": "metadata.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "sample_id",
                            "dtype": "integer",
                            "description": "Sample ID",
                        },
                        {
                            "name": "batch",
                            "dtype": "string",
                            "description": "Batch ID",
                        },
                        {
                            "name": "regulator_locus_tag",
                            "dtype": "string",
                            "description": "TF locus tag",
                            "role": "regulator_identifier",
                        },
                        {
                            "name": "regulator_symbol",
                            "dtype": "string",
                            "description": "TF symbol",
                            "role": "regulator_identifier",
                        },
                    ]
                },
            },
        ],
    }


class TestDataCard:
    """Test suite for DataCard class."""

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_init(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        test_token,
    ):
        """Test DataCard initialization."""
        datacard = DataCard(test_repo_id, token=test_token)

        assert datacard.repo_id == test_repo_id
        assert datacard.token == test_token
        assert datacard._dataset_card is None
        assert datacard._metadata_cache == {}
        assert datacard._metadata_fields_map == {}

        # Check that fetchers were initialized
        mock_card_fetcher.assert_called_once_with(token=test_token)
        mock_structure_fetcher.assert_called_once_with(token=test_token)
        mock_size_fetcher.assert_called_once_with(token=test_token)

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_init_without_token(
        self, mock_size_fetcher, mock_structure_fetcher, mock_card_fetcher, test_repo_id
    ):
        """Test DataCard initialization without token."""
        datacard = DataCard(test_repo_id)

        assert datacard.repo_id == test_repo_id
        assert datacard.token is None

        # Check that fetchers were initialized without token
        mock_card_fetcher.assert_called_once_with(token=None)
        mock_structure_fetcher.assert_called_once_with(token=None)
        mock_size_fetcher.assert_called_once_with(token=None)

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_load_and_validate_card_success(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test successful card loading and validation."""
        # Setup mock
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)

        # Access dataset_card property to trigger loading
        card = datacard.dataset_card

        assert card is not None
        assert len(card.configs) == 4
        assert card.pretty_name == "Test Genomics Dataset"
        mock_fetcher_instance.fetch.assert_called_once_with(test_repo_id)

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_load_card_no_data(
        self, mock_size_fetcher, mock_structure_fetcher, mock_card_fetcher, test_repo_id
    ):
        """Test handling when no dataset card is found."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = {}

        datacard = DataCard(test_repo_id)

        with pytest.raises(DataCardValidationError, match="No dataset card found"):
            _ = datacard.dataset_card

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_load_card_validation_error(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        invalid_dataset_card_data,
    ):
        """Test handling of validation errors."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = invalid_dataset_card_data

        datacard = DataCard(test_repo_id)

        with pytest.raises(
            DataCardValidationError, match="Dataset card validation failed"
        ):
            _ = datacard.dataset_card

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_load_card_fetch_error(
        self, mock_size_fetcher, mock_structure_fetcher, mock_card_fetcher, test_repo_id
    ):
        """Test handling of fetch errors."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.side_effect = HfDataFetchError("Fetch failed")

        datacard = DataCard(test_repo_id)

        with pytest.raises(DataCardError, match="Failed to fetch dataset card"):
            _ = datacard.dataset_card

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_configs_property(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test getting all configurations via property."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        configs = datacard.configs

        assert len(configs) == 4
        config_names = [config.config_name for config in configs]
        assert "genomic_features" in config_names
        assert "binding_data" in config_names
        assert "genome_map_data" in config_names
        assert "experiment_metadata" in config_names

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_get_config_by_name(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test getting a specific configuration by name."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)

        config = datacard.get_config("binding_data")
        assert config is not None
        assert config.config_name == "binding_data"
        assert config.dataset_type == DatasetType.ANNOTATED_FEATURES

        # Test non-existent config
        assert datacard.get_config("nonexistent") is None

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_get_metadata_relationships(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test getting metadata relationships."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)

        relationships = datacard.get_metadata_relationships()

        # Should have explicit relationship between binding_data and experiment_metadata
        explicit_rels = [r for r in relationships if r.relationship_type == "explicit"]
        assert len(explicit_rels) == 1
        assert explicit_rels[0].data_config == "binding_data"
        assert explicit_rels[0].metadata_config == "experiment_metadata"

        # Should have embedded relationship for binding_data (has metadata_fields)
        embedded_rels = [r for r in relationships if r.relationship_type == "embedded"]
        assert len(embedded_rels) == 1
        assert embedded_rels[0].data_config == "binding_data"
        assert embedded_rels[0].metadata_config == "binding_data_embedded"

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_info_repo_level(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
        sample_repo_structure,
    ):
        """Test info() without arguments returns repository-level metadata."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.fetch.return_value = sample_repo_structure

        datacard = DataCard(test_repo_id)

        info = datacard.info()

        assert info["repo_id"] == test_repo_id
        assert info["pretty_name"] == "Test Genomics Dataset"
        assert info["license"] == "mit"
        assert info["num_configs"] == 4
        assert info["total_files"] == 5
        assert info["last_modified"] == "2023-12-01T10:30:00Z"
        assert info["has_default_config"] is True
        config_types = [c["dataset_type"] for c in info["configs"]]
        assert "genomic_features" in config_types
        assert "annotated_features" in config_types
        assert "genome_map" in config_types
        assert "metadata" in config_types
        config_names = [c["config_name"] for c in info["configs"]]
        assert "genomic_features" in config_names
        assert "binding_data" in config_names
        assert "genome_map_data" in config_names
        assert "experiment_metadata" in config_names
        default_configs = [c for c in info["configs"] if c["default"]]
        assert len(default_configs) == 1

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_info_repo_level_fetch_error(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test info() degrades gracefully when structure fetch fails."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.fetch.side_effect = HfDataFetchError(
            "Structure fetch failed"
        )

        datacard = DataCard(test_repo_id)

        info = datacard.info()

        assert info["repo_id"] == test_repo_id
        assert info["total_files"] is None
        assert info["last_modified"] is None

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_info_dataset_level(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
        sample_repo_structure,
    ):
        """Test info(config_name) returns dataset-level metadata."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.fetch.return_value = sample_repo_structure

        datacard = DataCard(test_repo_id)

        info = datacard.info("genomic_features")

        assert info["config_name"] == "genomic_features"
        assert info["dataset_type"] == "genomic_features"
        assert info["default"] is True
        assert isinstance(info["description"], str)
        assert isinstance(info["features"], list)
        assert all("name" in f for f in info["features"])
        assert "experimental_conditions" in info
        assert "metadata_schema" in info

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_info_dataset_level_not_found(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test info() raises DataCardError for unknown config name."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)

        with pytest.raises(DataCardError):
            datacard.info("nonexistent_config")

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_extract_partition_values(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test extracting partition values."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.get_partition_values.return_value = [
            "TF1",
            "TF2",
            "TF3",
        ]

        datacard = DataCard(test_repo_id)

        # Get the genome_map_data config which has partitioning enabled
        config = datacard.get_config("genome_map_data")
        assert config is not None
        assert config.dataset_info.partitioning.enabled is True  # type: ignore

        values = datacard._extract_partition_values(config, "regulator")
        assert values == {"TF1", "TF2", "TF3"}
        mock_structure_fetcher_instance.get_partition_values.assert_called_once_with(
            test_repo_id, "regulator"
        )

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_extract_partition_values_no_partitioning(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test extracting partition values when partitioning is disabled."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)

        # Get a config without partitioning
        config = datacard.get_config("genomic_features")
        assert config is not None
        assert config.dataset_info.partitioning is None

        values = datacard._extract_partition_values(config, "some_field")
        assert values == set()
        mock_structure_fetcher_instance.get_partition_values.assert_not_called()

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_extract_partition_values_field_not_in_partitions(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test extracting partition values when field is not a partition column."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)

        # Get the genome_map_data config which has partitioning enabled
        config = datacard.get_config("genome_map_data")
        assert config is not None

        # Try to extract values for a field that's not in partition_by
        values = datacard._extract_partition_values(config, "not_a_partition_field")
        assert values == set()
        mock_structure_fetcher_instance.get_partition_values.assert_not_called()

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_extract_partition_values_fetch_error(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test extracting partition values when fetch fails."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.get_partition_values.side_effect = (
            HfDataFetchError("Fetch failed")
        )

        datacard = DataCard(test_repo_id)

        config = datacard.get_config("genome_map_data")
        values = datacard._extract_partition_values(config, "regulator")  # type: ignore

        # Should return empty set on error
        assert values == set()


class TestGetMetadataFields:
    """Tests for DataCard.get_metadata_fields()."""

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_embedded_metadata_fields(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Embedded metadata_fields on the data config are returned."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_metadata_fields("binding_data")

        assert result == ["regulator_symbol", "experimental_condition"]

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_external_metadata_fields(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """External metadata via applies_to returns feature names."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = _external_metadata_card_data()

        datacard = DataCard(test_repo_id)
        result = datacard.get_metadata_fields("coverage_data")

        assert result == [
            "sample_id",
            "batch",
            "regulator_locus_tag",
            "regulator_symbol",
        ]

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_no_metadata_returns_none(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Config with no metadata returns None."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_metadata_fields("genomic_features")

        assert result is None

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_unknown_config_returns_none(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Unknown config name returns None."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_metadata_fields("nonexistent")

        assert result is None

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_extract_schema_includes_external_features(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """extract_metadata_schema includes roles from external metadata."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = _external_metadata_card_data()

        datacard = DataCard(test_repo_id)
        schema = datacard.extract_metadata_schema("coverage_data")

        # External metadata features with role=regulator_identifier
        assert "regulator_locus_tag" in schema["regulator_fields"]
        assert "regulator_symbol" in schema["regulator_fields"]
        # metadata_fields key populated
        assert schema["metadata_fields"] is not None
        assert "sample_id" in schema["metadata_fields"]


class TestGetMetadataConfigName:
    """Tests for DataCard.get_metadata_config_name()."""

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_external_metadata_returns_config_name(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """Returns metadata config name when applies_to matches."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = _external_metadata_card_data()

        datacard = DataCard(test_repo_id)
        result = datacard.get_metadata_config_name("coverage_data")

        assert result == "sample_metadata"

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_embedded_metadata_returns_none(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Returns None when metadata is embedded."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_metadata_config_name("binding_data")

        assert result is None

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_unknown_config_returns_none(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Returns None for unknown config name."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_metadata_config_name("nonexistent")

        assert result is None


class TestGetDataColNames:
    """Tests for DataCard.get_data_col_names()."""

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_returns_feature_names(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Returns column names from the data config's features."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_data_col_names("binding_data")

        # binding_data features: regulator_symbol, target_gene,
        # experimental_condition, binding_score
        assert isinstance(result, set)
        assert result == {
            "regulator_symbol",
            "target_gene",
            "experimental_condition",
            "binding_score",
        }

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_external_metadata_config_returns_data_features(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """For external metadata, returns data config features only."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = _external_metadata_card_data()

        datacard = DataCard(test_repo_id)
        result = datacard.get_data_col_names("coverage_data")

        # coverage_data features: sample_id, chr, coverage
        assert result == {"sample_id", "chr", "coverage"}
        # Must NOT include metadata-only columns
        assert "batch" not in result
        assert "regulator_locus_tag" not in result

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_unknown_config_returns_empty_set(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Returns empty set for unknown config name."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_data_col_names("nonexistent")

        assert result == set()


class TestGetDatasetSchema:
    """Tests for DataCard.get_dataset_schema()."""

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_embedded_metadata_returns_correct_schema(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Embedded metadata produces correct data/metadata column split."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        # binding_data has metadata_fields: [regulator_symbol,
        # experimental_condition] and features: regulator_symbol,
        # target_gene, experimental_condition, binding_score
        result = datacard.get_dataset_schema("binding_data")

        assert result is not None
        assert isinstance(result, DatasetSchema)
        assert result.metadata_source == "embedded"
        assert result.external_metadata_config is None
        assert result.join_columns == set()
        assert result.metadata_columns == {
            "regulator_symbol",
            "experimental_condition",
        }
        # data_columns = all features minus metadata_columns
        assert result.data_columns == {
            "target_gene",
            "binding_score",
        }

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_external_metadata_returns_correct_schema(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """External metadata produces correct split and join columns."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = _external_metadata_card_data()

        datacard = DataCard(test_repo_id)
        # coverage_data features: sample_id, chr, coverage
        # sample_metadata features: sample_id, batch, regulator_locus_tag,
        #   regulator_symbol
        # join_columns = intersection = {sample_id}
        result = datacard.get_dataset_schema("coverage_data")

        assert result is not None
        assert result.metadata_source == "external"
        assert result.external_metadata_config == "sample_metadata"
        assert result.data_columns == {"sample_id", "chr", "coverage"}
        assert result.metadata_columns == {
            "sample_id",
            "batch",
            "regulator_locus_tag",
            "regulator_symbol",
        }
        assert result.join_columns == {"sample_id"}

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_no_metadata_returns_all_cols_as_data(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Config with no metadata relationship has all cols as data."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        # genomic_features has no metadata_fields and no applies_to
        result = datacard.get_dataset_schema("genomic_features")

        assert result is not None
        assert result.metadata_source == "none"
        assert result.external_metadata_config is None
        assert result.metadata_columns == set()
        assert result.join_columns == set()
        assert result.data_columns == {
            "gene_id",
            "gene_symbol",
            "chromosome",
            "start",
            "end",
        }

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_unknown_config_returns_none(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Returns None for an unknown config name."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)
        result = datacard.get_dataset_schema("nonexistent")

        assert result is None


class TestGetCitation:
    """Tests for DataCard.get_citation()."""

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_repository_level_citation_only(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """Test getting repository-level citation when no dataset-specific citation
        exists."""
        card_data = {
            "citation": "Repository citation for all datasets",
            "configs": [
                {
                    "config_name": "test_config",
                    "description": "Test config",
                    "dataset_type": "annotated_features",
                    "data_files": [{"path": "data.parquet"}],
                    "dataset_info": {
                        "features": [
                            {"name": "id", "dtype": "string", "description": "ID"}
                        ]
                    },
                }
            ],
        }

        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = card_data

        datacard = DataCard(test_repo_id)

        # Repository-level citation
        repo_citation = datacard.get_citation()
        assert repo_citation == "Repository citation for all datasets"

        # Dataset-specific should fall back to repository-level
        dataset_citation = datacard.get_citation("test_config")
        assert dataset_citation == "Repository citation for all datasets"

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_dataset_overrides_repository_citation(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """Test that dataset-level citation overrides repository-level citation."""
        card_data = {
            "citation": "Repository citation for all datasets",
            "configs": [
                {
                    "config_name": "special_dataset",
                    "description": "Special dataset with its own citation",
                    "dataset_type": "annotated_features",
                    "citation": "Special dataset citation that overrides repo citation",
                    "data_files": [{"path": "special.parquet"}],
                    "dataset_info": {
                        "features": [
                            {"name": "id", "dtype": "string", "description": "ID"}
                        ]
                    },
                },
                {
                    "config_name": "normal_dataset",
                    "description": "Normal dataset without citation",
                    "dataset_type": "annotated_features",
                    "data_files": [{"path": "normal.parquet"}],
                    "dataset_info": {
                        "features": [
                            {"name": "id", "dtype": "string", "description": "ID"}
                        ]
                    },
                },
            ],
        }

        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = card_data

        datacard = DataCard(test_repo_id)

        # Repository-level citation
        repo_citation = datacard.get_citation()
        assert repo_citation == "Repository citation for all datasets"

        # Dataset with specific citation should override repository
        special_citation = datacard.get_citation("special_dataset")
        assert (
            special_citation == "Special dataset citation that overrides repo citation"
        )

        # Dataset without specific citation should fall back to repository
        normal_citation = datacard.get_citation("normal_dataset")
        assert normal_citation == "Repository citation for all datasets"

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_no_citation_returns_none(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
    ):
        """Test that None is returned when no citation is defined at any level."""
        card_data = {
            "configs": [
                {
                    "config_name": "test_config",
                    "description": "Test config",
                    "dataset_type": "annotated_features",
                    "data_files": [{"path": "data.parquet"}],
                    "dataset_info": {
                        "features": [
                            {"name": "id", "dtype": "string", "description": "ID"}
                        ]
                    },
                }
            ],
        }

        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = card_data

        datacard = DataCard(test_repo_id)

        # No citation at any level should return None
        repo_citation = datacard.get_citation()
        assert repo_citation is None

        dataset_citation = datacard.get_citation("test_config")
        assert dataset_citation is None

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_unknown_config_raises_error(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test that unknown config name raises DataCardError."""
        mock_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_fetcher_instance
        mock_fetcher_instance.fetch.return_value = sample_dataset_card_data

        datacard = DataCard(test_repo_id)

        with pytest.raises(
            DataCardError, match="Configuration 'nonexistent' not found"
        ):
            datacard.get_citation("nonexistent")
