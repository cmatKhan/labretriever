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
    def test_get_repository_info_success(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
        sample_repo_structure,
    ):
        """Test getting repository information."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.fetch.return_value = sample_repo_structure

        datacard = DataCard(test_repo_id)

        info = datacard.get_repository_info()

        assert info["repo_id"] == test_repo_id
        assert info["pretty_name"] == "Test Genomics Dataset"
        assert info["license"] == "mit"
        assert info["num_configs"] == 4
        assert "genomic_features" in info["dataset_types"]
        assert "annotated_features" in info["dataset_types"]
        assert "genome_map" in info["dataset_types"]
        assert "metadata" in info["dataset_types"]
        assert info["total_files"] == 5
        assert info["last_modified"] == "2023-12-01T10:30:00Z"
        assert info["has_default_config"] is True

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_get_repository_info_fetch_error(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
    ):
        """Test getting repository info when structure fetch fails."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.fetch.side_effect = HfDataFetchError(
            "Structure fetch failed"
        )

        datacard = DataCard(test_repo_id)

        info = datacard.get_repository_info()

        assert info["repo_id"] == test_repo_id
        assert info["total_files"] is None
        assert info["last_modified"] is None

    @patch("labretriever.datacard.HfDataCardFetcher")
    @patch("labretriever.datacard.HfRepoStructureFetcher")
    @patch("labretriever.datacard.HfSizeInfoFetcher")
    def test_summary(
        self,
        mock_size_fetcher,
        mock_structure_fetcher,
        mock_card_fetcher,
        test_repo_id,
        sample_dataset_card_data,
        sample_repo_structure,
    ):
        """Test getting a summary of the dataset."""
        mock_card_fetcher_instance = Mock()
        mock_structure_fetcher_instance = Mock()
        mock_card_fetcher.return_value = mock_card_fetcher_instance
        mock_structure_fetcher.return_value = mock_structure_fetcher_instance

        mock_card_fetcher_instance.fetch.return_value = sample_dataset_card_data
        mock_structure_fetcher_instance.fetch.return_value = sample_repo_structure

        datacard = DataCard(test_repo_id)

        summary = datacard.summary()

        assert "Dataset: Test Genomics Dataset" in summary
        assert f"Repository: {test_repo_id}" in summary
        assert "License: mit" in summary
        assert "Configurations: 4" in summary
        assert "genomic_features" in summary
        assert "binding_data" in summary
        assert "genome_map_data" in summary
        assert "experiment_metadata" in summary
        assert "(default)" in summary  # genomic_features is marked as default

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
