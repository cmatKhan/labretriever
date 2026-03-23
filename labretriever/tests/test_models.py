"""
Tests for datainfo Pydantic models.

These tests validate the minimal, flexible models that parse HuggingFace dataset cards.

"""

import pytest
from pydantic import ValidationError

from labretriever.models import (
    DataFileInfo,
    DatasetCard,
    DatasetConfig,
    DatasetInfo,
    DatasetType,
    ExtractedMetadata,
    FeatureInfo,
    MetadataConfig,
    MetadataRelationship,
    PartitioningInfo,
)


class TestDatasetType:
    """Tests for DatasetType enum."""

    def test_dataset_type_values(self):
        """Test that all expected dataset types are defined."""
        assert DatasetType.GENOMIC_FEATURES == "genomic_features"
        assert DatasetType.ANNOTATED_FEATURES == "annotated_features"
        assert DatasetType.GENOME_MAP == "genome_map"
        assert DatasetType.METADATA == "metadata"
        assert DatasetType.COMPARATIVE == "comparative"

    def test_dataset_type_from_string(self):
        """Test creating DatasetType from string."""
        dt = DatasetType("genomic_features")
        assert dt == DatasetType.GENOMIC_FEATURES

    def test_invalid_dataset_type(self):
        """Test that invalid dataset type raises error."""
        with pytest.raises(ValueError):
            DatasetType("invalid_type")


class TestFeatureInfo:
    """Tests for FeatureInfo model."""

    def test_minimal_feature_info(self):
        """Test creating FeatureInfo with minimal fields."""
        feature = FeatureInfo(
            name="gene_id", dtype="string", description="Gene identifier"
        )
        assert feature.name == "gene_id"
        assert feature.dtype == "string"
        assert feature.description == "Gene identifier"
        assert feature.role is None
        assert feature.definitions is None

    def test_feature_info_with_role(self):
        """Test FeatureInfo with role field."""
        feature = FeatureInfo(
            name="condition",
            dtype="string",
            description="Experimental condition",
            role="experimental_condition",
        )
        assert feature.role == "experimental_condition"

    def test_feature_info_with_definitions(self):
        """Test FeatureInfo with definitions for experimental_condition."""
        feature = FeatureInfo(
            name="condition",
            dtype={"class_label": {"names": ["control", "treated"]}},
            description="Treatment condition",
            role="experimental_condition",
            definitions={
                "control": {"temperature_celsius": 30},
                "treated": {"temperature_celsius": 37},
            },
        )
        assert feature.definitions is not None
        assert "control" in feature.definitions
        assert feature.definitions["control"]["temperature_celsius"] == 30

    def test_feature_info_with_dict_dtype(self):
        """Test FeatureInfo with class_label dtype."""
        feature = FeatureInfo(
            name="category",
            dtype={"class_label": {"names": ["A", "B", "C"]}},
            description="Categorical field",
        )
        assert isinstance(feature.dtype, dict)
        assert "class_label" in feature.dtype


class TestPartitioningInfo:
    """Tests for PartitioningInfo model."""

    def test_default_partitioning_info(self):
        """Test PartitioningInfo with defaults."""
        partitioning = PartitioningInfo()
        assert partitioning.enabled is False
        assert partitioning.partition_by is None
        assert partitioning.path_template is None

    def test_enabled_partitioning_info(self):
        """Test PartitioningInfo with partitioning enabled."""
        partitioning = PartitioningInfo(
            enabled=True,
            partition_by=["accession"],
            path_template="data/accession={accession}/*.parquet",
        )
        assert partitioning.enabled is True
        assert partitioning.partition_by == ["accession"]
        assert partitioning.path_template == "data/accession={accession}/*.parquet"


class TestDataFileInfo:
    """Tests for DataFileInfo model."""

    def test_default_data_file_info(self):
        """Test DataFileInfo with default split."""
        data_file = DataFileInfo(path="data.parquet")
        assert data_file.split == "train"
        assert data_file.path == "data.parquet"

    def test_custom_data_file_info(self):
        """Test DataFileInfo with custom split."""
        data_file = DataFileInfo(split="test", path="test_data.parquet")
        assert data_file.split == "test"
        assert data_file.path == "test_data.parquet"


class TestDatasetInfo:
    """Tests for DatasetInfo model."""

    def test_minimal_dataset_info(self):
        """Test DatasetInfo with minimal features."""
        dataset_info = DatasetInfo(
            features=[
                FeatureInfo(
                    name="gene_id", dtype="string", description="Gene identifier"
                )
            ]
        )
        assert len(dataset_info.features) == 1
        assert dataset_info.partitioning is None

    def test_dataset_info_with_partitioning(self):
        """Test DatasetInfo with partitioning."""
        dataset_info = DatasetInfo(
            features=[
                FeatureInfo(name="chr", dtype="string", description="Chromosome"),
                FeatureInfo(name="pos", dtype="int32", description="Position"),
            ],
            partitioning=PartitioningInfo(enabled=True, partition_by=["chr"]),
        )
        assert len(dataset_info.features) == 2
        assert dataset_info.partitioning.enabled is True  # type: ignore


class TestDatasetConfig:
    """Tests for DatasetConfig model."""

    def test_minimal_dataset_config(self):
        """Test DatasetConfig with minimal required fields."""
        config = DatasetConfig(
            config_name="test_data",
            description="Test dataset",
            dataset_type=DatasetType.ANNOTATED_FEATURES,
            data_files=[DataFileInfo(path="data.parquet")],
            dataset_info=DatasetInfo(
                features=[FeatureInfo(name="id", dtype="string", description="ID")]
            ),
        )
        assert config.config_name == "test_data"
        assert config.dataset_type == DatasetType.ANNOTATED_FEATURES
        assert config.default is False
        assert config.applies_to is None
        assert config.metadata_fields is None

    def test_dataset_config_with_applies_to(self):
        """Test DatasetConfig with applies_to for metadata."""
        config = DatasetConfig(
            config_name="metadata",
            description="Metadata",
            dataset_type=DatasetType.METADATA,
            applies_to=["data_config_1", "data_config_2"],
            data_files=[DataFileInfo(path="metadata.parquet")],
            dataset_info=DatasetInfo(
                features=[
                    FeatureInfo(
                        name="sample_id", dtype="string", description="Sample ID"
                    )
                ]
            ),
        )
        assert config.applies_to == ["data_config_1", "data_config_2"]

    def test_dataset_config_applies_to_validation_error(self):
        """Test that applies_to raises error for non-metadata configs."""
        with pytest.raises(ValidationError):
            DatasetConfig(
                config_name="data",
                description="Data",
                dataset_type=DatasetType.ANNOTATED_FEATURES,
                applies_to=["other_config"],
                data_files=[DataFileInfo(path="data.parquet")],
                dataset_info=DatasetInfo(
                    features=[FeatureInfo(name="id", dtype="string", description="ID")]
                ),
            )

    def test_dataset_config_with_metadata_fields(self):
        """Test DatasetConfig with metadata_fields."""
        config = DatasetConfig(
            config_name="data",
            description="Data",
            dataset_type=DatasetType.ANNOTATED_FEATURES,
            metadata_fields=["regulator_symbol", "condition"],
            data_files=[DataFileInfo(path="data.parquet")],
            dataset_info=DatasetInfo(
                features=[
                    FeatureInfo(
                        name="regulator_symbol", dtype="string", description="TF symbol"
                    ),
                    FeatureInfo(
                        name="condition", dtype="string", description="Condition"
                    ),
                ]
            ),
        )
        assert config.metadata_fields == ["regulator_symbol", "condition"]

    def test_dataset_config_empty_metadata_fields_error(self):
        """Test that empty metadata_fields raises error."""
        with pytest.raises(ValidationError):
            DatasetConfig(
                config_name="data",
                description="Data",
                dataset_type=DatasetType.ANNOTATED_FEATURES,
                metadata_fields=[],
                data_files=[DataFileInfo(path="data.parquet")],
                dataset_info=DatasetInfo(
                    features=[FeatureInfo(name="id", dtype="string", description="ID")]
                ),
            )

    def test_dataset_config_accepts_extra_fields(self):
        """Test that DatasetConfig accepts extra fields like experimental_conditions."""
        config_data = {
            "config_name": "data",
            "description": "Data",
            "dataset_type": "annotated_features",
            "experimental_conditions": {
                "temperature_celsius": 30,
                "media": {"name": "YPD"},
            },
            "data_files": [{"path": "data.parquet"}],
            "dataset_info": {
                "features": [{"name": "id", "dtype": "string", "description": "ID"}]
            },
        }
        config = DatasetConfig(**config_data)
        assert hasattr(config, "model_extra")
        assert "experimental_conditions" in config.model_extra


class TestDatasetCard:
    """Tests for DatasetCard model."""

    def test_minimal_dataset_card(self):
        """Test DatasetCard with minimal structure."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data",
                    description="Data",
                    dataset_type=DatasetType.ANNOTATED_FEATURES,
                    data_files=[DataFileInfo(path="data.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                )
            ]
        )
        assert len(card.configs) == 1

    def test_dataset_card_accepts_extra_fields(self):
        """Test that DatasetCard accepts extra top-level fields."""
        card_data = {
            "license": "mit",
            "pretty_name": "Test Dataset",
            "tags": ["biology", "genomics"],
            "experimental_conditions": {"strain_background": "BY4741"},
            "configs": [
                {
                    "config_name": "data",
                    "description": "Data",
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
        card = DatasetCard(**card_data)
        assert hasattr(card, "model_extra")
        assert "license" in card.model_extra
        assert "experimental_conditions" in card.model_extra

    def test_empty_configs_error(self):
        """Test that empty configs raises error."""
        with pytest.raises(ValidationError):
            DatasetCard(configs=[])

    def test_duplicate_config_names_error(self):
        """Test that duplicate config names raises error."""
        with pytest.raises(ValidationError):
            DatasetCard(
                configs=[
                    DatasetConfig(
                        config_name="data",
                        description="Data 1",
                        dataset_type=DatasetType.ANNOTATED_FEATURES,
                        data_files=[DataFileInfo(path="data1.parquet")],
                        dataset_info=DatasetInfo(
                            features=[
                                FeatureInfo(name="id", dtype="string", description="ID")
                            ]
                        ),
                    ),
                    DatasetConfig(
                        config_name="data",
                        description="Data 2",
                        dataset_type=DatasetType.ANNOTATED_FEATURES,
                        data_files=[DataFileInfo(path="data2.parquet")],
                        dataset_info=DatasetInfo(
                            features=[
                                FeatureInfo(name="id", dtype="string", description="ID")
                            ]
                        ),
                    ),
                ]
            )

    def test_multiple_default_configs_error(self):
        """Test that multiple default configs raises error."""
        with pytest.raises(ValidationError):
            DatasetCard(
                configs=[
                    DatasetConfig(
                        config_name="data1",
                        description="Data 1",
                        dataset_type=DatasetType.ANNOTATED_FEATURES,
                        default=True,
                        data_files=[DataFileInfo(path="data1.parquet")],
                        dataset_info=DatasetInfo(
                            features=[
                                FeatureInfo(name="id", dtype="string", description="ID")
                            ]
                        ),
                    ),
                    DatasetConfig(
                        config_name="data2",
                        description="Data 2",
                        dataset_type=DatasetType.ANNOTATED_FEATURES,
                        default=True,
                        data_files=[DataFileInfo(path="data2.parquet")],
                        dataset_info=DatasetInfo(
                            features=[
                                FeatureInfo(name="id", dtype="string", description="ID")
                            ]
                        ),
                    ),
                ]
            )

    def test_get_config_by_name(self):
        """Test get_config_by_name method."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data1",
                    description="Data 1",
                    dataset_type=DatasetType.ANNOTATED_FEATURES,
                    data_files=[DataFileInfo(path="data1.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
                DatasetConfig(
                    config_name="data2",
                    description="Data 2",
                    dataset_type=DatasetType.METADATA,
                    data_files=[DataFileInfo(path="data2.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
            ]
        )
        config = card.get_config_by_name("data1")
        assert config is not None
        assert config.config_name == "data1"
        assert card.get_config_by_name("nonexistent") is None

    def test_get_configs_by_type(self):
        """Test get_configs_by_type method."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data",
                    description="Data",
                    dataset_type=DatasetType.ANNOTATED_FEATURES,
                    data_files=[DataFileInfo(path="data.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
                DatasetConfig(
                    config_name="metadata",
                    description="Metadata",
                    dataset_type=DatasetType.METADATA,
                    data_files=[DataFileInfo(path="metadata.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
            ]
        )
        data_configs = card.get_configs_by_type(DatasetType.ANNOTATED_FEATURES)
        assert len(data_configs) == 1
        assert data_configs[0].config_name == "data"

    def test_get_default_config(self):
        """Test get_default_config method."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data1",
                    description="Data 1",
                    dataset_type=DatasetType.ANNOTATED_FEATURES,
                    data_files=[DataFileInfo(path="data1.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
                DatasetConfig(
                    config_name="data2",
                    description="Data 2",
                    dataset_type=DatasetType.ANNOTATED_FEATURES,
                    default=True,
                    data_files=[DataFileInfo(path="data2.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
            ]
        )
        default = card.default_config
        assert default is not None
        assert default.config_name == "data2"

    def test_get_data_configs(self):
        """Test get_data_configs method."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data",
                    description="Data",
                    dataset_type=DatasetType.ANNOTATED_FEATURES,
                    data_files=[DataFileInfo(path="data.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
                DatasetConfig(
                    config_name="metadata",
                    description="Metadata",
                    dataset_type=DatasetType.METADATA,
                    data_files=[DataFileInfo(path="metadata.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
            ]
        )
        data_configs = card.get_data_configs()
        assert len(data_configs) == 1
        assert data_configs[0].dataset_type != DatasetType.METADATA

    def test_get_metadata_configs(self):
        """Test get_metadata_configs method."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data",
                    description="Data",
                    dataset_type=DatasetType.ANNOTATED_FEATURES,
                    data_files=[DataFileInfo(path="data.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
                DatasetConfig(
                    config_name="metadata",
                    description="Metadata",
                    dataset_type=DatasetType.METADATA,
                    data_files=[DataFileInfo(path="metadata.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
            ]
        )
        metadata_configs = card.get_metadata_configs()
        assert len(metadata_configs) == 1
        assert metadata_configs[0].dataset_type == DatasetType.METADATA


class TestExtractedMetadata:
    """Tests for ExtractedMetadata model."""

    def test_extracted_metadata_creation(self):
        """Test creating ExtractedMetadata."""
        metadata = ExtractedMetadata(
            config_name="test_config",
            field_name="regulator_symbol",
            values={"CBF1", "GAL4", "GCN4"},
            extraction_method="distinct",
        )
        assert metadata.config_name == "test_config"
        assert metadata.field_name == "regulator_symbol"
        assert len(metadata.values) == 3
        assert "CBF1" in metadata.values


class TestMetadataRelationship:
    """Tests for MetadataRelationship model."""

    def test_metadata_relationship_creation(self):
        """Test creating MetadataRelationship."""
        relationship = MetadataRelationship(
            data_config="binding_data",
            metadata_config="experiment_metadata",
            relationship_type="explicit",
        )
        assert relationship.data_config == "binding_data"
        assert relationship.metadata_config == "experiment_metadata"
        assert relationship.relationship_type == "explicit"


# ------------------------------------------------------------------
# Minimal valid YAML snippets reused across MetadataConfig tests
# ------------------------------------------------------------------

_MINIMAL_CONFIG = {
    "repositories": {
        "BrentLab/harbison": {
            "dataset": {
                "harbison_2004": {
                    "sample_id": {"field": "sample_id"},
                }
            }
        }
    }
}


class TestMetadataConfig:
    """Tests for MetadataConfig Pydantic model validation."""

    def test_valid_minimal_config(self):
        """Minimal config with one repo and one dataset parses successfully."""
        config = MetadataConfig.model_validate(_MINIMAL_CONFIG)
        assert "BrentLab/harbison" in config.repositories

    def test_missing_repositories_key_raises(self):
        """Config missing 'repositories' raises ValueError."""
        with pytest.raises((ValidationError, ValueError)):
            MetadataConfig.model_validate({})

    def test_empty_repositories_raises(self):
        """Config with empty 'repositories' dict raises ValueError."""
        with pytest.raises((ValidationError, ValueError)):
            MetadataConfig.model_validate({"repositories": {}})

    def test_repository_with_no_dataset_raises(self):
        """Repository with no 'dataset' key raises ValueError."""
        with pytest.raises((ValidationError, ValueError)):
            MetadataConfig.model_validate({"repositories": {"BrentLab/harbison": {}}})

    def test_optional_sections_absent_succeeds(self):
        """Parsing succeeds when optional sections are absent."""
        config = MetadataConfig.model_validate(_MINIMAL_CONFIG)
        assert config.factor_aliases == {}
        assert config.missing_value_labels == {}

    def test_optional_sections_present(self):
        """Optional sections are parsed correctly when present."""
        data = {
            "repositories": {
                "BrentLab/harbison": {
                    "dataset": {
                        "harbison_2004": {
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                }
            },
            "factor_aliases": {"carbon_source": {"glucose": ["glu", "dextrose"]}},
            "missing_value_labels": {"carbon_source": "unspecified"},
        }
        config = MetadataConfig.model_validate(data)
        assert "carbon_source" in config.factor_aliases
        assert config.missing_value_labels != {}

    def test_duplicate_db_name_raises(self):
        """Duplicate db_name across datasets raises ValueError."""
        with pytest.raises((ValidationError, ValueError)):
            MetadataConfig.model_validate(
                {
                    "repositories": {
                        "BrentLab/harbison": {
                            "dataset": {
                                "harbison_2004": {
                                    "db_name": "shared_name",
                                    "sample_id": {"field": "sample_id"},
                                }
                            }
                        },
                        "BrentLab/kemmeren": {
                            "dataset": {
                                "kemmeren_2014": {
                                    "db_name": "shared_name",
                                    "sample_id": {"field": "sample_id"},
                                }
                            }
                        },
                    }
                }
            )
