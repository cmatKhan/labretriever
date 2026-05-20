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
    ExtractedMetadata,
    FeatureInfo,
    MetadataConfig,
    MetadataRelationship,
    PartitioningInfo,
    SharedFeatureGroup,
)


class TestDatasetType:
    """Tests for dataset_type string field behavior."""

    def test_reserved_types_are_strings(self):
        """Reserved types are plain string constants."""
        from labretriever.models import DATASET_TYPE_COMPARATIVE, DATASET_TYPE_METADATA

        assert DATASET_TYPE_METADATA == "metadata"
        assert DATASET_TYPE_COMPARATIVE == "comparative"

    def test_arbitrary_dataset_type_accepted(self):
        """Any string is accepted as dataset_type."""
        cfg = DatasetConfig(
            config_name="c",
            dataset_type="my_collection_type",
            data_files=[DataFileInfo(path="f.parquet")],
            dataset_info=DatasetInfo(features=[]),
        )
        assert cfg.dataset_type == "my_collection_type"

    def test_reserved_type_metadata_accepted(self):
        """The reserved type 'metadata' parses correctly."""
        cfg = DatasetConfig(
            config_name="c",
            dataset_type="metadata",
            data_files=[DataFileInfo(path="f.parquet")],
            dataset_info=DatasetInfo(features=[]),
        )
        assert cfg.dataset_type == "metadata"


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
            dataset_type="annotated_features",
            data_files=[DataFileInfo(path="data.parquet")],
            dataset_info=DatasetInfo(
                features=[FeatureInfo(name="id", dtype="string", description="ID")]
            ),
        )
        assert config.config_name == "test_data"
        assert config.dataset_type == "annotated_features"
        assert config.default is False
        assert config.applies_to is None
        assert config.metadata_fields is None

    def test_dataset_config_with_applies_to(self):
        """Test DatasetConfig with applies_to for metadata."""
        config = DatasetConfig(
            config_name="metadata",
            description="Metadata",
            dataset_type="metadata",
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
                dataset_type="annotated_features",
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
            dataset_type="annotated_features",
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
                dataset_type="annotated_features",
                metadata_fields=[],
                data_files=[DataFileInfo(path="data.parquet")],
                dataset_info=DatasetInfo(
                    features=[FeatureInfo(name="id", dtype="string", description="ID")]
                ),
            )

    def test_dataset_config_accepts_experimental_conditions(self):
        """Test that DatasetConfig parses experimental_conditions as a declared
        field."""
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
        assert config.experimental_conditions == {
            "temperature_celsius": 30,
            "media": {"name": "YPD"},
        }
        assert "experimental_conditions" not in config.model_extra


class TestDatasetCard:
    """Tests for DatasetCard model."""

    def test_minimal_dataset_card(self):
        """Test DatasetCard with minimal structure."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data",
                    description="Data",
                    dataset_type="annotated_features",
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
        """Test that DatasetCard parses experimental_conditions as a declared field
        while still accepting other arbitrary top-level fields via model_extra."""
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
        assert card.experimental_conditions == {"strain_background": "BY4741"}
        assert "experimental_conditions" not in card.model_extra
        assert "license" in card.model_extra

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
                        dataset_type="annotated_features",
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
                        dataset_type="annotated_features",
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
                        dataset_type="annotated_features",
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
                        dataset_type="annotated_features",
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
                    dataset_type="annotated_features",
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
                    dataset_type="metadata",
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
                    dataset_type="annotated_features",
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
                    dataset_type="metadata",
                    data_files=[DataFileInfo(path="metadata.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                ),
            ]
        )
        data_configs = card.get_configs_by_type("annotated_features")
        assert len(data_configs) == 1
        assert data_configs[0].config_name == "data"

    def test_get_default_config(self):
        """Test get_default_config method."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data1",
                    description="Data 1",
                    dataset_type="annotated_features",
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
                    dataset_type="annotated_features",
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
                    dataset_type="annotated_features",
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
                    dataset_type="metadata",
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
        assert data_configs[0].dataset_type != "metadata"

    def test_get_metadata_configs(self):
        """Test get_metadata_configs method."""
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data",
                    description="Data",
                    dataset_type="annotated_features",
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
                    dataset_type="metadata",
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
        assert metadata_configs[0].dataset_type == "metadata"

    def test_dataset_card_citation_field(self):
        """Test that DatasetCard accepts citation field."""
        card_data = {
            "citation": "Repository-level citation for all datasets",
            "configs": [
                {
                    "config_name": "data",
                    "description": "Test dataset",
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
        assert card.citation == "Repository-level citation for all datasets"

    def test_dataset_config_citation_field(self):
        """Test that DatasetConfig accepts citation field."""
        config = DatasetConfig(
            config_name="special_dataset",
            description="Dataset with specific citation",
            dataset_type="annotated_features",
            citation="Dataset-specific citation that overrides repository level",
            data_files=[DataFileInfo(path="special.parquet")],
            dataset_info=DatasetInfo(
                features=[FeatureInfo(name="id", dtype="string", description="ID")]
            ),
        )
        assert (
            config.citation
            == "Dataset-specific citation that overrides repository level"
        )

    def test_citation_fields_optional(self):
        """Test that citation fields are optional and default to None."""
        # DatasetCard without citation
        card = DatasetCard(
            configs=[
                DatasetConfig(
                    config_name="data",
                    description="Data",
                    dataset_type="annotated_features",
                    data_files=[DataFileInfo(path="data.parquet")],
                    dataset_info=DatasetInfo(
                        features=[
                            FeatureInfo(name="id", dtype="string", description="ID")
                        ]
                    ),
                )
            ]
        )
        assert card.citation is None
        assert card.configs[0].citation is None


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


def _make_config(name: str, features: list[dict]) -> DatasetConfig:
    """Return a minimal DatasetConfig with the given name and features."""
    return DatasetConfig(
        config_name=name,
        description=name,
        dataset_type="annotated_features",
        data_files=[DataFileInfo(path=f"{name}.parquet")],
        dataset_info=DatasetInfo(features=[FeatureInfo(**f) for f in features]),
    )


class TestSharedFeatures:
    """Tests for repo-level shared feature inheritance in DatasetCard."""

    def test_fields_inherited_by_single_config(self):
        """A config listed in applies_to receives the shared fields."""
        card = DatasetCard(
            features=[
                SharedFeatureGroup(
                    applies_to=["cfg_a"],
                    fields=[
                        FeatureInfo(
                            name="target_locus_tag",
                            dtype="string",
                            description="Shared description",
                            role="target_identifier",
                        )
                    ],
                )
            ],
            configs=[
                _make_config("cfg_a", []),
                _make_config("cfg_b", []),
            ],
        )
        cfg_a = card.get_config_by_name("cfg_a")
        assert cfg_a is not None
        names = [f.name for f in cfg_a.dataset_info.features]
        assert "target_locus_tag" in names

        cfg_b = card.get_config_by_name("cfg_b")
        assert cfg_b is not None
        assert cfg_b.dataset_info.features == []

    def test_fields_inherited_by_multiple_configs(self):
        """Two configs listed in applies_to both receive the shared fields."""
        card = DatasetCard(
            features=[
                SharedFeatureGroup(
                    applies_to=["cfg_a", "cfg_b"],
                    fields=[
                        FeatureInfo(name="shared", dtype="string", description="S")
                    ],
                )
            ],
            configs=[
                _make_config("cfg_a", []),
                _make_config("cfg_b", []),
            ],
        )
        for name in ("cfg_a", "cfg_b"):
            cfg = card.get_config_by_name(name)
            assert cfg is not None
            assert any(f.name == "shared" for f in cfg.dataset_info.features)

    def test_config_overrides_description_only(self):
        """Config-level entry overrides only description; dtype and role are
        inherited."""
        card = DatasetCard(
            features=[
                SharedFeatureGroup(
                    applies_to=["cfg_a"],
                    fields=[
                        FeatureInfo(
                            name="pval",
                            dtype="float64",
                            description="Shared description",
                            role="quantitative_measure",
                        )
                    ],
                )
            ],
            configs=[
                _make_config(
                    "cfg_a",
                    [{"name": "pval", "dtype": "float64", "description": "Override"}],
                ),
            ],
        )
        cfg_a = card.get_config_by_name("cfg_a")
        assert cfg_a is not None
        feature = next(f for f in cfg_a.dataset_info.features if f.name == "pval")
        assert feature.description == "Override"
        assert feature.dtype == "float64"
        assert feature.role == "quantitative_measure"

    def test_later_group_wins_on_field_name_collision(self):
        """When two groups both supply a field with the same name, the later group
        wins."""
        card = DatasetCard(
            features=[
                SharedFeatureGroup(
                    applies_to=["cfg_a"],
                    fields=[FeatureInfo(name="x", dtype="string", description="First")],
                ),
                SharedFeatureGroup(
                    applies_to=["cfg_a"],
                    fields=[FeatureInfo(name="x", dtype="int32", description="Second")],
                ),
            ],
            configs=[_make_config("cfg_a", [])],
        )
        cfg_a = card.get_config_by_name("cfg_a")
        assert cfg_a is not None
        feature = next(f for f in cfg_a.dataset_info.features if f.name == "x")
        assert feature.dtype == "int32"
        assert feature.description == "Second"

    def test_config_only_fields_preserved(self):
        """Fields declared only in dataset_info.features (not inherited) are kept."""
        card = DatasetCard(
            features=[
                SharedFeatureGroup(
                    applies_to=["cfg_a"],
                    fields=[
                        FeatureInfo(name="inherited", dtype="string", description="I")
                    ],
                )
            ],
            configs=[
                _make_config(
                    "cfg_a",
                    [
                        {"name": "inherited", "dtype": "string", "description": "I"},
                        {"name": "local_only", "dtype": "int32", "description": "L"},
                    ],
                ),
            ],
        )
        cfg_a = card.get_config_by_name("cfg_a")
        assert cfg_a is not None
        names = [f.name for f in cfg_a.dataset_info.features]
        assert "inherited" in names
        assert "local_only" in names

    def test_unknown_applies_to_raises(self):
        """An applies_to referencing a non-existent config name raises ValueError."""
        with pytest.raises((ValidationError, ValueError)):
            DatasetCard(
                features=[
                    SharedFeatureGroup(
                        applies_to=["nonexistent"],
                        fields=[FeatureInfo(name="x", dtype="string", description="X")],
                    )
                ],
                configs=[_make_config("cfg_a", [])],
            )

    def test_no_features_key_unchanged(self):
        """DatasetCard with no top-level features key is unaffected."""
        card = DatasetCard(
            configs=[
                _make_config(
                    "cfg_a",
                    [{"name": "col", "dtype": "string", "description": "C"}],
                )
            ]
        )
        cfg_a = card.get_config_by_name("cfg_a")
        assert cfg_a is not None
        assert len(cfg_a.dataset_info.features) == 1
        assert cfg_a.dataset_info.features[0].name == "col"
