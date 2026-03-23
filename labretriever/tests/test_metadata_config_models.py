"""
Tests for metadata configuration Pydantic models.

Tests validation, error messages, and config loading for MetadataBuilder.

"""

import pytest
import yaml  # type: ignore
from pydantic import ValidationError

from labretriever.models import (
    MetadataConfig,
    PropertyMapping,
    RepositoryConfig,
)


class TestPropertyMapping:
    """Tests for PropertyMapping model."""

    def test_valid_field_level_mapping(self):
        """Test valid field-level property mapping."""
        mapping = PropertyMapping(field="condition", path="media.carbon_source")
        assert mapping.field == "condition"
        assert mapping.path == "media.carbon_source"

    def test_valid_repo_level_mapping(self):
        """Test valid repo-level property mapping (no field)."""
        mapping = PropertyMapping(path="temperature_celsius")
        assert mapping.field is None
        assert mapping.path == "temperature_celsius"

    def test_invalid_empty_path(self):
        """Test that empty path is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            PropertyMapping(path="")
        assert "cannot be empty" in str(exc_info.value)

    def test_invalid_whitespace_path(self):
        """Test that whitespace-only path is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            PropertyMapping(path="   ")
        assert "cannot be empty" in str(exc_info.value)

    def test_invalid_empty_field(self):
        """Test that empty field string is rejected."""
        with pytest.raises(ValidationError) as exc_info:
            PropertyMapping(field="", path="media.carbon_source")
        assert "cannot be empty" in str(exc_info.value)

    def test_path_whitespace_stripped(self):
        """Test that path whitespace is stripped."""
        mapping = PropertyMapping(path="  media.carbon_source  ")
        assert mapping.path == "media.carbon_source"

    def test_valid_field_only_mapping(self):
        """Test valid field-only mapping (column alias)."""
        mapping = PropertyMapping(field="condition")
        assert mapping.field == "condition"
        assert mapping.path is None

    def test_invalid_neither_field_nor_path(self):
        """Test that at least one of field, path, or expression is required."""
        with pytest.raises(ValidationError) as exc_info:
            PropertyMapping()
        assert (
            "At least one of 'field', 'path', or 'expression' must be specified"
            in str(exc_info.value)
        )

    def test_valid_expression_only(self):
        """Test valid expression-only mapping (derived field)."""
        mapping = PropertyMapping(expression="dto_fdr < 0.05")
        assert mapping.expression == "dto_fdr < 0.05"
        assert mapping.field is None
        assert mapping.path is None

    def test_invalid_expression_with_field(self):
        """Test that expression cannot be combined with field."""
        with pytest.raises(ValidationError) as exc_info:
            PropertyMapping(expression="dto_fdr < 0.05", field="sample_id")
        assert "expression cannot be used with field or path" in str(exc_info.value)

    def test_invalid_expression_with_path(self):
        """Test that expression cannot be combined with path."""
        with pytest.raises(ValidationError) as exc_info:
            PropertyMapping(expression="dto_fdr < 0.05", path="media.carbon_source")
        assert "expression cannot be used with field or path" in str(exc_info.value)


class TestDatasetVirtualDBConfig:
    """Tests for DatasetVirtualDBConfig model."""

    def test_valid_config_with_sample_id(self):
        """Test valid dataset config with sample_id."""
        from labretriever.models import DatasetVirtualDBConfig, PropertyMapping

        config = DatasetVirtualDBConfig(sample_id=PropertyMapping(field="sample_id"))
        assert config.sample_id is not None
        assert config.sample_id.field == "sample_id"

    def test_valid_config_with_field_mappings_and_links(self):
        """Test valid dataset config with field mappings and links for comparative
        datasets."""
        from labretriever.models import DatasetVirtualDBConfig

        config_dict = {
            "sample_id": {"field": "sample_id"},
            "dto_fdr": {"field": "dto_fdr"},
            # Field mapping for aliasing: dto_pvalue displays dto_empirical_pvalue
            "dto_pvalue": {"field": "dto_empirical_pvalue"},
            "links": {
                "binding_id": [
                    ["BrentLab/harbison_2004", "harbison_2004"],
                    ["BrentLab/callingcards", "annotated_features"],
                ]
            },
        }
        config = DatasetVirtualDBConfig.model_validate(config_dict)
        assert config.sample_id is not None
        # Check field mapping for aliasing via property_mappings
        assert "dto_pvalue" in config.property_mappings
        assert config.property_mappings["dto_pvalue"].field == "dto_empirical_pvalue"
        assert "binding_id" in config.links
        assert len(config.links["binding_id"]) == 2
        assert config.links["binding_id"][0] == [
            "BrentLab/harbison_2004",
            "harbison_2004",
        ]

    def test_config_with_extra_property_mappings(self):
        """Test that extra fields are parsed as PropertyMappings."""
        from labretriever.models import DatasetVirtualDBConfig

        config_dict = {
            "sample_id": {"field": "sample_id"},
            "regulator_locus_tag": {"field": "regulator_locus_tag"},
            "dto_fdr": {"expression": "dto_fdr < 0.05"},
        }
        config = DatasetVirtualDBConfig.model_validate(config_dict)

        # Access extra fields via model_extra
        assert "regulator_locus_tag" in config.model_extra
        assert "dto_fdr" in config.model_extra

    def test_valid_db_name(self):
        """Test valid db_name is accepted."""
        from labretriever.models import DatasetVirtualDBConfig

        config = DatasetVirtualDBConfig.model_validate(
            {"db_name": "harbison", "sample_id": {"field": "sample_id"}}
        )
        assert config.db_name == "harbison"

    def test_db_name_none_by_default(self):
        """Test db_name defaults to None."""
        from labretriever.models import DatasetVirtualDBConfig

        config = DatasetVirtualDBConfig.model_validate(
            {"sample_id": {"field": "sample_id"}}
        )
        assert config.db_name is None

    def test_db_name_invalid_sql_identifier(self):
        """Test that invalid SQL identifiers are rejected."""
        from labretriever.models import DatasetVirtualDBConfig

        with pytest.raises(ValidationError) as exc_info:
            DatasetVirtualDBConfig.model_validate(
                {"db_name": "123bad", "sample_id": {"field": "sample_id"}}
            )
        assert "not a valid SQL identifier" in str(exc_info.value)

    def test_db_name_with_spaces_rejected(self):
        """Test that db_name with spaces is rejected."""
        from labretriever.models import DatasetVirtualDBConfig

        with pytest.raises(ValidationError) as exc_info:
            DatasetVirtualDBConfig.model_validate(
                {"db_name": "my table", "sample_id": {"field": "sample_id"}}
            )
        assert "not a valid SQL identifier" in str(exc_info.value)

    def test_db_name_reserved_samples(self):
        """Test that 'samples' is reserved and rejected."""
        from labretriever.models import DatasetVirtualDBConfig

        with pytest.raises(ValidationError) as exc_info:
            DatasetVirtualDBConfig.model_validate(
                {"db_name": "samples", "sample_id": {"field": "sample_id"}}
            )
        assert "reserved" in str(exc_info.value)

    def test_db_name_reserved_case_insensitive(self):
        """Test that reserved name check is case-insensitive."""
        from labretriever.models import DatasetVirtualDBConfig

        with pytest.raises(ValidationError) as exc_info:
            DatasetVirtualDBConfig.model_validate(
                {"db_name": "Samples", "sample_id": {"field": "sample_id"}}
            )
        assert "reserved" in str(exc_info.value)

    def test_db_name_underscores_allowed(self):
        """Test that underscores are allowed in db_name."""
        from labretriever.models import DatasetVirtualDBConfig

        config = DatasetVirtualDBConfig.model_validate(
            {"db_name": "_my_table_2", "sample_id": {"field": "sample_id"}}
        )
        assert config.db_name == "_my_table_2"


class TestRepositoryConfig:
    """Tests for RepositoryConfig model."""

    def test_valid_repo_config_with_datasets(self):
        """Test valid repository config with dataset section."""
        config_data = {
            "temperature_celsius": {"path": "temperature_celsius"},
            "dataset": {
                "dataset1": {
                    "carbon_source": {
                        "field": "condition",
                        "path": "media.carbon_source",
                    }
                }
            },
        }
        config = RepositoryConfig.model_validate(config_data)
        assert config.dataset is not None
        assert "dataset1" in config.dataset

    def test_valid_repo_config_no_datasets(self):
        """Test valid repository config without dataset section."""
        config_data = {"temperature_celsius": {"path": "temperature_celsius"}}
        config = RepositoryConfig.model_validate(config_data)
        assert config.dataset is None

    def test_invalid_dataset_not_dict(self):
        """Test that dataset section must be a dict."""
        config_data = {"dataset": "not a dict"}
        with pytest.raises(ValidationError) as exc_info:
            RepositoryConfig.model_validate(config_data)
        assert "'dataset' key must contain a dict" in str(exc_info.value)

    def test_valid_field_only_property(self):
        """Test that field-only properties are valid (column aliases)."""
        config_data = {
            "dataset": {"dataset1": {"carbon_source": {"field": "condition"}}}
        }
        config = RepositoryConfig.model_validate(config_data)
        assert config.dataset is not None
        assert "dataset1" in config.dataset
        # Access extra field via model_extra
        dataset_config = config.dataset["dataset1"]
        assert "carbon_source" in dataset_config.model_extra
        assert dataset_config.model_extra["carbon_source"].field == "condition"
        assert dataset_config.model_extra["carbon_source"].path is None

    def test_valid_repo_wide_field_only_property(self):
        """Test that repo-wide field-only properties are valid."""
        config_data = {"environmental_condition": {"field": "condition"}}
        config = RepositoryConfig.model_validate(config_data)
        assert "environmental_condition" in config.properties
        assert config.properties["environmental_condition"].field == "condition"
        assert config.properties["environmental_condition"].path is None


class TestMetadataConfig:
    """Tests for MetadataConfig model."""

    def test_valid_config_with_aliases(self, tmp_path):
        """Test valid config with factor aliases."""
        config_data = {
            "factor_aliases": {
                "carbon_source": {
                    "glucose": ["D-glucose", "dextrose"],
                    "galactose": ["D-galactose", "Galactose"],
                }
            },
            "repositories": {
                "BrentLab/test": {
                    "dataset": {
                        "test": {"carbon_source": {"path": "media.carbon_source"}}
                    }
                }
            },
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        config = MetadataConfig.from_yaml(config_path)
        assert "carbon_source" in config.factor_aliases
        assert "glucose" in config.factor_aliases["carbon_source"]
        assert config.factor_aliases["carbon_source"]["glucose"] == [
            "D-glucose",
            "dextrose",
        ]

    def test_valid_config_without_aliases(self, tmp_path):
        """Test that factor_aliases is optional."""
        config_data = {
            "repositories": {
                "BrentLab/test": {
                    "dataset": {
                        "test": {"carbon_source": {"path": "media.carbon_source"}}
                    }
                }
            }
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        config = MetadataConfig.from_yaml(config_path)
        assert config.factor_aliases == {}

    def test_valid_config_empty_aliases(self, tmp_path):
        """Test that empty factor_aliases dict is allowed."""
        config_data = {
            "factor_aliases": {},
            "repositories": {
                "BrentLab/test": {
                    "dataset": {
                        "test": {"carbon_source": {"path": "media.carbon_source"}}
                    }
                }
            },
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        config = MetadataConfig.from_yaml(config_path)
        assert config.factor_aliases == {}

    def test_invalid_alias_not_dict(self):
        """Test that property aliases must be a dict."""
        config_data = {
            "factor_aliases": {
                "carbon_source": ["D-glucose"]  # Should be dict, not list
            },
            "repositories": {
                "BrentLab/test": {"dataset": {"test": {"prop": {"path": "path"}}}}
            },
        }

        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        # Pydantic catches this with type validation before our custom validator
        assert "valid dictionary" in str(exc_info.value) or "must be a dict" in str(
            exc_info.value
        )

    def test_invalid_alias_value_not_list(self):
        """Test that alias values must be lists."""
        config_data = {
            "factor_aliases": {
                "carbon_source": {"glucose": "D-glucose"}  # Should be list, not string
            },
            "repositories": {
                "BrentLab/test": {"dataset": {"test": {"prop": {"path": "path"}}}}
            },
        }

        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        # Pydantic catches this with type validation before our custom validator
        assert "valid list" in str(exc_info.value) or "must map to a list" in str(
            exc_info.value
        )

    def test_invalid_alias_empty_list(self):
        """Test that alias value lists cannot be empty."""
        config_data = {
            "factor_aliases": {"carbon_source": {"glucose": []}},
            "repositories": {
                "BrentLab/test": {"dataset": {"test": {"prop": {"path": "path"}}}}
            },
        }

        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        assert "cannot have empty value list" in str(exc_info.value)

    def test_aliases_allow_numeric_values(self):
        """Test that aliases can map to numeric values."""
        config_data = {
            "factor_aliases": {
                "temperature_celsius": {
                    "thirty": [30, "30"],  # Integer and string
                    "thirty_seven": [37, 37.0],  # Integer and float
                }
            },
            "repositories": {
                "BrentLab/test": {
                    "dataset": {
                        "test": {"temperature": {"path": "temperature_celsius"}}
                    }
                }
            },
        }

        config = MetadataConfig.model_validate(config_data)
        assert config.factor_aliases["temperature_celsius"]["thirty"] == [30, "30"]
        assert config.factor_aliases["temperature_celsius"]["thirty_seven"] == [
            37,
            37.0,
        ]

    def test_invalid_no_repositories(self):
        """Test that at least one repository is required."""
        config_data = {"factor_aliases": {"carbon_source": {"glucose": ["D-glucose"]}}}
        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        assert "at least one repository" in str(exc_info.value)

    def test_get_repository_config(self, tmp_path):
        """Test get_repository_config method."""
        config_data = {
            "factor_aliases": {"carbon_source": {"glucose": ["D-glucose"]}},
            "repositories": {
                "BrentLab/harbison_2004": {
                    "dataset": {
                        "harbison_2004": {
                            "carbon_source": {
                                "field": "condition",
                                "path": "media.carbon_source",
                            }
                        }
                    }
                }
            },
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        config = MetadataConfig.from_yaml(config_path)
        repo_config = config.get_repository_config("BrentLab/harbison_2004")
        assert repo_config is not None
        assert isinstance(repo_config, RepositoryConfig)
        assert repo_config.dataset is not None
        assert "harbison_2004" in repo_config.dataset

        # Non-existent repo
        assert config.get_repository_config("BrentLab/nonexistent") is None

    def test_get_property_mappings(self, tmp_path):
        """Test get_property_mappings method."""
        config_data = {
            "factor_aliases": {
                "carbon_source": {"glucose": ["D-glucose"]},
                "temperature": {"thirty": [30]},
            },
            "repositories": {
                "BrentLab/kemmeren_2014": {
                    "temperature": {"path": "temperature_celsius"},  # Repo-wide
                    "dataset": {
                        "kemmeren_2014": {
                            "carbon_source": {"path": "media.carbon_source"}
                        }
                    },
                }
            },
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        config = MetadataConfig.from_yaml(config_path)
        mappings = config.get_property_mappings(
            "BrentLab/kemmeren_2014", "kemmeren_2014"
        )

        # Should have both repo-wide and dataset-specific
        assert "temperature" in mappings
        assert "carbon_source" in mappings
        # Mappings are PropertyMapping objects, not dicts
        assert isinstance(mappings["temperature"], PropertyMapping)
        assert mappings["temperature"].path == "temperature_celsius"
        assert mappings["carbon_source"].path == "media.carbon_source"

    def test_dataset_specific_overrides_repo_wide(self, tmp_path):
        """Test that dataset-specific mappings override repo-wide."""
        config_data = {
            "repositories": {
                "BrentLab/test": {
                    "carbon_source": {"path": "repo.level.path"},  # Repo-wide
                    "dataset": {
                        "test_dataset": {
                            "carbon_source": {"path": "dataset.level.path"}  # Override
                        }
                    },
                }
            },
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        config = MetadataConfig.from_yaml(config_path)
        mappings = config.get_property_mappings("BrentLab/test", "test_dataset")

        # Dataset-specific should win
        assert mappings["carbon_source"].path == "dataset.level.path"

    def test_file_not_found(self):
        """Test that FileNotFoundError is raised for missing file."""
        with pytest.raises(FileNotFoundError):
            MetadataConfig.from_yaml("/nonexistent/path/config.yaml")

    def test_invalid_yaml_structure(self, tmp_path):
        """Test that non-dict YAML is rejected."""
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            f.write("- not\\n- a\\n- dict\\n")

        with pytest.raises(ValueError) as exc_info:
            MetadataConfig.from_yaml(config_path)
        assert "Configuration file must contain a YAML dictionary" in str(
            exc_info.value
        )

    def test_nested_alias_property_names(self, tmp_path):
        """Test that alias property names can use dot notation."""
        config_data = {
            "factor_aliases": {
                "carbon_source": {"glucose": ["D-glucose"]},
                "carbon_source.concentration_percent": {"two_percent": [2]},
                "carbon_source.specifications": {"no_aa": ["without_amino_acids"]},
            },
            "repositories": {
                "BrentLab/test": {
                    "dataset": {
                        "test": {
                            "carbon_source": {
                                "field": "condition",
                                "path": "media.carbon_source",
                            }
                        }
                    }
                }
            },
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_data, f)

        config = MetadataConfig.from_yaml(config_path)

        # All alias properties should be preserved
        assert "carbon_source" in config.factor_aliases
        assert "carbon_source.concentration_percent" in config.factor_aliases
        assert "carbon_source.specifications" in config.factor_aliases

        # Values should be correct
        assert config.factor_aliases["carbon_source"]["glucose"] == ["D-glucose"]
        assert config.factor_aliases["carbon_source.concentration_percent"][
            "two_percent"
        ] == [2]
        assert config.factor_aliases["carbon_source.specifications"]["no_aa"] == [
            "without_amino_acids"
        ]

    def test_unique_db_names_valid(self):
        """Test that unique db_names across datasets pass validation."""
        config_data = {
            "repositories": {
                "BrentLab/repo1": {
                    "dataset": {
                        "dataset_a": {
                            "db_name": "alpha",
                            "sample_id": {"field": "sample_id"},
                        },
                        "dataset_b": {
                            "db_name": "beta",
                            "sample_id": {"field": "sample_id"},
                        },
                    }
                }
            }
        }
        config = MetadataConfig.model_validate(config_data)
        repo = config.get_repository_config("BrentLab/repo1")
        assert repo.dataset["dataset_a"].db_name == "alpha"
        assert repo.dataset["dataset_b"].db_name == "beta"

    def test_duplicate_db_names_rejected(self):
        """Test that duplicate db_names are rejected."""
        config_data = {
            "repositories": {
                "BrentLab/repo1": {
                    "dataset": {
                        "dataset_a": {
                            "db_name": "same_name",
                            "sample_id": {"field": "sample_id"},
                        },
                        "dataset_b": {
                            "db_name": "same_name",
                            "sample_id": {"field": "sample_id"},
                        },
                    }
                }
            }
        }
        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        assert "Duplicate db_name" in str(exc_info.value)

    def test_duplicate_db_name_case_insensitive(self):
        """Test that db_name uniqueness is case-insensitive."""
        config_data = {
            "repositories": {
                "BrentLab/repo1": {
                    "dataset": {
                        "dataset_a": {
                            "db_name": "Alpha",
                            "sample_id": {"field": "sample_id"},
                        },
                        "dataset_b": {
                            "db_name": "alpha",
                            "sample_id": {"field": "sample_id"},
                        },
                    }
                }
            }
        }
        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        assert "Duplicate db_name" in str(exc_info.value)

    def test_db_name_falls_back_to_config_name(self):
        """Test that config_name is used when db_name is not set."""
        config_data = {
            "repositories": {
                "BrentLab/repo1": {
                    "dataset": {
                        "harbison_2004": {
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                },
                "BrentLab/repo2": {
                    "dataset": {
                        "kemmeren_2014": {
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                },
            }
        }
        # Should pass -- different config_names used as fallback
        config = MetadataConfig.model_validate(config_data)
        assert config is not None

    def test_db_name_collides_with_config_name(self):
        """Test that db_name colliding with another config_name is rejected."""
        config_data = {
            "repositories": {
                "BrentLab/repo1": {
                    "dataset": {
                        "harbison": {
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                },
                "BrentLab/repo2": {
                    "dataset": {
                        "kemmeren": {
                            "db_name": "harbison",
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                },
            }
        }
        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        assert "Duplicate db_name" in str(exc_info.value)

    def test_duplicate_db_names_across_repos(self):
        """Test that db_name uniqueness spans across repositories."""
        config_data = {
            "repositories": {
                "BrentLab/repo1": {
                    "dataset": {
                        "ds1": {
                            "db_name": "shared",
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                },
                "BrentLab/repo2": {
                    "dataset": {
                        "ds2": {
                            "db_name": "shared",
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                },
            }
        }
        with pytest.raises(ValidationError) as exc_info:
            MetadataConfig.model_validate(config_data)
        assert "Duplicate db_name" in str(exc_info.value)
