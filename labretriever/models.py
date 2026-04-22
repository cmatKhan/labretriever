"""
Pydantic models for dataset card validation and metadata configuration.

These models provide minimal structure for parsing HuggingFace dataset cards while
remaining flexible enough to accommodate diverse experimental systems. Most fields use
extra="allow" to accept domain-specific additions without requiring code changes.

Also includes models for VirtualDB metadata normalization configuration.

"""

import logging
from enum import Enum
from functools import cached_property
from pathlib import Path
from typing import Any, TypeAlias

import yaml  # type: ignore[import-untyped]
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)

# Type aliases for improved readability
FactorAliases: TypeAlias = dict[str, dict[str, list[str | int | float | bool]]]


logger = logging.getLogger(__name__)


class DatasetType(str, Enum):
    """Supported dataset types."""

    GENOMIC_FEATURES = "genomic_features"
    ANNOTATED_FEATURES = "annotated_features"
    GENOME_MAP = "genome_map"
    METADATA = "metadata"
    COMPARATIVE = "comparative"


class FeatureInfo(BaseModel):
    """
    Information about a dataset feature/column.

    Minimal required fields with flexible dtype handling.

    """

    name: str = Field(..., description="Column name in the data")
    dtype: str | dict[str, Any] = Field(
        ...,
        description="Data type (string, int64, float64, etc.) or class_label dict",
    )
    description: str = Field(..., description="Description of the field")
    role: str | None = Field(
        default=None,
        description="Optional semantic role. 'experimental_condition' "
        "has special behavior.",
    )
    definitions: dict[str, Any] | None = Field(
        default=None,
        description="For experimental_condition fields: definitions per value",
    )


class PartitioningInfo(BaseModel):
    """Partitioning configuration for datasets."""

    enabled: bool = Field(default=False, description="Whether partitioning is enabled")
    partition_by: list[str] | None = Field(
        default=None, description="Partition column names"
    )
    path_template: str | None = Field(
        default=None, description="Path template for partitioned files"
    )


class DatasetInfo(BaseModel):
    """Dataset structure information."""

    features: list[FeatureInfo] = Field(..., description="Feature definitions")
    partitioning: PartitioningInfo | None = Field(
        default=None, description="Partitioning configuration"
    )


class DataFileInfo(BaseModel):
    """Information about data files."""

    split: str = Field(default="train", description="Dataset split name")
    path: str = Field(..., description="Path to data file(s)")


class DatasetConfig(BaseModel):
    """
    Configuration for a dataset within a repository.

    Uses extra="allow" to accept arbitrary experimental_conditions and other fields.

    """

    config_name: str = Field(..., description="Unique configuration identifier")
    description: str = Field(..., description="Human-readable description")
    dataset_type: DatasetType = Field(..., description="Type of dataset")
    default: bool = Field(
        default=False, description="Whether this is the default config"
    )
    applies_to: list[str] | None = Field(
        default=None, description="Configs this metadata applies to"
    )
    metadata_fields: list[str] | None = Field(
        default=None, description="Fields for embedded metadata extraction"
    )
    data_files: list[DataFileInfo] = Field(..., description="Data file information")
    dataset_info: DatasetInfo = Field(..., description="Dataset structure information")
    citation: str | None = Field(
        default=None,
        description="Dataset-specific citation that overrides "
        "repository-level citation",
    )
    doi: str | None = Field(
        default=None,
        description="DOI or URL for the primary publication associated with "
        "this dataset configuration",
    )

    model_config = ConfigDict(extra="allow")

    @field_validator("applies_to", mode="after")
    @classmethod
    def applies_to_only_for_metadata(
        cls, v: list[str] | None, info
    ) -> list[str] | None:
        """
        Validate that applies_to is only used for metadata or comparative configs.

        :param v: The applies_to field value
        :param info: Validation info containing other field values
        :return: The validated applies_to value
        :raises ValueError: If applies_to is used with invalid dataset type

        """
        if v is not None:
            dataset_type = info.data.get("dataset_type")
            if dataset_type not in (DatasetType.METADATA, DatasetType.COMPARATIVE):
                raise ValueError(
                    "applies_to field is only valid "
                    "for metadata and comparative dataset types"
                )
        return v

    @field_validator("metadata_fields", mode="after")
    @classmethod
    def metadata_fields_not_empty(cls, v: list[str] | None) -> list[str] | None:
        """
        Validate metadata_fields is not an empty list.

        :param v: The metadata_fields value
        :return: The validated metadata_fields value
        :raises ValueError: If metadata_fields is an empty list

        """
        if v is not None and len(v) == 0:
            raise ValueError("metadata_fields cannot be empty list, use None instead")
        return v


class DatasetCard(BaseModel):
    """
    Complete dataset card model.

    Uses extra="allow" to accept arbitrary top-level metadata and
    experimental_conditions.

    """

    configs: list[DatasetConfig] = Field(..., description="Dataset configurations")
    citation: str | None = Field(
        default=None, description="Repository-level citation for all datasets"
    )
    doi: str | None = Field(
        default=None,
        description="DOI or URL for the primary publication associated with "
        "this repository",
    )

    model_config = ConfigDict(extra="allow")

    @field_validator("configs", mode="after")
    @classmethod
    def validate_configs(cls, v: list[DatasetConfig]) -> list[DatasetConfig]:
        """
        Validate configs list.

        Ensures at least one config exists, all config names are unique, and at most one
        config is marked as default.

        :param v: The list of DatasetConfig objects
        :return: The validated list of configs
        :raises ValueError: If validation fails

        """
        # Check non-empty
        if not v:
            raise ValueError("At least one dataset configuration is required")

        # Check unique names
        names = [config.config_name for config in v]
        if len(names) != len(set(names)):
            raise ValueError("Configuration names must be unique")

        # Check at most one default
        defaults = sum(1 for config in v if config.default)
        if defaults > 1:
            raise ValueError("At most one configuration can be marked as default")

        return v

    # Computed properties for better discoverability
    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def default_config(self) -> DatasetConfig | None:
        """
        Get the default configuration if one exists.

        :return: The default DatasetConfig or None if no default is set

        """
        for config in self.configs:
            if config.default:
                return config
        return None

    @computed_field  # type: ignore[prop-decorator]
    @cached_property
    def config_names(self) -> list[str]:
        """
        Get all configuration names.

        :return: List of all config_name values

        """
        return [config.config_name for config in self.configs]

    # Utility methods (not serialized)
    def get_config_by_name(self, name: str) -> DatasetConfig | None:
        """
        Get a configuration by name.

        :param name: The configuration name to search for
        :return: The matching DatasetConfig or None if not found

        """
        for config in self.configs:
            if config.config_name == name:
                return config
        return None

    def get_configs_by_type(self, dataset_type: DatasetType) -> list[DatasetConfig]:
        """
        Get all configurations of a specific type.

        :param dataset_type: The DatasetType to filter by
        :return: List of matching DatasetConfig objects

        """
        return [
            config for config in self.configs if config.dataset_type == dataset_type
        ]

    def get_data_configs(self) -> list[DatasetConfig]:
        """
        Get all non-metadata configurations.

        :return: List of DatasetConfig objects excluding metadata types

        """
        return [
            config
            for config in self.configs
            if config.dataset_type != DatasetType.METADATA
        ]

    def get_metadata_configs(self) -> list[DatasetConfig]:
        """
        Get all metadata configurations.

        :return: List of DatasetConfig objects with metadata type

        """
        return [
            config
            for config in self.configs
            if config.dataset_type == DatasetType.METADATA
        ]


class ExtractedMetadata(BaseModel):
    """Metadata extracted from datasets."""

    config_name: str = Field(..., description="Source configuration name")
    field_name: str = Field(
        ..., description="Field name the metadata was extracted from"
    )
    values: set[str] = Field(..., description="Unique values found")
    extraction_method: str = Field(..., description="How the metadata was extracted")

    @field_serializer("values", mode="plain")
    def serialize_values(self, value: set[str]) -> list[str]:
        """
        Serialize set as sorted list for JSON compatibility.

        :param value: Set of string values
        :return: Sorted list of strings

        """
        return sorted(value)


class MetadataRelationship(BaseModel):
    """Relationship between a data config and its metadata."""

    data_config: str = Field(..., description="Data configuration name")
    metadata_config: str = Field(..., description="Metadata configuration name")
    relationship_type: str = Field(
        ..., description="Type of relationship (explicit, embedded)"
    )


# ============================================================================
# VirtualDB Metadata Configuration Models
# ============================================================================


class PropertyMapping(BaseModel):
    """
    Mapping specification for a single property.

    :ivar field: Optional field name for field-level properties.
        When specified, looks in this field's definitions.
        When omitted, uses repo/config-level resolution.
    :ivar path: Optional dot-notation path to the property value.
        For repo/config-level: relative to datacard/config root
        (e.g., "experimental_conditions.media.carbon_source" or "description")
        For field-level: relative to the field's definitions dict
        (e.g., "temperature_celsius" resolves within each sample's definition)
        When omitted with field specified, creates a column alias.
    :ivar expression: Optional SQL expression for derived/computed fields.
        When specified, creates a computed column.
        Cannot be used with field or path.
    :ivar dtype: Optional data type specification for type conversion.
        Supported values: 'string', 'numeric', 'bool'.
        When specified, extracted values are converted to this type.

    Examples::

        # Repo/config-level property (explicit path from datacard root)
        PropertyMapping(path="experimental_conditions.media.carbon_source.compound")

        # Repo/config-level property outside experimental_conditions
        PropertyMapping(path="description")

        # Field-level property with path (relative to field definitions)
        PropertyMapping(field="condition", path="temperature_celsius")

        # Field-level column alias (no path)
        PropertyMapping(field="condition")

        # Derived field with expression
        PropertyMapping(expression="dto_fdr < 0.05")

    """

    field: str | None = Field(None, description="Field name for field-level properties")
    path: str | None = Field(None, description="Dot-notation path to property")
    expression: str | None = Field(
        None, description="SQL expression for derived fields"
    )
    dtype: str | None = Field(
        None,
        description=(
            "Data type for conversion: 'string', 'numeric', 'bool', or 'factor'. "
            "When 'factor', the field must reference a DataCard field with a "
            "class_label dtype specifying the allowed levels. VirtualDB will "
            "register a DuckDB ENUM type and cast the column to it."
        ),
    )

    @field_validator("path", "field", "expression", mode="before")
    @classmethod
    def strip_whitespace(cls, v: str | None) -> str | None:
        """
        Strip whitespace and validate non-empty strings.

        :param v: String value to validate
        :return: Stripped string or None
        :raises ValueError: If string is empty or only whitespace

        """
        if v is None:
            return None
        v = v.strip()
        if not v:
            raise ValueError("Value cannot be empty or whitespace")
        return v

    @model_validator(mode="after")
    def validate_field_types(self) -> "PropertyMapping":
        """
        Ensure at least one field type is specified and mutually exclusive.

        Also validates dtype='factor' requires a field (not expression or path-only).

        :return: The validated PropertyMapping instance
        :raises ValueError: If validation constraints are violated

        """
        if self.expression is not None:
            if self.field is not None or self.path is not None:
                raise ValueError(
                    "expression cannot be used with field or path - "
                    "derived fields are computed, not extracted"
                )
        elif self.field is None and self.path is None:
            raise ValueError(
                "At least one of 'field', 'path', or 'expression' must be specified"
            )
        if self.dtype == "factor":
            if self.expression is not None or self.field is None:
                raise ValueError(
                    "dtype='factor' requires 'field' to be specified and "
                    "cannot be used with 'expression' or as a path-only mapping"
                )
        return self


class DatasetVirtualDBConfig(BaseModel):
    """
    VirtualDB configuration for a specific dataset within a repository.

    Additional property mappings can be provided as extra fields and will be
    automatically parsed as PropertyMapping objects.

    :ivar sample_id: Mapping for the sample identifier field (required for
        primary datasets)
    :ivar links: For comparative datasets, map link_field -> list of
        [repo_id, config_name] pairs specifying which primary datasets
        are linked through each link field.

    Example - Primary dataset::

        annotated_features:
          sample_id:
            field: sample_id
          regulator_locus_tag:
            field: regulator_locus_tag

    Example - Comparative dataset::

        dto:
          # Field mappings - use this to rename fields
          dto_fdr:
            field: dto_fdr
          dto_pvalue:
            field: empirical_pvalue  # renames empirical_pvalue to dto_pvalue
          # Links to primary datasets
          links:
            binding_id:
              - [BrentLab/harbison_2004, harbison_2004]
              - [BrentLab/callingcards, annotated_features]
            perturbation_id:
              - [BrentLab/kemmeren_2014, kemmeren_2014]

    """

    sample_id: PropertyMapping | None = Field(
        None, description="Mapping for sample identifier field"
    )
    description: str | None = Field(
        None,
        description=(
            "Human-readable description of this dataset in the VirtualDB context. "
            "Overrides the description from the DataCard when present."
        ),
    )
    db_name: str | None = Field(
        None,
        description=(
            "Short name for this dataset in the SQL interface. "
            "Falls back to the config_name (YAML dict key) if not "
            "specified. Must be a valid SQL identifier."
        ),
    )
    links: dict[str, list[list[str]]] = Field(
        default_factory=dict,
        description="For comparative datasets: map link_field -> "
        "[repo_id, config_name] pairs",
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Arbitrary key/value annotations for this dataset",
    )

    model_config = ConfigDict(extra="allow")

    @field_validator("links", mode="after")
    @classmethod
    def validate_links(
        cls, v: dict[str, list[list[str]]]
    ) -> dict[str, list[list[str]]]:
        """
        Validate that each link is a [repo_id, config_name] pair.

        :param v: Links dictionary
        :return: Validated links
        :raises ValueError: If any link is not a valid pair

        """
        for link_field, datasets in v.items():
            for i, dataset_pair in enumerate(datasets):
                if not isinstance(dataset_pair, list) or len(dataset_pair) != 2:
                    raise ValueError(
                        f"Link {i} for link_field '{link_field}' must be "
                        f"[repo_id, config_name], got: {dataset_pair}"
                    )
        return v

    @field_validator("db_name", mode="after")
    @classmethod
    def validate_db_name(cls, v: str | None) -> str | None:
        """
        Validate db_name is a valid SQL identifier and not reserved.

        :param v: db_name value
        :return: Validated db_name
        :raises ValueError: If db_name is invalid

        """
        if v is None:
            return None
        import re

        if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", v):
            raise ValueError(
                f"db_name '{v}' is not a valid SQL identifier. "
                "Use only letters, digits, and underscores, "
                "starting with a letter or underscore."
            )
        reserved = {"samples"}
        if v.lower() in reserved:
            raise ValueError(f"db_name '{v}' is reserved for internal use.")
        return v

    @model_validator(mode="before")
    @classmethod
    def parse_property_mappings(cls, data: Any) -> dict[str, Any]:
        """
        Parse extra fields as PropertyMapping objects.

        :param data: Raw input data
        :return: Processed data with PropertyMapping objects
        :raises ValueError: If PropertyMapping validation fails

        """
        if not isinstance(data, dict):
            return data

        result = {}
        for key, value in data.items():
            # Known typed fields - let Pydantic handle them
            if key in ("sample_id", "links", "db_name", "tags"):
                result[key] = value
            # Dict values should be PropertyMappings
            elif isinstance(value, dict):
                try:
                    result[key] = PropertyMapping.model_validate(value)
                except Exception as e:
                    raise ValueError(
                        f"Invalid PropertyMapping for field '{key}': {e}"
                    ) from e
            # Already parsed PropertyMapping or other type
            else:
                result[key] = value

        return result

    @property
    def property_mappings(self) -> dict[str, PropertyMapping]:
        """
        Get all property mappings from extra fields.

        :return: Dictionary of property names to PropertyMapping objects

        """
        if not self.model_extra:
            return {}

        return {
            key: value
            for key, value in self.model_extra.items()
            if isinstance(value, PropertyMapping)
        }


class RepositoryConfig(BaseModel):
    """
    Configuration for a single repository.

    For example: BrentLab/harbison_2004

    :ivar properties: Repo-wide property mappings that apply to all datasets
    :ivar dataset: Dataset-specific configurations including sample_id,
        comparative_analyses, and property mappings

    Example::

        BrentLab/harbison_2004:
          temperature_celsius:
            path: temperature_celsius
          dataset:
            harbison_2004:
              sample_id:
                field: sample_id
              carbon_source:
                field: condition
                path: media.carbon_source

    """

    properties: dict[str, PropertyMapping] = Field(
        default_factory=dict, description="Repo-wide property mappings"
    )
    dataset: dict[str, DatasetVirtualDBConfig] | None = Field(
        None, description="Dataset-specific configurations"
    )
    tags: dict[str, str] = Field(
        default_factory=dict,
        description="Arbitrary key/value annotations for all datasets in this repo",
    )

    @model_validator(mode="before")
    @classmethod
    def parse_structure(cls, data: Any) -> dict[str, Any]:
        """
        Parse raw dict structure into typed objects.

        :param data: Raw input data
        :return: Processed data with typed objects
        :raises ValueError: If validation fails

        """
        if not isinstance(data, dict):
            return data

        # Parse dataset section
        parsed_datasets: dict[str, DatasetVirtualDBConfig] | None = None
        dataset_section = data.get("dataset")

        if dataset_section:
            if not isinstance(dataset_section, dict):
                raise ValueError("'dataset' key must contain a dict")

            parsed_datasets = {}
            for dataset_name, config_dict in dataset_section.items():
                if not isinstance(config_dict, dict):
                    raise ValueError(f"Dataset '{dataset_name}' must contain a dict")

                try:
                    parsed_datasets[dataset_name] = (
                        DatasetVirtualDBConfig.model_validate(config_dict)
                    )
                except Exception as e:
                    raise ValueError(
                        f"Invalid configuration for dataset '{dataset_name}': {e}"
                    ) from e

        # Parse repo-wide properties (all keys except 'dataset' and 'tags')
        parsed_properties = {}
        for key, value in data.items():
            if key in ("dataset", "tags"):
                continue

            try:
                parsed_properties[key] = PropertyMapping.model_validate(value)
            except Exception as e:
                raise ValueError(f"Invalid repo-wide property '{key}': {e}") from e

        return {
            "properties": parsed_properties,
            "dataset": parsed_datasets,
            "tags": data.get("tags") or {},
        }


class MetadataConfig(BaseModel):
    """
    Configuration for building standardized metadata tables.

    Specifies optional alias mappings for normalizing factor levels across
    heterogeneous datasets, plus property path mappings for each repository.

    :ivar factor_aliases: Optional mappings of standardized names to actual values.
        Example: {"carbon_source": {"glucose": ["D-glucose", "dextrose"]}}
    :ivar missing_value_labels: Labels for missing values by property name
    :ivar description: Human-readable descriptions for each property
    :ivar repositories: Dict mapping repository IDs to their configurations

    Example::

        repositories:
          BrentLab/harbison_2004:
            dataset:
              harbison_2004:
                carbon_source:
                  field: condition
                  path: media.carbon_source

          BrentLab/kemmeren_2014:
            temperature:
              path: temperature_celsius
            dataset:
              kemmeren_2014:
                carbon_source:
                  path: media.carbon_source

          # Comparative dataset with aliases and links
          BrentLab/yeast_comparative_analysis:
            dataset:
              dto:
                dto_fdr:
                  field: dto_fdr
                aliases:
                  dto_pvalue: dto_empirical_pvalue
                links:
                  binding_id:
                    - [BrentLab/harbison_2004, harbison_2004]

        factor_aliases:
          carbon_source:
            glucose: ["D-glucose", "dextrose"]
            galactose: ["D-galactose", "Galactose"]

        missing_value_labels:
          carbon_source: "unspecified"

        description:
          carbon_source: "Carbon source in growth media"

    """

    factor_aliases: FactorAliases = Field(
        default_factory=dict,
        description="Optional alias mappings for normalizing factor levels",
    )
    missing_value_labels: dict[str, str] = Field(
        default_factory=dict,
        description="Labels for missing values by property name",
    )
    description: dict[str, str] = Field(
        default_factory=dict,
        description="Human-readable descriptions for each property",
    )
    repositories: dict[str, RepositoryConfig] = Field(
        ..., description="Repository configurations keyed by repo ID"
    )

    @field_validator("missing_value_labels", "description", mode="before")
    @classmethod
    def filter_none_values(cls, v: dict[str, str] | None) -> dict[str, str]:
        """
        Filter out None values that may come from empty YAML values.

        :param v: Dictionary that may contain None values
        :return: Dictionary with None values filtered out

        """
        if not v:
            return {}
        # Pydantic will validate it's a dict, we just filter None values
        return {k: val for k, val in v.items() if val is not None}

    @field_validator("factor_aliases", mode="after")
    @classmethod
    def validate_factor_aliases(cls, v: FactorAliases) -> FactorAliases:
        """
        Validate factor alias structure and value types.

        :param v: Factor aliases dictionary
        :return: Validated factor aliases
        :raises ValueError: If any alias has an empty value list

        """
        for prop_name, aliases in v.items():
            for alias_name, actual_values in aliases.items():
                if not actual_values:
                    raise ValueError(
                        f"Alias '{alias_name}' for '{prop_name}' cannot "
                        f"have empty value list"
                    )
        return v

    @model_validator(mode="after")
    def validate_repositories_have_datasets(self) -> "MetadataConfig":
        """
        Validate that every repository defines at least one dataset.

        :return: The validated MetadataConfig instance
        :raises ValueError: If any repository has no datasets defined

        """
        for repo_id, repo_config in self.repositories.items():
            if not repo_config.dataset:
                raise ValueError(
                    f"Repository '{repo_id}' must define at least one "
                    "dataset under the 'dataset' key."
                )
        return self

    @model_validator(mode="after")
    def validate_unique_db_names(self) -> "MetadataConfig":
        """
        Validate that all resolved db_names are unique across datasets.

        Each dataset resolves to db_name or config_name. These must be unique to avoid
        SQL view name collisions.

        :return: The validated MetadataConfig instance
        :raises ValueError: If duplicate db_names are found

        """
        seen: dict[str, str] = {}
        for repo_id, repo_config in self.repositories.items():
            if not repo_config.dataset:
                continue
            for config_name, dataset_config in repo_config.dataset.items():
                resolved = dataset_config.db_name or config_name
                key = resolved.lower()
                if key in seen:
                    raise ValueError(
                        f"Duplicate db_name '{resolved}': used by "
                        f"'{seen[key]}' and "
                        f"'{repo_id}/{config_name}'"
                    )
                seen[key] = f"{repo_id}/{config_name}"
        return self

    @model_validator(mode="before")
    @classmethod
    def parse_config(cls, data: Any) -> dict[str, Any]:
        """
        Parse and validate all top-level sections of the VirtualDB configuration.

        Handles the four top-level sections: ``repositories`` (required),
        ``factor_aliases``, ``missing_value_labels``, and ``description``
        (all optional). Logs an INFO message for each optional section that
        is absent from the configuration.

        :param data: Raw configuration data
        :return: Processed configuration dict ready for Pydantic field validation
        :raises ValueError: If ``repositories`` is missing or empty, or if
            any repository config is invalid

        """
        if not isinstance(data, dict):
            return data

        repositories_data = data.get("repositories", {})

        if not repositories_data:
            raise ValueError(
                "Configuration must have a 'repositories' key "
                "with at least one repository"
            )

        for optional_key in ("factor_aliases", "missing_value_labels", "description"):
            if not data.get(optional_key):
                logger.info(
                    "No '%s' section found in VirtualDB configuration.",
                    optional_key,
                )

        # Parse each repository config
        repositories = {}
        for repo_id, repo_config in repositories_data.items():
            try:
                repositories[repo_id] = RepositoryConfig.model_validate(repo_config)
            except Exception as e:
                raise ValueError(
                    f"Invalid configuration for repository '{repo_id}': {e}"
                ) from e

        return {
            "factor_aliases": data.get("factor_aliases", {}),
            "missing_value_labels": data.get("missing_value_labels", {}),
            "description": data.get("description", {}),
            "repositories": repositories,
        }

    @classmethod
    def from_yaml(cls, path: Path | str) -> "MetadataConfig":
        """
        Load and validate configuration from YAML file.

        :param path: Path to YAML configuration file
        :return: Validated MetadataConfig instance
        :raises ValidationError: If configuration is invalid
        :raises FileNotFoundError: If file doesn't exist
        :raises ValueError: If YAML file does not contain a dictionary

        """
        with open(Path(path)) as f:
            data = yaml.safe_load(f)

        if not isinstance(data, dict):
            raise ValueError(
                f"Configuration file must contain a YAML dictionary, "
                f"got {type(data).__name__} instead"
            )

        return cls.model_validate(data)

    def get_repository_config(self, repo_id: str) -> RepositoryConfig | None:
        """
        Get configuration for a specific repository.

        :param repo_id: Repository ID (e.g., "BrentLab/harbison_2004")
        :return: RepositoryConfig instance or None if not found

        """
        return self.repositories.get(repo_id)

    def get_property_mappings(
        self, repo_id: str, config_name: str
    ) -> dict[str, PropertyMapping]:
        """
        Get merged property mappings for a repo/dataset combination.

        Merges repo-wide and dataset-specific mappings, with dataset-specific taking
        precedence.

        :param repo_id: Repository ID
        :param config_name: Dataset/config name
        :return: Dict mapping property names to PropertyMapping objects

        """
        repo_config = self.get_repository_config(repo_id)
        if not repo_config:
            return {}

        # Start with repo-wide properties
        mappings: dict[str, PropertyMapping] = dict(repo_config.properties)

        # Override with dataset-specific properties
        if repo_config.dataset and config_name in repo_config.dataset:
            dataset_config = repo_config.dataset[config_name]
            mappings.update(dataset_config.property_mappings)

        return mappings

    def get_tags(self, repo_id: str, config_name: str) -> dict[str, str]:
        """
        Get merged tags for a repo/dataset combination.

        Merges repo-level and dataset-level tags, with dataset-level tags taking
        precedence for the same key.

        :param repo_id: Repository ID
        :param config_name: Dataset/config name
        :return: Dict of merged tags

        """
        repo_config = self.get_repository_config(repo_id)
        if not repo_config:
            return {}

        merged: dict[str, str] = dict(repo_config.tags)

        if repo_config.dataset and config_name in repo_config.dataset:
            merged.update(repo_config.dataset[config_name].tags)

        return merged

    def get_sample_id_field(self, repo_id: str, config_name: str) -> str:
        """
        Resolve the actual column name for the sample identifier.

        Checks dataset-level ``sample_id`` first, then repo-level,
        falling back to ``"sample_id"`` if neither is configured.

        :param repo_id: Repository ID
        :param config_name: Dataset/config name
        :return: Column name for the sample identifier

        """
        repo_cfg = self.get_repository_config(repo_id)
        if not repo_cfg:
            return "sample_id"

        # Dataset-level takes precedence
        if repo_cfg.dataset and config_name in repo_cfg.dataset:
            ds_cfg = repo_cfg.dataset[config_name]
            if ds_cfg.sample_id is not None and ds_cfg.sample_id.field:
                return ds_cfg.sample_id.field

        # Repo-level fallback
        repo_sample_id = repo_cfg.properties.get("sample_id")
        if repo_sample_id is not None and repo_sample_id.field is not None:
            return repo_sample_id.field

        return "sample_id"
