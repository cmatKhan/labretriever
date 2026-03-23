"""
DataCard class for parsing and exploring HuggingFace dataset metadata.

This module provides the DataCard class for parsing HuggingFace dataset cards
into structured Python objects that can be easily explored. The focus is on
enabling users to drill down into the YAML structure to understand:

- Dataset configurations and their types
- Feature definitions and roles
- Experimental conditions at all hierarchy levels (top/config/field)
- Field-level condition definitions
- Metadata relationships

Users can then use this information to plan metadata table structures and
data loading strategies.

"""

import logging
from dataclasses import dataclass
from typing import Any

from pydantic import ValidationError

from labretriever.errors import DataCardError, DataCardValidationError, HfDataFetchError
from labretriever.fetchers import (
    HfDataCardFetcher,
    HfRepoStructureFetcher,
    HfSizeInfoFetcher,
)
from labretriever.models import (
    DatasetCard,
    DatasetConfig,
    ExtractedMetadata,
    FeatureInfo,
    MetadataRelationship,
)


@dataclass
class DatasetSchema:
    """
    Complete schema summary for a data configuration.

    Derived entirely from the DataCard YAML -- no DuckDB introspection needed. Used by
    VirtualDB to determine column partitioning between data and metadata parquets.

    :ivar data_columns: Column names present in the data parquet.
    :ivar metadata_columns: Column names that are metadata.
    :ivar join_columns: Columns common to both data and metadata parquets (used as JOIN
        keys for external metadata). Empty for embedded metadata (same parquet, no JOIN
        needed).
    :ivar metadata_source: One of ``"embedded"``, ``"external"``, or ``"none"``.
    :ivar external_metadata_config: Config name of the external metadata config, or
        ``None`` if metadata is embedded or absent.
    :ivar is_partitioned: Whether the data parquet is partitioned.

    """

    data_columns: set[str]
    metadata_columns: set[str]
    join_columns: set[str]
    metadata_source: str
    external_metadata_config: str | None
    is_partitioned: bool


class DataCard:
    """
    Parser and explorer for HuggingFace dataset metadata.

    The parsed structure uses Pydantic models with `extra="allow"` to accept
    arbitrary fields (like experimental_conditions) without requiring code
    changes.

    Key capabilities:
    - Parse dataset card YAML into structured objects
    - Navigate experimental conditions at 3 levels (top/config/field)
    - Explore field definitions and roles
    - Extract metadata schema for table design
    - Discover metadata relationships

    Example:
        >>> card = DataCard("BrentLab/harbison_2004")
        >>> # Use context manager for config exploration
        >>> with card.config("harbison_2004") as cfg:
        ...     # Get all experimental conditions
        ...     conds = cfg.experimental_conditions()
        ...     # Get condition fields with definitions
        ...     fields = cfg.condition_fields()
        ...     # Drill down into specific field
        ...     for name, info in fields.items():
        ...         for value, definition in info['definitions'].items():
        ...             print(f"{name}={value}: {definition}")

    Example (legacy API still supported):
        >>> card = DataCard("BrentLab/harbison_2004")
        >>> conditions = card.get_experimental_conditions("harbison_2004")
        >>> defs = card.get_field_definitions("harbison_2004", "condition")

    """

    def __init__(self, repo_id: str, token: str | None = None):
        """
        Initialize DataCard for a repository.

        :param repo_id: HuggingFace repository identifier (e.g., "user/dataset")
        :param token: Optional HuggingFace token for authentication

        """
        self.repo_id = repo_id
        self.token = token
        self.logger = logging.getLogger(self.__class__.__name__)

        # Initialize fetchers
        self._card_fetcher = HfDataCardFetcher(token=token)
        self._structure_fetcher = HfRepoStructureFetcher(token=token)
        self._size_fetcher = HfSizeInfoFetcher(token=token)

        # Cache for parsed card
        self._dataset_card: DatasetCard | None = None
        self._metadata_cache: dict[str, list[ExtractedMetadata]] = {}
        self._metadata_fields_map: dict[str, list[str]] = {}

    @property
    def dataset_card(self) -> DatasetCard:
        """Get the validated dataset card."""
        if self._dataset_card is None:
            self._load_and_validate_card()
        # this is here for type checking purposes. _load_and_validate_card()
        # will either set the _dataset_card or raise an error
        assert self._dataset_card is not None
        return self._dataset_card

    def _load_and_validate_card(self) -> None:
        """Load and validate the dataset card from HuggingFace."""
        try:
            self.logger.debug(f"Loading dataset card for {self.repo_id}")
            card_data = self._card_fetcher.fetch(self.repo_id)

            if not card_data:
                raise DataCardValidationError(
                    f"No dataset card found for {self.repo_id}"
                )

            # Validate using Pydantic model
            self._dataset_card = DatasetCard(**card_data)
            self._build_metadata_fields_map()
            self.logger.debug(f"Successfully validated dataset card for {self.repo_id}")

        except ValidationError as e:
            # Create a more user-friendly error message
            error_details = []
            for error in e.errors():
                field_path = " -> ".join(str(x) for x in error["loc"])
                error_type = error["type"]
                error_msg = error["msg"]
                input_value = error.get("input", "N/A")

                if "dtype" in field_path and error_type == "string_type":
                    error_details.append(
                        f"Field '{field_path}': Expected a simple data type "
                        "string (like 'string', 'int64', 'float64') "
                        "but got a complex structure. This might be a categorical "
                        "field with class labels. "
                        f"Actual value: {input_value}"
                    )
                else:
                    error_details.append(
                        f"Field '{field_path}': {error_msg} (got: {input_value})"
                    )

            detailed_msg = (
                f"Dataset card validation failed for {self.repo_id}:\n"
                + "\n".join(f"  - {detail}" for detail in error_details)
            )
            self.logger.error(detailed_msg)
            raise DataCardValidationError(detailed_msg) from e
        except HfDataFetchError as e:
            raise DataCardError(f"Failed to fetch dataset card: {e}") from e

    @property
    def configs(self) -> list[DatasetConfig]:
        """Get all dataset configurations."""
        return self.dataset_card.configs

    def get_config(self, config_name: str) -> DatasetConfig | None:
        """Get a specific configuration by name."""
        return self.dataset_card.get_config_by_name(config_name)

    def get_features(self, config_name: str) -> list[FeatureInfo]:
        """
        Get all feature definitions for a configuration.

        :param config_name: Configuration name
        :return: List of FeatureInfo objects
        :raises DataCardError: If config not found

        """
        config = self.get_config(config_name)
        if not config:
            raise DataCardError(f"Configuration '{config_name}' not found")

        return config.dataset_info.features

    def _extract_partition_values(
        self, config: DatasetConfig, field_name: str
    ) -> set[str]:
        """Extract values from partition structure."""
        if (
            not config.dataset_info.partitioning
            or not config.dataset_info.partitioning.enabled
        ):
            return set()

        partition_columns = config.dataset_info.partitioning.partition_by or []
        if field_name not in partition_columns:
            return set()

        try:
            # Get partition values from repository structure
            partition_values = self._structure_fetcher.get_partition_values(
                self.repo_id, field_name
            )
            return set(partition_values)
        except HfDataFetchError:
            self.logger.warning(f"Failed to extract partition values for {field_name}")
            return set()

    def get_metadata_relationships(
        self, refresh_cache: bool = False
    ) -> list[MetadataRelationship]:
        """
        Get relationships between data configs and their metadata.

        :param refresh_cache: If True, force refresh dataset card from remote

        """
        # Clear cached dataset card if refresh requested
        if refresh_cache:
            self._dataset_card = None

        relationships = []
        data_configs = self.dataset_card.get_data_configs()
        metadata_configs = self.dataset_card.get_metadata_configs()

        for data_config in data_configs:
            # Check for explicit applies_to relationships
            for meta_config in metadata_configs:
                if (
                    meta_config.applies_to
                    and data_config.config_name in meta_config.applies_to
                ):
                    relationships.append(
                        MetadataRelationship(
                            data_config=data_config.config_name,
                            metadata_config=meta_config.config_name,
                            relationship_type="explicit",
                        )
                    )

            # Check for embedded metadata (always runs regardless of
            # explicit relationships)
            if data_config.metadata_fields:
                relationships.append(
                    MetadataRelationship(
                        data_config=data_config.config_name,
                        metadata_config=f"{data_config.config_name}_embedded",
                        relationship_type="embedded",
                    )
                )

        return relationships

    def _build_metadata_fields_map(self) -> None:
        """
        Build a mapping from data config names to their metadata fields.

        Called during card loading. For each data config, resolves metadata
        fields from two sources:

        1. Embedded: the data config has ``metadata_fields`` listing which
           of its own columns are metadata.
        2. External: a separate metadata-type config has ``applies_to``
           including this config name. The metadata fields are the feature
           names from that metadata config.

        Embedded takes priority. For external, the first matching metadata
        config wins.

        """
        assert self._dataset_card is not None
        self._metadata_fields_map = {}
        meta_configs = self._dataset_card.get_metadata_configs()

        for data_cfg in self._dataset_card.get_data_configs():
            name = data_cfg.config_name
            # Embedded case
            if data_cfg.metadata_fields:
                self._metadata_fields_map[name] = list(data_cfg.metadata_fields)
                continue
            # External case: find metadata config with applies_to
            for meta_cfg in meta_configs:
                if meta_cfg.applies_to and name in meta_cfg.applies_to:
                    self._metadata_fields_map[name] = [
                        f.name for f in meta_cfg.dataset_info.features
                    ]
                    break
            else:
                self.logger.info(
                    "No metadata fields found for data config '%s' "
                    "in repo '%s' -- no embedded metadata_fields and "
                    "no metadata config with applies_to",
                    name,
                    self.repo_id,
                )

    def get_metadata_fields(self, config_name: str) -> list[str] | None:
        """
        Get metadata field names for a data configuration.

        Returns pre-computed metadata fields resolved during card loading.
        Handles both embedded metadata (``metadata_fields`` on the data
        config) and external metadata (separate metadata config with
        ``applies_to``).

        :param config_name: Name of the data configuration
        :return: List of metadata field names, or None if no metadata

        """
        # Ensure card is loaded (triggers _build_metadata_fields_map)
        _ = self.dataset_card
        return self._metadata_fields_map.get(config_name)

    def get_data_col_names(self, config_name: str) -> set[str]:
        """
        Return the column names from the data config's feature list.

        These are the columns present in the data parquet file, derived directly from
        the DataCard feature definitions without any DuckDB introspection.

        :param config_name: Name of the data configuration
        :return: Set of column names, empty if config not found

        """
        _ = self.dataset_card  # ensure loaded
        config = self.get_config(config_name)
        if not config:
            return set()
        return {f.name for f in config.dataset_info.features}

    def get_metadata_config_name(self, config_name: str) -> str | None:
        """
        Return the config_name of the external metadata config, if any.

        If the data config has embedded ``metadata_fields``, or if no
        metadata config with ``applies_to`` references this config,
        returns None.

        :param config_name: Name of the data configuration
        :return: The metadata config name, or None

        """
        _ = self.dataset_card  # ensure loaded
        data_cfg = self.get_config(config_name)
        if not data_cfg:
            return None
        # Embedded metadata -- no external config needed
        if data_cfg.metadata_fields:
            return None
        # Find external metadata config with applies_to
        for meta_cfg in self.dataset_card.get_metadata_configs():
            if meta_cfg.applies_to and config_name in meta_cfg.applies_to:
                return meta_cfg.config_name
        return None

    def get_dataset_schema(self, config_name: str) -> DatasetSchema | None:
        """
        Return schema summary for a data configuration.

        Determines whether metadata is embedded or external, which
        columns belong to data vs metadata parquets, and which columns
        are shared between them (join keys for external metadata).
        All information is derived from the DataCard YAML -- no DuckDB
        introspection is needed.

        :param config_name: Name of the data configuration
        :return: DatasetSchema instance, or None if config not found

        Example -- embedded metadata::

            schema = card.get_dataset_schema("harbison_2004")
            # schema.metadata_source == "embedded"
            # schema.join_columns == set()  (same parquet, no JOIN)

        Example -- external metadata::

            schema = card.get_dataset_schema("annotated_features")
            # schema.metadata_source == "external"
            # schema.external_metadata_config == "annotated_features_meta"
            # schema.join_columns == {"id"}  (common to both parquets)

        """
        _ = self.dataset_card  # ensure loaded
        config = self.get_config(config_name)
        if not config:
            return None

        is_partitioned = bool(
            config.dataset_info.partitioning
            and config.dataset_info.partitioning.enabled
        )

        # Embedded: metadata_fields lists which of the config's own
        # columns are metadata; all live in the same parquet
        if config.metadata_fields:
            all_cols = {f.name for f in config.dataset_info.features}
            meta_cols = set(config.metadata_fields)
            data_cols = all_cols - meta_cols
            return DatasetSchema(
                data_columns=data_cols,
                metadata_columns=meta_cols,
                join_columns=set(),
                metadata_source="embedded",
                external_metadata_config=None,
                is_partitioned=is_partitioned,
            )

        # External: find metadata config with applies_to
        for meta_cfg in self.dataset_card.get_metadata_configs():
            if meta_cfg.applies_to and config_name in meta_cfg.applies_to:
                data_cols = {f.name for f in config.dataset_info.features}
                meta_cols = {f.name for f in meta_cfg.dataset_info.features}
                join_cols = data_cols & meta_cols
                return DatasetSchema(
                    data_columns=data_cols,
                    metadata_columns=meta_cols,
                    join_columns=join_cols,
                    metadata_source="external",
                    external_metadata_config=meta_cfg.config_name,
                    is_partitioned=is_partitioned,
                )

        # No metadata relationship -- treat all columns as data
        all_cols = {f.name for f in config.dataset_info.features}
        return DatasetSchema(
            data_columns=all_cols,
            metadata_columns=set(),
            join_columns=set(),
            metadata_source="none",
            external_metadata_config=None,
            is_partitioned=is_partitioned,
        )

    def get_repository_info(self) -> dict[str, Any]:
        """Get general repository information."""
        card = self.dataset_card

        try:
            structure = self._structure_fetcher.fetch(self.repo_id)
            total_files = structure.get("total_files", 0)
            last_modified = structure.get("last_modified")
        except HfDataFetchError:
            total_files = None
            last_modified = None

        return {
            "repo_id": self.repo_id,
            "pretty_name": card.pretty_name,
            "license": card.license,
            "tags": card.tags,
            "language": card.language,
            "size_categories": card.size_categories,
            "num_configs": len(card.configs),
            "dataset_types": [config.dataset_type.value for config in card.configs],
            "total_files": total_files,
            "last_modified": last_modified,
            "has_default_config": self.dataset_card.default_config is not None,
        }

    def extract_metadata_schema(self, config_name: str) -> dict[str, Any]:
        """
        Extract complete metadata schema for planning metadata table structure.

        This is the primary method for understanding what metadata is available and
        how to structure it into a metadata table. It consolidates information from
        all sources:

        - **Field roles**: Which fields are regulators, targets, conditions, etc.
        - **Top-level conditions**: Repo-wide conditions (constant for all samples)
        - **Config-level conditions**: Config-specific conditions
          (constant for this config)
        - **Field-level definitions**: Per-sample condition definitions

        The returned schema provides all the information needed to:
        1. Identify sample identifier fields (regulator_identifier, etc.)
        2. Determine which conditions are constant vs. variable
        3. Access condition definitions for creating flattened columns
        4. Plan metadata table structure

        :param config_name: Configuration name to extract schema for
        :return: Dict with comprehensive schema including:
            - regulator_fields: List of regulator identifier field names
            - target_fields: List of target identifier field names
            - condition_fields: List of experimental_condition field names
            - condition_definitions: Dict mapping field -> value -> definition
            - top_level_conditions: Dict of repo-wide conditions
            - config_level_conditions: Dict of config-specific conditions
        :raises DataCardError: If configuration not found

        Example:
            >>> schema = card.extract_metadata_schema('harbison_2004')
            >>> # Identify identifier fields
            >>> print(f"Regulator fields: {schema['regulator_fields']}")
            >>> # Check for constant conditions
            >>> if schema['top_level_conditions']:
            ...     print("Has repo-wide constant conditions")
            >>> # Get field-level definitions for metadata table
            >>> for field in schema['condition_fields']:
            ...     defs = schema['condition_definitions'][field]
            ...     print(f"{field} has {len(defs)} levels")

        """
        config = self.get_config(config_name)
        if not config:
            raise DataCardError(f"Configuration '{config_name}' not found")

        schema: dict[str, Any] = {
            "regulator_fields": [],
            "target_fields": [],
            "condition_fields": [],
            "condition_definitions": {},
            "metadata_fields": None,
            "top_level_conditions": None,
            "config_level_conditions": None,
        }

        for feature in config.dataset_info.features:
            if feature.role == "regulator_identifier":
                schema["regulator_fields"].append(feature.name)
            elif feature.role == "target_identifier":
                schema["target_fields"].append(feature.name)
            elif feature.role == "experimental_condition":
                schema["condition_fields"].append(feature.name)
                if feature.definitions:
                    schema["condition_definitions"][feature.name] = feature.definitions

        # Include features from external metadata config
        meta_fields = self.get_metadata_fields(config_name)
        schema["metadata_fields"] = meta_fields
        if meta_fields is not None and not config.metadata_fields:
            for meta_cfg in self.dataset_card.get_metadata_configs():
                if meta_cfg.applies_to and config_name in meta_cfg.applies_to:
                    for feature in meta_cfg.dataset_info.features:
                        if feature.role == "regulator_identifier":
                            schema["regulator_fields"].append(feature.name)
                        elif feature.role == "target_identifier":
                            schema["target_fields"].append(feature.name)
                        elif feature.role == "experimental_condition":
                            schema["condition_fields"].append(feature.name)
                            if feature.definitions:
                                schema["condition_definitions"][
                                    feature.name
                                ] = feature.definitions
                    break

        # Add top-level conditions (applies to all configs/samples)
        if self.dataset_card.model_extra:
            top_level = self.dataset_card.model_extra.get("experimental_conditions")
            if top_level:
                schema["top_level_conditions"] = top_level

        # Add config-level conditions (applies to this config's samples)
        if config.model_extra:
            config_level = config.model_extra.get("experimental_conditions")
            if config_level:
                schema["config_level_conditions"] = config_level

        return schema

    def get_experimental_conditions(
        self, config_name: str | None = None
    ) -> dict[str, Any]:
        """
        Get experimental conditions with proper hierarchy handling.

        This method enables drilling down into the experimental conditions hierarchy:
        - Top-level (repo-wide): Common to all configs/samples
        - Config-level: Specific to a config, common to its samples
        - Field-level: Per-sample variation (use get_field_definitions instead)

        Returns experimental conditions at the appropriate level:
        - If config_name is None: returns top-level (repo-wide) conditions only
        - If config_name is provided: returns merged (top + config) conditions

        All conditions are returned as flexible dicts that preserve the original
        YAML structure. Navigate nested dicts to access specific values.

        :param config_name: Optional config name. If provided, merges top
          and config levels
        :return: Dict of experimental conditions (empty dict if none defined)

        Example:
            >>> # Get top-level conditions
            >>> top = card.get_experimental_conditions()
            >>> temp = top.get('temperature_celsius', 30)
            >>>
            >>> # Get merged conditions for a config
            >>> merged = card.get_experimental_conditions('config_name')
            >>> media = merged.get('media', {})
            >>> media_name = media.get('name', 'unspecified')

        """
        # Get top-level conditions (stored in model_extra)
        top_level = (
            self.dataset_card.model_extra.get("experimental_conditions", {})
            if self.dataset_card.model_extra
            else {}
        )

        # If no config specified, return top-level only
        if config_name is None:
            return top_level.copy() if isinstance(top_level, dict) else {}

        # Get config-level conditions
        config = self.get_config(config_name)
        if not config:
            raise DataCardError(f"Configuration '{config_name}' not found")

        config_level = (
            config.model_extra.get("experimental_conditions", {})
            if config.model_extra
            else {}
        )

        # Merge: config-level overrides top-level
        merged = {}
        if isinstance(top_level, dict):
            merged.update(top_level)
        if isinstance(config_level, dict):
            merged.update(config_level)

        return merged

    def get_field_definitions(
        self, config_name: str, field_name: str
    ) -> dict[str, Any]:
        """
        Get definitions for a specific field (field-level conditions).

        This is the third level of the experimental conditions hierarchy - conditions
        that vary per sample. Returns a dict mapping each possible field value to its
        detailed specification.

        For fields with role=experimental_condition, the definitions typically include
        nested structures like media composition, temperature, treatments, etc. that
        define what each categorical value means experimentally.

        :param config_name: Configuration name
        :param field_name: Field name (typically has role=experimental_condition)
        :return: Dict mapping field values to their definition dicts
          (empty if no definitions)
        :raises DataCardError: If config or field not found

        Example:
            >>> # Get condition definitions
            >>> defs = card.get_field_definitions('harbison_2004', 'condition')
            >>> # defs = {'YPD': {...}, 'HEAT': {...}, ...}
            >>>
            >>> # Drill down into a specific condition
            >>> ypd = defs['YPD']
            >>> env_conds = ypd.get('environmental_conditions', {})
            >>> media = env_conds.get('media', {})
            >>> media_name = media.get('name')

        """
        config = self.get_config(config_name)
        if not config:
            raise DataCardError(f"Configuration '{config_name}' not found")

        # Find the feature
        feature = None
        for f in config.dataset_info.features:
            if f.name == field_name:
                feature = f
                break

        if not feature:
            raise DataCardError(
                f"Field '{field_name}' not found in config '{config_name}'"
            )

        # Return definitions if present, otherwise empty dict
        return feature.definitions if feature.definitions else {}

    def summary(self) -> str:
        """Get a human-readable summary of the dataset."""
        card = self.dataset_card
        info = self.get_repository_info()

        lines = [
            f"Dataset: {card.pretty_name or self.repo_id}",
            f"Repository: {self.repo_id}",
            f"License: {card.license or 'Not specified'}",
            f"Configurations: {len(card.configs)}",
            f"Dataset Types: {', '.join(info['dataset_types'])}",
        ]

        if card.tags:
            lines.append(f"Tags: {', '.join(card.tags)}")

        # Add config summaries
        lines.append("\nConfigurations:")
        for config in card.configs:
            default_mark = " (default)" if config.default else ""
            lines.append(
                f"  - {config.config_name}: {config.dataset_type.value}{default_mark}"
            )
            lines.append(f"    {config.description}")

        return "\n".join(lines)
