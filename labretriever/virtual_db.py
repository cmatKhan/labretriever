"""
VirtualDB provides a SQL query interface across heterogeneous datasets.

A developer creates huggingface repos with datacards. Datacard specifications
specific to labretriever can be found at
https://cmatKhan.github.io/labretriever/huggingface_datacard/. Next, a developer can
create a virtualDB configuration file that describes which huggingface repos and
datasets to use, a set of common fields, datasets that contain comparative analytics,
and more. VirtualDB, this code, then uses DuckDB to construct views over Parquet
files cached locally on initialization. For primary datasets, VirtualDB creates
metadata views (one row per sample with derived columns) and full data views
(measurement-level data joined to metadata). For comparative analysis datasets,
VirtualDB creates expanded views that parse composite ID fields into ``_source``
(aliased to the configured db_name) and ``_id`` (sample identifier) columns.
The expectation is that a developer will use this interface to write SQL queries
against the views to provide an API to downstream users and applications.

Example Usage::

    from labretriever.virtual_db import VirtualDB

    vdb = VirtualDB("config.yaml", token=token)

    # Discover views
    vdb.tables()
    vdb.describe("harbison")

    # Raw SQL
    df = vdb.query("SELECT * FROM harbison WHERE sample_id = 42")

    # Parameterized SQL
    df = vdb.query(
        "SELECT * FROM harbison_meta WHERE carbon_source = $cs",
        cs="glucose",
    )

    # Prepared queries
    vdb.prepare("sig", "SELECT * FROM harbison_meta LIMIT $n")
    df = vdb.query("sig", n=10)

"""

from __future__ import annotations

import logging
import re
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
from duckdb import BinderException

from labretriever.datacard import DataCard, DatasetSchema
from labretriever.models import DATASET_TYPE_COMPARATIVE, MetadataConfig, RegionSetInfo

logger = logging.getLogger(__name__)


@dataclass
class ColumnMeta:
    """
    Metadata for a single column in a VirtualDB ``_meta`` view.

    :param description: Human-readable description of the column.
    :param role: Semantic role from the DataCard feature definition
        (e.g. ``"experimental_condition"``).
    :param level_definitions: For ``role="experimental_condition"`` columns
        that have per-level definitions: maps each level value to its
        description string.  ``None`` when no per-level definitions exist.

    """

    description: str | None = None
    role: str | None = None
    level_definitions: dict[str, str] | None = None


class QueryError(Exception):
    """Raised when a VirtualDB query fails at execution time."""

    pass


def get_nested_value(data: dict | list, path: str) -> Any:
    """
    Navigate nested dict/list using dot notation.

    Handles missing intermediate keys gracefully by returning None.
    When an intermediate value is a list of dicts, extracts the
    remaining path from each item and returns a list of results.

    :param data: Dictionary or list of dicts to navigate
    :param path: Dot-separated path (e.g., "media.carbon_source.compound")
    :return: Value at path, list of values, or None if not found

    :raises TypeError: If an unexpected type is encountered during navigation of the
        dict/list structure according to the provided path.

    Example -- dict input::

        >>> get_nested_value({"media": {"name": "YPD"}}, "media.name")
        'YPD'

    Example -- list-of-dicts at an intermediate node::

        >>> data = {
        ...     "media": {
        ...         "carbon_source": [
        ...             {"compound": "glucose"},
        ...         ]
        ...     }
        ... }
        >>> get_nested_value(data, "media.carbon_source.compound")
        ['glucose']

    """
    if not isinstance(data, (dict, list)):
        return None

    # If top-level data is a list, extract path from each item
    if isinstance(data, list):
        results = []
        for item in data:
            if isinstance(item, dict):
                val = get_nested_value(item, path)
                if val is not None:
                    results.append(val)
        return results if results else None

    keys = path.split(".")
    current = data

    for i, key in enumerate(keys):
        if isinstance(current, dict):
            if key not in current:
                logger.warning(
                    "Key '%s' not found at path '%s' (current keys: %s)",
                    key,
                    ".".join(keys[: i + 1]),
                    list(current.keys()),
                )
                return None
            current = current[key]
        elif isinstance(current, list):
            # Extract the remaining path from each list item
            remaining_path = ".".join(keys[i:])
            results = []
            for item in current:
                if isinstance(item, dict):
                    val = get_nested_value(item, remaining_path)
                    if val is not None:
                        results.append(val)
            return results if results else None
        else:
            error_msg = (
                f"Unexpected type '{type(current).__name__}' at "
                f"path '{'.'.join(keys[:i])}'; expected dict or "
                f"list of dicts"
            )
            logger.error(error_msg)
            raise TypeError(error_msg)

    return current


def _quote_ident(name: str) -> str:
    """Double-quote a SQL identifier, escaping any embedded double-quotes."""
    return '"' + name.replace('"', '""') + '"'


@lru_cache(maxsize=32)
def _cached_datacard(repo_id: str, token: str | None = None) -> Any:
    """
    Return a cached DataCard instance.

    :param repo_id: HuggingFace repository ID
    :param token: Optional HuggingFace token
    :return: DataCard instance

    """
    return DataCard(repo_id, token=token)


class VirtualDB:
    """
    A query interface across heterogeneous datasets.

    DuckDB views are lazily registered over Parquet files on first
    ``query()`` call. The user writes SQL against named views.

    :ivar config: Validated MetadataConfig
    :ivar token: Optional HuggingFace token

    """

    def __init__(
        self,
        config_path: Path | str,
        token: str | None = None,
        duckdb_connection: duckdb.DuckDBPyConnection | None = None,
        local_files_only: bool = False,
        cache_dir: Path | str | None = None,
    ):
        """
        Initialize VirtualDB with configuration.

        Creates the DuckDB connection and registers all views immediately.

        :param config_path: Path to YAML configuration file
        :param token: Optional HuggingFace token for private datasets
        :param duckdb_connection: Optional DuckDB connection. If provided, views will be
            registered on this connection instead of creating a new in-memory database.
            This provides a method of using a persistent database file. If not provided,
            an in-memory DuckDB connection is created.
        :param local_files_only: If ``True``, skip HuggingFace network checks and use
            only locally cached files. Eliminates per-dataset ``repo_info``
            HTTP round-trips on warm restarts. Raises
            ``huggingface_hub.utils.LocalEntryNotFoundError`` if
            any required file is absent from the local cache.
        :param cache_dir: Override the HuggingFace cache directory. When ``None``,
            ``huggingface_hub`` resolves the location from ``HF_CACHE_DIR`` /
            ``HF_HOME`` / its own default. Pass an explicit path to store or read
            parquet snapshots from a non-default location (e.g. a bundled directory
            inside the deployed application).
        :raises FileNotFoundError: If config file does not exist
        :raises ValueError: If configuration is invalid

        """
        self.config = MetadataConfig.from_yaml(config_path)
        self.token = token
        self.local_files_only = local_files_only
        # Explicit argument wins; fall back to get_cache_dir() which reads
        # HF_CACHE_DIR at call time so CLI --cache-dir is respected even when
        # VirtualDB is constructed without the parameter.
        from labretriever.constants import get_cache_dir

        self.cache_dir: Path = (
            Path(cache_dir) if cache_dir is not None else get_cache_dir()
        )

        self._conn: duckdb.DuckDBPyConnection = (
            duckdb_connection
            if duckdb_connection is not None
            else duckdb.connect(":memory:")
        )

        # db_name -> (repo_id, config_name)
        self.db_name_map = self._build_db_name_map()

        # Prepared queries: name -> sql
        self._prepared_queries: dict[str, str] = {}

        _t0 = time.monotonic()

        t = time.monotonic()
        self._load_datacards()
        logger.debug("_load_datacards completed in %.3fs", time.monotonic() - t)

        t = time.monotonic()
        self._validate_datacards()
        logger.debug("_validate_datacards completed in %.3fs", time.monotonic() - t)

        t = time.monotonic()
        self._update_cache()
        logger.debug("_update_cache completed in %.3fs", time.monotonic() - t)

        t = time.monotonic()
        self._register_all_views()
        logger.debug("_register_all_views completed in %.3fs", time.monotonic() - t)

        t = time.monotonic()
        self._build_column_metadata()
        logger.debug("_build_column_metadata completed in %.3fs", time.monotonic() - t)

        logger.debug("VirtualDB.__init__ total: %.3fs", time.monotonic() - _t0)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, sql: str, **params: Any) -> pd.DataFrame:
        """
        Execute SQL or a prepared query and return a DataFrame.

        If *sql* matches a registered prepared-query name the stored
        SQL template is used instead. Keyword arguments are passed as
        named parameters to DuckDB.

        :param sql: Raw SQL string **or** name of a prepared query
        :param params: Named parameters (DuckDB ``$name`` syntax)
        :return: Query result as a pandas DataFrame

        Examples::

            # Raw SQL
            df = vdb.query("SELECT * FROM harbison LIMIT 5")

            # With parameters
            df = vdb.query(
                "SELECT * FROM harbison_meta WHERE carbon_source = $cs",
                cs="glucose",
            )

            # Prepared query
            vdb.prepare("top", "SELECT * FROM harbison_meta LIMIT $n")
            df = vdb.query("top", n=10)

        """
        # param `sql` may be a prepared query name, a raw sql statement, or
        # a parameterized sql statement that is not prepared. If it exists as a key
        # in the _prepared_queries dict, we use the prepared sql. Otherwise, we
        # use the sql as passed to query().
        resolved = self._prepared_queries.get(sql, sql)
        try:
            if params:
                return self._conn.execute(resolved, params).fetchdf()
            return self._conn.execute(resolved).fetchdf()
        except Exception as exc:
            import pprint

            params_repr = pprint.pformat(params, indent=2)
            raise QueryError(
                f"query failed: {exc}\n\n" f"SQL:\n{sql}\n\n" f"params:\n{params_repr}"
            ) from exc

    def prepare(self, name: str, sql: str, overwrite: bool = False) -> None:
        """
        Register a named parameterized query for later use.

        Parameters use DuckDB ``$name`` syntax.

        :param name: Query name (must not collide with a view name)
        :param sql: SQL template with ``$name`` parameters
        :param overwrite: If True, overwrite existing prepared query
            with same name
        :raises ValueError: If *name* collides with an existing view

        Example::

            vdb.prepare("glucose_regs", '''
                SELECT regulator_symbol, COUNT(*) AS n
                FROM harbison_meta
                WHERE carbon_source = $cs
                GROUP BY regulator_symbol
                HAVING n >= $min_n
            ''')
            df = vdb.query("glucose_regs", cs="glucose", min_n=2)

        """

        if name in self._list_views() and not overwrite:
            error_msg = (
                f"Prepared-query name '{name}' collides with "
                f"an existing view. Choose a different name or set "
                f"overwrite=True."
            )
            logger.error(error_msg)
            raise ValueError(error_msg)
        self._prepared_queries[name] = sql

    def tables(self) -> list[str]:
        """
        Return sorted list of registered view names.

        :return: Sorted list of view names

        """

        return sorted(self._list_views())

    def describe(self, table: str | None = None) -> pd.DataFrame:
        """
        Describe column names and types for one or all views.

        :param table: View name, or None for all views
        :return: DataFrame with columns ``table``, ``column_name``,
                 ``column_type``

        """

        if table is not None:
            df = self._conn.execute(f"DESCRIBE {table}").fetchdf()
            df.insert(0, "table", table)
            return df

        frames = []
        for view in sorted(self._list_views()):
            df = self._conn.execute(f"DESCRIBE {view}").fetchdf()
            df.insert(0, "table", view)
            frames.append(df)
        if not frames:
            return pd.DataFrame(columns=["table", "column_name", "column_type"])
        return pd.concat(frames, ignore_index=True)

    def get_fields(self, table: str | None = None) -> list[str]:
        """
        Return column names for a view or all unique columns.

        :param table: View name, or None for all views
        :return: Sorted list of column names

        """

        if table is not None:
            cols = self._conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table}'"
            ).fetchdf()
            return sorted(cols["column_name"].tolist())

        all_cols: set[str] = set()
        for view in self._list_views():
            cols = self._conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{view}'"
            ).fetchdf()
            all_cols.update(cols["column_name"].tolist())
        return sorted(all_cols)

    def get_common_fields(self) -> list[str]:
        """
        Return columns present in ALL primary ``_meta`` views.

        Primary dataset views are those without ``links`` in their
        config (i.e. not comparative datasets).

        :return: Sorted list of common column names

        """

        meta_views = self._get_primary_meta_view_names()
        if not meta_views:
            return []

        sets = []
        for view in meta_views:
            cols = self._conn.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{view}'"
            ).fetchdf()
            sets.append(set(cols["column_name"].tolist()))

        common = set.intersection(*sets)
        return sorted(common)

    def get_datasets(self) -> list[str]:
        """
        Return the sorted list of dataset names known to this VirtualDB.

        Dataset names are the resolved ``db_name`` values from the
        configuration (falling back to the config_name when ``db_name``
        is not explicitly set). These are the names accepted by
        :meth:`get_tags` and queryable via :meth:`query`.

        Unlike :meth:`tables`, this method reads directly from the
        configuration and does not require views to be registered, so
        no data is downloaded.

        :return: Sorted list of dataset names

        """
        return sorted(self.db_name_map)

    def materialize(self) -> None:
        """
        Replace all registered dataset views with in-memory DuckDB tables.

        For each dataset known to this VirtualDB, reads the public data view
        and the corresponding ``_meta`` view into in-memory tables, then
        re-points the original view names at those tables. Subsequent queries
        hit RAM instead of re-scanning parquet files, which substantially
        reduces per-query latency at the cost of increased startup time and
        memory usage.

        Only views that exist in the current DuckDB session are materialized;
        missing views are silently skipped. Safe to call multiple times —
        uses ``CREATE OR REPLACE`` semantics.

        """
        conn = self._conn
        for db_name in self.get_datasets():
            for view_name in (db_name, f"{db_name}_meta"):
                row = conn.execute(
                    "SELECT view_name FROM duckdb_views() WHERE view_name = ?",
                    [view_name],
                ).fetchone()
                if row is None:
                    continue
                table_name = f"_mat_{view_name}"
                conn.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {view_name}"
                )
                conn.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {table_name}"
                )
                logger.debug("materialize: %s -> %s", view_name, table_name)

    def get_tags(self, db_name: str) -> dict[str, str]:
        """
        Return the merged tags for a dataset.

        Tags are defined in the configuration at the repository and/or
        dataset level. Dataset-level tags override repository-level tags
        with the same key. See the ``tags`` section of the configuration
        guide for details.

        :param db_name: Dataset name as it appears in :meth:`tables` (the
            resolved ``db_name`` from the configuration, or the
            ``config_name`` if ``db_name`` was not explicitly set).
        :return: Dict of merged tags, or empty dict if the dataset has no
            tags or the name is not found.

        """
        if db_name not in self.db_name_map:
            return {}
        repo_id, config_name = self.db_name_map[db_name]
        return self.config.get_tags(repo_id, config_name)

    def get_region_sets(self, db_name: str) -> dict[str, RegionSetInfo]:
        """
        Return merged region set info for a dataset.

        Merges three layers in order (later overrides earlier):

        1. Datacard repo-level ``genome_resources.region_sets``
        2. Datacard config-level ``genome_resources.region_sets``
        3. VirtualDB genome-resource repo entries (repos with ``genome_resources``
           and no ``dataset`` key)

        :param db_name: Dataset name as returned by :meth:`tables`.
        :returns: Dict mapping region set name to
            :class:`~labretriever.models.RegionSetInfo`.
            Empty dict if none are configured.

        """
        if db_name not in self.db_name_map:
            return {}

        repo_id, config_name = self.db_name_map[db_name]
        card = self.datacards.get(repo_id)

        merged: dict[str, RegionSetInfo] = {}

        # Layer 1: datacard repo-level genome_resources
        if card is not None and card.dataset_card.genome_resources is not None:
            for name, info in card.dataset_card.genome_resources.region_sets.items():
                merged[name] = RegionSetInfo(
                    path=info.path,
                    join_column=info.join_column,
                )

        # Layer 2: datacard config-level genome_resources
        if card is not None:
            dc_config = card.get_config(config_name)
            if dc_config is not None and dc_config.genome_resources is not None:
                for name, info in dc_config.genome_resources.region_sets.items():
                    if name in merged:
                        # config-level overrides repo-level fields individually
                        existing = merged[name]
                        merged[name] = RegionSetInfo(
                            description=existing.description,
                            path=info.path if info.path is not None else existing.path,
                            join_column=(
                                info.join_column
                                if info.join_column is not None
                                else existing.join_column
                            ),
                        )
                    else:
                        merged[name] = RegionSetInfo(
                            path=info.path,
                            join_column=info.join_column,
                        )

        # Layer 3: VirtualDB genome-resource repo entries
        for _, repo_cfg in self.config.repositories.items():
            if repo_cfg.genome_resources is None or repo_cfg.dataset is not None:
                continue
            for name, vdb_info in repo_cfg.genome_resources.region_sets.items():
                if name in merged:
                    existing = merged[name]
                    merged[name] = RegionSetInfo(
                        description=(
                            vdb_info.description
                            if vdb_info.description is not None
                            else existing.description
                        ),
                        path=(
                            vdb_info.path
                            if vdb_info.path is not None
                            else existing.path
                        ),
                        join_column=(
                            vdb_info.join_column
                            if vdb_info.join_column is not None
                            else existing.join_column
                        ),
                    )
                else:
                    merged[name] = RegionSetInfo(
                        description=vdb_info.description,
                        path=vdb_info.path,
                        join_column=vdb_info.join_column,
                    )

        return merged

    def get_region_set_info(self, db_name: str, name: str) -> RegionSetInfo | None:
        """
        Return info for a specific region set by name, or None.

        :param db_name: Dataset name as returned by :meth:`tables`.
        :param name: Region set name (e.g. ``'yiming_promoters'``).
        :returns: Merged :class:`~labretriever.models.RegionSetInfo`, or
            ``None`` if not found.

        """
        return self.get_region_sets(db_name).get(name)

    def get_condition_field_info(self, db_name: str) -> dict[str, Any] | None:
        """
        Return hierarchically linked property column groups for a dataset.

        Some datasets expose multiple derived property columns in their
        ``_meta`` view (e.g. ``"Carbon source"`` and ``"Temperature"``) that
        are both extracted from the same per-sample experimental-condition
        field via field+path ``PropertyMapping`` entries in the VirtualDB
        configuration.  Because these columns are co-derived from the same
        source field, the values that can appear in one column are constrained
        by the value selected in another (selecting ``Carbon source =
        glucose`` limits which ``Temperature`` values are meaningful).

        This method identifies such groups and enriches them with per-level
        descriptions from the DataCard, making it straightforward for
        downstream tools to build conditional filter interfaces.

        :param db_name: Dataset name as returned by :meth:`get_datasets`.
        :type db_name: str
        :returns: A dict keyed by source parquet field name.  Each value is
            a dict with two keys:

            ``"property_cols"`` — ``list[str]``
                The derived property column names that appear in the
                ``{db_name}_meta`` view and share this source field.

            ``"level_descriptions"`` — ``dict[str, str]``
                Maps each categorical level of the source field to a
                human-readable description string retrieved from the
                DataCard via :meth:`~labretriever.datacard.DataCard.get_field_definitions`. # noqa: E501
                Levels without a ``"description"`` key in their definition
                dict default to ``"Description unavailable"``.

            Returns ``None`` when the dataset has no linked property groups
            (no Type-A field-only mapping exists, or there are no other
            property columns to serve as downstream members),
            or when ``db_name`` is not found.
        :rtype: dict[str, Any] | None

        Example::

            info = vdb.get_condition_field_info("harbison")
            if info is not None:
                for src_field, group in info.items():
                    print(f"Source field '{src_field}' links:")
                    print(f"  property columns: {group['property_cols']}")
                    for level, desc in group["level_descriptions"].items():
                        print(f"  {level}: {desc}")

        """
        warnings.warn(
            "get_condition_field_info is deprecated; use get_column_metadata instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if db_name not in self.db_name_map:
            return None

        repo_id, config_name = self.db_name_map[db_name]
        mappings = self.config.get_property_mappings(repo_id, config_name)

        # Collect Type-C (path-only) columns — repo-level experimental condition
        # extractions.  These are shared downstream members for any Type-A anchor
        # that has at least one co-occurring Type-B sibling or Type-C companion.
        type_c_cols = [
            prop_col
            for prop_col, pm in mappings.items()
            if pm.field is None and pm.path is not None
        ]

        # Build per-src_field groups anchored by Type-A (field-only) mappings.
        # A Type-A mapping is a condition anchor only when its prop_col name
        # differs from pm.field — this distinguishes semantic aliases like
        # "Experimental condition" (field=condition) from identity renames like
        # "regulator_locus_tag" (field=regulator_locus_tag).
        # The group requires at least one downstream member: a Type-B col sharing
        # the same src_field, or a Type-C col (when no Type-B siblings exist).
        linked: dict[str, dict[str, list[str]]] = {}
        for prop_col, pm in mappings.items():
            if pm.field is None or pm.path is not None:
                continue  # not Type-A
            # Exclude identity renames: prop_col and pm.field are the same name,
            # possibly differing only in case or whitespace (e.g. "Regulator locus tag"
            # -> "regulator_locus_tag").  Normalise both sides before comparing.
            if prop_col.lower().replace(" ", "_") == pm.field.lower().replace(" ", "_"):
                continue
            src_field = pm.field
            type_b_for_field = [
                col
                for col, m in mappings.items()
                if m.field == src_field and m.path is not None
            ]
            type_c_for_group = [c for c in type_c_cols if c not in type_b_for_field]
            downstream = type_b_for_field + type_c_for_group
            if downstream:
                linked[src_field] = {
                    "prop_cols": downstream,
                    "type_b_cols": type_b_for_field,
                }

        card = self.datacards.get(repo_id)

        # Fallback: when no Type-A aliases exist but there are Type-C downstream
        # cols, use the DataCard's condition_fields (role=experimental_condition)
        # as implicit anchors.  Use the first condition field only; multiple
        # condition fields sharing identical downstream cols would produce
        # duplicate input IDs in the UI.
        # Fallback: DataCard condition_fields as implicit anchors.  All Type-C
        # cols are downstream; none are Type-B (no per-level field derivation).
        if not linked and type_c_cols and card is not None:
            try:
                schema = card.extract_metadata_schema(config_name)
                cond_fields = schema.get("condition_fields", [])
                if cond_fields:
                    linked[cond_fields[0]] = {
                        "prop_cols": list(type_c_cols),
                        "type_b_cols": [],
                    }
            except Exception:
                pass

        if not linked:
            return None

        # Resolve the config name to try for field definitions.  For datasets
        # with external metadata, the definitions live in the _meta config rather
        # than the primary annotated-features config, so prefer that when present.
        meta_config_name = self._external_meta_configs.get(db_name, config_name)

        result: dict[str, Any] = {}
        for src_field, col_info in linked.items():
            prop_cols = col_info["prop_cols"]
            type_b_cols = col_info["type_b_cols"]
            level_descriptions: dict[str, str] = {}
            if card is not None:
                for _cfg in (meta_config_name, config_name):
                    try:
                        defs = card.get_field_definitions(_cfg, src_field)
                        if defs:
                            for level_val, definition in defs.items():
                                desc = (
                                    definition.get(
                                        "description", "Description unavailable"
                                    )
                                    if isinstance(definition, dict)
                                    else "Description unavailable"
                                )
                                level_descriptions[str(level_val)] = desc
                            break  # found definitions — stop trying configs
                    except Exception:
                        logger.debug(
                            "No field definitions for %s/%s field '%s'",
                            db_name,
                            _cfg,
                            src_field,
                        )
            result[src_field] = {
                "property_cols": prop_cols,
                "type_b_cols": type_b_cols,
                "level_descriptions": level_descriptions,
            }
        return result

    def get_column_metadata(self, db_name: str) -> dict[str, ColumnMeta] | None:
        """
        Return per-column metadata for a primary dataset.

        Metadata is collected from the DataCard during VirtualDB construction
        and includes the feature description, semantic role, and per-level
        definitions for ``experimental_condition`` columns.

        :param db_name: Dataset name as registered in the VirtualDB config.
        :returns: Dict mapping column name to :class:`ColumnMeta`, or ``None``
            if ``db_name`` is not found or has no recorded metadata.
        :rtype: dict[str, ColumnMeta] | None

        """
        return self._column_metadata.get(db_name)

    def get_dataset_description(self, db_name: str) -> str | None:
        """
        Return the description for a dataset.

        The VirtualDB config ``description`` field takes precedence over the
        DataCard config description. Returns ``None`` if neither is defined.

        :param db_name: Dataset name as registered in the VirtualDB config.
        :returns: Description string, or ``None`` if not found.
        :rtype: str | None

        """
        if db_name not in self.db_name_map:
            return None
        repo_id, config_name = self.db_name_map[db_name]
        # Check VirtualDB config description override first
        repo_cfg = self.config.repositories.get(repo_id)
        if repo_cfg and repo_cfg.dataset:
            vdb_dataset_cfg = repo_cfg.dataset.get(config_name)
            if vdb_dataset_cfg and vdb_dataset_cfg.description is not None:
                return vdb_dataset_cfg.description
        # Fall back to DataCard description
        card = self.datacards.get(repo_id)
        if card is None:
            return None
        cfg = card.get_config(config_name)
        return cfg.description if cfg is not None else None

    def get_citation(self, db_name: str) -> str | None:
        """
        Return the citation for a dataset.

        The dataset-level citation takes precedence over the repository-level
        citation. If neither is defined, returns ``None``.

        :param db_name: Dataset name as registered in the VirtualDB config.
        :returns: Citation string, or ``None`` if no citation is defined.
        :rtype: str | None

        """
        if db_name not in self.db_name_map:
            return None
        repo_id, config_name = self.db_name_map[db_name]
        card = self.datacards.get(repo_id)
        if card is None:
            return None
        return card.get_citation(config_name)

    # ------------------------------------------------------------------
    # Initialisation phases
    # ------------------------------------------------------------------

    def _load_datacards(self) -> None:
        """
        Fetch (or load from cache) the DataCard for every distinct repo.

        Populates ``self.datacards`` keyed by ``repo_id``. The underlying
        ``DatasetCard.load`` calls are dispatched concurrently so all cards
        are warm in memory before ``_validate_datacards`` runs. Failures are
        logged as warnings and the repo is omitted from the dict so that
        subsequent phases can skip it gracefully.

        """
        self.datacards: dict[str, DataCard] = {}
        seen_repos: set[str] = set()
        repo_ids: list[str] = []

        for repo_id, _ in self.db_name_map.values():
            if repo_id in seen_repos:
                continue
            # Skip genome-resource reference repos — they have no HF data files.
            repo_cfg = self.config.repositories.get(repo_id)
            if (
                repo_cfg is not None
                and repo_cfg.genome_resources is not None
                and not repo_cfg.dataset
            ):
                seen_repos.add(repo_id)
                continue
            seen_repos.add(repo_id)
            repo_ids.append(repo_id)

        def _load_one(repo_id: str) -> tuple[str, DataCard | None]:
            _t = time.monotonic()
            try:
                card = _cached_datacard(repo_id, token=self.token)
                # Eagerly trigger the lazy DatasetCard.load so the parsed card
                # is cached in memory before _validate_datacards accesses it.
                _ = card.dataset_card
                logger.debug(
                    "_load_datacards: %s loaded in %.3fs",
                    repo_id,
                    time.monotonic() - _t,
                )
                return repo_id, card
            except Exception as exc:
                logger.warning(
                    "Could not load datacard for repo '%s': %s",
                    repo_id,
                    exc,
                )
                return repo_id, None

        with ThreadPoolExecutor(max_workers=len(repo_ids) or 1) as pool:
            for repo_id, card in pool.map(_load_one, repo_ids):
                if card is not None:
                    self.datacards[repo_id] = card

    def _validate_datacards(self) -> None:
        """
        Cross-check the VirtualDB config against the loaded datacards.

        Checks that every dataset with a ``links`` field in the VirtualDB
        config has ``dataset_type: comparative`` in its HuggingFace datacard.
        Also resolves ``self._dataset_schemas`` and
        ``self._external_meta_configs`` (keyed by ``db_name``) for use by
        ``_update_cache`` and ``_register_all_views``.

        :raises ValueError: If a dataset with ``links`` does not have
            ``dataset_type: comparative`` in its datacard.

        """
        self._dataset_schemas: dict[str, DatasetSchema] = {}
        # db_name -> external metadata config_name (for applies_to datasets)
        self._external_meta_configs: dict[str, str] = {}

        for db_name, (repo_id, config_name) in self.db_name_map.items():
            repo_cfg = self.config.repositories.get(repo_id)
            ds_cfg = (
                repo_cfg.dataset.get(config_name)
                if repo_cfg and repo_cfg.dataset
                else None
            )
            card = self.datacards.get(repo_id)

            # Validate comparative dataset_type agreement.
            if ds_cfg and ds_cfg.links:
                if card is not None:
                    dc_config = card.get_config(config_name)
                    if (
                        dc_config is not None
                        and dc_config.dataset_type != DATASET_TYPE_COMPARATIVE
                    ):
                        raise ValueError(
                            f"Dataset '{config_name}' in repo '{repo_id}' has "
                            f"'links' in the VirtualDB config, indicating a "
                            f"comparative dataset, but the HuggingFace datacard "
                            f"declares dataset_type='{dc_config.dataset_type}'. "
                            f"Update the datacard to use dataset_type: comparative."
                        )
                continue  # comparative datasets need no schema resolution

            # Resolve dataset schema and external metadata config for
            # primary datasets.
            if card is None:
                continue
            try:
                schema = card.get_dataset_schema(config_name)
            except Exception as exc:
                logger.warning(
                    "Could not get dataset schema for %s/%s: %s",
                    repo_id,
                    config_name,
                    exc,
                )
                continue
            if schema is not None:
                self._dataset_schemas[db_name] = schema
            if (
                schema is not None
                and schema.metadata_source == "external"
                and schema.external_metadata_config
            ):
                self._external_meta_configs[db_name] = schema.external_metadata_config

    def _update_cache(self) -> None:
        """
        Download (or locate cached) Parquet files for all dataset configs.

        Populates ``self._parquet_files`` keyed by ``db_name``. For datasets
        with external metadata (identified during ``_validate_datacards``),
        also downloads those files and stores them under the key
        ``"__<db_name>_meta"`` so ``_register_all_views`` can read them
        without further network calls.

        All ``snapshot_download`` calls are dispatched concurrently via a
        thread pool because each is independent I/O-bound work (local cache
        path resolution even when ``local_files_only=True`` takes ~100-165ms
        per call due to filesystem glob and symlink resolution in
        ``huggingface_hub``).

        """
        self._parquet_files: dict[str, list[str]] = {}
        for db_name, (repo_id, config_name) in self.db_name_map.items():
            _t = time.monotonic()
            files = self._resolve_parquet_files(repo_id, config_name)
            self._parquet_files[db_name] = files
            logger.debug(
                "_update_cache: %s resolved %d file(s) in %.3fs",
                db_name,
                len(files),
                time.monotonic() - _t,
            )

        for db_name, ext_config_name in self._external_meta_configs.items():
            repo_id, _ = self.db_name_map[db_name]
            _t = time.monotonic()
            files = self._resolve_parquet_files(repo_id, ext_config_name)
            self._parquet_files[f"__{db_name}_meta"] = files
            logger.debug(
                "_update_cache: %s (ext meta) resolved %d file(s) in %.3fs",
                db_name,
                len(files),
                time.monotonic() - _t,
            )

    def _register_all_views(self) -> None:
        """
        Register all DuckDB views in dependency order.

        Expects ``self._parquet_files``, ``self._dataset_schemas``, and
        ``self._external_meta_configs`` to have been populated by the earlier
        init phases. No network or disk access occurs here.

        """
        # 1. Raw per-dataset views (internal __<db_name>_parquet
        # plus public <db_name> for primary datasets only)
        _t = time.monotonic()
        for db_name, (repo_id, config_name) in self.db_name_map.items():
            comparative = self._is_comparative(repo_id, config_name)
            self._register_raw_view(
                db_name,
                parquet_only=comparative,
            )
        logger.debug(
            "_register_all_views: raw views completed in %.3fs", time.monotonic() - _t
        )

        # 2. External metadata parquet views.
        # When a data config's metadata lives in a separate HF config
        # (applies_to), register its parquet as __<db_name>_metadata_parquet.
        _t = time.monotonic()
        self._external_meta_views: dict[str, str] = {}
        for db_name, ext_config_name in self._external_meta_configs.items():
            meta_view = f"__{db_name}_metadata_parquet"
            files = self._parquet_files.get(f"__{db_name}_meta", [])
            if not files:
                logger.warning(
                    "No parquet files for external metadata config "
                    "'%s' (db_name '%s') -- skipping external metadata view",
                    ext_config_name,
                    db_name,
                )
                continue
            files_sql = ", ".join(f"'{f}'" for f in files)
            try:
                self._conn.execute(
                    f"CREATE OR REPLACE VIEW {meta_view} AS "
                    f"SELECT * FROM read_parquet([{files_sql}])"
                )
            except Exception as exc:
                logger.warning(
                    "Failed to create external metadata view '%s': %s",
                    meta_view,
                    exc,
                )
                continue
            self._external_meta_views[db_name] = meta_view
        logger.debug(
            "_register_all_views: external meta views completed in %.3fs",
            time.monotonic() - _t,
        )

        # 3. Metadata views for primary datasets (<db_name>_meta)
        _t = time.monotonic()
        for db_name, (repo_id, config_name) in self.db_name_map.items():
            if not self._is_comparative(repo_id, config_name):
                self._register_meta_view(db_name, repo_id, config_name)
        logger.debug(
            "_register_all_views: meta views completed in %.3fs", time.monotonic() - _t
        )

        # 4. Replace primary raw views with join to _meta so
        # derived columns (e.g. carbon_source) are available
        _t = time.monotonic()
        for db_name, (repo_id, config_name) in self.db_name_map.items():
            if not self._is_comparative(repo_id, config_name):
                self._enrich_raw_view(db_name)
        logger.debug(
            "_register_all_views: enrich raw views completed in %.3fs",
            time.monotonic() - _t,
        )

        # 5. Comparative expanded views (pre-parsed composite IDs)
        _t = time.monotonic()
        for db_name, (repo_id, config_name) in self.db_name_map.items():
            repo_cfg = self.config.repositories.get(repo_id)
            if not repo_cfg or not repo_cfg.dataset:
                continue
            ds_cfg = repo_cfg.dataset.get(config_name)
            if ds_cfg and ds_cfg.links:
                self._register_comparative_expanded_view(db_name, ds_cfg)
        logger.debug(
            "_register_all_views: comparative views completed in %.3fs",
            time.monotonic() - _t,
        )

    def _build_column_metadata(self) -> None:
        """
        Collect per-column metadata from DataCards for all primary datasets.

        Populates ``self._column_metadata`` keyed by ``db_name``.  For each
        primary (non-comparative) dataset, features are gathered from the
        primary DataCard config and, when present, from the external metadata
        config (``_external_meta_configs``).  Property mappings are then
        walked to propagate metadata to output column names that differ from
        the underlying DataCard feature names (e.g. Type-A renames).

        """
        self._column_metadata: dict[str, dict[str, ColumnMeta]] = {}

        for db_name, (repo_id, config_name) in self.db_name_map.items():
            if self._is_comparative(repo_id, config_name):
                continue

            card = self.datacards.get(repo_id)
            if card is None:
                continue

            # Collect features from primary config and external meta config.
            feature_meta: dict[str, ColumnMeta] = {}
            configs_to_check: list[str] = [config_name]
            ext_cfg_name = self._external_meta_configs.get(db_name)
            if ext_cfg_name:
                configs_to_check.append(ext_cfg_name)

            for cfg in configs_to_check:
                try:
                    features = card.get_features(cfg)
                except Exception:
                    continue
                for feat in features:
                    level_defs: dict[str, str] | None = None
                    if feat.role == "experimental_condition" and feat.definitions:
                        level_defs = {
                            str(lv): (
                                d.get("description", "")
                                if isinstance(d, dict)
                                else str(d)
                            )
                            for lv, d in feat.definitions.items()
                        }
                    feature_meta[feat.name] = ColumnMeta(
                        description=feat.description,
                        role=feat.role,
                        level_definitions=level_defs,
                    )

            # Propagate metadata to Type-A rename output column names.
            # Type-A: field-only mapping (no path, no expression) — the output col
            # is a rename or alias of the source feature and inherits its metadata.
            # Type-B (field+path) and Type-C (path-only) produce derived columns
            # with independent semantics and do not inherit the source's role.
            try:
                mappings = self.config.get_property_mappings(repo_id, config_name)
            except Exception:
                mappings = {}

            for out_col, pm in mappings.items():
                if out_col in feature_meta:
                    continue  # already recorded under this name
                # Only propagate for Type-A (field-only, no path, no expression).
                if pm.field is None or pm.path is not None or pm.expression is not None:
                    continue
                src = pm.field
                if src in feature_meta:
                    feature_meta[out_col] = feature_meta[src]

            # Register stub ColumnMeta for Type-B and Type-C derived output columns.
            # These have no FeatureInfo of their own but exist in the _meta view.
            # A stub entry (role=None, level_definitions=None) ensures callers can
            # discover all queryable columns, e.g. to build cascade filter effects.
            for out_col in mappings:
                if out_col not in feature_meta:
                    feature_meta[out_col] = ColumnMeta()

            if feature_meta:
                self._column_metadata[db_name] = feature_meta

    # ------------------------------------------------------------------
    # db_name mapping
    # ------------------------------------------------------------------

    def _build_db_name_map(self) -> dict[str, tuple[str, str]]:
        """
        Build mapping from resolved db_name to (repo_id, config_name).

        :return: Dict mapping db_name -> (repo_id, config_name)

        """
        mapping: dict[str, tuple[str, str]] = {}
        for repo_id, repo_cfg in self.config.repositories.items():
            if not repo_cfg.dataset:
                continue
            for config_name, ds_cfg in repo_cfg.dataset.items():
                resolved = ds_cfg.db_name or config_name
                mapping[resolved] = (repo_id, config_name)
        return mapping

    # ------------------------------------------------------------------
    # Parquet file resolution
    # ------------------------------------------------------------------

    def _snapshot_path_from_cache(self, repo_id: str) -> Path | None:
        """
        Resolve the active snapshot directory from the local HuggingFace cache without
        invoking ``snapshot_download``.

        Reads ``{cache_dir}/datasets--{owner}--{repo}/refs/main`` to get the
        commit hash, then returns the corresponding snapshots subdirectory.
        Returns ``None`` if the ref file or snapshot directory does not exist.

        :param repo_id: HuggingFace repository ID (e.g. ``"BrentLab/callingcards"``).
        :returns: Path to the snapshot directory, or ``None``.

        """
        slug = "datasets--" + repo_id.replace("/", "--")
        ref_file = self.cache_dir / slug / "refs" / "main"
        if not ref_file.exists():
            return None
        commit = ref_file.read_text().strip()
        snap = self.cache_dir / slug / "snapshots" / commit
        return snap if snap.exists() else None

    def _resolve_parquet_files(self, repo_id: str, config_name: str) -> list[str]:
        """
        Download (or locate cached) Parquet files for a dataset config.

        When ``local_files_only=True`` the snapshot directory is resolved
        directly from the local cache ref file, bypassing ``snapshot_download``
        entirely (saves ~100-170 ms of filesystem work per call). When
        ``local_files_only=False`` or the local ref is absent,
        ``snapshot_download`` is called normally.

        :param repo_id: HuggingFace repository ID
        :param config_name: Dataset configuration name
        :return: List of absolute paths to Parquet files

        """
        # Fast path: resolve snapshot directory from the local cache ref file
        # without touching the network or loading the datacard.
        downloaded_path: str | None = None
        if self.local_files_only:
            snap = self._snapshot_path_from_cache(repo_id)
            if snap is not None:
                downloaded_path = str(snap)
                logger.debug(
                    "snapshot resolved from cache ref: repo=%s path=%s",
                    repo_id,
                    downloaded_path,
                )

        # Load the datacard for file_patterns. When the fast path already resolved
        # a snapshot (local_files_only + cache hit), a missing datacard is not fatal:
        # fall back to globbing the snapshot for all parquet files. When we still need
        # to call snapshot_download (downloaded_path is None), a missing card is an
        # error because we need the patterns to filter the download.
        #
        # DataCard is lazy — __init__ succeeds but get_config() triggers the fetch,
        # so both calls must be inside the same try block.
        card = self.datacards.get(repo_id)
        try:
            if card is None:
                card = DataCard(repo_id, token=self.token)
            config = card.get_config(config_name)
            if not config:
                logger.warning(
                    "Config '%s' not found in repo '%s'",
                    config_name,
                    repo_id,
                )
                return []
            file_patterns = [df.path for df in config.data_files]
        except Exception as exc:
            if downloaded_path is None:
                raise
            logger.warning(
                "Could not load datacard for '%s'; "
                "falling back to full parquet glob: %s",
                repo_id,
                exc,
            )
            file_patterns = ["**/*.parquet"]

        if downloaded_path is None:
            from huggingface_hub import snapshot_download

            logger.debug(
                "snapshot_download start: repo=%s patterns=%s", repo_id, file_patterns
            )
            t0 = time.monotonic()
            downloaded_path = snapshot_download(
                repo_id=repo_id,
                repo_type="dataset",
                allow_patterns=file_patterns,
                token=self.token,
                local_files_only=self.local_files_only,
                cache_dir=self.cache_dir,
            )
            logger.debug(
                "snapshot_download done: repo=%s elapsed=%.3fs path=%s",
                repo_id,
                time.monotonic() - t0,
                downloaded_path,
            )

        parquet_files: list[str] = []
        for pattern in file_patterns:
            file_path = Path(downloaded_path) / pattern
            if file_path.exists() and file_path.suffix == ".parquet":
                parquet_files.append(str(file_path))
            elif "*" in pattern:
                base = Path(downloaded_path)
                parquet_files.extend(
                    str(f) for f in base.glob(pattern) if f.suffix == ".parquet"
                )
            else:
                parent_dir = Path(downloaded_path) / Path(pattern).parent
                if parent_dir.exists():
                    parquet_files.extend(str(f) for f in parent_dir.glob("*.parquet"))

        return parquet_files

    # ------------------------------------------------------------------
    # View registration helpers
    # ------------------------------------------------------------------

    def _register_raw_view(
        self,
        db_name: str,
        *,
        parquet_only: bool = False,
    ) -> None:
        """
        Register a raw DuckDB view over pre-resolved Parquet files.

        Creates an internal ``__<db_name>_parquet`` view that reads
        directly from the Parquet files. For primary datasets, also
        creates a public ``<db_name>`` view (initially identical)
        that may later be replaced by ``_enrich_raw_view``.

        For comparative datasets, only the internal parquet view is
        created; the public view is the ``_expanded`` view instead.

        Parquet files must have been resolved by ``_update_cache``
        before this method is called.

        :param db_name: View name
        :param parquet_only: If True, only create the internal
            ``__<db_name>_parquet`` view (no public ``<db_name>``).

        """
        files = self._parquet_files.get(db_name, [])
        if not files:
            logger.warning(
                "No parquet files for db_name '%s' -- skipping view",
                db_name,
            )
            return

        files_sql = ", ".join(f"'{f}'" for f in files)
        parquet_sql = f"SELECT * FROM read_parquet([{files_sql}])"
        self._conn.execute(
            f"CREATE OR REPLACE VIEW __{db_name}_parquet AS " f"{parquet_sql}"
        )
        if not parquet_only:
            sample_col = self._get_sample_id_col(db_name)
            if sample_col == "sample_id":
                public_select = f"SELECT * FROM __{db_name}_parquet"
            else:
                raw_cols = self._get_view_columns(f"__{db_name}_parquet")
                parts: list[str] = []
                for col in raw_cols:
                    if col == sample_col:
                        parts.append(f"{col} AS sample_id")
                    elif col == "sample_id":
                        parts.append(f"{col} AS sample_id_orig")
                    else:
                        parts.append(col)
                cols_sql = ", ".join(parts)
                public_select = f"SELECT {cols_sql} FROM __{db_name}_parquet"
            self._conn.execute(f"CREATE OR REPLACE VIEW {db_name} AS {public_select}")

    def _register_meta_view(self, db_name: str, repo_id: str, config_name: str) -> None:
        """
        Register a ``<db_name>_meta`` view with one row per sample.

        Includes metadata columns from the DataCard plus any derived columns
        from config property mappings (resolved against DataCard definitions
        with factor aliases applied).

        For datasets with external metadata (a separate HF config with
        ``applies_to``), JOINs the data parquet to the metadata parquet
        on the configured sample_id column. The actual columns in the metadata
        parquet are determined by DuckDB introspection (``DESCRIBE``) rather
        than the DataCard feature list, because DataCard feature lists are
        conceptual schemas that may include columns not physically present
        in the parquet files.

        :param db_name: Base view name for the primary dataset
        :param repo_id: Repository ID
        :param config_name: Configuration name

        raises ValueError: If no metadata fields are found.
        raises BinderException: If view creation fails, with SQL details.

        """
        _t0 = time.monotonic()
        parquet_view = f"__{db_name}_parquet"
        if not self._view_exists(parquet_view):
            return

        sample_col = self._get_sample_id_col(db_name)

        # Pull ext_meta_view early -- needed for both meta_cols and
        # FROM clause construction.
        schema: DatasetSchema | None = self._dataset_schemas.get(db_name)
        ext_meta_view: str | None = self._external_meta_views.get(db_name)

        is_external = (
            ext_meta_view is not None
            and schema is not None
            and schema.metadata_source == "external"
        )

        if is_external:
            # DataCard feature lists are conceptual -- columns listed there
            # may not be physically present in the parquet file. Use DuckDB
            # introspection to get the actual columns in the metadata parquet.
            assert ext_meta_view is not None
            actual_meta_cols: set[str] = set(self._get_view_columns(ext_meta_view))
            meta_cols: list[str] = sorted(actual_meta_cols)
        elif schema is not None:
            actual_meta_cols = schema.metadata_columns
            meta_cols = sorted(actual_meta_cols)
        else:
            meta_cols = self._resolve_metadata_fields(repo_id, config_name) or []
            actual_meta_cols = set(meta_cols)

        if not meta_cols:
            raise ValueError(
                f"No metadata fields found for {repo_id}/{config_name}. "
                f"Cannot create meta view '{db_name}_meta'."
            )

        # FROM clause: JOIN data + metadata parquets when external,
        # plain parquet view otherwise.
        if is_external:
            assert ext_meta_view is not None
            # Use the configured sample_id column as the join key.
            # The DataCard feature intersection (schema.join_columns)
            # is unreliable because a data config's feature list may
            # document columns that are physically only in the metadata
            # parquet (present conceptually after a join, not in the
            # physical data parquet file).
            from_clause = (
                f"{parquet_view} d " f"JOIN {ext_meta_view} m " f"USING ({sample_col})"
            )
            is_join = True
        else:
            from_clause = parquet_view
            is_join = False

        def qualify(col: str) -> str:
            """Return qualified column name for JOIN context."""
            if not is_join:
                return col
            if col == sample_col:
                return col  # USING makes join key unqualified
            # Use the actual metadata parquet columns (from DuckDB
            # introspection) to decide qualification, not the DataCard
            # feature list which may be inaccurate.
            if col in actual_meta_cols:
                return f"m.{col}"
            return f"d.{col}"

        # Resolve derived property expressions first.
        # When a factor mapping has the same output name as its source
        # field (e.g. time -> time), the raw column must be renamed to
        # avoid a duplicate column name in the SELECT.  The rename uses
        # "<col>_orig", or "<col>_orig_1", etc., to avoid collisions with
        # other columns that already exist in the parquet.
        prop_result = self._resolve_property_columns(repo_id, config_name)

        # Collect all column names that exist in the parquet so we can
        # find a unique _orig suffix when needed.
        all_parquet_cols: set[str] = set(self._get_view_columns(parquet_view))

        # Map: raw_col -> alias_in_select for factor-overridden cols
        factor_col_renames: dict[str, str] = {}
        if prop_result is not None:
            _derived_exprs, _prop_raw_cols = prop_result
            for expr in _derived_exprs:
                # Detect factor CAST expressions of the form:
                # CAST(CAST(<field> AS VARCHAR) AS _enum_<key>) AS <key>
                # where <field> == <key> (in-place factor override).
                # The output column name is the last " AS <name>" token.
                parts = expr.rsplit(" AS ", 1)
                if len(parts) != 2:
                    continue
                # Strip double-quotes added by _quote_ident so we can
                # compare the bare name against parquet column names.
                out_col = parts[1].strip().strip('"')
                # Extract the innermost source field from the CAST chain.
                # Handles both:
                #   CAST(CAST(<field> AS VARCHAR) AS _enum_<key>)
                #   CAST(CAST(CAST(<field> AS BIGINT) AS VARCHAR) AS _enum_<key>)
                m = re.match(
                    r"CAST\(CAST\((?:CAST\()?(\w+)(?:\s+AS\s+BIGINT\))?"
                    r"\s+AS\s+VARCHAR\)\s+AS\s+_enum_\w+\)",
                    parts[0],
                )
                if m is None:
                    continue
                src_field = m.group(1)
                if src_field == out_col and out_col in all_parquet_cols:
                    # Find a unique _orig name
                    candidate = f"{out_col}_orig"
                    n = 1
                    while candidate in all_parquet_cols or candidate in (
                        v for v in factor_col_renames.values()
                    ):
                        candidate = f"{out_col}_orig_{n}"
                        n += 1
                    factor_col_renames[src_field] = candidate

        # Build SELECT: sample_id + metadata cols (deduplicated).
        # Raw columns that are factor-overridden are emitted with their
        # _orig alias instead of their original name.
        # If the configured sample_id column differs from "sample_id",
        # rename it so all views expose a consistent "sample_id" column.
        # If the parquet also has a literal "sample_id" column, preserve
        # it as "sample_id_orig" to avoid losing data.
        seen: set[str] = set()
        select_parts: list[str] = []
        rename_sample = sample_col != "sample_id"

        def add_col(col: str) -> None:
            if col in seen:
                return
            seen.add(col)
            alias = factor_col_renames.get(col)
            if alias:
                select_parts.append(f"{qualify(col)} AS {_quote_ident(alias)}")
            elif rename_sample and col == sample_col:
                select_parts.append(f"{qualify(col)} AS sample_id")
            elif rename_sample and col == "sample_id":
                select_parts.append(f"{qualify(col)} AS sample_id_orig")
            else:
                select_parts.append(qualify(col))

        add_col(sample_col)
        # When renaming, check if the parquet source also has a literal
        # "sample_id" column; if so, preserve it as "sample_id_orig".
        if rename_sample:
            source_cols = set(self._get_view_columns(parquet_view))
            if "sample_id" in source_cols:
                add_col("sample_id")
        for col in meta_cols:
            add_col(col)

        # Add derived property expressions from the VirtualDB config
        if prop_result is not None:
            derived_exprs, prop_raw_cols = prop_result
            # Ensure source columns needed by expressions are selected.
            # For external metadata datasets, restrict to columns physically
            # present in the metadata parquet -- data columns must not bleed
            # into the meta view.
            allowed_raw_cols = (
                [c for c in prop_raw_cols if c in actual_meta_cols]
                if is_external
                else prop_raw_cols
            )
            for col in allowed_raw_cols:
                add_col(col)
            # Rewrite CAST expressions to use the _orig alias when the
            # source field was renamed to avoid collision.
            if factor_col_renames:
                rewritten = []
                for expr in derived_exprs:
                    for orig, alias in factor_col_renames.items():
                        # Replace "CAST(<orig> AS" with "CAST(<alias> AS"
                        expr = expr.replace(f"CAST({orig} AS", f"CAST({alias} AS")
                    rewritten.append(expr)
                derived_exprs = rewritten
            # Qualify source column references inside expressions.
            # Covers:
            #   - simple alias:   "field AS ..."  → "m.field AS ..."
            #   - CAST alias:     "CAST(field AS ..." → "CAST(m.field AS ..."
            #   - CASE WHEN:      "CASE field WHEN..." / "field = ..."
            if is_join:
                qualified_exprs = []
                for expr in derived_exprs:
                    for raw_col in prop_raw_cols:
                        q = qualify(raw_col)
                        if q != raw_col:
                            expr = (
                                expr
                                # simple alias: bare field at start before " AS"
                                .replace(f"{raw_col} AS ", f"{q} AS ")
                                # CAST alias: field inside CAST(
                                .replace(f"CAST({raw_col} AS", f"CAST({q} AS")
                                # CASE WHEN patterns
                                .replace(f"CASE {raw_col} ", f"CASE {q} ").replace(
                                    f" {raw_col} = ", f" {q} = "
                                )
                            )
                    qualified_exprs.append(expr)
                derived_exprs = qualified_exprs
            select_parts.extend(derived_exprs)

        cols_sql = ", ".join(select_parts)
        sql = (
            f"CREATE OR REPLACE VIEW {db_name}_meta AS "
            f"SELECT DISTINCT {cols_sql} FROM {from_clause}"
        )
        try:
            self._conn.execute(sql)
        except BinderException as exc:
            raise BinderException(
                f"Failed to create meta view '{db_name}_meta'.\n"
                f"  schema: {schema}\n"
                f"  from_clause: {from_clause}\n"
                f"  SQL: {sql}\n"
                f"  error: {exc}"
            ) from exc
        logger.debug(
            "_register_meta_view: %s_meta completed in %.3fs",
            db_name,
            time.monotonic() - _t0,
        )

    def _enrich_raw_view(self, db_name: str) -> None:
        """
        Replace a primary raw view with a join to its ``_meta`` view.

        If ``<db_name>_meta`` has derived columns not present in the
        raw parquet view, recreates ``<db_name>`` as a join so derived
        columns (e.g. ``carbon_source``) appear alongside measurement
        data.

        :param db_name: Base view name for the primary dataset

        """
        meta_name = f"{db_name}_meta"
        parquet_name = f"__{db_name}_parquet"
        if not self._view_exists(meta_name) or not self._view_exists(parquet_name):
            return

        raw_cols_list = self._get_view_columns(parquet_name)
        raw_cols = set(raw_cols_list)
        meta_cols = set(self._get_view_columns(meta_name))

        sample_col = self._get_sample_id_col(db_name)
        rename_sample = sample_col != "sample_id"

        # Columns to pull from _meta that aren't already in raw parquet,
        # accounting for the sample_id rename: when renaming, "sample_id"
        # will appear in meta_cols (as the renamed column) but not in
        # raw_cols (which has the original name), so we must exclude it
        # from extra_cols since the rename in the raw SELECT already
        # provides it.
        if rename_sample:
            # "sample_id" and "sample_id_orig" come from the raw SELECT
            # rename, not from meta
            extra_cols = meta_cols - raw_cols - {"sample_id", "sample_id_orig"}
        else:
            extra_cols = meta_cols - raw_cols

        if not extra_cols:
            # No derived columns to add -- the view created in
            # _register_raw_view (which already handles the rename)
            # is sufficient.
            return

        if rename_sample:
            # Build explicit SELECT to rename the sample column
            raw_parts: list[str] = []
            for col in raw_cols_list:
                if col == sample_col:
                    raw_parts.append(f"r.{col} AS sample_id")
                elif col == "sample_id":
                    raw_parts.append(f"r.{col} AS sample_id_orig")
                else:
                    raw_parts.append(f"r.{col}")
            raw_select = ", ".join(raw_parts)
        else:
            raw_select = "r.*"

        if extra_cols:
            extra_select = ", ".join(f"m.{_quote_ident(c)}" for c in sorted(extra_cols))
            full_select = f"{raw_select}, {extra_select}"
        else:
            full_select = raw_select

        if rename_sample:
            join_clause = f"JOIN {meta_name} m ON r.{sample_col} = m.sample_id"
        else:
            join_clause = f"JOIN {meta_name} m USING ({sample_col})"

        self._conn.execute(
            f"CREATE OR REPLACE VIEW {db_name} AS "
            f"SELECT {full_select} "
            f"FROM {parquet_name} r "
            f"{join_clause}"
        )

    def _get_view_columns(self, view: str) -> list[str]:
        """
        Return column names for a view.

        Uses ``DESCRIBE`` rather than ``information_schema`` to force
        eager schema resolution for ``read_parquet``-backed views,
        which DuckDB may evaluate lazily.

        """
        df = self._conn.execute(f"DESCRIBE {view}").fetchdf()
        return df["column_name"].tolist()

    def _get_sample_id_col(self, db_name: str) -> str:
        """
        Resolve the sample identifier column name for a dataset.

        :param db_name: Resolved database view name
        :return: Actual column name for the sample identifier

        """
        repo_id, config_name = self.db_name_map[db_name]
        return self.config.get_sample_id_field(repo_id, config_name)

    def _resolve_metadata_fields(
        self, repo_id: str, config_name: str
    ) -> list[str] | None:
        """
        Get metadata field names from the DataCard.

        Delegates to ``DataCard.get_metadata_fields()`` which handles
        both embedded metadata_fields and external metadata configs
        (via applies_to).

        :param repo_id: Repository ID
        :param config_name: Configuration name
        :return: List of metadata field names, or None if not found

        """
        try:
            card = self.datacards.get(repo_id) or _cached_datacard(
                repo_id, token=self.token
            )
            return card.get_metadata_fields(config_name)
        except Exception:
            logger.error(
                "Could not resolve metadata_fields for %s/%s",
                repo_id,
                config_name,
            )
        return None

    def _get_class_label_names(
        self, card: Any, config_name: str, field: str
    ) -> list[str]:
        """
        Return the ENUM levels for a field with class_label dtype.

        Looks up the FeatureInfo for ``field`` in the DataCard config and
        extracts the ``names`` list from its ``class_label`` dtype dict.

        :param card: DataCard instance
        :param config_name: Configuration name
        :param field: Field name to look up
        :return: List of level strings
        :raises ValueError: If the field is not found, has no class_label dtype,
            or the class_label dict has no ``names`` key

        """
        try:
            features = card.get_features(config_name)
        except Exception as exc:
            raise ValueError(
                f"Could not retrieve features for config '{config_name}': {exc}"
            ) from exc

        feature = next((f for f in features if f.name == field), None)
        if feature is None:
            raise ValueError(
                f"Field '{field}' not found in DataCard config '{config_name}'. "
                "dtype='factor' requires the field to be declared in the DataCard."
            )

        dtype = feature.dtype
        if not isinstance(dtype, dict) or "class_label" not in dtype:
            raise ValueError(
                f"dtype='factor' is set for field '{field}' in config "
                f"'{config_name}', but the DataCard dtype is {dtype!r} rather "
                "than a class_label dict. "
                "The DataCard must declare dtype: {class_label: {names: [...]}}."
            )

        class_label = dtype["class_label"]
        names = class_label.get("names") if isinstance(class_label, dict) else None
        if not names:
            raise ValueError(
                f"class_label for field '{field}' in config '{config_name}' "
                "has no 'names' key or the names list is empty. "
                "Specify levels as: class_label: {names: [level1, level2, ...]}."
            )

        return [str(n) for n in names]

    def _ensure_enum_type(self, type_name: str, levels: list[str]) -> None:
        """
        Create or replace a DuckDB ENUM type with the given levels.

        DuckDB ENUM types must be registered before use in CAST expressions. Drops any
        existing type with the same name first to allow re-registration on repeated view
        builds.

        :param type_name: SQL identifier for the ENUM type
        :param levels: Ordered list of allowed string values

        """
        try:
            self._conn.execute(f"DROP TYPE IF EXISTS {type_name}")
        except Exception:
            pass  # type may not exist yet
        escaped = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in levels)
        self._conn.execute(f"CREATE TYPE {type_name} AS ENUM ({escaped})")

    def _resolve_alias(self, col: str, value: str) -> str:
        """
        Apply factor alias to a value if one is configured.

        :param col: Column name (e.g., "carbon_source")
        :param value: Raw value (e.g., "D-glucose")
        :return: Canonical alias (e.g., "glucose") or original value

        """
        aliases = self.config.factor_aliases.get(col)
        if not aliases:
            return value
        lower_val = str(value).lower()
        for canonical, actuals in aliases.items():
            if lower_val in [str(a).lower() for a in actuals]:
                return canonical
        return value

    def _resolve_property_columns(
        self,
        repo_id: str,
        config_name: str,
    ) -> tuple[list[str], list[str]] | None:
        """
        Build SQL column expressions for derived property columns.

        Resolves config property mappings against the DataCard to
        produce SQL expressions that add derived columns to the
        ``_meta`` view.

        :param repo_id: Repository ID
        :param config_name: Configuration name
        :return: Tuple of (sql_expressions, raw_cols_needed) or None
            if no property mappings are configured.
            ``sql_expressions`` are SQL fragments like
            ``"'glucose' AS carbon_source"`` or
            ``"CASE WHEN ... END AS carbon_source"``.
            ``raw_cols_needed`` are raw parquet column names that must
            be present in the inner SELECT.

        """
        mappings = self.config.get_property_mappings(repo_id, config_name)
        if not mappings and not self.config.missing_value_labels:
            return None

        expressions: list[str] = []
        raw_cols: set[str] = set()

        card = None
        if mappings:
            try:
                card = self.datacards.get(repo_id) or _cached_datacard(
                    repo_id, token=self.token
                )
            except Exception as exc:
                logger.warning(
                    "Could not load DataCard for %s: %s",
                    repo_id,
                    exc,
                )

        produced: set[str] = set()

        for key, mapping in mappings.items():
            if card is None:
                # Cannot resolve field/path mappings without a DataCard;
                # skip this mapping and fall through to missing_value_labels.
                continue
            if mapping.expression is not None:
                # Type D: expression
                expressions.append(f"({mapping.expression}) AS {_quote_ident(key)}")
                produced.add(key)
                continue

            if mapping.field is not None and mapping.path is None:
                # Type A: field-only (alias or ENUM cast)
                raw_cols.add(mapping.field)
                if mapping.dtype == "factor":
                    # Fetch class_label levels from DataCard, register ENUM,
                    # and emit a CAST expression. Raises ValueError if the
                    # DataCard does not declare a class_label dtype.
                    enum_type = f"_enum_{key}"
                    levels = self._get_class_label_names(
                        card, config_name, mapping.field
                    )
                    self._ensure_enum_type(enum_type, levels)
                    # If all levels are integer-valued strings (e.g. '0',
                    # '90'), the parquet column may be DOUBLE (e.g. 90.0).
                    # Cast through BIGINT first to strip the decimal before
                    # converting to VARCHAR so '90.0' becomes '90'.
                    all_int = all(re.fullmatch(r"-?\d+", lv) for lv in levels)
                    inner = (
                        f"CAST({mapping.field} AS BIGINT)" if all_int else mapping.field
                    )
                    expressions.append(
                        f"CAST(CAST({inner} AS VARCHAR)"
                        f" AS {enum_type}) AS {_quote_ident(key)}"
                    )
                elif key == mapping.field:
                    # no-op -- column already present as raw col
                    pass
                else:
                    expressions.append(f"{mapping.field} AS {_quote_ident(key)}")
                produced.add(key)
                continue

            if mapping.field is not None and mapping.path is not None:
                # Type B: field + path -- resolve from definitions.
                # dtype='factor' is not supported here: levels come from a
                # class_label field, not a definitions path.
                if mapping.dtype == "factor":
                    raise ValueError(
                        f"dtype='factor' is not supported for field+path mappings "
                        f"(key='{key}'). Use dtype='factor' only with field-only "
                        "mappings that reference a class_label field in the DataCard."
                    )
                raw_cols.add(mapping.field)
                expr = self._build_field_path_expr(
                    key,
                    mapping.field,
                    mapping.path,
                    mapping.dtype,
                    config_name,
                    card,
                )
                if expr is not None:
                    expressions.append(expr)
                    produced.add(key)
                continue

            if mapping.field is None and mapping.path is not None:
                # Type C: path-only -- constant from config
                expr = self._build_path_only_expr(
                    key,
                    mapping.path,
                    mapping.dtype,
                    config_name,
                    card,
                )
                if expr is not None:
                    expressions.append(expr)
                    produced.add(key)
                continue

        # For any key in missing_value_labels that was not covered by a
        # mapping that successfully resolved an expression, emit a constant
        # literal so every _meta view exposes the column (with the fallback
        # value). Keys in mappings whose path/field resolution returned None
        # are treated the same as unmapped keys.
        for key, label in self.config.missing_value_labels.items():
            if key not in produced:
                escaped = label.replace("'", "''")
                expressions.append(f"'{escaped}' AS {_quote_ident(key)}")

        if not expressions and not raw_cols:
            return None

        return expressions, sorted(raw_cols)

    def _build_field_path_expr(
        self,
        key: str,
        field: str,
        path: str,
        dtype: str | None,
        config_name: str,
        card: Any,
    ) -> str | None:
        """
        Build a SQL expression for a field+path property mapping.

        Resolves each definition value via ``get_nested_value``,
        applies factor aliases, and returns either a constant or
        a CASE WHEN expression.

        :param key: Output column name
        :param field: Source field in parquet (e.g., "condition")
        :param path: Dot-notation path within definitions
        :param dtype: Optional data type ("numeric", "string", "bool")
        :param config_name: Configuration name
        :param card: DataCard instance
        :return: SQL expression string, or None on failure

        """
        try:
            defs = card.get_field_definitions(config_name, field)
        except Exception as exc:
            logger.warning(
                "Could not get definitions for field '%s' " "in config '%s': %s",
                field,
                config_name,
                exc,
            )
            return None

        if not defs:
            return None

        # Resolve each definition value
        value_map: dict[str, str] = {}
        for def_key, definition in defs.items():
            raw = get_nested_value(definition, path)
            if raw is None:
                logger.debug(
                    "Path '%s' resolved to None for " "definition key '%s' (keys: %s)",
                    path,
                    def_key,
                    (
                        list(definition.keys())
                        if isinstance(definition, dict)
                        else type(definition).__name__
                    ),
                )
                continue
            # Handle list results (e.g., carbon_source returns
            # [{"compound": "D-glucose"}])
            if isinstance(raw, list):
                raw = raw[0] if len(raw) == 1 else ", ".join(str(v) for v in raw)
            resolved = self._resolve_alias(key, str(raw))
            value_map[str(def_key)] = resolved

        if not value_map:
            return None

        # If all values are the same, emit a constant
        unique_vals = set(value_map.values())
        if len(unique_vals) == 1:
            val = next(iter(unique_vals))
            return self._literal_expr(key, val, dtype)

        # Otherwise, build CASE WHEN
        whens = []
        for def_key, resolved in value_map.items():
            escaped_key = def_key.replace("'", "''")
            escaped_val = resolved.replace("'", "''")
            whens.append(f"WHEN {field} = '{escaped_key}' " f"THEN '{escaped_val}'")
        case_sql = " ".join(whens)
        missing = self.config.missing_value_labels.get(key)
        if missing is not None:
            escaped_missing = missing.replace("'", "''")
            expr = f"CASE {case_sql} " f"ELSE '{escaped_missing}' END"
        else:
            expr = f"CASE {case_sql} ELSE NULL END"
        if dtype == "numeric":
            expr = f"CAST({expr} AS DOUBLE)"
        return f"{expr} AS {_quote_ident(key)}"

    def _build_path_only_expr(
        self,
        key: str,
        path: str,
        dtype: str | None,
        config_name: str,
        card: Any,
    ) -> str | None:
        """
        Build a constant column expression for a path-only mapping.

        Resolves a single value from the DataCard's raw model_extra,
        which preserves the full dict structure (including any
        ``experimental_conditions`` wrapper).

        :param key: Output column name
        :param path: Dot-notation path (may include
            ``experimental_conditions.`` prefix)
        :param dtype: Optional data type
        :param config_name: Configuration name
        :param card: DataCard instance
        :return: SQL literal expression, or None on failure

        """
        # Build merged dict from all top-level fields + config-level fields.
        # model_extra only contains undeclared keys; explicit fields like
        # experimental_conditions, doi, citation must be sourced via
        # model_dump() so they are reachable by dot-path traversal.
        merged: dict[str, Any] = {}
        try:
            top_dump = card.dataset_card.model_dump(exclude_none=True)
            merged.update(top_dump)
            config_obj = card.get_config(config_name)
            if config_obj:
                config_dump = config_obj.model_dump(exclude_none=True)
                merged.update(config_dump)
        except Exception:
            logger.debug(
                "Could not dump model for %s/%s",
                card.repo_id if hasattr(card, "repo_id") else "?",
                config_name,
            )
            return None

        if not merged:
            return None

        raw = get_nested_value(merged, path)
        if raw is None:
            logger.debug(
                "Path '%s' resolved to None in model_extra for "
                "%s/%s. Available keys: %s",
                path,
                card.repo_id if hasattr(card, "repo_id") else "?",
                config_name,
                list(merged.keys()),
            )
            return None

        if isinstance(raw, list):
            raw = raw[0] if len(raw) == 1 else ", ".join(str(v) for v in raw)

        resolved = self._resolve_alias(key, str(raw))
        return self._literal_expr(key, resolved, dtype)

    @staticmethod
    def _literal_expr(key: str, value: str, dtype: str | None) -> str:
        """
        Build a SQL literal expression with optional type cast.

        :param key: Column alias
        :param value: Literal value
        :param dtype: Optional type ("numeric", "string", "bool")
        :return: SQL expression

        """
        escaped = value.replace("'", "''")
        if dtype == "numeric":
            return f"CAST('{escaped}' AS DOUBLE) AS {_quote_ident(key)}"
        return f"'{escaped}' AS {_quote_ident(key)}"

    def _register_comparative_expanded_view(
        self,
        db_name: str,
        ds_cfg: Any,
    ) -> None:
        """
        Create ``<db_name>_expanded`` view with parsed composite ID cols.

        For each link_field in the dataset config, adds two columns:

        - ``<link_field>_source`` -- the ``repo_id;config_name`` prefix,
          aliased to the configured ``db_name`` when available.
        - ``<link_field>_id`` -- the sample identifier component.

        :param db_name: Base view name for the comparative dataset
        :param ds_cfg: DatasetVirtualDBConfig with ``links``

        """
        parquet_view = f"__{db_name}_parquet"
        if not self._view_exists(parquet_view):
            return

        extra_cols = []
        for link_field, primaries in ds_cfg.links.items():
            # _id column: third component of composite ID
            id_col = f"{link_field}_id"
            extra_cols.append(f"SPLIT_PART({link_field}, ';', 3) " f"AS {id_col}")

            # _source column: first two components, aliased
            # to db_name when the pair is in the config
            raw_expr = (
                f"SPLIT_PART({link_field}, ';', 1) || ';' "
                f"|| SPLIT_PART({link_field}, ';', 2)"
            )
            whens = []
            for pair in primaries:
                repo_id, config_name = pair[0], pair[1]
                alias = self._get_db_name_for(repo_id, config_name)
                if alias:
                    key = f"{repo_id};{config_name}".replace("'", "''")
                    whens.append(f"WHEN '{key}' THEN '{alias}'")
            if whens:
                case_sql = " ".join(whens)
                source_expr = f"CASE {raw_expr} {case_sql} " f"ELSE {raw_expr} END"
            else:
                source_expr = raw_expr
            source_col = f"{link_field}_source"
            extra_cols.append(f"{source_expr} AS {source_col}")

        if not extra_cols:
            return

        cols_sql = ", ".join(extra_cols)
        self._conn.execute(
            f"CREATE OR REPLACE VIEW {db_name}_expanded AS "
            f"SELECT *, {cols_sql} FROM {parquet_view}"
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _is_comparative(self, repo_id: str, config_name: str) -> bool:
        """Return True if the dataset has links (i.e. is comparative)."""
        repo_cfg = self.config.repositories.get(repo_id)
        if not repo_cfg or not repo_cfg.dataset:
            return False
        ds_cfg = repo_cfg.dataset.get(config_name)
        return bool(ds_cfg and ds_cfg.links)

    def _list_views(self) -> list[str]:
        """Return list of public views (excludes internal __ prefixed)."""
        df = self._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'VIEW'"
        ).fetchdf()
        return [n for n in df["table_name"].tolist() if not n.startswith("__")]

    def _view_exists(self, name: str) -> bool:
        """Check whether a view is registered (including internal)."""
        df = self._conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_type = 'VIEW' "
            f"AND table_name = '{name}'"
        ).fetchdf()
        return len(df) > 0

    def _get_primary_view_names(self) -> list[str]:
        """
        Return db_names of primary (non-comparative) raw views.

        A primary dataset is one whose config has no ``links``.

        """
        names = []
        for db_name, (repo_id, config_name) in self.db_name_map.items():
            if not self._is_comparative(repo_id, config_name):
                if self._view_exists(db_name):
                    names.append(db_name)
        return sorted(names)

    def _get_primary_meta_view_names(self) -> list[str]:
        """Return names of primary ``_meta`` views."""
        return [
            f"{n}_meta"
            for n in self._get_primary_view_names()
            if self._view_exists(f"{n}_meta")
        ]

    def _get_db_name_for(self, repo_id: str, config_name: str) -> str | None:
        """Resolve db_name for a (repo_id, config_name) pair."""
        for db_name, (r, c) in self.db_name_map.items():
            if r == repo_id and c == config_name:
                return db_name
        return None

    def __repr__(self) -> str:
        """String representation."""
        n_repos = len(self.config.repositories)
        n_datasets = len(self.db_name_map)
        n_views = len(self._list_views())
        return (
            f"VirtualDB({n_repos} repos, "
            f"{n_datasets} datasets, "
            f"{n_views} views)"
        )
