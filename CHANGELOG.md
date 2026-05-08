# Changelog

## [0.4.1] - 2026-05-08

### Added

- `cache_dir` parameter on `VirtualDB.__init__`. Accepts a path or string that
  overrides the HuggingFace cache directory for all `snapshot_download` calls
  made during dataset registration. When `None` (default), the location is
  resolved from `HF_CACHE_DIR` / `HF_HOME` / the `huggingface_hub` default at
  call time. Useful for bundled deployments that store parquet snapshots in a
  non-standard location.

### Changed

- `constants.CACHE_DIR` (a module-level `Path` constant) replaced by
  `constants.get_cache_dir()` (a function). The function reads `HF_CACHE_DIR`
  at call time rather than at import time, so setting the variable after import
  (e.g. via a CLI flag) is now respected.

## [0.4.0] - 2026-05-08

### Added

- `VirtualDB.materialize()` replaces all registered dataset views with
  in-memory DuckDB tables, re-pointing the original view names at those tables.
  Subsequent queries hit RAM instead of re-scanning parquet files, reducing
  per-query latency at the cost of increased startup time and memory usage.
  Uses `CREATE OR REPLACE` semantics; safe to call multiple times.

## [0.3.1] - 2026-05-08

### Added

- `local_files_only` parameter on `VirtualDB.__init__`. When `True`, all
  `snapshot_download` calls skip HuggingFace network checks and use only the
  local cache, eliminating per-dataset `repo_info` HTTP round-trips on warm
  restarts. Raises `huggingface_hub.utils.LocalEntryNotFoundError` if a
  required file is absent from the local cache.
- Debug-level timing logs throughout `VirtualDB.__init__` and its internal
  phases (`_load_datacards`, `_validate_datacards`, `_update_cache`,
  `_register_all_views`, `_build_column_metadata`) to aid performance
  profiling.
- Debug-level timing logs in `HfDataCardFetcher` and `HfRepoStructureFetcher`
  for individual `DatasetCard.load` and `repo_info` calls.

## [0.3.0] - 2026-04-21

### Added

- `doi` field on `DatasetConfig` (dataset-level) and `DatasetCard`
  (repository-level) models. Stores a DOI URL for the primary publication,
  separate from the `citation` text field. Dataset-level value overrides
  repository-level when present.
- `DataCard.info()` now includes a `doi` key at both the repository and
  dataset levels.
- `description` field on `DatasetVirtualDBConfig`. When set in the VirtualDB
  YAML configuration, overrides the DataCard config description returned by
  `VirtualDB.get_dataset_description(db_name)`.

### Changed

- `VirtualDB.get_dataset_description(db_name)` now checks the VirtualDB config
  `description` field first, falling back to the DataCard config description.
- HuggingFace dataset card convention: the `citation` YAML field now holds the
  full bibliographic text; the DOI URL is stored separately under `doi`.

## [0.2.0] - 2026-04-20

### Added

- `citation` field on `DatasetConfig` (dataset-level) and `DatasetCard`
  (repository-level) models. Dataset-level citation overrides the
  repository-level citation when present.
- `DataCard.get_citation(config_name=None)` - returns the citation at the
  appropriate hierarchy level.
- `DataCard.info(config_name=None)` - unified introspection method replacing
  the former `get_repository_info()` and `summary()`. Called without arguments
  it returns repository-level metadata (license, citation, tags, config list,
  file counts). Called with a configuration name it returns dataset-level detail
  (description, features, citation, experimental conditions, metadata schema).
- `VirtualDB.get_citation(db_name)` - returns the citation for the dataset
  registered under `db_name`, delegating to `DataCard.get_citation`.
- SQL column-alias qualification fix: field aliases with spaces (e.g.
  `"Regulator locus tag"`) are now correctly qualified with their table prefix
  (`m.` or `d.`) in JOIN contexts, resolving an ambiguous-column error when
  both data and metadata parquets share a column name.

### Changed

- `DataCard.get_repository_info()` removed; use `DataCard.info()` instead.
- `DataCard.summary()` removed; use `DataCard.info()` instead. The `configs`
  key in the returned dict provides the per-configuration listing that
  `summary()` previously formatted as a string.

## [0.1.0] - 2026-04-16

### Added

- `ColumnMeta` dataclass exported from the top-level package. Carries
  `description`, `role`, and `level_definitions` for a single column in a
  `_meta` view.
- `VirtualDB.get_column_metadata(db_name)` - returns a `dict[str, ColumnMeta]`
  built at construction time from the DataCard features and property mappings
  for a primary dataset.
- `VirtualDB.get_dataset_description(db_name)` - returns the DataCard config
  description string for a dataset.
- Internal `_build_column_metadata()` phase runs during `VirtualDB.__init__`
  and populates `_column_metadata` for all primary (non-comparative) datasets.
  Handles Type-A rename propagation and stubs for Type-B/C derived columns.

### Changed

- `VirtualDB._db_name_map` renamed to `VirtualDB.db_name_map` (public
  attribute). Downstream code using the private name must be updated.
- `VirtualDB._datacards` renamed to `VirtualDB.datacards` (public attribute).
- Version bumped from `0.0.1` to `0.1.0`.

### Deprecated

- `VirtualDB.get_condition_field_info(db_name)` now emits a `DeprecationWarning`
  directing callers to `get_column_metadata` instead. The method remains
  functional but will be removed in a future release.
