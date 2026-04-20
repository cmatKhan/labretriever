# Changelog

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
