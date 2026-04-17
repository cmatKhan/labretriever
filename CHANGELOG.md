# Changelog

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
