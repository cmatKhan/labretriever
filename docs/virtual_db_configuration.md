# VirtualDB Configuration Guide

VirtualDB requires a YAML configuration file that defines which datasets to
include, how to map their fields to common names, and how to normalize factor
levels.

## Basic Example

```yaml
repositories:
  # Each repository defines a "table" in the virtual database
  BrentLab/harbison_2004:
    # REQUIRED: Specify which column is the sample identifier. The `field`
    # value is the actual column name in the parquet data. At the repo level,
    # it applies to all datasets in this repository. If not specified at
    # either level, the default column name "sample_id" is assumed.
    sample_id:
      field: sample_id
    # Repository-wide properties (apply to all datasets in this repository)
    # Paths are explicit from the datacard root
    nitrogen_source:
      path: experimental_conditions.media.nitrogen_source.name

    dataset:
      # Each dataset gets its own view with standardized fields
      harbison_2004:
        # note: this is optional. If not specified, then the config_name is used.
        # This is useful if the config_name isn't suited to a table name, or if it
        # were to conflict with another dataset in the configuration
        db_name: harbison
        # Dataset-specific properties (constant for all samples)
        # Explicit path from datacard/config root
        phosphate_source:
          path: experimental_conditions.media.phosphate_source.compound

        # Field-level properties (vary per sample)
        # Path is relative to field's definitions dict
        carbon_source:
          field: condition
          path: media.carbon_source.compound
          dtype: string  # Optional: specify data type

        # Field without path (column alias with normalization)
        environmental_condition:
          field: condition

  BrentLab/kemmeren_2014:
    dataset:
      kemmeren_2014:
        # optional -- see the note for `db_name` in harbison above
        db_name: kemmeren
        # REQUIRED: If `sample_id` isn't defined at the repo level, it must be
        # defined at the dataset level. The `field` value is the actual column
        # name in the parquet data (does not need to be literally "sample_id").
        sample_id:
          field: sample_id
        # Same logical fields, different physical paths
        # Explicit path from datacard/config root
        carbon_source:
          path: experimental_conditions.media.carbon_source.compound
          dtype: string
        temperature_celsius:
          path: experimental_conditions.temperature_celsius
          dtype: numeric  # Enables numeric filtering with comparison operators

  # Comparative dataset example
  BrentLab/yeast_comparative_analysis:
    dataset:
      dto:
        # Use field mappings to change a field's displayed name. If not specifically
        # listed, then the field is included as it exists in the source data
        dto_fdr:
          field: dto_fdr
        dto_empirical_pvalue:
          field: empirical_pvalue

        # links specify which primary datasets are referenced by composite ID fields
        links:
          binding_id:
            - [BrentLab/harbison_2004, harbison_2004]
          perturbation_id:
            - [BrentLab/kemmeren_2014, kemmeren_2014]

# ===== Normalization Rules =====
# Map varying terminologies to standardized values
factor_aliases:
  carbon_source:
    glucose: [D-glucose, glu, dextrose]
    galactose: [D-galactose, gal]

# Handle missing values with defaults
missing_value_labels:
  carbon_source: "unspecified"

# ===== Documentation =====
description:
  carbon_source: The carbon source provided to the cells during growth
```

### Property Hierarchy

Properties are extracted at three hierarchy levels:

1. **Repository-wide**: Common to all datasets in a repository
   - Paths relative to datacard/config root (explicit)
   - Example: `path: experimental_conditions.media.nitrogen_source.name`

2. **Dataset-specific**: Specific to one dataset configuration
   - Paths relative to datacard/config root (explicit)
   - Example: `path: experimental_conditions.media.phosphate_source.compound`

3. **Field-level**: Vary per sample, defined in field definitions
   - `field` specifies which field to extract from
   - `path` relative to that field's definitions dict
   - Example: `field: condition, path: media.carbon_source.compound`

**Special case**: Field without path creates a column alias
- `field: condition` (no path) renames `condition` column, enables normalization

### Path Resolution

Paths use dot notation to navigate nested structures:

**Repository/Dataset-level** (explicit paths from datacard root):
- `path: experimental_conditions.temperature_celsius` - access experimental conditions
- `path: experimental_conditions.media.carbon_source.compound` - nested condition data
- `path: description` - access fields outside experimental_conditions

**Field-level** (paths relative to field definitions):
- `field: condition, path: media.carbon_source.compound` looks in field
  `condition`'s definitions and navigates to `media.carbon_source.compound`

### Data Type Specifications

Field mappings support an optional `dtype` parameter to ensure proper type handling
during metadata extraction and query filtering.

**Supported dtypes**:
- `string` - Text data (default if not specified)
- `numeric` - Numeric values (integers or floating-point numbers)
- `bool` - Boolean values (true/false)
- `factor` - Categorical data backed by a DuckDB ENUM type (see below)

**When to use dtype**:

1. **Numeric filtering**: Required for fields used with comparison operators
   (`<`, `>`, `<=`, `>=`, `between`)
2. **Type consistency**: When source data might be extracted with incorrect type
3. **Categorical columns**: Use `factor` when a field has a fixed, known set of
   levels and you want DuckDB to enforce membership and enable efficient storage

### factor dtype (DuckDB ENUM)

When `dtype: factor` is set on a field-only mapping, VirtualDB registers a DuckDB
ENUM type from the field's `class_label` definition in the DataCard and casts the
column to that type in the `_meta` view.

**Requirements**:

- `dtype: factor` may only be used with field-only mappings (`field:` specified,
  no `path:` or `expression:`).
- The DataCard must declare the field with `dtype: {class_label: {names: [...]}}`.
  If the field is missing, has a non-`class_label` dtype, or the `names` list is
  absent or empty, VirtualDB raises a `ValueError` at view-registration time.

**Column naming when the output name matches the source field**:

When the mapping key equals the source field name (the common case, e.g.
`time: {field: time, dtype: factor}`), the raw column is preserved in the view
under a `_orig` alias so that the original values remain accessible:

- `time` -- ENUM-typed column with levels from the DataCard
- `time_orig` -- original raw column (e.g., DOUBLE or VARCHAR)

If `time_orig` already exists in the parquet, VirtualDB finds the next available
name: `time_orig_1`, `time_orig_2`, etc.

**Example DataCard feature definition** (in the HuggingFace dataset card YAML):

```yaml
- name: time
  dtype:
    class_label:
      names:
        - 0
        - 5
        - 10
        - 15
        - 20
        - 45
        - 90
  description: Time point in minutes after induction
```

**Example VirtualDB config**:

```yaml
repositories:
  BrentLab/hackett_2020:
    dataset:
      hackett_2020:
        db_name: hackett
        sample_id:
          field: sample_id
        time:
          field: time
          dtype: factor
```

After view registration, `hackett_meta` will contain:
- `time` -- ENUM column, queryable as `WHERE time = '45'`
- `time_orig` -- original numeric column

## Tags

Tags are arbitrary string key/value pairs for annotating datasets. They follow
the same hierarchy as property mappings: repo-level tags apply to all datasets
in the repository, dataset-level tags apply only to that dataset, and
dataset-level tags override repo-level tags with the same key.

```yaml
repositories:
  BrentLab/harbison_2004:
    # Repo-level tags apply to all datasets in this repository
    tags:
      assay: binding
      organism: yeast
    dataset:
      harbison_2004:
        sample_id:
          field: sample_id
        # Dataset-level tags override repo-level tags with the same key
        tags:
          assay: chip-chip

  BrentLab/kemmeren_2014:
    tags:
      assay: perturbation
      organism: yeast
    dataset:
      kemmeren_2014:
        sample_id:
          field: sample_id
```

Access merged tags via `vdb.get_tags(db_name)`, identifying datasets by
their name as it appears in `vdb.tables()`:

```python
from labretriever.virtual_db import VirtualDB

vdb = VirtualDB("datasets.yaml")

# Returns {"assay": "chip-chip", "organism": "yeast"}
# (dataset-level assay overrides repo-level)
vdb.get_tags("harbison")

# Returns {"assay": "perturbation", "organism": "yeast"}
vdb.get_tags("kemmeren")
```

The underlying `MetadataConfig` (available as `vdb.config`) exposes the same
data via `(repo_id, config_name)` pairs for programmatic or developer use:

```python
# Equivalent to vdb.get_tags("harbison") above
vdb.config.get_tags("BrentLab/harbison_2004", "harbison_2004")
```

## Missing Value Labels

`missing_value_labels` is a top-level mapping from property name to a default
string value. When a property is listed here, every dataset's `_meta` view will
include that column -- even datasets that have no explicit mapping for it. For
those datasets, the column is emitted as the constant fallback value.

Datasets that *do* have an explicit mapping for the property are unaffected; they
resolve the value normally (from field definitions, a path, or an expression).

```yaml
missing_value_labels:
  carbon_source: "unspecified"
  temperature_celsius: "unspecified"
```

**Behavior by dataset**:

| Dataset | `carbon_source` mapping | `carbon_source` in `_meta` |
|---------|------------------------|---------------------------|
| harbison | `field: condition, path: media.carbon_source.compound` | resolved from DataCard definitions |
| degron | (none) | `'unspecified'` (fallback) |

Without `missing_value_labels`, datasets that lack the mapping simply do not
include the column in their `_meta` view, making cross-dataset queries on that
column error or require `COALESCE`.

## Comparative Datasets

Comparative datasets differ from other dataset types in that they represent
relationships between samples across datasets rather than individual samples.
Each row relates 2+ samples from other datasets.

### Structure

Comparative datasets use `source_sample` fields instead of a single sample
identifier column:
- Multiple fields with `role: source_sample`
- Each contains composite identifier: `"repo_id;config_name;sample_id_value"`
- Example: `binding_id = "BrentLab/harbison_2004;harbison_2004;42"`

### Fields

All fields in the comparative dataset are included. But they may be re-named
(aliased) by specifically mapping them in the configuration.

```yaml
dto:
  # this would make the displayed field name 'dto_pvalue'
  instead of 'empirical_pvalue'
  dto_pvalue:
    field: empirical_pvalue
```

### Link Structure

the `links` section specifies how the composite IDs map to primary datasets. The first
sub-element under `links` is the name of the field in the comparative dataset that
contains the composite IDs. The value is a list of `[repo_id, config_name]`
pairs indicating which primary datasets are referenced by that field. Those primary
datasets must also be defined in the overall VirtualDB configuration.

```yaml
# Within the comparative dataset config
dto:
  links:
    binding_id:
      - [BrentLab/harbison_2004, harbison_2004]  # [repo_id, config_name]
      - [BrentLab/callingcards, annotated_features]
    perturbation_id:
      - [BrentLab/kemmeren_2014, kemmeren_2014]
```

See the [huggingface datacard documentation](huggingface_datacard.md#5-comparative)
for more detailed explanation of comparative datasets and composite IDs.

## Internal Structure

VirtualDB uses an in-memory DuckDB database to construct a layered hierarchy
of SQL views over locally cached Parquet files. Views are created on initialization and are not persisted to disk.

### View Hierarchy

For each configured dataset, VirtualDB registers a series of views that
build on each other. Using `harbison` as an example primary dataset and
`dto` as a comparative dataset:

**1. Metadata view**

One row per unique sample identifier (the column configured via
`sample_id: {field: <column_name>}`). Derived columns from the
configuration (e.g., `carbon_source`, `temperature_celsius`) are resolved
here using datacard definitions, factor aliases, and missing value labels.
This is the primary view for querying sample-level metadata.

**2. Raw data view**

The full parquet data joined to the metadata view so that every row
carries both the raw measurement columns and the derived metadata
columns. **Developer note**: There is an internal view called __<db_name>_parquet that
is just the raw parquet data without any metadata joins or derived columns.
This is used as the base for joining to the metadata view, but is not exposed directly
to users. 

**3. Expanded view (comparative only)** -- `dto_expanded`

For comparative datasets, each composite ID field (e.g. `binding_id`
with format `"repo_id;config_name;sample_id"`) is parsed into two
additional columns:

- `<link_field>_source` -- the `repo_id;config_name` prefix, aliased
  to the configured `db_name` when the pair is in the VirtualDB config.
  For example, `BrentLab/harbison_2004;harbison_2004` becomes `harbison`.
- `<link_field>_id` -- the sample_id component.

This makes it straightforward to join back to primary dataset views
or filter by source dataset without parsing composite IDs in SQL.

### View Diagram

```
__harbison_parquet  (raw parquet, not directly exposed)
  |
  +-> harbison_meta  (deduplicated, one row per sample identifier,
  |                   with derived columns from config)
  |
  +-> harbison  (full parquet joined to harbison_meta)

__dto_parquet  (raw parquet, not directly exposed)
  |
  +-> dto_expanded  (parquet + parsed columns:
                     binding_id_source, binding_id_id,
                     perturbation_id_source, perturbation_id_id)
```

## Usage

For usage examples and tutorials,
see the [VirtualDB Tutorial](tutorials/virtual_db_tutorial.ipynb).