# HuggingFace Dataset Card Format

This document describes the expected YAML metadata format for HuggingFace dataset
repositories used with the labretriever package. The metadata is defined in the repository's
README.md file, at the top in a yaml block, and provides structured information about
the dataset configuration and contents.  

This documentation is intended for developers preparing or augmenting a HuggingFace
dataset repository to be compatible with labretriever. It describes the format only.
Field naming conventions, expected dataset types, and vocabulary standards are
collection-specific and should be documented in a
[collection context document](brentlab_yeastresources_collection.md). The BrentLab
yeast resources collection provides a concrete example of both the datacard format
and its accompanying collection context document.

## Reserved Dataset Types

The `dataset_type` field is a free-form string on each config. Any value is
accepted. Your collection context document should define which types your
collection uses and what each one means.

Two values are **reserved** by labretriever and trigger specific runtime
behavior. All other values are treated as opaque collection-defined strings.

### `metadata`

Experimental metadata and sample descriptions.

- **Use case**: Sample information, experimental conditions, protocol details,
  per-sample QC metrics. For cross-sample analysis see
  [`comparative`](#comparative) below.
- **Structure**: One row per sample.
- **Special field**: `applies_to` — an optional list of config names this
  metadata config applies to. This field is only permitted on `metadata` and
  `comparative` configs. It is rejected by validation on any other type.

### `comparative`

Quality control metrics, validation results, and cross-dataset analysis outputs.

- **Use cases**: Cross-dataset quality assessments, analysis results relating
  samples across datasets or repositories, comparative analyses.
- **Structure**: One row represents an observation on two or more samples. The
  name of the column containing sample references is user-defined, but its role
  and format are strictly defined. See
  [Defining Sample References](#defining-sample-references) below.

#### Defining Sample References

The name of the field which contains the sample reference is user-defined. However,
the contents of that field, and its role, must be as follows:

- **`source_sample`**: Fields containing composite sample identifiers. This must be in
  the format `"repo_id;config_name;sample_id"`.

```
"repo_id;config_name;sample_id"
```

Examples:
- `"org/dataset_a;config_1;42"`
- `"org/dataset_b;main_config;sample_99"`

## Experimental Conditions

`experimental_conditions` is a **reserved property** in labretriever with
built-in retrieval and merging logic. It can be specified at three scopes,
and all three levels are surfaced together by `DataCard.extract_metadata_schema`
and `DataCard.get_experimental_conditions`.

1. **Top-level** (`experimental_conditions` at the same level as `configs`):
   Values here are constant across every config in the repository. Returned
   by `get_experimental_conditions()` with no argument.
2. **Config-level** (`experimental_conditions` inside a specific config entry):
   Values here are constant across all samples in that config and override
   top-level keys of the same name. Returned merged with top-level by
   `get_experimental_conditions(config_name)`.
3. **Field-level** (`role: experimental_condition` on a feature column; see
   [Feature Roles](#feature-roles)): Per-sample variation captured as a data
   column. Columns with this role are collected into `condition_fields` by
   `extract_metadata_schema` and their `definitions` are exposed as
   `level_definitions` in column metadata.

When the same key appears at multiple scopes the resolution order is:

field-level > config-level > top-level

The content and structure of `experimental_conditions` dicts is entirely
collection-defined. For a concrete example of how this property is used in
practice, see the
[BrentLab Yeast Resources Collection](brentlab_yeastresources_collection.md#standard-experimental-conditions).

**Example showing all three scopes:**

```yaml
# Top-level: constant across the entire repo
experimental_conditions:
  temperature_celsius: 30
configs:
- config_name: treated_samples
  dataset_type: annotated_features
  # Config-level: constant across all samples in this config only;
  # overrides top-level keys of the same name
  experimental_conditions:
    treatment: compound_x
  data_files:
    - split: train
      path: treated.parquet
  dataset_info:
    features:
      - name: batch
        dtype: string
        description: Experimental batch identifier
        # Field-level: per-row variation stored as a data column
        role: experimental_condition
      - name: score
        dtype: float
        role: quantitative_measure
```

### Other Repo and Config Properties

The format also accepts arbitrary additional properties at the repo or config
level via Pydantic's `extra="allow"`. These pass through to `model_extra` and
are available to collection-specific tooling, but labretriever has no built-in
retrieval logic for them. Only `experimental_conditions` has first-class support.

## Citation and DOI

Publication metadata is split into two separate fields, each usable at the repository
level or overridden at the individual dataset config level:

- **`doi`**: A URL or DOI string pointing to the primary publication. Use the full
  DOI URL (e.g., `https://doi.org/10.1038/nature02800`) rather than the short form.
- **`citation`**: A full bibliographic citation string for the publication. Include
  enough detail for a reader to locate the original work.

Both fields follow the same precedence rule: the dataset-level value overrides the
repository-level value when present.

**Example:**
```yaml
# Repository-level fields (apply to all datasets unless overridden)
doi: https://doi.org/10.1038/nature02800
citation: >-
  Harbison CT, Gordon DB, Lee TI, Rinaldi NJ, Macisaac KD, et al. 2004.
  Transcriptional regulatory code of a eukaryotic genome. Nature 431:99-104.

configs:
- config_name: harbison_2004
  description: ChIP-chip binding data from Harbison et al. 2004
  dataset_type: annotated_features
  # Uses repository-level doi and citation since none specified here
  data_files:
    - split: train
      path: harbison_data.parquet
  dataset_info:
    # ... feature definitions ...

- config_name: reprocessed_binding
  description: Reprocessed version using updated analysis pipeline
  dataset_type: annotated_features
  # Dataset-specific fields override repository-level
  doi: https://doi.org/10.1093/bioinformatics/example
  citation: "Smith J, et al. Reanalysis of Harbison ChIP-chip data. Bioinformatics. 2023."
  data_files:
    - split: train
      path: reprocessed_data.parquet
  dataset_info:
    # ... feature definitions ...
```

## Genome Resources

Datasets that are built against a specific set of genomic intervals
(e.g. promoter annotations, gene bodies) can declare that information directly
in the datacard using a `genome_resources` block. This keeps data provenance
co-located with the dataset rather than in a separate config file.

`genome_resources` may appear at the repo level (applies to all configs) or
at the config level (applies to that config only, overrides repo-level entries
with the same name).

The dict key under `region_sets` is the region set name. No redundant `name:`
sub-field is needed.

```yaml
# Repo-level: applies to all configs in this datacard
genome_resources:
  region_sets:
    promoters:
      path: https://huggingface.co/datasets/org/reference_data/resolve/main/promoters.bed
      join_column: gene_id

configs:
  - config_name: analysis_set
    description: Binding data
    dataset_type: annotated_features
    # Config-level: overrides repo-level for this config only
    genome_resources:
      region_sets:
        promoters:
          path: https://huggingface.co/datasets/org/reference_data/resolve/main/promoters_v2.bed
          join_column: gene_id
    data_files:
      - split: train
        path: data/analysis_set.parquet
    dataset_info:
      features: []
```

Sub-fields per region set entry:

| Field | Description |
|-------|-------------|
| `path` | Relative path within this repo or a full URL to the region BED/parquet file. Full URLs (e.g. `https://huggingface.co/datasets/Org/repo/resolve/main/file.bed`) are preferred when the file lives in a different HuggingFace repo. |
| `join_column` | Column in this dataset used to join to the region set (e.g. `target_locus_tag`). |

Both fields are optional, and arbitrary additional fields are permitted.

VirtualDB reads these entries when `vdb.get_region_sets(db_name)` is called and
merges them with any overrides declared in the VirtualDB config. See the
[VirtualDB configuration guide](virtual_db_configuration.md#genome-resources) for
details on the merge order and the `get_region_sets` / `get_region_set_info` accessors.

## Feature Definitions

Each config must include detailed feature definitions in `dataset_info.features`:
```yaml
dataset_info:
  features:
    - name: field_name           # Column name in the data
      dtype: string              # Data type (string, int64, float64, etc.)
      description: "Detailed description of what this field contains"
      role: "target_identifier"  # Optional: semantic role of the feature
```

### Categorical Fields with Value Definitions

For fields with `role: experimental_condition` that contain categorical values, you can
provide structured definitions for each value using the `definitions` field. This allows
machine-parsable specification of what each condition value means experimentally:
```yaml
- name: condition
  dtype:
    class_label:
      names: ["standard", "heat_shock"]
  role: experimental_condition
  description: Growth condition of the sample
  definitions:
    standard:
      media:
        name: synthetic_complete
        carbon_source:
          - compound: D-glucose
            concentration_percent: 2
        nitrogen_source:
          - compound: yeast_nitrogen_base
            # lastname et al 2025 used 6.71 g/L
            concentration_percent: 0.671
            specifications:
              - without_amino_acids
              - without_ammonium_sulfate
          - compound: ammonium_sulfate
            # lastname et al 2025 used 5 g/L
            concentration_percent: 0.5
          - compound: amino_acid_dropout_mix
            # lastname et al 2025 used 2 g/L
            concentration_percent: 0.2
    heat_shock:
      temperature_celsius: 37
      duration_minutes: 10
```

Each key in `definitions` must correspond to a possible value in the field.
The structure under each value provides experimental parameters specific to that
condition using the same nested format as `experimental_conditions` at config or
top level.

### Naming Conventions

Field naming conventions are collection-defined. Consult your
[collection context document](brentlab_yeastresources_collection.md) for the
canonical names used in your collection.

**Genomic Coordinates:**
Unless otherwise noted, assume that coordinates are 0-based, half-open intervals.

- `chr`: Chromosome identifier
- `start`, `end`: Genomic coordinates
- `pos`: Single position
- `strand`: Strand information (+ or -)

## Shared Feature Definitions

When a repo has multiple configs that share most of the same fields, you can declare
those fields once at the repo level rather than repeating them in every
`dataset_info.features` block. This is a labretriever convention; it is not rendered
by the HuggingFace Hub.

Add a top-level `features` key (parallel to `configs`) containing a list of groups.
Each group has an `applies_to` list of `config_name` strings and a `fields` list in the
same format as `dataset_info.features`.

```yaml
features:
  - applies_to:
      - dataset_a
      - dataset_b
    fields:
      - name: target_locus_tag
        dtype: string
        description: Systematic gene identifier for the target gene
        role: target_identifier
      - name: poisson_pval
        dtype: float64
        description: P-value from Poisson test
        role: quantitative_measure

  - applies_to:
      - dataset_b
      - dataset_c
    fields:
      - name: field_specific_to_b_and_c
        dtype: string
        description: >-
          This field only appears in dataset_b and dataset_c.
          dataset_b gets both this, and those above, since it is
          present in both groups.
```

In this example `annotated_feature_mindel` appears in both groups, so it inherits fields
from each. `annotated_features_orig_reprocess` only appears in the first group and
inherits only those fields. This lets two configs share a common base while each
accumulating additional group-specific fields.

### Merge rules

The hierarchy of shared vs config-specific fields is the same as for other properties: config-level fields override shared fields with the same name.
However, merge rules are property-specific, so if a field has the same name 
at the repo and dataset level, but the description at the dataset level 
differs, then that description will be used for that field in that dataset, but all other properties (dtype, role) will be inherited from the shared definition.

```yaml
features:
  # both dataset_a and dataset_b have a pvalue field, but the method in
  # which they are calculated differs. So, dataset_b overrides the description but inherits dtype and role from the repo level features
  - applies_to: [dataset_a, dataset_b]
    fields:
      - name: pval
        dtype: float64
        description: P-value from Poisson test
        role: quantitative_measure

configs:
- config_name: dataset_a
  # ...
  dataset_info:
    features:
      - name: pval
        description: A hypergeometric pvalue 
        # dtype and role are inherited from the shared definition above
```

## Feature Roles

The optional `role` field is a free-form string that provides semantic meaning to
features. All role values are stored and exposed via `get_column_metadata()` without
modification. Only one role has built-in library behavior:

- `experimental_condition` — marks a feature column as per-sample condition
  variation. Triggers collection into `condition_fields` by `extract_metadata_schema`
  and population of `level_definitions` in column metadata. Categorical columns with
  this role may also carry a `definitions` block; see
  [Categorical Fields with Value Definitions](#categorical-fields-with-value-definitions).

All other role values are collection-defined. The library stores them as metadata but
takes no action on them. Your collection context document should define which role
values are used and what they mean. See the
[BrentLab Yeast Resources Collection](brentlab_yeastresources_collection.md#field-naming-conventions)
for an example of collection-defined identifier roles.

The `experimental_conditions` property name at repo/config scope is also reserved;
see [Experimental Conditions](#experimental-conditions).

## Partitioned Datasets

For large datasets (eg most genome_map datasets), use partitioning:

```yaml
dataset_info:
  partitioning:
    enabled: true
    partition_by: ["accession"]  # Partition column(s)
    path_template: "data/accession={accession}/*.parquet"
```

This allows efficient querying of subsets without loading the entire dataset.

## Metadata 

### Metadata Relationships with `applies_to`

For metadata configs, you can explicitly specify which other configs the metadata
applies to using the `applies_to` field. This provides more control than automatic
type-based matching.

```yaml
configs:
# Data configs
- config_name: genome_map_data
  dataset_type: genome_map
  # ... rest of config

- config_name: binding_scores
  dataset_type: annotated_features
  # ... rest of config

- config_name: expression_data
  dataset_type: annotated_features
  # ... rest of config

# Metadata config that applies to multiple data configs
- config_name: repo_metadata
  dataset_type: metadata
  applies_to: ["genome_map_data", "binding_scores", "expression_data"]
  # ... rest of config
```

### Embedded Metadata with `metadata_fields`

When no explicit metadata config exists, you can extract metadata directly from the
dataset's own files using the `metadata_fields` field. This specifies which fields
should be treated as metadata.

### Single File Embedded Metadata

For single parquet files, the system extracts distinct values using `SELECT DISTINCT`:

```yaml
- config_name: binding_data
  dataset_type: annotated_features
  metadata_fields: ["regulator_symbol", "experimental_condition"]
  data_files:
  - split: train
    path: binding_measurements.parquet
  dataset_info:
    features:
    - name: regulator_symbol
      dtype: string
      description: Transcription factor name
    - name: experimental_condition
      dtype: string
      description: Experimental treatment
    - name: binding_score
      dtype: float64
      description: Quantitative measurement
```

### Partitioned Dataset Embedded Metadata

For partitioned datasets, partition values are extracted from directory structure:

```yaml
- config_name: genome_map_data
  dataset_type: genome_map
  metadata_fields: ["run_accession", "regulator_symbol"]
  data_files:
  - split: train
    path: genome_map/accession=*/regulator=*/*.parquet
  dataset_info:
    features:
    - name: chr
      dtype: string
      description: Chromosome
    - name: pos
      dtype: int32
      description: Position
    - name: signal
      dtype: float32
      description: Signal intensity
    partitioning:
      enabled: true
      partition_by: ["run_accession", "regulator_symbol"]
```

## Data File Organization

### Single Files
```yaml
data_files:
- split: train
  path: single_file.parquet
```

### Multiple Files/Partitioned Data
```yaml
data_files:
- split: train
  path: data_directory/*/*.parquet  # Glob patterns supported
```

## Complete Example Structure

```yaml
license: mit
language: [en]
tags: [biology, genomics]
pretty_name: "Example Genomics Dataset"
size_categories: [100K<n<1M]

doi: https://doi.org/10.0000/example
citation: "Author A, Author B. Example study. Journal. 2024."

configs:
- config_name: genomic_features
  description: Reference feature annotations
  dataset_type: genomic_features
  data_files:
  - split: train
    path: features.parquet
  dataset_info:
    features:
    - name: gene_id
      dtype: string
      description: Systematic feature identifier
    - name: chr
      dtype: string
      description: Chromosome name
    - name: start
      dtype: int64
      description: Feature start position (0-based)
    - name: end
      dtype: int64
      description: Feature end position (half-open)

- config_name: measurement_data
  description: Quantitative measurements per feature per sample
  dataset_type: annotated_features
  default: true
  data_files:
  - split: train
    path: measurements.parquet
  dataset_info:
    features:
    - name: source_id
      dtype: string
      description: Identifier for the entity whose effect is measured
      role: regulator_identifier
    - name: gene_id
      dtype: string
      description: Systematic identifier of the measured feature
      role: target_identifier
    - name: score
      dtype: float64
      description: Quantitative measurement value
      role: quantitative_measure
    - name: sample_id
      dtype: int64
      description: Unique integer identifier for this experimental sample
      role: experimental_condition

- config_name: experiment_metadata
  description: Experimental conditions and sample information
  dataset_type: metadata
  applies_to: ["measurement_data"]
  data_files:
  - split: train
    path: metadata.parquet
  dataset_info:
    features:
    - name: sample_id
      dtype: int64
      description: Unique sample identifier
    - name: condition
      dtype: string
      description: Experimental treatment or condition
      role: experimental_condition
```

## Terms and definitions

### field/feature/attribute/column
In a collection of samples (see below), the fields record information about the
record. For example, if there are two samples each of which report results for 6000
genes and the way in which the samples differ is by growth media, then growth_media
would be a feature with two levels. If the two samples are stored in the same parquet
file, there would be a column where the entry for all 6000 rows of the first sample
would be one value and the entry for all 6000 rows of the second sample would be
another.

### record/row
A row in a table, or a single observation in a single sample (see below).

### metadata
Data about data. In labretriever usage this applies at both the dataset level and the
repo level.

### sample
The result of a single biological experiment. For example, if a given dataset has
20 entities measured in 3 replicates in 2 conditions, then there would be 20x3x2
samples. If results are reported over 6000 features, all 20x3x2 samples would each
have 6000 records.

### huggingface repo
HuggingFace is a thin layer on top of GitHub. HuggingFace repos are GitHub repos with
additional functionality.

### datacard
A README file in the HuggingFace repo. In HuggingFace, this is called a datacard and
has an additional YAML section at the top. This YAML section stores information on
the repo and is extensible. It is in this YAML section that we record a defined set
of attributes and features that allow us to search/filter/subset the data in the
collection (see below). See the datacard format documentation for a full description.

### dataset
In a HuggingFace repo, one or more datasets may be stored, each with a defined type.
A dataset should refer to a single cohesive collection of data and may require
further specification beyond a name (e.g., a repo may contain both raw and
reprocessed versions of the same experiment as separate dataset configs).

### huggingface collection
HuggingFace allows you to group repositories together into a collection. A
labretriever collection context document captures the conventions shared across
all repos in such a group.

### labretriever
A Python package which provides the interface to a HuggingFace collection of
labretriever-compatible dataset repositories.