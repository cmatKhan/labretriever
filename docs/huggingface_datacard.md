# HuggingFace Dataset Card Format

This document describes the expected YAML metadata format for HuggingFace dataset
repositories used with the labretriever package. The metadata is defined in the repository's
README.md file, at the top in a yaml block, and provides structured information about
the dataset configuration and contents.  

This documentation is intended for developers preparing or augmenting a huggingface
dataset repository to be compatible with labretriever. Before reading, please review the
[BrentLab/hackett_2020](https://huggingface.co/datasets/BrentLab/hackett_2020/blob/main/README.md) 
datacard as an example of a complete implementation of a simple repository. After
reviewing Hackett 2020 and this documentation, it might be helpful to review a more
complex example such as:

- [BrentLab/barkai_compendium](https://huggingface.co/datasets/BrentLab/barkai_compendium):
  This contains a `genome_map` partitioned dataset with separate metadata applied via
  the `applies_to` field. 
- [Brentlab/rossi_2021](https://huggingface.co/datasets/BrentLab/rossi_2021):
  This contains multiple `annotated_features` datasets with embedded metadata
- [Brentlab/yeast_genomic_features](https://huggingface.co/datasets/BrentLab/yeast_genomic_features):
  This contains a simple `genomic_features` dataset used as a reference for other
  datasets in the collection.

## Dataset Types

The `dataset_type` field is a property of each config (hierarchically under
`config_name`). `labretriever` recognizes the following dataset types:

### 1. `genomic_features`
Static information about genomic features (genes, promoters, etc.)
- **Use case**: Gene annotations, regulatory classifications, static feature data
- **Structure**: One row per genomic feature
- **Required fields**: Usually includes gene identifiers, coordinates, classifications

### 2. `annotated_features`
Quantitative data associated with genomic features. A field `sample_id` should exist
to identify single experiments in a single set of conditions.
- **Use case**: Expression data, binding scores, differential expression results
- **Structure**: Each sample will have one row per genomic feature measured. The
  role `quantitative_measure` should be used to identify measurement columns.
- **Common fields**: `regulator_*`, `target_*` fields with the roles
  `regulator_identifier` and `target_identifier` respectively. Fields with the role
  `quantitative_measure` for measurements.

### 3. `genome_map`
Position-level data across genomic coordinates
- **Use case**: Signal tracks, coverage data, genome-wide binding profiles
- **Structure**: Position-value pairs, often large datasets
- **Required fields**: `chr` (chromosome), `pos` (position), signal values

### 4. `metadata`
Experimental metadata and sample descriptions
- **Use case**: Sample information, experimental conditions, protocol details. Note
  that this can also include per-sample QC metrics. For cross-sample QC or analysis,
  see [comparative](#5-comparative) below.
- **Structure**: One row per sample
- **Common fields**: Sample identifiers, experimental conditions, publication info
- **Special field**: `applies_to` - Optional list of config names this metadata applies to

### 5. `comparative`

Quality control metrics, validation results, and cross-dataset analysis outputs.

**Use cases**:
- Cross-dataset quality assessments and validation metrics
- Analysis results relating samples across datasets or repositories
- Comparative analyses (e.g., binding vs expression correlation)

**Structure**: One row represents an observation on 2 or more samples. Note that the
  name of the column containing the sample references isn't specified. However, the
  role and format of the sample references are strictly defined. See
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
- `"BrentLab/harbison_2004;harbison_2004;CBF1_YPD"`
- `"BrentLab/kemmeren_2014;kemmeren_2014;sample_42"`

## Experimental Conditions

Experimental conditions can be specified in three ways:
1. **Top-level** `experimental_conditions`: Apply to all configs in the repository.
  Use when experimental parameters are common across all datasets. This will occur
  at the same level as `configs`
2. **Config-level** `experimental_conditions`: Apply to a specific config
  ([dataset](#dataset)). Use when certain datasets have experimental parameters that
  are not shared by all other datasets in the [repository](#huggingface-repo), but
  are common to all [samples](#sample) within that dataset.
3. **Field-level** with `role: experimental_condition` ([feature-roles](#feature-roles)): For
  per-sample or per-measurement variation in experimental conditions stored as
  data columns. This is specified in the
  `dataset_info.features` ([feature-definitions](#feature-definitions))
  section of a config. `experimental_condition` fields which are categorical can are
  specifically defined in [categorical fields with value definitions](#categorical-fields-with-value-definitions).

The priority of experimental conditions is:

field-level > config-level > top-level

**Example of all three methods:**
```yaml
# Top-level experimental conditions (apply to all [datasets](#dataset) in the repo)
experimental_conditions:
  temperature_celsius: 30
configs:
- config_name: overexpression_data
  description: TF overexpression perturbation data
  dataset_type: annotated_features
  # The overexpression_data [dataset](#dataset) has an additional experimental
  # condition that is specific to this dataset
  experimental_conditions:
    strain_background: "BY4741"
  data_files:
    - split: train
      path: overexpression.parquet
  dataset_info:
    features:
      - name: time
        dtype: float
        description: Time point in minutes
        role: experimental_condition
      - name: mechanism
        dtype: string
        description: Induction mechanism (GEV or ZEV)
        role: experimental_condition
        definitions:
          GEV:
            perturbation_method:
              type: inducible_overexpression
              system: GEV
              inducer: beta-estradiol
              description: "Galactose-inducible estrogen receptor-VP16 fusion system"
          ZEV:
            perturbation_method:
              type: inducible_overexpression
              system: ZEV
              inducer: beta-estradiol
              description: >-
                "Z3 (synthetic zinc finger)-estrogen receptor-VP16 fusion system"
      - name: log2_ratio
        dtype: float
        description: Log2 fold change
        role: quantitative_measure
```

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

**Gene/Feature Identifiers:**
- `(regulator/target)_locus_tag`: Systematic gene identifiers (e.g., "YJR060W"). Must
  be able to join to a genomic_features dataset. If none is specific,
  then the BrentLab/yeast_genomic_features is used
- `(regulator/target)_symbol`: Standard gene symbols (e.g., "CBF1"). Must be able to
  join to a genomic_features dataset. If none is specific,
  then the BrentLab/yeast_genomic_features is used

**Genomic Coordinates:**  
Unless otherwise noted, assume that coordinates are 0-based, half-open intervals

- `chr`: Chromosome identifier
- `start`, `end`: Genomic coordinates
- `pos`: Single position
- `strand`: Strand information (+ or -)

## Feature Roles

The optional `role` field provides semantic meaning to features, especially useful
for annotated_features datasets. The following roles are recognized by labretriever.
**NOTE** `experimental_condition` is a reserved role with additional behavior
as described above.

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
tags: [biology, genomics, transcription-factors]
pretty_name: "Example Genomics Dataset"
size_categories: [100K<n<1M]

configs:
- config_name: genomic_features
  description: Gene annotations and regulatory features
  dataset_type: genomic_features
  data_files:
  - split: train
    path: features.parquet
  dataset_info:
    features:
    - name: gene_id
      dtype: string
      description: Systematic gene identifier
    - name: chr
      dtype: string
      description: Chromosome name
    - name: start
      dtype: int64
      description: Gene start position

- config_name: binding_data
  description: Transcription factor binding measurements
  dataset_type: annotated_features
  default: true
  data_files:
  - split: train
    path: binding.parquet
  dataset_info:
    features:
    - name: regulator_symbol
      dtype: string
      description: Transcription factor name
      role: regulator_identifier
    - name: target_locus_tag
      dtype: string
      description: Target gene systematic identifier
      role: target_identifier
    - name: target_symbol
      dtype: string
      description: Target gene name
      role: target_identifier
    - name: binding_score
      dtype: float64
      description: Quantitative binding measurement
      role: quantitative_measure

- config_name: experiment_metadata
  description: Experimental conditions and sample information
  dataset_type: metadata
  applies_to: ["genomic_features", "binding_data"]
  data_files:
  - split: train
    path: metadata.parquet
  dataset_info:
    features:
    - name: sample_id
      dtype: string
      description: Unique sample identifier
    - name: experimental_condition
      dtype: string
      description: Experimental treatment or condition
    - name: publication_doi
      dtype: string
      description: DOI of associated publication
```

## Terms and definitions

### field/feature/attribute/column
In a collection of samples (see below), the fields record information about the
record. For example, if there are two samples each of which report results for 6000
genes and the way in which the samples differ is by growth media, then growth_media
would be a feature with two levels, eg YPD and SC. If the two samples are stored in
the same parquet file, then there would be a column where the entry for all 6000
rows of the first sample would be YPD and the entry for all 6000 rows of the second
sample would be SC.

### record/row
A row in a table, or a single observation in a single sample (see below).

### metadata
Data about data. However, there are multiple objects to which metadata is attached in
our usage, in particular at the dataset level and at the repo level (see below for
those terms).

### sample
The result of a single biological experiment. For example, if a given dataset has 20
regulators, in 3 replicates in 2 conditions, then there would be 20×3×2 samples.
If the way the results are reported is over 6000 genes, then we would expect all
20×3×2 of those samples to have 6000 records.

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
In our HuggingFace repos, we store one or more datasets. These datasets have
defined types. In general, we try to refer to datasets by the first author and year
of the paper from which they originate, eg 'Mahendrawada 2025'. However, the
distinction between a dataset and a repo can be complicated, as in the case of
Mahendrawada 2025 there is ChEC-seq, ChIP-seq and RNA-seq data. Each of those may be
provided in multiple datasets, eg one which was reported by the authors, and another
reprocessed in our lab. A dataset should refer to a single one of those collections
and may require further specification beyond the first author's name and year published.

### huggingface collection
HuggingFace allows you to group repositories together, which is what we are doing
with all repos storing data related to the yeast database project.

### regulator
A superset that includes "TF" or "transcription factor". These are proteins which
are assayed for their effect on gene expression.

### target
Genes on which the regulator's effect is measured.

### labretriever
A Python package which provides the interface to the HuggingFace collection.

### active set (of samples)
In order to conduct analysis, a user will need to define a set of samples. A sample
(see definition above) is defined by the metadata features, eg regulator_locus_tag.
If the user is interested in all datasets in which this regulator exists, then the
active set would be the set of samples, across the entire collection (see HuggingFace
collection above), with this regulator_locus_tag. The user may choose to filter on
additional features in order to further refine the active set (eg, if a different
dataset has 2 conditions for that regulator, then the user may wish to only retain
1 of those conditions in their active set. They may wish to completely exclude a
different dataset, etc).