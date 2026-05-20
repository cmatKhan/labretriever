# BrentLab Yeast Resources Collection

## What Is a Collection Context Document?

A **collection context document** captures the conventions that apply across all
repositories in a labretriever-compatible collection. Individual datacards stay
concise because they can rely on these shared conventions, and tooling — in
particular the `audit_collection` MCP tool — uses the document as authoritative
context when auditing repos.

Any developer building a labretriever-compatible collection should write one.
The document typically covers:

- **Field naming conventions**: canonical names for identifiers, measurements,
  and condition columns (e.g. `regulator_locus_tag`, `pval`, `sample_id`).
- **Standardized vocabulary**: controlled terms for media names, strain
  backgrounds, growth phases, and cultivation methods so that per-sample
  condition values are comparable across repos.
- **Dataset type usage**: which `dataset_type` values are used in the collection
  and what each means for the data structure.
- **Categorical value standards**: the expected set of values for condition
  columns that appear across multiple repos.
- **Cross-cutting best practices**: any other notes that apply to all repos in
  the collection.

When you pass this document to `audit_collection` via the `collection_context`
parameter, the tool uses its **Field Naming Conventions** and **Dataset Type
Usage Examples** sections to flag deviations in individual datacards, and uses
the **Standardized** vocabulary sections to identify non-standard values.

The BrentLab yeast resources collection serves as a concrete reference
implementation of this pattern. Its conventions are described below.

---

## Collection Overview

The BrentLab yeast resources collection contains 11 datasets related to yeast transcription factor binding and gene expression regulation:

1. **barkai_compendium** - ChEC-seq binding data across multiple GEO series
2. **callingcards** - Calling Cards transposon-based binding data
3. **hackett_2020** - TF overexpression with nutrient limitation
4. **harbison_2004** - ChIP-chip binding across 14 environmental conditions
5. **hu_2007_reimand_2010** - TF knockout expression data
6. **hughes_2006** - TF perturbation screen (overexpression and knockout)
7. **kemmeren_2014** - TF deletion expression profiling
8. **mahendrawada_2025** - ChEC-seq and nascent RNA-seq data
9. **rossi_2021** - ChIP-exo binding data
10. **yeast_comparative_analysis** - Cross-dataset comparative analyses
11. **yeast_genome_resources** - Reference genomic features

## Standard Experimental Conditions

None of these are required. However, when information
about these conditions is available, the following
standardized fields and values should be used to ensure
comparability across datasets.

### Strain

Strain background may be included, for example:

```yaml
experimental_conditions:
  strain_background: BY4741
```

### Growth Temperature

Standard growth temperature across the collection is **30°C** unless otherwise noted.

Exceptions:
- **rossi_2021**: 25°C baseline with 37°C heat shock for some samples
- **hu_2007_reimand_2010**: Heat shock at 39°C for heat shock response TFs
- **callingcards**: the experiments are performed at room temperature (~22-25°C)

### Growth Phase

Common growth phase specifications:

These labels are taken from the original publications. In some cases the OD600
is noted

- **early_log_phase**
- **mid_log_phase**
- **late_log_phase**
- **stationary_phase** - eg barkai_compendium, which are allowed to grow overnight. The
  cells are harvested at a very high density (OD600 4.0).

Example:
```yaml
experimental_conditions:
  growth_phase_at_harvest:
    stage: mid_log_phase
    od600: 0.6
    od600_tolerance: 0.1
```

### Cultivation Methods

Standard cultivation methods used:

- **liquid_culture** - Standard batch culture in flasks
- **batch** - Batch culture
- **plate** - Growth on agar plates
- **chemostat** - Continuous culture (hackett_2020)

## Concentration Specifications

**Always use `concentration_percent`** for all concentration specifications.
Convert other units to percentage:

- **mg/ml to percent**: divide by 10 (e.g., 5 mg/ml = 0.5%)
- **g/L to percent**: divide by 10 (e.g., 6.71 g/L = 0.671%)
- **Molar to percent**: convert using molecular weight
  - Example: 100 nM rapamycin = 9.142e-6%

### Examples from the Collection

```yaml
# Yeast nitrogen base: 6.71 g/L = 0.671%
- compound: yeast_nitrogen_base
  concentration_percent: 0.671

# Alpha factor: 5 mg/ml = 0.5%
- compound: alpha_factor_pheromone
  concentration_percent: 0.5

# Rapamycin: 100 nM = 9.142e-6%
chemical_treatment:
  compound: rapamycin
  concentration_percent: 9.142e-6
```

## Field Naming Conventions

The collection follows these field naming conventions:

### Gene/Feature Identifiers

The BrentLab yeast collection uses these canonical identifier field names:

- **regulator_locus_tag**: Systematic ID of the regulatory factor (e.g., "YJR060W")
- **regulator_symbol**: Common gene name of the regulatory factor (e.g., "CBF1")
- **target_locus_tag**: Systematic ID of the target gene
- **target_symbol**: Common gene name of the target gene

All locus tag and symbol fields must be joinable to
`BrentLab/yeast_genome_resources` via the corresponding identifier column.

#### Identifier Roles

Fields carrying these identifiers should be marked with the following
collection-defined roles:

- `regulator_identifier` — applied to `regulator_locus_tag` and `regulator_symbol`.
- `target_identifier` — applied to `target_locus_tag` and `target_symbol`.

These roles are not reserved by labretriever itself (the library stores them but
takes no special action). They are BrentLab conventions. Their value is that
`get_column_metadata()` will return them on the appropriate columns, allowing an AI
assistant or analysis script to identify which columns refer to the regulator and
which refer to the target without relying on naming conventions alone.

### Quantitative Measurements Examples

Common measurement field names:

- **effect**, **log2fc**, **log2_ratio** - Log fold change measurements
- **pvalue**, **pval**, **p_value** - Statistical significance
- **padj**, **adj_p_value** - FDR-adjusted p-values
- **binding_score**, **peak_score** - Binding strength metrics
- **enrichment** - Enrichment ratios

### Experimental Metadata Examples

- **sample_id** - Unique sample identifier (integer)
- **db_id** - Legacy database identifier (deprecated, do not use)
- **batch** - Experimental batch identifier
- **replicate** - Biological replicate number
- **time** - Timepoint in timecourse experiments

## Dataset Types in This Collection

The BrentLab yeast resources collection defines three collection-specific
`dataset_type` values in addition to the two reserved by labretriever. For the
reserved types (`metadata`, `comparative`) and their runtime behavior, see
[Reserved Dataset Types](huggingface_datacard.md#reserved-dataset-types).

### `genomic_features`

Static reference annotations for genomic features (genes, promoters, etc.).

- **Structure**: One row per genomic feature.
- **Fields**: Identifiers, coordinates, and classification columns. Field names
  are collection-defined; see [Gene/Feature Identifiers](#genefeature-identifiers).

### `annotated_features`

Quantitative data associated with genomic features.

- **Structure**: One row per genomic feature per sample. A `sample_id` field
  should uniquely identify each experimental sample.
- **Fields**: Identifier fields (roles `regulator_identifier`,
  `target_identifier`) and measurement fields (role `quantitative_measure`).
  See [Field Naming Conventions](#field-naming-conventions).

### `genome_map`

Position-level data across genomic coordinates, typically for large signal
track or coverage datasets.

- **Structure**: Position-value pairs; usually partitioned.
- **Fields**: Standard coordinate fields in this collection are `chr` and `pos`
  for single-position data, or `chr`, `start`, `end` for interval data.

The following sections describe how each type is used in specific repos.

### genomic_features

**yeast_genome_resources** provides reference annotations:
- Gene coordinates and strand information
- Systematic IDs (`locus_tag`) and common names (`symbol`)
- Feature types (gene, ncRNA_gene, tRNA_gene, etc.)

Standard coordinate field names in this collection: `chr`, `pos` for single
positions; `chr`, `start`, `end` for intervals. All coordinates are 0-based,
half-open unless otherwise noted.

Used for joining regulator/target identifiers across all other datasets.

### annotated_features

Most common dataset type in the collection. Examples:

- **hackett_2020**: TF overexpression with timecourse measurements
- **harbison_2004**: ChIP-chip binding with condition field definitions
- **kemmeren_2014**: TF deletion expression data
- **mahendrawada_2025**: ChEC-seq binding scores

Typical structure: regulator x target x measurements, with optional condition fields.

### genome_map

Position-level data, typically partitioned by sample or accession. Examples:

- **barkai_compendium**: ChEC-seq pileup data partitioned by Series/Accession
- **rossi_2021**: ChIP-exo 5' tag coverage partitioned by sample
- **callingcards**: Transposon insertion density partitioned by batch

Standard coordinate field names: `chr`, `pos`.

### metadata

Separate metadata configs or embedded metadata via `metadata_fields`:

**Separate config example** (barkai_compendium):
```yaml
- config_name: GSE178430_metadata
  dataset_type: metadata
  applies_to: ["genomic_coverage"]
```

**Embedded metadata example** (harbison_2004):
```yaml
- config_name: harbison_2004
  dataset_type: annotated_features
  metadata_fields: ["regulator_locus_tag", "regulator_symbol", "condition"]
```

### comparative

**yeast_comparative_analysis** provides cross-dataset analysis results:

- **dto config**: Direct Target Overlap analysis comparing binding and perturbation experiments
- Uses `source_sample` role for composite identifiers
- Format: `"repo_id;config_name;sample_id"` (semicolon-separated)
- Contains 8 quantitative measures: rank thresholds, set sizes, FDR, p-values
- Partitioned by binding_repo_dataset and perturbation_repo_dataset

**Composite Sample Identifiers**:
Comparative datasets use composite identifiers to reference samples from other datasets:
- `binding_id`: Points to a binding experiment (e.g., `BrentLab/callingcards;annotated_features;1`)
- `perturbation_id`: Points to a perturbation experiment (e.g., `BrentLab/hackett_2020;hackett_2020;200`)

**Typical structure**: source_sample_1 x source_sample_2 x ... x measurements

**Use case**: Answer questions like "Which binding experiments show significant overlap with perturbation effects?"

## Categorical Condition Definitions

Many datasets define categorical experimental conditions using the `definitions` field.

### harbison_2004 Environmental Conditions

14 conditions with detailed specifications:
- **YPD** (rich media baseline)
- **SM** (amino acid starvation)
- **RAPA** (rapamycin treatment)
- **H2O2Hi**, **H2O2Lo** (oxidative stress)
- **HEAT** (heat shock)
- **GAL**, **RAFF** (alternative carbon sources)
- And 6 more...

Each condition definition includes media composition, temperature, growth phase, and treatments.

### hackett_2020 Nutrient Limitations

```yaml
restriction:
  definitions:
    P:  # Phosphate limitation
      media:
        phosphate_source:
          - compound: potassium_phosphate_monobasic
            concentration_percent: 0.002
    N:  # Nitrogen limitation
      media:
        nitrogen_source:
          - compound: ammonium_sulfate
            concentration_percent: 0.004
    M:  # Undefined limitation
      description: "Not defined in the paper"
```

### hu_2007_reimand_2010 Treatment Conditions

```yaml
heat_shock:
  definitions:
    true:
      temperature_celsius: 39
      duration_minutes: 15
    false:
      description: Standard growth conditions at 30°C
```

## Partitioning Strategies

Large genome_map datasets use partitioning:

**barkai_compendium** - Two-level partitioning:
```yaml
partitioning:
  partition_by: ["Series", "Accession"]
  path_template: "genome_map/*/*/part-0.parquet"
```

**callingcards** - Batch partitioning:
```yaml
partitioning:
  enabled: true
  partition_by: ["batch"]
  path_template: "genome_map/batch={batch}/*.parquet"
```

## Collection-Wide Best Practices

### 1. Omit unspecified fields with a comment

`labretriever` will handle adding "unspecified" to fields which are not common across
datasets.

```yaml
# CORRECT
experimental_conditions:
  temperature_celsius: 30
  # cultivation_method is note noted in the paper and is omitted

# INCORRECT
experimental_conditions:
  temperature_celsius: unspecified
```

### 2. Document Source Publications

If the original paper used something like g/L, then convert that to
`concentration_percent` and add a comment with the original value and units.

```yaml
carbon_source:
  - compound: D-glucose
    # Saldanha et al 2004: 10 g/L
    concentration_percent: 1
```

### 3. Use Standard Field Roles

Apply semantic roles consistently across all repos in the collection. See
[Feature Roles](huggingface_datacard.md#feature-roles) for the full list of
recognized roles.

### 4. Provide sample_id

All annotated_features datasets should include `sample_id` to uniquely identify experimental samples. This enables cross-dataset joining and metadata management.

### 5. Specify metadata_fields or applies_to

For datasets with metadata, either:
- Use `metadata_fields` to extract from the data itself, OR
- Create separate metadata config with `applies_to` field

### 6. Use Consistent Gene Identifiers

All regulator/target identifiers must be joinable to **yeast_genome_resources**:
- Use current systematic IDs (ORF names)
- Include both locus_tag and symbol fields
- Mark with appropriate roles

---

## Terms and Definitions

### regulator

A protein assayed for its effect on gene expression, including but not limited to
transcription factors (TFs). In this collection, "regulator" and "TF" are used
interchangeably because the collection focuses on TF binding and perturbation
experiments.

### target

A gene whose expression or accessibility is measured in the context of a regulator
experiment. In binding datasets (e.g., ChIP-chip, ChEC-seq, Calling Cards), a target
is a genomic locus at which the regulator's occupancy is measured. In perturbation
datasets (e.g., overexpression, deletion), a target is a gene whose expression
changes in response to the regulator perturbation.

### active set (of samples)

To conduct analysis a user defines a set of samples. A sample is identified by its
metadata features — for example, `regulator_locus_tag`. If the user is interested in
all samples across the collection that assay a given regulator, that constitutes the
active set. The user may further filter on additional features (e.g., retain only
one condition per regulator, exclude specific datasets) to refine the active set.
