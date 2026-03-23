# flake8: noqa
"""
Three diverse datacard examples for testing datacard parsing and database construction.

These examples capture different patterns of experimental condition specification:
1. Top-level conditions with field-level variations (minimal media)
2. Complex field-level definitions with multiple environmental conditions
3. Partitioned dataset with separate metadata configs using applies_to

"""

EXAMPLE_1_SIMPLE_TOPLEVEL = """---
license: mit
language:
  - en
tags:
  - genomics
  - yeast
  - transcription
pretty_name: "Example Dataset 1 - TF Perturbation"
size_categories:
  - 100K<n<1M
experimental_conditions:
  environmental_conditions:
    temperature_celsius: 30
    cultivation_method: batch_culture
    media:
      name: minimal
      carbon_source:
        - compound: D-glucose
          concentration_percent: 2
      nitrogen_source:
        - compound: ammonium_sulfate
          # 5 g/L
          concentration_percent: 0.5
configs:
  - config_name: perturbation_data
    description: TF perturbation expression data
    default: true
    dataset_type: annotated_features
    metadata_fields: ["sample_id", "regulator_locus_tag", "regulator_symbol", "time", "treatment"]
    data_files:
      - split: train
        path: perturbation.parquet
    dataset_info:
      features:
        - name: sample_id
          dtype: integer
          description: Unique identifier for each sample
        - name: regulator_locus_tag
          dtype: string
          description: Systematic gene identifier of the perturbed transcription factor
          role: regulator_identifier
        - name: regulator_symbol
          dtype: string
          description: Standard gene symbol of the perturbed transcription factor
          role: regulator_identifier
        - name: target_locus_tag
          dtype: string
          description: Systematic gene identifier of the target gene
          role: target_identifier
        - name: target_symbol
          dtype: string
          description: Standard gene symbol of the target gene
          role: target_identifier
        - name: time
          dtype: float
          description: Time point in minutes after perturbation
          role: experimental_condition
        - name: treatment
          dtype:
            class_label:
              names: ["control", "nitrogen_depletion", "phosphate_depletion"]
          description: Nutrient limitation treatment applied
          role: experimental_condition
          definitions:
            control:
              description: Standard minimal media with normal nutrient levels
              environmental_conditions:
                media:
                  nitrogen_source:
                    - compound: ammonium_sulfate
                      # 5 g/L
                      concentration_percent: 0.5
                  phosphate_source:
                    - compound: potassium_phosphate_monobasic
                      # 1 g/L
                      concentration_percent: 0.1
            nitrogen_depletion:
              description: Nitrogen-limited minimal media
              environmental_conditions:
                media:
                  nitrogen_source:
                    - compound: ammonium_sulfate
                      # 0.04 g/L
                      concentration_percent: 0.004
                  phosphate_source:
                    - compound: potassium_phosphate_monobasic
                      # 1 g/L
                      concentration_percent: 0.1
            phosphate_depletion:
              description: Phosphate-limited minimal media
              environmental_conditions:
                media:
                  nitrogen_source:
                    - compound: ammonium_sulfate
                      # 5 g/L
                      concentration_percent: 0.5
                  phosphate_source:
                    - compound: potassium_phosphate_monobasic
                      # 0.02 g/L
                      concentration_percent: 0.002
        - name: log2_fold_change
          dtype: float64
          description: Log2 fold change relative to unperturbed control
          role: quantitative_measure
        - name: pvalue
          dtype: float64
          description: Statistical significance of differential expression
          role: quantitative_measure
---
"""


EXAMPLE_2_COMPLEX_FIELD_DEFINITIONS = """---
license: mit
language:
  - en
tags:
  - genomics
  - yeast
  - binding
  - chip-seq
pretty_name: "Example Dataset 2 - Multi-Condition ChIP"
size_categories:
  - 1M<n<10M
strain_information:
  background: S288C
  base_strain: BY4741
configs:
  - config_name: chip_binding
    description: ChIP-seq binding data across environmental conditions
    dataset_type: annotated_features
    default: true
    metadata_fields: ["sample_id", "regulator_locus_tag", "regulator_symbol", "condition"]
    data_files:
      - split: train
        path: chip_data.parquet
    dataset_info:
      features:
        - name: sample_id
          dtype: integer
          description: Unique identifier for each sample
        - name: regulator_locus_tag
          dtype: string
          description: >-
            Systematic gene identifier of the ChIP-targeted transcription factor
          role: regulator_identifier
        - name: regulator_symbol
          dtype: string
          description: Standard gene symbol of the ChIP-targeted transcription factor
          role: regulator_identifier
        - name: target_locus_tag
          dtype: string
          description: Systematic gene identifier of the target gene
          role: target_identifier
        - name: target_symbol
          dtype: string
          description: Standard gene symbol of the target gene
          role: target_identifier
        - name: condition
          dtype:
            class_label:
              names: ["YPD", "galactose", "heat_shock", "oxidative_stress",
                      "amino_acid_starvation"]
          description: Environmental or stress condition of the experiment
          role: experimental_condition
          definitions:
            YPD:
              description: Rich media baseline condition
              environmental_conditions:
                temperature_celsius: 30
                cultivation_method: liquid_culture
                growth_phase_at_harvest:
                  od600: 0.6
                  stage: mid_log_phase
                media:
                  name: YPD
                  carbon_source:
                    - compound: D-glucose
                      concentration_percent: 2
                  nitrogen_source:
                    - compound: yeast_extract
                      concentration_percent: 1
                    - compound: peptone
                      concentration_percent: 2
            galactose:
              description: Alternative carbon source condition
              environmental_conditions:
                temperature_celsius: 30
                cultivation_method: liquid_culture
                growth_phase_at_harvest:
                  od600: 0.6
                  stage: mid_log_phase
                media:
                  name: YPD
                  carbon_source:
                    - compound: D-galactose
                      concentration_percent: 2
                  nitrogen_source:
                    - compound: yeast_extract
                      concentration_percent: 1
                    - compound: peptone
                      concentration_percent: 2
            heat_shock:
              description: Temperature stress condition
              environmental_conditions:
                temperature_celsius: 37
                cultivation_method: liquid_culture
                growth_phase_at_harvest:
                  od600: 0.6
                  stage: mid_log_phase
                media:
                  name: YPD
                  carbon_source:
                    - compound: D-glucose
                      concentration_percent: 2
                  nitrogen_source:
                    - compound: yeast_extract
                      concentration_percent: 1
                    - compound: peptone
                      concentration_percent: 2
                heat_treatment:
                  duration_minutes: 15
            oxidative_stress:
              description: Hydrogen peroxide stress condition
              environmental_conditions:
                temperature_celsius: 30
                cultivation_method: liquid_culture
                growth_phase_at_harvest:
                  od600: 0.6
                  stage: mid_log_phase
                media:
                  name: YPD
                  carbon_source:
                    - compound: D-glucose
                      concentration_percent: 2
                  nitrogen_source:
                    - compound: yeast_extract
                      concentration_percent: 1
                    - compound: peptone
                      concentration_percent: 2
                chemical_treatment:
                  compound: hydrogen_peroxide
                  concentration_percent: 0.004
                  duration_minutes: 20
            amino_acid_starvation:
              description: Amino acid starvation via chemical inhibition
              environmental_conditions:
                temperature_celsius: 30
                cultivation_method: liquid_culture
                growth_phase_at_harvest:
                  od600: 0.5
                  stage: mid_log_phase
                media:
                  name: synthetic_complete
                  carbon_source:
                    - compound: D-glucose
                      concentration_percent: 2
                  nitrogen_source:
                    - compound: yeast_nitrogen_base
                      # 6.71 g/L
                      concentration_percent: 0.671
                      specifications:
                        - without_amino_acids
                        - without_ammonium_sulfate
                    - compound: ammonium_sulfate
                      # 5 g/L
                      concentration_percent: 0.5
                    - compound: amino_acid_dropout_mix
                      # 2 g/L
                      concentration_percent: 0.2
                chemical_treatment:
                  compound: 3-amino-1,2,4-triazole
                  concentration_percent: 0.01
                  duration_hours: 1
        - name: binding_score
          dtype: float64
          description: ChIP-seq binding enrichment score
          role: quantitative_measure
        - name: peak_pvalue
          dtype: float64
          description: Statistical significance of binding peak
          role: quantitative_measure
        - name: peak_qvalue
          dtype: float64
          description: FDR-adjusted p-value for binding peak
          role: quantitative_measure
---
"""


EXAMPLE_3_PARTITIONED_WITH_METADATA = """---
license: mit
language:
  - en
tags:
  - genomics
  - yeast
  - binding
  - genome-wide
  - chec-seq
pretty_name: "Example Dataset 3 - Genome Coverage Compendium"
size_categories:
  - 10M<n<100M
experimental_conditions:
  environmental_conditions:
    temperature_celsius: 30
    cultivation_method: liquid_culture
    growth_phase_at_harvest:
      od600: 0.8
      stage: late_log_phase
    media:
      name: synthetic_complete
      carbon_source:
        - compound: D-glucose
          concentration_percent: 2
      nitrogen_source:
        - compound: yeast_nitrogen_base
          # 6.71 g/L
          concentration_percent: 0.671
          specifications:
            - without_amino_acids
            - without_ammonium_sulfate
        - compound: ammonium_sulfate
          # 5 g/L
          concentration_percent: 0.5
        - compound: amino_acid_dropout_mix
          # 2 g/L
          concentration_percent: 0.2
configs:
  - config_name: genome_coverage
    description: Genome-wide binding coverage at base-pair resolution
    dataset_type: genome_map
    default: true
    data_files:
      - split: train
        path: genome_map/batch=*/regulator=*/*.parquet
    dataset_info:
      features:
        - name: sample_id
          dtype: integer
          description: Unique identifier for each sample
        - name: chr
          dtype: string
          description: Chromosome identifier (chrI, chrII, etc.)
          role: genomic_coordinate
        - name: pos
          dtype: int32
          description: Genomic position (0-based)
          role: genomic_coordinate
        - name: coverage
          dtype: float32
          description: Normalized coverage value at this position
          role: quantitative_measure
      partitioning:
        enabled: true
        partition_by: ["batch", "regulator"]
        path_template: "genome_map/batch={batch}/regulator={regulator}/*.parquet"

  - config_name: standard_batch_metadata
    description: Metadata for standard ChEC-seq experiments
    dataset_type: metadata
    applies_to: ["genome_coverage"]
    data_files:
      - split: train
        path: standard_metadata.parquet
    dataset_info:
      features:
        - name: sample_id
          dtype: integer
          description: Unique identifier for each sample
        - name: batch
          dtype: string
          description: Experimental batch identifier
        - name: regulator
          dtype: string
          description: Transcription factor systematic identifier
        - name: regulator_locus_tag
          dtype: string
          description: Systematic gene identifier of the transcription factor
          role: regulator_identifier
        - name: regulator_symbol
          dtype: string
          description: Standard gene symbol of the transcription factor
          role: regulator_identifier
        - name: accession
          dtype: string
          description: SRA accession number for the sequencing data
        - name: replicate
          dtype: int32
          description: Biological replicate number
        - name: sequencing_depth
          dtype: int64
          description: Total number of sequencing reads

  - config_name: variant_batch_metadata
    description: Metadata for TF variant experiments with altered conditions
    dataset_type: metadata
    applies_to: ["genome_coverage"]
    experimental_conditions:
      environmental_conditions:
        temperature_celsius: 25
        cultivation_method: liquid_culture
        growth_phase_at_harvest:
          od600: 0.6
          stage: mid_log_phase
        media:
          name: synthetic_complete
          carbon_source:
            - compound: D-raffinose
              concentration_percent: 2
          nitrogen_source:
            - compound: yeast_nitrogen_base
              # 6.71 g/L
              concentration_percent: 0.671
              specifications:
                - without_amino_acids
                - without_ammonium_sulfate
            - compound: ammonium_sulfate
              # 5 g/L
              concentration_percent: 0.5
            - compound: amino_acid_dropout_mix
              # 2 g/L
              concentration_percent: 0.2
              specifications:
                - minus_uracil
    data_files:
      - split: train
        path: variant_metadata.parquet
    dataset_info:
      features:
        - name: sample_id
          dtype: integer
          description: Unique identifier for each sample
        - name: batch
          dtype: string
          description: Experimental batch identifier (prefixed with 'VAR')
        - name: regulator
          dtype: string
          description: Transcription factor systematic identifier
        - name: regulator_locus_tag
          dtype: string
          description: Systematic gene identifier of the transcription factor
          role: regulator_identifier
        - name: regulator_symbol
          dtype: string
          description: Standard gene symbol of the transcription factor
          role: regulator_identifier
        - name: variant_type
          dtype: string
          description: Type of transcription factor variant (DBD_swap, truncation, etc.)
        - name: accession
          dtype: string
          description: SRA accession number for the sequencing data
        - name: replicate
          dtype: int32
          description: Biological replicate number

  - config_name: qc_metrics
    description: Quality control metrics for all genome coverage samples
    dataset_type: comparative
    applies_to: ["genome_coverage"]
    data_files:
      - split: train
        path: comparative_data.parquet
    dataset_info:
      features:
        - name: sample_id
          dtype: integer
          description: Unique identifier for each sample
        - name: batch
          dtype: string
          description: Experimental batch identifier
        - name: regulator
          dtype: string
          description: Transcription factor systematic identifier
        - name: accession
          dtype: string
          description: SRA accession number for the sequencing data
        - name: total_reads
          dtype: int64
          description: Total number of sequencing reads
        - name: mapped_reads
          dtype: int64
          description: Number of reads successfully mapped to genome
        - name: mapping_rate
          dtype: float64
          description: Percentage of reads successfully mapped
        - name: peak_count
          dtype: int32
          description: Number of binding peaks identified
        - name: signal_to_noise
          dtype: float64
          description: Ratio of signal in peaks to background
        - name: qc_pass
          dtype: bool
          description: Whether sample passes quality control thresholds
---
"""
