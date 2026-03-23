"""
Test real datacards from the HuggingFace collection.

This test suite validates that all real datacards from the BrentLab collection parse
correctly with the updated models.py and specification.

"""

import pytest
import yaml  # type: ignore

from labretriever.models import DatasetCard

# Real datacard YAML strings from the collection
BARKAI_COMPENDIUM = """
license: mit
language:
- en
tags:
- transcription-factor
- binding
- chec-seq
- genomics
- biology
pretty_name: Barkai ChEC-seq Compendium
size_categories:
  - 100M<n<1B
experimental_conditions:
  temperature_celsius: 30
  cultivation_method: liquid_culture
  growth_phase_at_harvest:
    od600: 4.0
    stage: overnight_stationary_phase
  media:
    name: synthetic_complete_dextrose
    carbon_source:
      - compound: D-dextrose
        concentration_percent: 2.0
    nitrogen_source: []
  strain_background: BY4741
configs:
- config_name: genomic_coverage
  description: Genomic coverage data with pileup counts at specific positions
  dataset_type: genome_map
  default: true
  data_files:
  - split: train
    path: genome_map/*/*/part-0.parquet
  dataset_info:
    features:
    - name: seqnames
      dtype: string
      description: Chromosome or sequence name (e.g., chrI, chrII, etc.)
    - name: start
      dtype: int32
      description: Start position of the genomic interval (1-based coordinates)
    partitioning:
      enabled: true
      partition_by: ["Series", "Accession"]
"""

CALLINGCARDS = """
license: mit
language:
- en
tags:
- biology
- genomics
- yeast
- transcription-factors
- callingcards
pretty_name: "Calling Cards Transcription Factor Binding Dataset"
experimental_conditions:
  environmental_conditions:
    temperature_celsius: 30
    cultivation_method: liquid_culture
    media:
      name: synthetic_complete_minus_ura_his_leu
      carbon_source:
        - compound: D-galactose
          concentration_percent: 2
      nitrogen_source:
        - compound: amino_acid_dropout_mix
          specifications:
            - minus_ura
            - minus_his
            - minus_leu
configs:
- config_name: annotated_features
  description: Calling Cards transcription factor binding data
  dataset_type: annotated_features
  default: true
  data_files:
  - split: train
    path: annotated_features/*/*.parquet
  dataset_info:
    features:
    - name: id
      dtype: string
      description: Unique identifier for each binding measurement
    - name: regulator_locus_tag
      dtype: string
      description: Systematic gene name (ORF identifier) of the transcription factor
      role: regulator_identifier
"""

HARBISON_2004 = """
license: mit
language:
  - en
tags:
  - genomics
  - yeast
  - transcription
  - binding
pretty_name: "Harbison, 2004 ChIP-chip"
configs:
- config_name: harbison_2004
  description: ChIP-chip transcription factor binding data with environmental conditions
  dataset_type: annotated_features
  default: true
  data_files:
  - split: train
    path: harbison_2004.parquet
  dataset_info:
    features:
    - name: condition
      dtype:
        class_label:
          names: ["YPD", "SM", "RAPA", "H2O2Hi", "H2O2Lo",
                  "Acid", "Alpha", "BUT14", "BUT90", "Thi-",
                  "GAL", "HEAT", "Pi-", "RAFF"]
      description: Environmental condition of the experiment
      role: experimental_condition
      definitions:
        YPD:
          description: Rich media baseline condition
          environmental_conditions:
            temperature_celsius: 30
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
        Acid:
          description: Acidic pH stress condition
          environmental_conditions:
            temperature_celsius: 30
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
              compound: succinic_acid
              concentration_percent: 0.59
              target_pH: 4.0
              duration_minutes: 30
        BUT14:
          description: Long-term filamentation induction with butanol
          environmental_conditions:
            temperature_celsius: 30
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
              additives:
                - compound: butanol
                  concentration_percent: 1
            incubation_duration_hours: 14
    - name: regulator_locus_tag
      dtype: string
      description: Systematic gene name of the transcription factor
      role: regulator_identifier
    - name: target_locus_tag
      dtype: string
      description: Systematic gene name of the target gene
      role: target_identifier
    - name: effect
      dtype: float64
      description: The chip channel ratio
      role: quantitative_measure
"""

HU_2007 = """
license: mit
language:
  - en
tags:
  - genomics
  - yeast
  - transcription
  - perturbation
  - knockout
  - TFKO
pretty_name: Hu 2007/Reimand 2010 TFKO
experimental_conditions:
  environmental_conditions:
    temperature_celsius: 30
    cultivation_method: batch
    growth_phase_at_harvest:
      phase: mid_log
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
  strain_background: BY4741
configs:
  - config_name: data
    description: Regulator knockout expression data
    dataset_type: annotated_features
    default: true
    data_files:
    - split: train
      path: hu_2007_reimand_2010.parquet
    dataset_info:
      features:
        - name: regulator_locus_tag
          dtype: string
          description: Systematic ID of the knocked out regulator
          role: regulator_identifier
        - name: target_locus_tag
          dtype: string
          description: Systematic ID of the target gene
          role: target_identifier
        - name: effect
          dtype: float
          description: log fold change of mutant vs wt
          role: quantitative_measure
        - name: heat_shock
          dtype: bool
          description: Whether heat shock was applied
          role: experimental_condition
          definitions:
            "true":
              environmental_conditions:
                temperature_celsius: 39
                duration_minutes: 15
              strain_background: BY4741
"""

HUGHES_2006 = """
license: mit
language:
- en
tags:
- biology
- genomics
- yeast
- transcription-factors
pretty_name: "Hughes 2006 Yeast Transcription Factor Perturbation Dataset"
configs:
- config_name: overexpression
  description: Overexpression perturbation normalized log2 fold changes
  dataset_type: annotated_features
  default: true
  data_files:
  - split: train
    path: overexpression.parquet
  dataset_info:
    features:
    - name: regulator_locus_tag
      dtype: string
      description: Systematic gene name of the perturbed transcription factor
      role: regulator_identifier
    - name: target_locus_tag
      dtype: string
      description: Systematic gene name of the target gene
      role: target_identifier
    - name: mean_norm_log2fc
      dtype: float64
      description: Average log2 fold change across dye orientations
      role: quantitative_measure
  experimental_conditions:
    media:
      name: selective_medium
      carbon_source:
        - compound: D-raffinose
          concentration_percent: 2
      nitrogen_source: []
    induction:
      inducer:
        compound: D-galactose
        concentration_percent: 2
      duration_hours: 3
"""

KEMMEREN_2014 = """
license: mit
language:
- en
tags:
- genomics
- yeast
- transcription
pretty_name: "Kemmeren, 2014 Overexpression"
experimental_conditions:
  temperature_celsius: 30
  cultivation_method: plate
  growth_phase_at_harvest:
    phase: early_mid_log
    od600: 0.6
    od600_tolerance: 0.1
  media:
    name: synthetic_complete
    carbon_source:
      - compound: D-glucose
        concentration_percent: 2
    nitrogen_source:
      - compound: yeast_nitrogen_base
        concentration_percent: 0.671
        specifications:
          - without_amino_acids
configs:
- config_name: kemmeren_2014
  description: Transcriptional regulator overexpression perturbation data
  dataset_type: annotated_features
  default: true
  data_files:
  - split: train
    path: kemmeren_2014.parquet
  dataset_info:
    features:
    - name: regulator_locus_tag
      dtype: string
      description: induced transcriptional regulator systematic ID
      role: regulator_identifier
    - name: target_locus_tag
      dtype: string
      description: The systematic ID of the feature
      role: target_identifier
    - name: M
      dtype: float64
      description: log₂ fold change (mutant vs wildtype)
      role: quantitative_measure
"""

MAHENDRAWADA_2025 = """
license: mit
language:
- en
tags:
- biology
- genomics
- yeast
pretty_name: "Mahendrawada 2025 ChEC-seq and Nascent RNA-seq data"
configs:
- config_name: mahendrawada_chec_seq
  description: ChEC-seq transcription factor binding data
  default: true
  dataset_type: annotated_features
  data_files:
  - split: train
    path: chec_mahendrawada_2025.parquet
  dataset_info:
    features:
    - name: regulator_locus_tag
      dtype: string
      description: Systematic gene name of the transcription factor
      role: regulator_identifier
    - name: target_locus_tag
      dtype: string
      description: Systematic gene name of the target gene
      role: target_identifier
    - name: peak_score
      dtype: float64
      description: ChEC signal around peak center
      role: quantitative_measure
  experimental_conditions:
    environmental_conditions:
      temperature_celsius: 30
      growth_phase_at_harvest:
        od600: 1.0
      media:
        name: synthetic_complete
        carbon_source: []
        nitrogen_source:
          - compound: yeast_nitrogen_base
            concentration_percent: 0.17
            specifications:
              - without_ammonium_sulfate
              - without_amino_acids
"""

ROSSI_2021 = """
license: mit
tags:
- transcription-factor
- binding
- chipexo
- genomics
language:
- en
pretty_name: Rossi ChIP-exo 2021
experimental_conditions:
  environmental_conditions:
    temperature_celsius: 25
    growth_phase_at_harvest:
      phase: mid_log
      od600: 0.8
    media:
      name: yeast_peptone_dextrose
      carbon_source:
        - compound: D-glucose
      nitrogen_source:
        - compound: yeast_extract
        - compound: peptone
  strain_background: W303
configs:
- config_name: rossi_annotated_features
  description: ChIP-exo regulator-target binding features
  dataset_type: annotated_features
  default: true
  data_files:
    - split: train
      path: yeastepigenome_annotatedfeatures.parquet
  dataset_info:
    features:
      - name: regulator_locus_tag
        dtype: string
        description: Systematic ORF name of the regulator
        role: regulator_identifier
      - name: target_locus_tag
        dtype: string
        description: The systematic ID of the feature
        role: target_identifier
      - name: max_fc
        dtype: float64
        description: Maximum fold change
        role: quantitative_measure
"""


@pytest.mark.parametrize(
    "datacard_yaml,dataset_name",
    [
        (BARKAI_COMPENDIUM, "barkai_compendium"),
        (CALLINGCARDS, "callingcards"),
        (HARBISON_2004, "harbison_2004"),
        (HU_2007, "hu_2007_reimand_2010"),
        (HUGHES_2006, "hughes_2006"),
        (KEMMEREN_2014, "kemmeren_2014"),
        (MAHENDRAWADA_2025, "mahendrawada_2025"),
        (ROSSI_2021, "rossi_2021"),
    ],
)
def test_real_datacard_parsing(datacard_yaml, dataset_name):
    """Test that real datacards parse correctly without ValidationError."""
    data = yaml.safe_load(datacard_yaml)

    # Should not raise validation error
    card = DatasetCard(**data)

    # Verify basic structure
    assert card.configs is not None
    assert len(card.configs) > 0

    # Verify config has required fields
    config = card.configs[0]
    assert config.config_name is not None
    assert config.dataset_type is not None
    assert config.dataset_info is not None
    assert config.dataset_info.features is not None
    assert len(config.dataset_info.features) > 0


def test_harbison_2004_condition_definitions():
    """Test that harbison_2004 field-level definitions parse correctly."""
    data = yaml.safe_load(HARBISON_2004)
    card = DatasetCard(**data)

    # Find the config
    config = card.configs[0]
    assert config.config_name == "harbison_2004"

    # Find condition feature
    condition_feature = next(
        f for f in config.dataset_info.features if f.name == "condition"
    )

    # Should have definitions
    assert condition_feature.definitions is not None
    assert "YPD" in condition_feature.definitions
    assert "Acid" in condition_feature.definitions
    assert "BUT14" in condition_feature.definitions

    # YPD definition should have environmental conditions
    ypd_def = condition_feature.definitions["YPD"]
    assert "environmental_conditions" in ypd_def

    # Acid definition should have target_pH in chemical_treatment
    acid_def = condition_feature.definitions["Acid"]
    assert "environmental_conditions" in acid_def
    assert "chemical_treatment" in acid_def["environmental_conditions"]
    assert "target_pH" in acid_def["environmental_conditions"]["chemical_treatment"]

    # BUT14 should have media additives
    but14_def = condition_feature.definitions["BUT14"]
    assert "environmental_conditions" in but14_def
    assert "media" in but14_def["environmental_conditions"]
    assert "additives" in but14_def["environmental_conditions"]["media"]


def test_hughes_2006_induction():
    """Test that hughes_2006 induction field parses correctly."""
    data = yaml.safe_load(HUGHES_2006)
    card = DatasetCard(**data)

    # Check experimental conditions (stored as dict in model_extra)
    assert card.configs[0].model_extra is not None
    assert "experimental_conditions" in card.configs[0].model_extra
    exp_conds = card.configs[0].model_extra["experimental_conditions"]

    # Check induction field
    assert "induction" in exp_conds
    induction = exp_conds["induction"]
    assert "inducer" in induction
    assert induction["inducer"]["compound"] == "D-galactose"
    assert induction["duration_hours"] == 3


def test_kemmeren_2014_growth_phase():
    """Test that kemmeren_2014 growth phase with od600_tolerance parses correctly."""
    data = yaml.safe_load(KEMMEREN_2014)
    card = DatasetCard(**data)

    # Check growth phase (stored as dict in model_extra)
    assert card.model_extra is not None
    assert "experimental_conditions" in card.model_extra
    exp_conds = card.model_extra["experimental_conditions"]

    assert "growth_phase_at_harvest" in exp_conds
    growth_phase = exp_conds["growth_phase_at_harvest"]
    assert growth_phase["phase"] == "early_mid_log"
    assert growth_phase["od600"] == 0.6
    assert growth_phase["od600_tolerance"] == 0.1


def test_hu_2007_strain_background_in_definitions():
    """Test that strain_background in field definitions parses correctly."""
    data = yaml.safe_load(HU_2007)
    card = DatasetCard(**data)

    # Find heat_shock feature
    config = card.configs[0]
    heat_shock_feature = next(
        f for f in config.dataset_info.features if f.name == "heat_shock"
    )

    # Check definitions
    assert heat_shock_feature.definitions is not None
    assert "true" in heat_shock_feature.definitions

    # Check strain_background in definition
    true_def = heat_shock_feature.definitions["true"]
    assert "strain_background" in true_def


def test_field_role_validation():
    """Test that role field accepts any string value."""
    # This should parse successfully with any role string
    data = yaml.safe_load(CALLINGCARDS)
    card = DatasetCard(**data)

    # Find a feature with a role
    config = card.configs[0]
    regulator_feature = next(
        f for f in config.dataset_info.features if f.name == "regulator_locus_tag"
    )

    # Verify role is a string (not an enum)
    assert regulator_feature.role == "regulator_identifier"
    assert isinstance(regulator_feature.role, str)


def test_concentration_fields():
    """Test that various concentration fields parse correctly."""
    data = yaml.safe_load(KEMMEREN_2014)
    card = DatasetCard(**data)

    # Check media compounds (stored as dict in model_extra)
    assert card.model_extra is not None
    assert "experimental_conditions" in card.model_extra
    exp_conds = card.model_extra["experimental_conditions"]
    assert "media" in exp_conds
    media = exp_conds["media"]

    # Check carbon source
    assert "carbon_source" in media
    carbon_sources = media["carbon_source"]
    assert len(carbon_sources) > 0
    carbon = carbon_sources[0]
    assert carbon["concentration_percent"] is not None

    # Check nitrogen source with specifications
    assert "nitrogen_source" in media
    nitrogen_sources = media["nitrogen_source"]
    assert len(nitrogen_sources) > 0
    nitrogen = nitrogen_sources[0]
    assert nitrogen["specifications"] is not None
    assert "without_amino_acids" in nitrogen["specifications"]


def test_extra_fields_do_not_raise_errors():
    """Test that extra fields are accepted (with warnings) but don't raise errors."""
    # All real datacards should parse without ValidationError
    # even if they have extra fields
    datacards = [
        BARKAI_COMPENDIUM,
        CALLINGCARDS,
        HARBISON_2004,
        HU_2007,
        HUGHES_2006,
        KEMMEREN_2014,
        MAHENDRAWADA_2025,
        ROSSI_2021,
    ]

    for datacard_yaml in datacards:
        data = yaml.safe_load(datacard_yaml)
        # This should not raise ValidationError
        card = DatasetCard(**data)
        assert card is not None


def test_empty_nitrogen_source_list():
    """Test that empty nitrogen_source lists are accepted."""
    data = yaml.safe_load(BARKAI_COMPENDIUM)
    card = DatasetCard(**data)

    # Check that nitrogen_source is an empty list (stored as dict in model_extra)
    assert card.model_extra is not None
    assert "experimental_conditions" in card.model_extra
    exp_conds = card.model_extra["experimental_conditions"]
    assert "media" in exp_conds
    media = exp_conds["media"]
    assert media["nitrogen_source"] == []


def test_media_additives():
    """Test that media additives parse correctly."""
    data = yaml.safe_load(HARBISON_2004)
    card = DatasetCard(**data)

    # Find BUT14 condition definition
    config = card.configs[0]
    condition_feature = next(
        f for f in config.dataset_info.features if f.name == "condition"
    )
    but14_def = condition_feature.definitions["BUT14"]  # type: ignore

    # Check additives
    env_conds_dict = but14_def["environmental_conditions"]
    media = env_conds_dict["media"]
    assert "additives" in media
    additives = media["additives"]
    assert len(additives) > 0
    assert additives[0]["compound"] == "butanol"
    assert additives[0]["concentration_percent"] == 1


def test_strain_background_formats():
    """Test that strain_background accepts both string and dict formats."""
    # String format
    data1 = yaml.safe_load(BARKAI_COMPENDIUM)
    card1 = DatasetCard(**data1)
    assert card1.model_extra is not None
    assert "experimental_conditions" in card1.model_extra
    exp_conds1 = card1.model_extra["experimental_conditions"]
    assert exp_conds1["strain_background"] == "BY4741"

    # String format in rossi
    data2 = yaml.safe_load(ROSSI_2021)
    card2 = DatasetCard(**data2)
    assert card2.model_extra is not None
    assert "experimental_conditions" in card2.model_extra
    exp_conds2 = card2.model_extra["experimental_conditions"]
    assert exp_conds2["strain_background"] == "W303"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
