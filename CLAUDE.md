# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Status and Compatibility

This project is under active development. Breaking changes may be introduced without deprecation warnings or backward compatibility support. All configuration formats, APIs, and data structures are subject to change.

## Commands

### Development Commands
- **Install dependencies**: `poetry install`
- **Run tests**: `poetry run pytest`
- **Run specific test**: `poetry run pytest labretriever/tests/test_<module_name>.py`
- **Run tests with coverage**: `poetry run pytest --cov=labretriever`
- **Type checking**: `poetry run mypy labretriever/`
- **Code formatting**: `poetry run black labretriever/`
- **Import sorting**: `poetry run isort labretriever/`
- **Linting**: `poetry run flake8 labretriever/`
- **Pre-commit hooks**: `pre-commit run --all-files`

### Documentation

The docstrings are written in the sphinx style and the documentation is built using MkDocs.

- **Build docs**: `poetry run mkdocs serve` (serves locally)
- **Build docs for production**: `poetry run mkdocs build`

### Environment
- Python 3.11+ required
- Uses Poetry for dependency management
- Pre-commit hooks configured for code quality

## Architecture

### Core API Framework
The repository implements a hierarchical API framework for interacting with django.tfbindingandmodeling.com:

The repository includes a modern Hugging Face integration system:

- **HfQueryAPI**: Main interface for querying HF datasets with intelligent downloading and SQL querying
- **HfCacheManager**: Manages HF cache with cleanup and size management features
- **HfRankResponse**: Response handling for HF-based ranking operations
- **datainfo/ package**: Dataset information management with models for DatasetSize, DataFileInfo, FeatureInfo, etc.

The purpose of these components is to interface with a collection of datasets hosted on Hugging Face, providing efficient querying, caching, and metadata management.
The datasets in this collection may store the following types of data:

- **genomic_features**: These data store labels and additional information about genomic features, eg a parsed GTF/GFF file possibly with additional columns.
- **annotated_features**: These data are quantified to a feature, most typically a gene.
- **genome_map**: These data are mapped to a genome coordinate.
- **metadata**: These data provide additional information about samples, eg cell type annotations, experimental conditions, etc and may be associated
with any of the other data types above.

data are stored in Apache Parquet format. However, data may be stored as a
single parquet file, or as a directory of parquet files (a parquet dataset). A single
dataset repository might contain all three types of data, or just one or two. The classes
and functions in this repository are designed to handle all of these cases. The structured
metadata provided in the dataset card of the huggingface repository is used to determine
the type of data stored in the repository, the structure of the parquet files, and the appropriate
way to query the data.

### Datainfo Package - Experimental Conditions

The `datainfo/` package includes comprehensive support for experimental conditions with a three-level hierarchy:

1. **Top-level conditions**: Common to all configs (e.g., strain background, standard temperature)
2. **Config-level conditions**: Specific to a dataset config (e.g., growth phase, base media)
3. **Field-level conditions**: Vary by sample, defined in field `definitions` (e.g., treatments, stress conditions)

#### Experimental Condition Models

The following Pydantic models in `datainfo/models.py` handle experimental conditions:

- **FieldRole**: Enum for semantic field roles (regulator_identifier, target_identifier, quantitative_measure, experimental_condition, genomic_coordinate)
- **CompoundInfo**: Chemical compounds with concentration (percent, g/L, or molar) and specifications
- **MediaInfo**: Growth media with carbon_source, nitrogen_source, phosphate_source, and additives
- **MediaAdditiveInfo**: Media additives (e.g., butanol for filamentation)
- **GrowthPhaseInfo**: Growth phase with stage/phase aliases, od600, and od600_tolerance
- **ChemicalTreatmentInfo**: Chemical treatments with concentration, duration, target_pH
- **DrugTreatmentInfo**: Drug treatments (alias for chemical treatment)
- **HeatTreatmentInfo**: Heat treatments with temperature and duration
- **TemperatureShiftInfo**: Temperature shifts for heat shock experiments
- **InductionInfo**: Induction systems (e.g., GAL, estradiol-inducible)
- **EnvironmentalConditions**: Container for all environmental parameters
- **ExperimentalConditions**: Top-level container with environmental_conditions and strain_background

#### Validation Features

Models include validators that:
- Enforce FieldRole enum values (rejects invalid roles like "identifier")
- Validate growth stage values (warns about unrecognized stages like "early_mid_log")
- Detect improper use of "unspecified" as a string value (warns to use null or empty list)
- Flag non-standard fields in EnvironmentalConditions and ExperimentalConditions (warns about extra fields)
- Support both string and dict formats for strain_background
- Check consistency between `stage` and `phase` aliases in GrowthPhaseInfo

#### Working with Experimental Conditions

See `docs/tutorials/experimental_conditions_tutorial.ipynb` for comprehensive examples of:
- Loading and parsing datacards with experimental conditions
- Accessing conditions at different hierarchy levels
- Merging conditions from multiple levels
- Specifying complete media compositions
- Documenting treatments and stress conditions
- Best practices for condition specification

Example usage:
```python
from labretriever.datainfo.datacard import DataCard

# Load a datacard
card = DataCard(repo_id="BrentLab/harbison_2004")

# Access top-level conditions
exp_conds = card.card.experimental_conditions
if exp_conds and exp_conds.environmental_conditions:
    print(f"Temperature: {exp_conds.environmental_conditions.temperature_celsius}°C")

# Access config-level conditions
config = card.get_config_by_name("harbison_2004")
if config.experimental_conditions:
    print("Config has specific conditions")

# Access field-level definitions
condition_feature = next(
    f for f in config.dataset_info.features
    if f.name == "condition"
)
if condition_feature.definitions:
    print(f"Defined conditions: {list(condition_feature.definitions.keys())}")
```

### Key Utilities
- **errors.py**: Custom exception classes
- **datainfo/models.py**: Pydantic models for datacards with experimental condition support
- **datainfo/datacard.py**: DataCard class for loading and validating HuggingFace datacards

### Testing Strategy
- Test files follow `test_*.py` pattern
- Integration tests available for cache functionality
- `test_real_datacards.py`: Validates all 8 real datacards from the HuggingFace collection
- `test_models.py`: Unit tests for all Pydantic models including experimental conditions

### Code Style

Lines should not exceed 88 characters. The codebase uses:

- Black formatter (line length 88)
- isort for import sorting
- mypy for type checking
- flake8 for linting
- Pre-commit hooks enforce all style checks
- **Sphinx-style docstrings** for all functions, classes, and modules

**IMPORTANT**: Do NOT use emojis, decorative Unicode symbols (e.g., degree symbols, arrows, bullets), or special characters (like →, ←, ↓, ↑, •, ★, ✓, ✗, etc.) in any code, documentation, comments, docstrings, or output. Use plain ASCII text only. For arrows, use ASCII equivalents like "->", "<-", or descriptive text.