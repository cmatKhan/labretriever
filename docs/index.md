# labretriever Documentation

## Development Commands

### Testing
- Run tests: `poetry run pytest`
- Run specific test: `poetry run pytest labretriever/tests/test_[module_name].py`
- Run tests with coverage: `poetry run pytest --cov=labretriever`

### Linting and Formatting
- Run all pre-commit checks: `poetry run pre-commit run --all-files`
- Format code with Black: `poetry run black labretriever/`
- Sort imports with isort: `poetry run isort labretriever/`
- Type check with mypy: `poetry run mypy labretriever/`
- Lint with flake8: `poetry run flake8 labretriever/`

### Installation
- Install dependencies: `poetry install`
- Install pre-commit hooks: `poetry run pre-commit install`

## Architecture

This is a Python package for interfacing with a collection of datasets hosted on Hugging Face. The modern architecture provides efficient querying, caching, and metadata management for genomic and transcriptomic datasets.

### Core Components

- **VirtualDB** (`labretriever/virtual_db.py`): Primary API for unified cross-dataset queries. Provides standardized query interface across heterogeneous datasets with varying experimental condition structures through external YAML configuration.

- **DataCard** (`labretriever/datacard.py`): Interface for exploring HuggingFace dataset metadata without loading actual data. Enables dataset structure discovery, experimental condition exploration, and query planning.

- **HfCacheManager** (`labretriever/hf_cache_manager.py`): Manages HuggingFace cache with intelligent downloading, DuckDB-based SQL querying, and automatic cleanup based on age/size thresholds.

### Supporting Components

- **Models** (`labretriever/models.py`): Pydantic models for dataset cards, configurations, features, and VirtualDB configuration (MetadataConfig, PropertyMapping, RepositoryConfig).

- **Fetchers** (`labretriever/fetchers.py`): Low-level components for retrieving data from HuggingFace Hub (HfDataCardFetcher, HfRepoStructureFetcher, HfSizeInfoFetcher).

### Data Types

The datasets in this collection store the following types of genomic data:

- **genomic_features**: Labels and information about genomic features (e.g., parsed GTF/GFF files)
- **annotated_features**: Data quantified to features, typically genes
- **genome_map**: Data mapped to genome coordinates
- **metadata**: Additional sample information (cell types, experimental conditions, etc.)

Data is stored in Apache Parquet format, either as single files or parquet datasets (directories of parquet files).

### Error Handling

- **errors.py** (`labretriever/errors.py`): Custom exception classes for dataset management including `HfDataFetchError`, `DataCardError`, and `DataCardValidationError`.

## Configuration

- Uses Poetry for dependency management
- Python 3.11+ required
- Black formatter with 88-character line length
- Pre-commit hooks include Black, isort, flake8, mypy, and various file checks
- pytest with comprehensive testing support
- Environment variables: `HF_TOKEN`, `HF_CACHE_DIR`

## Testing Patterns

- Tests use pytest with modern testing practices
- Integration tests for HuggingFace dataset functionality
- Test fixtures for dataset operations
- Comprehensive error handling testing

### mkdocs

#### Commands

After building the environment with poetry, you can use `poetry run` or a poetry shell
to execute the following:

* `mkdocs new [dir-name]` - Create a new project.
* `mkdocs serve` - Start the live-reloading docs server.
* `mkdocs build` - Build the documentation site.
* `mkdocs -h` - Print help message and exit.

#### Project layout

    mkdocs.yml    # The configuration file.
    docs/
        index.md  # The documentation homepage.
        ...       # Other markdown pages, images and other files.

To update the gh-pages documentation, use `poetry run mkdocs gh-deply`

