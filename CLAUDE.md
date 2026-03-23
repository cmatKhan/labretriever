# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**labretriever** is a Python package for interfacing with genomic and transcriptomic datasets hosted on HuggingFace Hub. It provides SQL-based querying, caching, and metadata management for heterogeneous datasets with varying experimental condition structures.

## Common Commands

```bash
# Install dependencies
poetry install
poetry run pre-commit install

# Run tests
poetry run pytest                                          # all tests
poetry run pytest labretriever/tests/test_<module>.py     # single test file
poetry run pytest --cov=labretriever                      # with coverage

# Code quality
poetry run pre-commit run --all-files   # runs black, isort, mypy, flake8, etc.

# Documentation
poetry run mkdocs serve     # live-reloading dev server
poetry run mkdocs build
poetry run mkdocs gh-deploy
```

## Architecture

The package has four main layers:

### 1. `VirtualDB` (`virtual_db.py`)
Primary user-facing API. Provides a unified SQL query interface (via DuckDB) across multiple heterogeneous HuggingFace datasets. Handles composite ID parsing for "comparative" datasets. Supports parameterized queries and metadata/full-data views.

### 2. `DataCard` (`datacard.py`)
Parses HuggingFace dataset card YAML into Pydantic models. Navigates experimental conditions at three levels (top/config/field). Provides schema discovery and metadata relationship extraction. Extended by `HfCacheManager`.

### 3. `HfCacheManager` (`hf_cache_manager.py`)
Extends `DataCard` with a 3-case retrieval strategy: check DuckDB → check local HF cache → download from HF Hub. Manages cache cleanup by age/size thresholds.

### 4. `Models` (`models.py`)
Pydantic v2 models for all data structures. Uses `extra="allow"` throughout for flexible extension. Key types: `DatasetType` (enum), `DatasetConfig`, `DatasetCard`, `MetadataConfig`, `RepositoryConfig`.

### Supporting modules
- `fetchers.py`: Low-level HF Hub HTTP fetchers (`HfDataCardFetcher`, `HfSizeInfoFetcher`, `HfRepoStructureFetcher`)
- `errors.py`: `HfDataFetchError`, `DataCardError`, `DataCardValidationError`
- `constants.py`: `CACHE_DIR` (from `HF_CACHE_DIR` env var) and `get_hf_token()` (from `HF_TOKEN`)

## Code Standards

- Python 3.11+, full type annotations, mypy enforced
- Black 88-char line length, isort with Black-compatible profile
- Docstrings in Sphinx style (enforced by docformatter)
- Pre-commit hooks enforce all of the above on every commit

## Environment Variables

- `HF_TOKEN`: HuggingFace authentication token
- `HF_CACHE_DIR`: Custom cache directory (defaults to `HF_HUB_CACHE`)
