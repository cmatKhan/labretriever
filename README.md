# labretriever

A Python package for querying and managing genomic and transcriptomic datasets hosted on [HuggingFace Hub](https://huggingface.co). It provides a unified SQL interface (via DuckDB) across heterogeneous datasets, with local caching and structured metadata exploration.

See the [documentation](https://cmatKhan.github.io/labretriever) for full usage guides and API reference. The [BrentLab yeast resources collection](https://huggingface.co/collections/BrentLab/yeastresources) is an example of datasets designed to work with this package.

## Installation

```bash
pip install labretriever
```

Set your HuggingFace token if accessing private datasets:

```bash
export HF_TOKEN=your_token_here
```

## Usage

```python
from labretriever import VirtualDB

vdb = VirtualDB("config.yaml")

# Discover available views
vdb.tables()
vdb.describe("harbison")

# Query with SQL
df = vdb.query("SELECT * FROM harbison_meta WHERE carbon_source = $cs", cs="glucose")
```

`VirtualDB` loads datasets from HuggingFace (caching locally), constructs DuckDB views over Parquet files, and exposes metadata and full-data views for SQL querying. See the docs for how to write a `config.yaml` and structure your HuggingFace dataset cards.

## Development

```bash
git clone https://github.com/cmatKhan/labretriever
cd labretriever
poetry install
poetry run pre-commit install
poetry run pytest
```
