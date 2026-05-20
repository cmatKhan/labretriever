---
name: repo
description: >
  This skill should be used when the user asks to "scaffold a datacard",
  "scaffold a README", "create a README for a HuggingFace repo",
  "audit a collection", "audit datacards", "check datacard completeness",
  "review a HuggingFace collection", "validate my dataset README",
  "inspect a HuggingFace dataset repo", "what configs are in this repo",
  "what columns does this dataset have", "what conditions are in this dataset",
  "download a parquet from HuggingFace", "query a HuggingFace dataset directly",
  or mentions DataCard, scaffold_readme, or audit_collection. Use to inspect,
  author, and audit labretriever-compatible HuggingFace dataset repositories
  without requiring a VirtualDB configuration.
version: 1.0.0
---

# labretriever-repo

This skill covers two modes of working with HuggingFace dataset repositories:

1. **Inspect** - read a repo's DataCard and query its files directly with Python
1. **Author / audit** - scaffold new DataCard READMEs and audit collections

Neither mode requires a VirtualDB config file.

## DataCard API Reference

All inspection starts with `DataCard`. Import it and construct with a repo ID:

```python
from labretriever.datacard import DataCard
card = DataCard("org/repo-name")
```

### Listing configs

```python
config_names = [c.config_name for c in card.configs]
```

### Full schema for a config: `get_dataset_schema`

This is the primary method for understanding a config's structure before writing
queries or downloading data. It returns a `DatasetSchema` with:

- `data_columns` — column names in the data parquet file(s)
- `metadata_columns` — column names in the metadata file
- `join_columns` — columns shared between data and metadata (use for JOINs)
- `metadata_source` — `"embedded"`, `"external"`, or `"none"`
- `external_metadata_config` — name of the metadata config (when external)
- `is_partitioned` — whether the data files are Hive-partitioned

```python
schema = card.get_dataset_schema("annotated_features_orig_reprocess")
print(schema.metadata_source)           # "external"
print(schema.external_metadata_config)  # "genome_map_meta"
print(schema.join_columns)             # {"id", "batch"}
print(schema.data_columns)             # set of column names in data parquet
print(schema.metadata_columns)         # set of column names in metadata parquet
print(schema.is_partitioned)           # True/False
```

Do not assume the metadata config follows a `_meta` naming convention.
`external_metadata_config` is the authoritative name.

### File paths

```python
cfg = card.get_config("annotated_features_orig_reprocess")
data_path_glob = cfg.data_files[0].path  # e.g. "annotated_features_orig_reprocess/*/*.parquet"

meta_cfg = card.get_config(schema.external_metadata_config)
meta_path = meta_cfg.data_files[0].path  # e.g. "genome_map_meta.parquet"
```

### Feature definitions for a config

```python
features = card.get_features("annotated_features_orig_reprocess")
for f in features:
    print(f.name, f.dtype, f.role, f.description)
```

### Experimental conditions

`get_experimental_conditions` returns merged conditions (repo-level and
config-level) for the fixed experimental context. `extract_metadata_schema`
returns per-sample condition column names and their value definitions.

```python
# Fixed conditions (not per-sample):
cond = card.get_experimental_conditions("annotated_features_orig_reprocess")

# Per-sample condition columns and definitions:
meta_schema = card.extract_metadata_schema("annotated_features_orig_reprocess")
print(meta_schema["condition_fields"])       # experimental_condition columns
print(meta_schema["condition_definitions"])  # value -> description mapping
print(meta_schema["metadata_fields"])        # all metadata column names
```

### Citation and DOI

```python
print(card.dataset_card.doi)
print(card.dataset_card.citation)
# Or per-config:
print(card.get_citation("annotated_features_orig_reprocess"))
```

## Querying Data

After reading the DataCard, download and query with DuckDB.

### Download a single parquet

```python
from huggingface_hub import hf_hub_download
import duckdb

local_path = hf_hub_download(
    repo_id="org/repo-name",
    filename="genome_map_meta.parquet",
    repo_type="dataset",
)
conn = duckdb.connect()
conn.execute("SELECT * FROM read_parquet(?) LIMIT 5", [local_path]).df()
```

### Download a partitioned dataset

```python
from huggingface_hub import snapshot_download
import duckdb

repo_path = snapshot_download(
    repo_id="org/repo-name",
    repo_type="dataset",
    allow_patterns="annotated_features_orig_reprocess/**",
)
conn = duckdb.connect()
conn.execute(
    "SELECT * FROM read_parquet(?) LIMIT 5",
    [f"{repo_path}/annotated_features_orig_reprocess/**/*.parquet"],
).df()
```

### JOIN data and metadata parquets

Use `schema.join_columns` to write the JOIN condition:

```python
schema = card.get_dataset_schema("annotated_features_orig_reprocess")
# schema.join_columns == {"id", "batch"}
# schema.external_metadata_config == "genome_map_meta"

# Download both files
data_path = snapshot_download(repo_id="org/repo", repo_type="dataset",
                               allow_patterns="annotated_features_orig_reprocess/**")
meta_path = hf_hub_download(repo_id="org/repo", filename="genome_map_meta.parquet",
                             repo_type="dataset")

conn = duckdb.connect()
conn.execute("""
    SELECT m.regulator_locus_tag, m.condition, d.callingcards_enrichment
    FROM read_parquet(?) d
    JOIN read_parquet(?) m ON d.id = m.id AND d.batch = m.batch
    WHERE m.condition = 'del_MET28'
""", [f"{data_path}/annotated_features_orig_reprocess/**/*.parquet", meta_path]).df()
```

## Authoring and Auditing (MCP Tools)

The `labretriever-repo` MCP server must be connected for these. Check `/plugins`
and confirm "labretriever-repo MCP - connected". If not connected:

1. Ensure `labretriever` is installed: `pip install labretriever`
1. Run `pyenv rehash` if using pyenv
1. Install the plugin:
   `/plugin marketplace add cmatKhan/labretriever`
   then `/plugin install labretriever@repo`

### scaffold_readme

Generate a minimal DataCard README skeleton from a repository's existing files.

```
scaffold_readme(repo_id="org/my-dataset")
```

Returns `{"readme": "<yaml>"}` on success, or `{"needs_input": [...]}` when
file types or column dtypes need clarification before proceeding.

### audit_collection

Review DataCard completeness and consistency across a set of repos.

```
audit_collection(source="/home/user/code/hf")
audit_collection(source="https://huggingface.co/collections/org/slug-abc123")
```

Pass `collection_context` for richer checks against documented field naming
conventions:

```
audit_collection(
    source="/home/user/code/hf",
    collection_context="/home/user/code/labretriever/docs/brentlab_yeastresources_collection.md",
)
```

## Additional Resources

- Full docs: <https://cmatkhan.github.io/labretriever/>
- DataCard format: <https://cmatkhan.github.io/labretriever/huggingface_datacard/>
- Collection context document: <https://cmatkhan.github.io/labretriever/brentlab_yeastresources_collection/>
