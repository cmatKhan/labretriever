---
name: labretriever
description: >
  This skill should be used when the user asks to "query genomic datasets",
  "list available datasets", "explore yeast data", "query harbison",
  "query callingcards", "find TF targets", "describe a dataset", "run a SQL
  query on genomic data", "what datasets are available", "get column metadata",
  or mentions labretriever, VirtualDB, or the yeast resources MCP. Use to
  orient the user and guide effective use of the labretriever MCP tools.
version: 1.0.0
---

# labretriever

labretriever exposes genomic and transcriptomic datasets hosted on HuggingFace
as DuckDB SQL views, accessible through a set of MCP tools. This skill orients
the user to those tools and guides effective query workflows.

## Prerequisites

The labretriever MCP server must be connected. Check `/plugins` and confirm
"labretriever MCP - connected". If not connected:

1. Ensure `labretriever` is installed: `pip install labretriever`
1. Run `pyenv rehash` if using pyenv
1. Install the plugin:
   `/plugin marketplace add cmatKhan/labretriever`
   then `/plugin install labretriever@labretriever`
1. Provide the path to a VirtualDB YAML config when prompted

For the BrentLab yeast collection, download the ready-to-use config from:

<https://github.com/BrentLab/tfbpshiny/blob/main/tfbpshiny/brentlab_yeast_collection.yaml>

## Standard Query Workflow

Always follow this order when starting a new analysis:

1. **Discover** - call `list_datasets` to see all registered view names
1. **Inspect schema** - call `describe_dataset("{name}")` and
   `describe_dataset("{name}_meta")` to see columns and types for both the
   data view and its sample metadata view
1. **Understand semantics** - call `get_column_metadata("{name}")` to learn
   which columns are measurement values, condition labels, identifiers, etc.
1. **Check provenance** - call `get_tags("{name}")` to see assay type,
   publication, and other annotation for the dataset
1. **Query** - use `query(sql)` with DuckDB SQL; pass `return_data=True` only
   when you need actual rows returned (omit it for shape/count checks)

## Available MCP Tools

| Tool | When to use |
|---|---|
| `list_datasets` | Always call first; returns all registered view names |
| `describe_dataset` | Get column names and types for `{name}` or `{name}_meta` |
| `get_column_metadata` | Get semantic roles and condition definitions |
| `get_tags` | Get assay type, publication, and provenance tags |
| `get_common_fields` | Find columns shared across all `_meta` views for joins |
| `query` | Execute DuckDB SQL against any registered view |

## Writing Effective Queries

Each dataset has two views:

- `{name}` - the measurement data (e.g., p-values, fold changes, scores)
- `{name}_meta` - sample/condition metadata

Always inspect both before querying. Use `describe_dataset` to know the exact
column names — do not guess them.

### Shape check before fetching data

```sql
-- Check row/column count without loading all data
SELECT COUNT(*) FROM harbison WHERE pvalue < 0.001
```

Call `query(sql)` without `return_data=True` to get shape only. Use
`return_data=True` only for the final result you need to analyze.

### Cross-dataset analysis

Use `get_common_fields` to find shared columns across `_meta` views, then join:

```sql
SELECT h.regulator_symbol, h.target_symbol, c.score
FROM harbison h
JOIN callingcards c ON h.target_symbol = c.target_symbol
WHERE h.condition = 'GAL' AND h.pvalue < 0.001
```

### Filtering by condition

Condition values are dataset-specific. Always use `get_column_metadata` to
learn valid condition values before filtering:

```sql
SELECT DISTINCT condition FROM harbison_meta
```

## Reproducing MCP Results in Python

When asked for Python code to reproduce a query result, use the
`LABRETRIEVER_CONFIG` environment variable for the config path — do not
substitute a placeholder. The MCP server is already running with that path set.

```python
import os
from labretriever.virtual_db import VirtualDB

vdb = VirtualDB(os.environ["LABRETRIEVER_CONFIG"])

results = vdb.query(
    "SELECT ...",  # paste the SQL from the MCP query here
    return_data=True,
)
print(results)
```

`vdb.query()` returns a pandas DataFrame when `return_data=True`.
If private repos are in use, pass `token=os.environ.get("HF_TOKEN")` as well.

## Error Handling

If a tool returns an error about `LABRETRIEVER_CONFIG` not being set, the MCP
server started without a config. Use `/plugin` to check configuration and
re-enable the plugin with a valid config path.

If a query touches a private HuggingFace repository and returns an access
error, the plugin needs an `HF_TOKEN`. Re-enable the plugin and provide a
token with access to the relevant repository.

## Additional Resources

- `references/query_patterns.md` - Common SQL patterns and Python API usage
  (how to reproduce MCP results in a notebook with `VirtualDB.query()`)
- Full docs: <https://cmatkhan.github.io/labretriever/>
- VirtualDB config format:
  <https://cmatkhan.github.io/labretriever/virtual_db_configuration/>
