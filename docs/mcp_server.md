# MCP Server Setup

`labretriever` ships two [MCP](https://modelcontextprotocol.io) servers:

- **`labretriever`** (`labretriever-mcp`): exposes a `VirtualDB` instance as
  SQL query tools. Requires a VirtualDB config file.
- **`labretriever-repo`** (`labretriever-mcp-repo`): provides DataCard scaffold
  and collection audit tools. Requires no config file.

## Quick Install (Claude Code Plugin)

First, [install labretriever](index.md#installation) so that `labretriever-mcp`
and `labretriever-mcp-repo` are available on your PATH.

Then add the marketplace and install the plugin:

```
/plugin marketplace add cmatKhan/labretriever
/plugin install labretriever@labretriever
```

The plugin will prompt you for a VirtualDB config file path and an optional
HuggingFace token at enable time. Both MCP servers are registered automatically.
If `labretriever-mcp` is not found on PATH when a session starts, Claude will
display installation instructions.

For the BrentLab yeast resources collection, download the ready-to-use config from:

[https://github.com/BrentLab/tfbpshiny/blob/main/tfbpshiny/brentlab_yeast_collection.yaml](https://github.com/BrentLab/tfbpshiny/blob/main/tfbpshiny/brentlab_yeast_collection.yaml)

Save it to a stable path and provide that path when the plugin prompts you.

## Manual Configuration (without the plugin)

Install the package first — see [Installation](index.md#installation).

Add the following to `.claude/settings.json` (or `~/.claude/settings.json` for
user-level):

```json
{
  "mcpServers": {
    "labretriever": {
      "command": "labretriever-mcp",
      "type": "stdio",
      "env": {
        "LABRETRIEVER_CONFIG": "/absolute/path/to/brentlab_yeast_collection.yaml",
        "HF_TOKEN": "${HF_TOKEN}"
      }
    },
    "labretriever-repo": {
      "command": "labretriever-mcp-repo",
      "type": "stdio",
      "env": {
        "HF_TOKEN": "${HF_TOKEN}"
      }
    }
  }
}
```

`HF_TOKEN` is only required for private HuggingFace repositories. If it is not
set and a query touches a private or gated repository, the server returns a
clear error naming the repository.

`LABRETRIEVER_CONFIG` is only required for the `labretriever` server.
`labretriever-repo` starts without it.

## Available Tools

### labretriever (VirtualDB query tools)

Requires `LABRETRIEVER_CONFIG`.

| Tool | Description |
|---|---|
| `list_datasets` | List all registered dataset names (call this first). |
| `describe_dataset` | Return column names and types for a `{name}` or `{name}_meta` view. |
| `get_column_metadata` | Return semantic roles and condition-level definitions for each column. |
| `get_tags` | Return provenance tags (assay type, publication, etc.) for a dataset. |
| `get_common_fields` | Return column names shared across all `_meta` views. |
| `query` | Execute DuckDB SQL; returns shape by default, rows when `return_data=True`. |
| `get_config_path` | Return the config file path (call before writing Python snippets). |

### labretriever-repo (DataCard tools)

No config file required.

| Tool | Description |
|---|---|
| `scaffold_readme` | Scaffold a minimal HF DataCard README from a repository's file structure. |
| `audit_collection` | Audit a collection of repos for DataCard completeness, schema consistency, and consolidation opportunities. |

## scaffold_readme

Inspects the files in a HuggingFace dataset repository and produces a skeleton
README in valid HuggingFace DataCard YAML format. The skeleton follows only the
official DataCard specification — no labretriever-specific extensions are added.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `repo_id` | `str` | HuggingFace dataset repository ID, e.g. `"BrentLab/callingcards"`. |

**Return value:**

One of two shapes:

- `{"readme": "<yaml_string>"}` — a complete skeleton YAML ready to paste into
  the repository README. All `description` fields are empty (`''`).
- `{"needs_input": [{"file": ..., "reason": ..., "question": ...}, ...]}` — the
  tool encountered files or column dtypes it cannot resolve without user guidance.
  Address each question and call the tool again.

**Behavior:**

- `.parquet` files: schema read via DuckDB.
- `.csv` / `.tsv` files: schema read via pandas.
- Standard non-data files (`.md`, `.py`, `.R`, `.sh`, `.yaml`, `.json`, etc.)
  are silently skipped.
- Any other extension returns `needs_input` immediately.
- Columns with unrecognized dtypes return `needs_input` rather than guessing.
- Columns whose distinct-value count is <= 10 in a sample of >= 100 rows are
  suggested as `class_label`; the `names` list is left empty for the author to fill.
- Hive-partitioned directories are detected automatically; the partition column
  is appended to the feature list with `dtype: string`.

**Example:**

```python
from labretriever.mcp_server._repo_server import _scaffold_readme_impl

result = _scaffold_readme_impl("BrentLab/callingcards", token=None)
if "readme" in result:
    print(result["readme"])
else:
    for item in result["needs_input"]:
        print(item["question"])
```

## audit_collection

Audits every repo in a local directory or a HuggingFace collection URL against
its DataCard specification. Reports completeness issues, schema inconsistencies,
and generates ready-to-paste YAML for consolidating repeated field definitions.

**Parameters:**

| Name | Type | Description |
|---|---|---|
| `source` | `str` | Local directory path (e.g. `"/home/user/code/hf"`) or HuggingFace collection URL (e.g. `"https://huggingface.co/collections/BrentLab/yeastresources-..."`). |
| `collection_context` | `str \| None` | Optional path to a [collection context document](brentlab_yeastresources_collection.md). When supplied, field naming conventions and dataset type expectations from the document are used to add context-aware findings. |

**Return value:**

```json
{
  "source": "<resolved source>",
  "schema_checks_performed": true,
  "repos": {
    "callingcards": {
      "findings": [
        {
          "severity": "warning",
          "check": "missing_feature_descriptions",
          "config": "annotated_features",
          "field": "experiment_hops",
          "message": "Field 'experiment_hops' in config 'annotated_features' has no description.",
          "doc_link": "https://cmatkhan.github.io/labretriever/huggingface_datacard/#feature-definitions"
        }
      ]
    }
  },
  "collection_suggestions": [
    {
      "type": "consolidate_to_shared_features",
      "scope": "same_repo",
      "repo": "callingcards",
      "field": "target_locus_tag",
      "configs": ["annotated_features", "annotated_features_combined"],
      "message": "...",
      "generated_yaml": "features:\n- applies_to:\n  ...",
      "doc_link": "https://cmatkhan.github.io/labretriever/huggingface_datacard/#shared-feature-definitions"
    }
  ]
}
```

**Checks performed:**

| Check | Severity | Description |
|---|---|---|
| `missing_description` | warning | Config description is empty or absent. |
| `missing_feature_descriptions` | warning | A feature in `dataset_info.features` has no description. |
| `undocumented_column` | error | Column in data file not listed in `dataset_info.features` (local only). |
| `phantom_column` | warning | Feature in `dataset_info.features` not found in data file (local only). |
| `dtype_mismatch` | error | File column dtype is incompatible with declared HF dtype (local only, lenient). |
| `missing_role` | warning | Feature has no `role` field. |
| `missing_doi` | info | No `doi` at repo level. |
| `missing_citation` | info | No `citation` at repo level. |
| `class_label_empty_names` | warning | `class_label` dtype has an empty `names` list. |
| `non_reserved_dataset_type` | info | Config uses a collection-defined `dataset_type` (not an error). |
| `partition_col_undocumented` | warning | Hive partition column not listed in features (local only). |

**Example:**

```python
from labretriever.mcp_server._repo_server import _audit_collection_impl

result = _audit_collection_impl(
    "/home/user/code/hf",
    collection_context="docs/brentlab_yeastresources_collection.md",
    token=None,
)
for repo, data in result["repos"].items():
    for finding in data["findings"]:
        print(f"[{finding['severity'].upper()}] {repo}: {finding['message']}")
for s in result["collection_suggestions"]:
    if s.get("generated_yaml"):
        print(s["generated_yaml"])
```

## Example Session

After connecting, a typical VirtualDB workflow in Claude Code looks like:

1. `list_datasets` - discover available views (`harbison`, `callingcards`, etc.)
2. `describe_dataset("harbison_meta")` - inspect sample-level columns
3. `get_column_metadata("harbison")` - understand condition values and measurement roles
4. `query("SELECT * FROM harbison_meta WHERE condition = 'GAL'", return_data=True)` - explore
5. `query("SELECT regulator_symbol, COUNT(*) FROM harbison WHERE condition = 'GAL' AND pvalue < 0.001 GROUP BY 1 ORDER BY 2 DESC")` - full analysis

See the [VirtualDB tutorial](tutorials/virtual_db_tutorial.ipynb) for more query patterns.
