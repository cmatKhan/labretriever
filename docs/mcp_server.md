# MCP Server Setup

`labretriever` ships an [MCP](https://modelcontextprotocol.io) server that exposes
a `VirtualDB` instance as a set of tools callable by Claude Code (or any other MCP
client). Once configured, Claude can discover datasets, inspect schema, and execute
DuckDB SQL queries against your collection without any manual Python.

## Quick Install (Claude Code Plugin)

**NOTE**: you must have [installed labretriever](index.md#installation) in
the same environment as Claude Code.

The easiest way to set up the server is via the Claude Code plugin:

```
/plugin add cmatKhan/labretriever-plugin
```

After installing the plugin, simply invoke any labretriever tool (e.g. `list_datasets`).
If `LABRETRIEVER_CONFIG` is not yet set, the server will return setup instructions and
Claude will guide you interactively — asking for a path or URL to a VirtualDB config
file and setting it in the right place (`~/.claude.json` for user-level or
`.claude/settings.json` for project-level) on your behalf.

## Manual Configuration (without the plugin)

`LABRETRIEVER_CONFIG` must point to a VirtualDB YAML file that you create or
download — it tells the server which HuggingFace datasets to expose and how to map
their fields. See the [VirtualDB Configuration](virtual_db_configuration.md) docs
for the full format.

For the BrentLab yeast resources collection, a ready-to-use config can be downloaded
from:

[https://github.com/BrentLab/tfbpshiny/blob/main/tfbpshiny/brentlab_yeast_collection.yaml](https://github.com/BrentLab/tfbpshiny/blob/main/tfbpshiny/brentlab_yeast_collection.yaml)

Save it to a stable path, then add the following to `.claude/settings.json` (or the
equivalent user-level settings file):

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
    }
  }
}
```

`HF_TOKEN` is only required for private HuggingFace repositories. If it is not set
and a query touches a private or gated repository, the server will return a clear
error naming the repository.

## Available Tools

Once the server is running, Claude has access to these tools:

| Tool | Description |
|---|---|
| `list_datasets` | List all registered dataset names (call this first). |
| `describe_dataset` | Return column names and types for a `{name}` or `{name}_meta` view. |
| `get_column_metadata` | Return semantic roles and condition-level definitions for each column. |
| `get_tags` | Return provenance tags (assay type, publication, etc.) for a dataset. |
| `get_common_fields` | Return column names shared across all `_meta` views. |
| `query` | Execute DuckDB SQL; returns shape by default, rows when `return_data=True`. |

## Example Session

After connecting, a typical workflow in Claude Code looks like:

1. `list_datasets` - discover available views (`harbison`, `callingcards`, etc.)
2. `describe_dataset("harbison_meta")` - inspect sample-level columns
3. `get_column_metadata("harbison")` - understand condition values and measurement roles
4. `query("SELECT * FROM harbison_meta WHERE condition = 'GAL'", return_data=True)` - explore
5. `query("SELECT regulator_symbol, COUNT(*) FROM harbison WHERE condition = 'GAL' AND pvalue < 0.001 GROUP BY 1 ORDER BY 2 DESC")` - full analysis

See the [VirtualDB tutorial](tutorials/virtual_db_tutorial.ipynb) for more query patterns.
