"""
MCP server exposing VirtualDB query tools.

Initialized from environment variables:

- ``LABRETRIEVER_CONFIG`` (required): path to the VirtualDB YAML config file.
- ``HF_TOKEN`` (optional): HuggingFace token for private datasets.

Run via the ``labretriever-mcp`` entry point or directly with
``python -m labretriever.mcp_server._vdb_server``.

"""

from __future__ import annotations

import dataclasses
import os

from huggingface_hub.errors import GatedRepoError, RepositoryNotFoundError
from mcp.server.fastmcp import FastMCP

from labretriever.virtual_db import VirtualDB

_CONFIG_HELP = (
    "LABRETRIEVER_CONFIG is not set. "
    "LABRETRIEVER_CONFIG is required. It must point to a VirtualDB YAML "
    "configuration file that tells the server which HuggingFace datasets to "
    "expose and how to map their fields — this is not a file that ships with "
    "labretriever; you create or download one. "
    "See https://cmatkhan.github.io/labretriever/virtual_db_configuration/ "
    "for the format. "
    "For the BrentLab yeast resources collection, a ready-to-use config is "
    "available at https://github.com/BrentLab/tfbpshiny/blob/main/"
    "tfbpshiny/brentlab_yeast_collection.yaml\n"
    "If you provide a path to a valid VirtualDB config file, I can set "
    "LABRETRIEVER_CONFIG for you in the appropriate Claude Code settings "
    "(user-level ~/.claude.json or project-level .claude/settings.json, "
    "whichever is most appropriate). "
    "If you plan to access private HuggingFace repositories, also provide "
    "an HF_TOKEN."
)

_CONFIG_NOT_FOUND_HELP = (
    "The config file configured for this plugin cannot be found at: {path}\n"
    "It may have been moved or deleted. Please provide a valid path to a "
    "VirtualDB YAML config file and I will update the plugin settings for you."
)

_PRIVATE_REPO_HELP = (
    "Access was denied for a private or gated HuggingFace repository ({repo}). "
    "Set the HF_TOKEN environment variable to a token with access to that repo "
    "and restart the MCP server."
)

_vdb: VirtualDB | None = None
_vdb_config_path: str | None = None
_startup_error: str | None = None


def _get_vdb() -> VirtualDB:
    """
    Return the singleton VirtualDB.

    If startup failed (missing or unreadable config), raises ``RuntimeError``
    with instructions intended to be read by Claude.

    :raises RuntimeError: If config was missing, not found, or VirtualDB was
        not initialized.
    :returns: The initialized VirtualDB instance.
    :rtype: VirtualDB

    """
    if _startup_error is not None:
        raise RuntimeError(_startup_error)
    if _vdb is None:
        raise RuntimeError(
            "VirtualDB is not initialized. This tool must be invoked via the "
            "labretriever-mcp entry point, not imported directly."
        )
    return _vdb


mcp = FastMCP("labretriever")


@mcp.tool()
def get_config_path() -> str:
    """
    Return the absolute path to the VirtualDB config file in use.

    Call this before generating Python code that instantiates ``VirtualDB``
    so the correct config path can be embedded directly in the snippet.

    :returns: Absolute path to the LABRETRIEVER_CONFIG file.
    :rtype: str

    """
    _get_vdb()  # raises if not initialized
    return _vdb_config_path  # type: ignore[return-value]


@mcp.tool()
def list_datasets() -> list[str]:
    """
    List all dataset names registered in the VirtualDB.

    Call this first to discover what data is available. Each returned name
    is a ``db_name`` that has two queryable DuckDB views:

    - ``{name}_meta``: one row per experimental sample. Use this for
      sample-level questions such as "how many experiments target CBF1?",
      "what conditions are represented?", or "which regulators are in the
      dataset?". Always start here when exploring a dataset.

    - ``{name}``: the full data view, containing regulator x target x
      measurement rows joined to sample metadata. Use this for gene-level
      questions such as "what is the enrichment score for CBF1 at YJR060W?"
      or "which targets show significant binding?".

    The ``yeast_genome_resources`` dataset provides reference gene annotations
    and joins to all other datasets via ``regulator_locus_tag`` and
    ``target_locus_tag``. The ``yeast_comparative_analysis`` dataset contains
    cross-dataset analytical results using composite sample identifiers.

    :returns: Sorted list of dataset names.
    :rtype: list[str]

    """
    return _get_vdb().get_datasets()


@mcp.tool()
def describe_dataset(dataset_name: str) -> list[dict]:
    """
    Return column names and DuckDB types for a view.

    Pass either ``{name}`` or ``{name}_meta`` as ``dataset_name``. Call
    :func:`list_datasets` first to see valid names, then call this with
    ``{name}_meta`` to understand sample-level columns, or ``{name}`` for
    measurement-level columns. Always call this before writing a query so
    you know the exact column names and types.

    :param dataset_name: View name, e.g. ``"harbison_meta"`` or ``"harbison"``.
    :type dataset_name: str
    :returns: List of dicts with keys ``table``, ``column_name``,
        ``column_type``, ``null``, ``key``, ``default``, and ``extra``.
    :rtype: list[dict]

    """
    df = _get_vdb().describe(dataset_name)
    return df.to_dict(orient="records")


@mcp.tool()
def get_column_metadata(dataset_name: str) -> dict[str, dict]:
    """
    Return semantic metadata for each column in a dataset's ``_meta`` view.

    Provides richer information than :func:`describe_dataset`, including the
    human-readable description, semantic role, and per-level definitions for
    categorical condition columns. Use this alongside :func:`describe_dataset`
    to understand column semantics before writing queries.

    The ``role`` field is collection-defined. Consult the collection context
    document for the role conventions used in this collection. The one role
    with built-in library behavior is:

    - ``experimental_condition``: categorical or numeric condition columns
      (e.g. growth media, temperature, treatment). The ``level_definitions``
      field maps each category value to a description — use this to understand
      what filter values mean.

    Other common roles (collection-defined, no special library behavior):

    - ``quantitative_measure``: the primary measurement column(s) to SELECT
      when retrieving data (e.g. enrichment scores, fold changes, p-values).

    :param dataset_name: Dataset name as returned by :func:`list_datasets`.
    :type dataset_name: str
    :returns: Dict mapping column name to a metadata dict with keys
        ``description`` (str or None), ``role`` (str or None), and
        ``level_definitions`` (dict mapping level value to description, or None).
        Returns an empty dict if the dataset is not found or has no metadata.
    :rtype: dict[str, dict]

    """
    result = _get_vdb().get_column_metadata(dataset_name)
    if result is None:
        return {}
    return {col: dataclasses.asdict(meta) for col, meta in result.items()}


@mcp.tool()
def get_tags(dataset_name: str) -> dict[str, str]:
    """
    Return provenance and classification tags for a dataset.

    Tags are defined in the VirtualDB configuration at the repository and/or
    dataset level and describe the biological and experimental context of the
    data. Typical tags include assay type, organism, publication reference,
    and data source.

    :param dataset_name: Dataset name as returned by :func:`list_datasets`.
    :type dataset_name: str
    :returns: Dict of tag key-value pairs, or empty dict if no tags are defined.
    :rtype: dict[str, str]

    """
    return _get_vdb().get_tags(dataset_name)


@mcp.tool()
def get_common_fields() -> list[str]:
    """
    Return column names present in all primary ``_meta`` views.

    These columns exist in every dataset's ``_meta`` view and can therefore
    be used in cross-dataset JOIN queries. Common fields typically include
    ``regulator_locus_tag``, ``regulator_symbol``, and ``sample_id``.

    To join two datasets on a shared regulator:

    .. code-block:: sql

        SELECT a.regulator_locus_tag, a.sample_id AS a_sample, b.sample_id AS b_sample
        FROM harbison_meta a
        JOIN rossi_meta b ON a.regulator_locus_tag = b.regulator_locus_tag

    To annotate results with gene information from ``yeast_genome_resources``,
    JOIN on ``regulator_locus_tag`` or ``target_locus_tag``.

    :returns: Sorted list of column names common to all primary ``_meta`` views.
    :rtype: list[str]

    """
    return _get_vdb().get_common_fields()


@mcp.tool()
def query(sql: str, return_data: bool = False) -> dict:
    """
    Execute DuckDB SQL against the yeast resources VirtualDB.

    By default returns only the echoed SQL and result shape. This is
    intentional — it allows you to validate and iterate on queries without
    transferring large datasets into the context window. Only set
    ``return_data=True`` for small, filtered result sets. Avoid it for
    whole-table scans or unfiltered full-data views.

    **Which view to query:**

    - Use ``{db_name}_meta`` for sample-level questions: filtering by
      condition, counting samples, finding regulators, or building the
      WHERE clause for a subsequent full-data query.
    - Use ``{db_name}`` for gene-level questions: retrieving enrichment
      scores, fold changes, p-values, or other per-target measurements.
      The full view includes all metadata columns joined in, so you can
      filter on conditions here too.

    **Recommended workflow:**

    1. Call :func:`list_datasets` to see available dataset names.
    2. Call :func:`describe_dataset` with ``{name}_meta`` and ``{name}``
       to learn column names and types.
    3. Call :func:`get_column_metadata` to understand roles and condition
       level definitions.
    4. Write a query against ``{name}_meta`` first to confirm sample counts
       and filter values (use ``return_data=True`` — these results are small).
    5. Write the full-data query against ``{name}`` with filters applied.

    **Joining to gene annotations:**

    .. code-block:: sql

        SELECT d.regulator_locus_tag, g.regulator_symbol, d.enrichment
        FROM harbison d
        JOIN yeast_genome_resources g
          ON d.target_locus_tag = g.target_locus_tag
        WHERE d.regulator_locus_tag = 'YJR060W'
        LIMIT 20

    **Note:** Only raw SQL is supported here — DuckDB ``$param`` syntax is
    not available via this tool. Embed literal values directly in the SQL.

    :param sql: DuckDB SQL statement to execute.
    :type sql: str
    :param return_data: If True, include result rows in the response.
        Use only for small, filtered results. Defaults to False.
    :type return_data: bool
    :returns: Dict with keys ``sql`` (the executed query), ``shape``
        ([nrows, ncols] as a list), and optionally ``rows`` (list of
        dicts, one per result row) when ``return_data=True``.
    :rtype: dict

    """
    try:
        df = _get_vdb().query(sql)
    except (GatedRepoError, RepositoryNotFoundError) as exc:
        repo = getattr(exc, "repo_id", str(exc))
        raise RuntimeError(_PRIVATE_REPO_HELP.format(repo=repo)) from exc
    out: dict = {
        "sql": sql,
        "shape": list(df.shape),
    }
    if return_data:
        out["rows"] = df.to_dict(orient="records")
    return out


def main() -> None:
    """
    Initialize VirtualDB from environment variables and start the MCP server.

    Reads ``LABRETRIEVER_CONFIG`` (required) and ``HF_TOKEN`` (optional) from
    the environment. If ``LABRETRIEVER_CONFIG`` is not set the server still
    starts, but every tool call returns setup instructions so Claude can guide
    the user interactively.

    """
    global _vdb, _vdb_config_path, _startup_error
    config_path = os.environ.get("LABRETRIEVER_CONFIG")
    if not config_path:
        _startup_error = _CONFIG_HELP
    elif not os.path.isfile(config_path):
        _startup_error = _CONFIG_NOT_FOUND_HELP.format(path=config_path)
    else:
        token = os.environ.get("HF_TOKEN")
        _vdb = VirtualDB(config_path, token=token)
        _vdb_config_path = config_path
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
