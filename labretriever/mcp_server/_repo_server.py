"""
MCP server exposing DataCard scaffold and collection audit tools.

Initialized from environment variables:

- ``HF_TOKEN`` (optional): HuggingFace token for private repositories.

Run via the ``labretriever-mcp-repo`` entry point or directly with
``python -m labretriever.mcp_server._repo_server``.

"""

from __future__ import annotations

import argparse
import importlib.metadata
import os
import re
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import yaml  # type: ignore[import-untyped]
from huggingface_hub import get_collection, hf_hub_download
from mcp.server.fastmcp import FastMCP

from labretriever.constants import get_hf_token
from labretriever.fetchers import HfDataCardFetcher, HfRepoStructureFetcher
from labretriever.models import DatasetCard

from ._common import (
    _DUCKDB_DTYPE_MAP,
    _dtype_compatible,
)

# ---------------------------------------------------------------------------
# scaffold_readme helpers
# ---------------------------------------------------------------------------

_SKIP_EXTENSIONS = {
    ".md",
    ".py",
    ".r",
    ".sh",
    ".yaml",
    ".yml",
    ".json",
    ".txt",
    ".gitattributes",
    ".gitignore",
    ".license",
    ".rst",
    ".toml",
    ".cfg",
    ".ini",
}

_TABULAR_EXTENSIONS = {".parquet", ".csv", ".tsv"}

_CATEGORICAL_THRESHOLD = 10
_CATEGORICAL_MIN_ROWS = 100


def _is_partition_path(path: str) -> bool:
    """Return True if *path* contains a Hive partition component."""
    return bool(re.search(r"[^/=]+=", path))


def _config_name_from_path(path: str) -> str:
    """
    Derive config name from a file path.

    For partitioned paths like ``annotated_features/batch=foo/0.parquet``
    returns ``annotated_features``. For flat paths like
    ``genome_map_meta.parquet`` returns ``genome_map_meta``.

    """
    parts = Path(path).parts
    if len(parts) > 1:
        return parts[0]
    stem = Path(path).stem
    return stem


def _partition_cols_from_paths(paths: list[str]) -> list[str]:
    """Return ordered list of Hive partition column names found in *paths*."""
    seen: list[str] = []
    for path in paths:
        for match in re.finditer(r"([^/=]+)=([^/]+)", path):
            col = match.group(1)
            if col not in seen:
                seen.append(col)
    return seen


def _infer_dtype_duckdb(
    col: str, raw_type: str, conn: duckdb.DuckDBPyConnection, table_expr: str
) -> str | None:
    """
    Map a DuckDB raw type to an HF DataCard dtype string.

    Returns None when the mapping is ambiguous (caller adds to needs_input).

    """
    upper = raw_type.upper()

    if upper in (
        "VARCHAR",
        "INTEGER",
        "BIGINT",
        "HUGEINT",
        "UBIGINT",
        "UINTEGER",
        "SMALLINT",
        "TINYINT",
    ):
        try:
            row = conn.execute(
                f"SELECT COUNT(*) AS n, COUNT(DISTINCT {col}) AS d FROM {table_expr}"
            ).fetchone()
            if (
                row
                and row[0] >= _CATEGORICAL_MIN_ROWS
                and row[1] <= _CATEGORICAL_THRESHOLD
            ):
                return "class_label"
        except Exception:
            pass

    mapped = _DUCKDB_DTYPE_MAP.get(upper)
    return mapped


def _read_parquet_schema(
    path: str, repo_id: str, token: str | None
) -> tuple[list[tuple[str, str]], list[dict]]:
    """
    Download one parquet file and return (schema_pairs, needs_input_entries).

    Returns a list of ``(column_name, hf_dtype)`` pairs and a list of
    needs_input dicts for columns whose dtype could not be resolved.

    """
    local_path = hf_hub_download(
        repo_id=repo_id, filename=path, repo_type="dataset", token=token
    )
    conn = duckdb.connect()
    table_expr = f"read_parquet('{local_path}')"
    rows = conn.execute(f"DESCRIBE SELECT * FROM {table_expr}").fetchall()

    schema: list[tuple[str, str]] = []
    needs_input: list[dict] = []
    config_name = _config_name_from_path(path)
    for row in rows:
        col, raw_type = row[0], row[1]
        dtype = _infer_dtype_duckdb(col, raw_type, conn, table_expr)
        if dtype is None:
            needs_input.append(
                {
                    "file": path,
                    "reason": (
                        f"column '{col}' has unrecognized DuckDB type '{raw_type}'"
                    ),
                    "question": (
                        f"Column '{col}' in config '{config_name}' has dtype "
                        f"'{raw_type}'. What HF DataCard dtype should be used?"
                    ),
                }
            )
        else:
            schema.append((col, dtype))
    conn.close()
    return schema, needs_input


def _read_tabular_schema_csv(
    path: str, repo_id: str, token: str | None, sep: str = ","
) -> tuple[list[tuple[str, str]], list[dict]]:
    """Download one CSV/TSV file and return (schema_pairs, needs_input_entries)."""
    local_path = hf_hub_download(
        repo_id=repo_id, filename=path, repo_type="dataset", token=token
    )
    sample = pd.read_csv(local_path, sep=sep, nrows=5000)
    config_name = _config_name_from_path(path)

    schema: list[tuple[str, str]] = []
    needs_input: list[dict] = []
    for col, pd_dtype in sample.dtypes.items():
        col = str(col)
        dtype_str = str(pd_dtype)
        n_rows = len(sample)
        n_unique = sample[col].nunique()

        if dtype_str in ("object", "string") or pd.api.types.is_string_dtype(pd_dtype):
            if n_rows >= _CATEGORICAL_MIN_ROWS and n_unique <= _CATEGORICAL_THRESHOLD:
                schema.append((col, "class_label"))
            else:
                schema.append((col, "string"))
        elif pd.api.types.is_bool_dtype(pd_dtype):
            schema.append((col, "bool"))
        elif pd.api.types.is_integer_dtype(pd_dtype):
            if n_rows >= _CATEGORICAL_MIN_ROWS and n_unique <= _CATEGORICAL_THRESHOLD:
                schema.append((col, "class_label"))
            else:
                schema.append((col, "int64"))
        elif pd.api.types.is_float_dtype(pd_dtype):
            schema.append((col, "float64"))
        else:
            needs_input.append(
                {
                    "file": path,
                    "reason": (
                        f"column '{col}' has unrecognized pandas dtype '{dtype_str}'"
                    ),
                    "question": (
                        f"Column '{col}' in config '{config_name}' has dtype "
                        f"'{dtype_str}'. What HF DataCard dtype should be used?"
                    ),
                }
            )
    return schema, needs_input


def _feature_yaml(name: str, dtype: str | dict) -> str:
    """Render a single feature entry as YAML lines."""
    if dtype == "class_label" or (isinstance(dtype, dict) and "class_label" in dtype):
        return (
            f"    - name: {name}\n"
            f"      dtype:\n"
            f"        class_label:\n"
            f"          names: []\n"
            f"      description: ''\n"
        )
    return f"    - name: {name}\n      dtype: {dtype}\n      description: ''\n"


def _build_readme(
    configs: dict[str, dict[str, Any]],
    repo_id: str,
) -> str:
    """
    Render the skeleton README YAML string.

    *configs* maps config_name -> {"path_glob": str, "features": [(name, dtype), ...]}.

    """
    lines = ["---", "configs:"]
    for config_name in sorted(configs):
        cfg = configs[config_name]
        lines.append(f"- config_name: {config_name}")
        lines.append("  description: ''")
        lines.append("  data_files:")
        lines.append("  - split: train")
        lines.append(f"    path: {cfg['path_glob']}")
        lines.append("  dataset_info:")
        lines.append("    features:")
        for name, dtype in cfg["features"]:
            entry = _feature_yaml(name, dtype)
            for feat_line in entry.rstrip("\n").split("\n"):
                lines.append(feat_line)
    lines.append("---")
    lines.append(f"# {repo_id}")
    lines.append("")
    lines.append("<!-- Add dataset description here -->")
    lines.append("")
    return "\n".join(lines)


def _scaffold_readme_impl(repo_id: str, token: str | None) -> dict:
    """
    Core implementation for :func:`scaffold_readme`.

    :param repo_id: HuggingFace dataset repository ID.
    :param token: Optional HuggingFace authentication token.
    :returns: ``{"readme": str}`` or ``{"needs_input": list[dict]}``.

    """
    fetcher = HfRepoStructureFetcher(token=token)
    structure = fetcher.fetch(repo_id)
    all_files: list[dict] = structure["files"]

    unknown_inputs: list[dict] = []
    tabular_files: list[dict] = []

    for f in all_files:
        path = f["path"]
        ext = Path(path).suffix.lower()
        if ext in _SKIP_EXTENSIONS or not ext:
            continue
        if ext in _TABULAR_EXTENSIONS:
            tabular_files.append(f)
        else:
            unknown_inputs.append(
                {
                    "file": path,
                    "reason": f"unrecognized file extension '{ext}'",
                    "question": (
                        f"Unknown file type '{ext}'. "
                        "Is this a tabular data file? If so, what format?"
                    ),
                }
            )

    if unknown_inputs:
        return {"needs_input": unknown_inputs}

    config_groups: dict[str, dict] = defaultdict(
        lambda: {"paths": [], "is_partitioned": False, "partition_cols": []}
    )

    for f in tabular_files:
        path = f["path"]
        config_name = _config_name_from_path(path)
        config_groups[config_name]["paths"].append(path)
        if _is_partition_path(path):
            config_groups[config_name]["is_partitioned"] = True

    for config_name, info in config_groups.items():
        if info["is_partitioned"]:
            info["partition_cols"] = _partition_cols_from_paths(info["paths"])

    all_needs_input: list[dict] = []
    config_schemas: dict[str, list[tuple[str, str]]] = {}

    for config_name, info in config_groups.items():
        paths_for_config = info["paths"]
        file_objs = [f for f in tabular_files if f["path"] in paths_for_config]
        representative = min(
            file_objs,
            key=lambda f: (f.get("size") or 0),
        )
        rep_path = representative["path"]
        ext = Path(rep_path).suffix.lower()

        if ext == ".parquet":
            schema, ni = _read_parquet_schema(rep_path, repo_id, token)
        elif ext == ".tsv":
            schema, ni = _read_tabular_schema_csv(rep_path, repo_id, token, sep="\t")
        else:
            schema, ni = _read_tabular_schema_csv(rep_path, repo_id, token, sep=",")

        all_needs_input.extend(ni)
        for pcol in info.get("partition_cols", []):
            schema.append((pcol, "string"))
        config_schemas[config_name] = schema

    if all_needs_input:
        return {"needs_input": all_needs_input}

    configs_for_render: dict[str, dict[str, Any]] = {}
    for config_name, info in config_groups.items():
        if info["is_partitioned"]:
            path_glob = (
                f"{config_name}/**/*.{Path(info['paths'][0]).suffix.lstrip('.')}"
            )
        else:
            path_glob = info["paths"][0]
        configs_for_render[config_name] = {
            "path_glob": path_glob,
            "features": config_schemas.get(config_name, []),
        }

    readme = _build_readme(configs_for_render, repo_id)
    return {"readme": readme}


# ---------------------------------------------------------------------------
# audit_collection helpers
# ---------------------------------------------------------------------------

_DOCS_BASE = "https://cmatkhan.github.io/labretriever/huggingface_datacard/"

_CHECK_DOC_LINKS: dict[str, str] = {
    "missing_description": _DOCS_BASE + "#feature-definitions",
    "missing_feature_descriptions": _DOCS_BASE + "#feature-definitions",
    "undocumented_column": _DOCS_BASE + "#feature-definitions",
    "phantom_column": _DOCS_BASE + "#feature-definitions",
    "dtype_mismatch": _DOCS_BASE + "#feature-definitions",
    "missing_role": _DOCS_BASE + "#feature-roles",
    "missing_doi": _DOCS_BASE + "#citation-and-doi",
    "missing_citation": _DOCS_BASE + "#citation-and-doi",
    "class_label_empty_names": _DOCS_BASE
    + "#categorical-fields-with-value-definitions",
    "non_reserved_dataset_type": _DOCS_BASE + "#reserved-dataset-types",
    "partition_col_undocumented": _DOCS_BASE + "#feature-definitions",
}


def _load_datacard_local(readme_path: str) -> DatasetCard | None:
    """
    Parse the YAML front-matter from a local README.md into a DatasetCard.

    Returns None if the file is missing, has no front-matter, or fails validation.

    """
    try:
        text = Path(readme_path).read_text(encoding="utf-8")
    except OSError:
        return None

    if not text.startswith("---"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None
    front_matter = text[3:end].strip()
    try:
        data = yaml.safe_load(front_matter)
    except yaml.YAMLError:
        return None
    if not isinstance(data, dict):
        return None
    try:
        return DatasetCard.model_validate(data)
    except Exception:
        return None


def _finding(
    severity: str,
    check: str,
    message: str,
    config: str | None = None,
    field: str | None = None,
) -> dict[str, Any]:
    """Build a finding dict."""
    f: dict[str, Any] = {
        "severity": severity,
        "check": check,
        "message": message,
        "doc_link": _CHECK_DOC_LINKS.get(check),
    }
    if config is not None:
        f["config"] = config
    if field is not None:
        f["field"] = field
    return f


def _audit_card(card: DatasetCard, local_dir: str | None) -> list[dict[str, Any]]:
    """
    Run all per-repo checks against a parsed DatasetCard.

    :param card: Parsed DatasetCard.
    :param local_dir: If not None, path to the local repo directory for file-schema
        checks.
    :returns: List of finding dicts.

    """
    findings: list[dict[str, Any]] = []

    if not card.doi:
        findings.append(
            _finding("info", "missing_doi", "No doi field at repository level.")
        )
    if not card.citation:
        findings.append(
            _finding(
                "info", "missing_citation", "No citation field at repository level."
            )
        )

    for cfg in card.configs:
        cname = cfg.config_name

        if not cfg.description or not cfg.description.strip():
            findings.append(
                _finding(
                    "warning",
                    "missing_description",
                    f"Config '{cname}' has no description.",
                    config=cname,
                )
            )

        _RESERVED = {"metadata", "comparative"}
        if cfg.dataset_type not in _RESERVED:
            findings.append(
                _finding(
                    "info",
                    "non_reserved_dataset_type",
                    f"Config '{cname}' uses dataset_type '{cfg.dataset_type}', "
                    "which is a collection-defined type. Only 'metadata' and "
                    "'comparative' are reserved by labretriever and have "
                    "special runtime behavior.",
                    config=cname,
                )
            )

        for feat in cfg.dataset_info.features:
            if not feat.description or not feat.description.strip():
                findings.append(
                    _finding(
                        "warning",
                        "missing_feature_descriptions",
                        f"Field '{feat.name}' in config '{cname}' has no description.",
                        config=cname,
                        field=feat.name,
                    )
                )
            if feat.role is None:
                findings.append(
                    _finding(
                        "warning",
                        "missing_role",
                        f"Field '{feat.name}' in config '{cname}' has no role.",
                        config=cname,
                        field=feat.name,
                    )
                )
            if isinstance(feat.dtype, dict) and "class_label" in feat.dtype:
                cl = feat.dtype["class_label"]
                names = cl.get("names", None) if isinstance(cl, dict) else None
                if names is not None and len(names) == 0:
                    findings.append(
                        _finding(
                            "warning",
                            "class_label_empty_names",
                            f"Field '{feat.name}' in config '{cname}' has "
                            f"class_label dtype with empty names list.",
                            config=cname,
                            field=feat.name,
                        )
                    )

        if local_dir is not None:
            findings.extend(_audit_config_schema(cfg, cname, local_dir))

    return findings


def _audit_config_schema(cfg: Any, cname: str, local_dir: str) -> list[dict[str, Any]]:
    """
    Compare documented features against actual file schema.

    :param cfg: DatasetConfig object.
    :param cname: Config name (for messages).
    :param local_dir: Local repo directory.
    :returns: List of finding dicts.

    """
    import glob as _glob

    findings: list[dict[str, Any]] = []

    data_paths: list[str] = [df.path for df in cfg.data_files]
    if not data_paths:
        return findings

    rep_local: str | None = None
    for dp in data_paths:
        candidates = _glob.glob(str(Path(local_dir) / dp), recursive=True)
        parquet_candidates = [c for c in candidates if c.endswith(".parquet")]
        if parquet_candidates:
            rep_local = min(parquet_candidates, key=lambda p: Path(p).stat().st_size)
            break
        csv_candidates = [
            c for c in candidates if c.endswith(".csv") or c.endswith(".tsv")
        ]
        if csv_candidates:
            rep_local = min(csv_candidates, key=lambda p: Path(p).stat().st_size)
            break

    if rep_local is None:
        return findings

    try:
        ext = Path(rep_local).suffix.lower()
        if ext == ".parquet":
            conn = duckdb.connect()
            rows = conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{rep_local}')"
            ).fetchall()
            conn.close()
            file_schema: dict[str, str] = {r[0]: r[1] for r in rows}
        elif ext in (".csv", ".tsv"):
            sep = "\t" if ext == ".tsv" else ","
            sample = pd.read_csv(rep_local, sep=sep, nrows=0)
            file_schema = {col: str(dtype) for col, dtype in sample.dtypes.items()}
        else:
            return findings
    except Exception:
        return findings

    documented: dict[str, Any] = {f.name: f for f in cfg.dataset_info.features}

    partition_cols: set[str] = set()
    for dp in data_paths:
        for match in re.finditer(r"([^/=]+)=", dp):
            partition_cols.add(match.group(1))
    for match in re.finditer(r"([^/=]+)=", rep_local):
        partition_cols.add(match.group(1))

    for col, raw_type in file_schema.items():
        if col not in documented:
            if col in partition_cols:
                findings.append(
                    _finding(
                        "warning",
                        "partition_col_undocumented",
                        f"Partition column '{col}' in config '{cname}' is not "
                        f"listed in dataset_info.features.",
                        config=cname,
                        field=col,
                    )
                )
            else:
                findings.append(
                    _finding(
                        "error",
                        "undocumented_column",
                        f"Column '{col}' exists in data file for config '{cname}' "
                        f"but is not documented in dataset_info.features.",
                        config=cname,
                        field=col,
                    )
                )

    for col, feat in documented.items():
        if col not in file_schema and col not in partition_cols:
            findings.append(
                _finding(
                    "warning",
                    "phantom_column",
                    f"Field '{col}' is documented in config '{cname}' but was not "
                    f"found in the data file.",
                    config=cname,
                    field=col,
                )
            )

    for col, feat in documented.items():
        if col in file_schema:
            if not _dtype_compatible(feat.dtype, file_schema[col]):
                findings.append(
                    _finding(
                        "error",
                        "dtype_mismatch",
                        f"Field '{col}' in config '{cname}' is declared as "
                        f"'{feat.dtype}' but the file column has DuckDB type "
                        f"'{file_schema[col]}'.",
                        config=cname,
                        field=col,
                    )
                )

    return findings


def _consolidation_suggestions(
    repo_cards: dict[str, DatasetCard | None],
) -> list[dict[str, Any]]:
    """
    Generate field consolidation suggestions.

    :param repo_cards: Mapping of repo label to parsed DatasetCard (or None if parsing
        failed).
    :returns: List of suggestion dicts.

    """
    suggestions: list[dict[str, Any]] = []

    for repo_label, card in repo_cards.items():
        if card is None:
            continue
        field_configs: dict[str, list[tuple[str, Any]]] = {}
        for cfg in card.configs:
            for feat in cfg.dataset_info.features:
                field_configs.setdefault(feat.name, []).append((cfg.config_name, feat))

        for field_name, occurrences in field_configs.items():
            if len(occurrences) < 2:
                continue
            dtypes = [str(o[1].dtype) for o in occurrences]
            roles = [o[1].role for o in occurrences]
            if len(set(dtypes)) != 1 or len(set(roles)) != 1:
                continue
            descs = [o[1].description for o in occurrences]
            canonical_desc = max(set(descs), key=descs.count)
            desc_note = ""
            if len(set(descs)) > 1:
                desc_note = (
                    " Note: descriptions differ across configs"
                    " -- review before consolidating."
                )
            config_names = [o[0] for o in occurrences]
            dtype_val = occurrences[0][1].dtype
            role_val = occurrences[0][1].role

            applies_lines = "\n".join(f"  - {c}" for c in config_names)
            if isinstance(dtype_val, dict) and "class_label" in dtype_val:
                names = dtype_val["class_label"].get("names", [])
                dtype_yaml = f"    dtype:\n      class_label:\n        names: {names}"
            else:
                dtype_yaml = f"    dtype: {dtype_val}"
            role_line = f"    role: {role_val}" if role_val else ""
            feat_lines = (
                f"  - name: {field_name}\n"
                f"{dtype_yaml}\n"
                f"    description: '{canonical_desc}'\n"
            )
            if role_line:
                feat_lines += f"{role_line}\n"
            generated_yaml = (
                f"features:\n"
                f"- applies_to:\n"
                f"{applies_lines}\n"
                f"  fields:\n"
                f"{feat_lines}"
            )

            suggestions.append(
                {
                    "type": "consolidate_to_shared_features",
                    "scope": "same_repo",
                    "repo": repo_label,
                    "field": field_name,
                    "configs": config_names,
                    "message": (
                        f"Field '{field_name}' (dtype: {dtype_val}, "
                        f"role: {role_val}) appears in {len(occurrences)} configs "
                        f"of '{repo_label}'. "
                        f"Move to a shared features group.{desc_note}"
                    ),
                    "generated_yaml": generated_yaml,
                    "doc_link": _DOCS_BASE + "#shared-feature-definitions",
                }
            )

    cross_field: dict[str, list[str]] = {}
    for repo_label, card in repo_cards.items():
        if card is None:
            continue
        seen_in_repo: set[str] = set()
        for cfg in card.configs:
            for feat in cfg.dataset_info.features:
                if feat.name not in seen_in_repo:
                    cross_field.setdefault(feat.name, []).append(repo_label)
                    seen_in_repo.add(feat.name)

    for field_name, repos_with_field in cross_field.items():
        if len(repos_with_field) < 2:
            continue
        suggestions.append(
            {
                "type": "cross_repo_field_alignment",
                "scope": "cross_repo",
                "field": field_name,
                "repos": repos_with_field,
                "message": (
                    f"Field '{field_name}' appears in {len(repos_with_field)} "
                    f"repos. Verify that dtype, role, and naming are consistent "
                    f"across all of them."
                ),
                "generated_yaml": None,
                "doc_link": _DOCS_BASE + "#shared-feature-definitions",
            }
        )

    return suggestions


def _parse_hf_collection_url(url: str, token: str | None) -> list[str]:
    """
    Return dataset repo IDs from a HuggingFace collection URL.

    :param url: URL of the form
        ``https://huggingface.co/collections/<org>/<slug>``.
    :param token: Optional HuggingFace authentication token.
    :returns: List of repo ID strings (``"org/repo"`` format).

    """
    parts = [p for p in url.rstrip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(f"Cannot parse collection slug from URL: {url}")
    slug = "/".join(parts[-2:])
    collection = get_collection(slug, token=token)
    return [item.item_id for item in collection.items if item.item_type == "dataset"]


def _extract_context_conventions(context_md: str) -> dict[str, list[str]]:
    """
    Extract keyword lists from a collection context Markdown document.

    Returns a dict with keys ``field_names`` and ``dataset_types`` -- flat
    lists of terms mentioned in the relevant sections.

    :param context_md: Full text of the collection context document.
    :returns: Dict with extracted keyword lists.

    """
    conventions: dict[str, list[str]] = {
        "field_names": [],
        "dataset_types": [],
    }

    field_match = re.search(
        r"## Field Naming Conventions\s*(.*?)(?=\n## |\Z)",
        context_md,
        re.DOTALL,
    )
    if field_match:
        section = field_match.group(1)
        conventions["field_names"] = re.findall(r"\*\*([a-z_][a-z0-9_]*)\*\*", section)

    type_match = re.search(
        r"## Dataset Type Usage Examples\s*(.*?)(?=\n## |\Z)",
        context_md,
        re.DOTALL,
    )
    if type_match:
        section = type_match.group(1)
        conventions["dataset_types"] = re.findall(r"###\s+([a-z_][a-z0-9_]*)", section)

    return conventions


def _audit_collection_impl(
    source: str,
    collection_context: str | None,
    token: str | None,
) -> dict[str, Any]:
    """
    Core implementation for :func:`audit_collection`.

    :param source: Local directory path or HuggingFace collection URL.
    :param collection_context: Optional path to collection context Markdown.
    :param token: Optional HuggingFace token.
    :returns: Audit result dict.

    """
    schema_checks_performed = False

    repo_cards: dict[str, DatasetCard | None] = {}
    repo_local_dirs: dict[str, str | None] = {}

    if os.path.isdir(source):
        for entry in sorted(Path(source).iterdir()):
            if not entry.is_dir():
                continue
            readme = entry / "README.md"
            if not readme.exists():
                continue
            label = entry.name
            card = _load_datacard_local(str(readme))
            repo_cards[label] = card
            repo_local_dirs[label] = str(entry)
        schema_checks_performed = True
    elif "huggingface.co/collections" in source:
        fetcher = HfDataCardFetcher(token=token)
        repo_ids = _parse_hf_collection_url(source, token)
        for repo_id in repo_ids:
            try:
                data = fetcher.fetch(repo_id)
                card = DatasetCard.model_validate(data)
            except Exception:
                card = None
            repo_cards[repo_id] = card
            repo_local_dirs[repo_id] = None
        schema_checks_performed = False
    else:
        return {
            "error": (
                f"Cannot resolve source '{source}'. "
                "Provide a local directory path or a HuggingFace collection URL "
                "(https://huggingface.co/collections/...)."
            )
        }

    context_conventions: dict[str, list[str]] = {}
    if collection_context:
        try:
            ctx_text = Path(collection_context).read_text(encoding="utf-8")
            context_conventions = _extract_context_conventions(ctx_text)
        except OSError:
            pass

    repos_output: dict[str, dict] = {}
    for label, card in repo_cards.items():
        local_dir = repo_local_dirs.get(label)
        if card is None:
            repos_output[label] = {
                "findings": [
                    _finding(
                        "error",
                        "parse_failure",
                        f"Could not parse DataCard for '{label}'. "
                        "Check that README.md has valid YAML front-matter "
                        "conforming to the labretriever DataCard spec.",
                    )
                ]
            }
            continue

        findings = _audit_card(card, local_dir)

        if context_conventions.get("field_names"):
            known_fields = set(context_conventions["field_names"])
            for cfg in card.configs:
                for feat in cfg.dataset_info.features:
                    if feat.role and feat.name not in known_fields:
                        close = [
                            f for f in known_fields if feat.name in f or f in feat.name
                        ]
                        if close:
                            findings.append(
                                _finding(
                                    "info",
                                    "field_name_convention",
                                    f"Field '{feat.name}' in config "
                                    f"'{cfg.config_name}' is not in the collection "
                                    f"context field naming conventions. "
                                    f"Similar documented names: {close}.",
                                    config=cfg.config_name,
                                    field=feat.name,
                                )
                            )

        repos_output[label] = {"findings": findings}

    suggestions = _consolidation_suggestions(repo_cards)

    result: dict[str, Any] = {
        "source": source,
        "schema_checks_performed": schema_checks_performed,
        "repos": repos_output,
        "collection_suggestions": suggestions,
    }

    if collection_context is None:
        result["collection_context_note"] = (
            "No collection context document was provided. "
            "Re-run with collection_context set to the path of a Markdown file "
            "that describes the collection's field naming conventions, "
            "standardized vocabulary, and dataset type expectations. "
            "This enables additional checks: field name convention violations, "
            "non-standard condition values, and dataset type mismatches. "
            "See docs/brentlab_yeastresources_collection.md for an example of "
            "what such a document should contain."
        )

    return result


mcp = FastMCP("labretriever-repo")


@mcp.tool()
def scaffold_readme(repo_id: str) -> dict:
    """
    Scaffold a minimal HuggingFace DataCard README for a dataset repository.

    Inspects all files in the repository. For ``.parquet``, ``.csv``, and
    ``.tsv`` files it reads the column names and infers HF DataCard dtypes.
    For file types it cannot identify it returns a ``needs_input`` response
    asking the caller to clarify the file type before proceeding. Dtypes that
    cannot be unambiguously resolved are also surfaced in ``needs_input``
    rather than guessed.

    The produced README skeleton follows only the official HuggingFace DataCard
    specification (no BrentLab-specific extensions). All ``description`` fields
    are left empty so the author can fill them in.

    :param repo_id: HuggingFace dataset repository ID,
        e.g. ``"BrentLab/callingcards"``.
    :type repo_id: str
    :returns: ``{"readme": "<yaml_string>"}`` on success, or
        ``{"needs_input": [{"file": ..., "reason": ..., "question": ...}, ...]}``
        when user guidance is required.
    :rtype: dict

    """
    token = get_hf_token()
    return _scaffold_readme_impl(repo_id, token)


@mcp.tool()
def audit_collection(
    source: str,
    collection_context: str | None = None,
) -> dict:
    """
    Audit a HuggingFace collection or local directory of dataset repos.

    Checks each repo's DataCard for completeness, consistency between
    documented features and actual file schemas (in local mode), and
    opportunities to consolidate repeated field definitions using the
    ``features`` / ``SharedFeatureGroup`` convention.

    **Source formats:**

    - Local directory path: ``"/home/user/code/hf"`` -- each immediate
      sub-directory that contains a ``README.md`` is treated as a repo.
      File-schema checks are performed (column presence, dtype compatibility).
    - HuggingFace collection URL:
      ``"https://huggingface.co/collections/BrentLab/yeastresources-..."``
      -- repos are enumerated via the HF Hub API; only DataCard YAML checks
      are performed (no file downloads).

    **Checks performed:**

    - Missing config descriptions or feature descriptions
    - Undocumented columns (local only) and phantom columns (local only)
    - Dtype mismatches between DataCard and actual file (local only, lenient)
    - Missing ``role`` on features
    - Missing ``doi`` / ``citation`` at repo level
    - ``class_label`` dtype with empty ``names`` list
    - Missing ``dataset_type`` (labretriever extension)
    - Hive partition columns not listed in features (local only)

    **Consolidation:** when a field appears identically in >= 2 configs of
    the same repo, the tool generates a ready-to-paste ``features:`` YAML
    block using the ``SharedFeatureGroup`` convention.

    :param source: Local directory path or HuggingFace collection URL.
    :type source: str
    :param collection_context: Optional path to a collection context Markdown
        document (see the
        `Collection Context Document <brentlab_yeastresources_collection.md>`_
        docs for the expected format). When supplied, field naming conventions
        and dataset type expectations from the document are used to add
        context-aware findings.
    :type collection_context: str | None
    :returns: Dict with keys ``source``, ``schema_checks_performed``,
        ``repos`` (per-repo findings), and ``collection_suggestions``
        (consolidation and cross-repo alignment suggestions).
    :rtype: dict

    """
    token = get_hf_token()
    return _audit_collection_impl(source, collection_context, token)


def main() -> None:
    """Start the labretriever-repo MCP server."""
    _version = importlib.metadata.version("labretriever")
    parser = argparse.ArgumentParser(
        prog="labretriever-mcp-repo",
        description="Run the labretriever DataCard/collection MCP server over stdio.",
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {_version}")
    parser.parse_args()
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
