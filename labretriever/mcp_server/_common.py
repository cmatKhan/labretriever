"""Shared constants and helpers for both MCP servers."""

from __future__ import annotations

# DuckDB type families used by audit_collection
_NUMERIC_DUCKDB = {
    "DOUBLE",
    "FLOAT",
    "BIGINT",
    "INTEGER",
    "HUGEINT",
    "UBIGINT",
    "UINTEGER",
    "SMALLINT",
    "TINYINT",
    "USMALLINT",
    "UTINYINT",
    "FLOAT32",
    "FLOAT64",
    "INT8",
    "INT16",
    "INT32",
    "INT64",
}
_STRING_DUCKDB = {"VARCHAR", "TEXT", "STRING", "CHAR", "BLOB"}
_BOOL_DUCKDB = {"BOOLEAN", "BOOL"}

# HF dtype families
_HF_NUMERIC_DTYPES = {
    "float64",
    "float32",
    "int64",
    "int32",
    "int16",
    "int8",
    "uint64",
    "uint32",
    "uint16",
    "uint8",
    "float",
    "int",
    "double",
}
_HF_STRING_DTYPES = {"string", "large_string", "utf8"}
_HF_BOOL_DTYPES = {"bool"}

# DuckDB type -> HF DataCard dtype (used by scaffold_readme)
_DUCKDB_DTYPE_MAP: dict[str, str] = {
    "DOUBLE": "float64",
    "FLOAT": "float32",
    "BIGINT": "int64",
    "INTEGER": "int32",
    "HUGEINT": "int64",
    "UBIGINT": "int64",
    "UINTEGER": "int32",
    "SMALLINT": "int32",
    "TINYINT": "int32",
    "USMALLINT": "int32",
    "UTINYINT": "int32",
    "BOOLEAN": "bool",
    "VARCHAR": "string",
    "DATE": "string",
    "TIMESTAMP": "string",
    "TIMESTAMP WITH TIME ZONE": "string",
    "INTERVAL": "string",
    "BLOB": "large_binary",
}


def _hf_dtype_family(dtype: str | dict) -> str:
    """
    Return the broad family of an HF DataCard dtype.

    :returns: One of ``"numeric"``, ``"string"``, ``"bool"``,
        ``"class_label"``, or ``"unknown"``.

    """
    if isinstance(dtype, dict):
        if "class_label" in dtype:
            return "class_label"
        return "unknown"
    lower = str(dtype).lower()
    if lower in _HF_NUMERIC_DTYPES:
        return "numeric"
    if lower in _HF_STRING_DTYPES:
        return "string"
    if lower in _HF_BOOL_DTYPES:
        return "bool"
    return "unknown"


def _duckdb_family(raw_type: str) -> str:
    """Return the broad family of a DuckDB raw type string."""
    upper = raw_type.upper().split("(")[0].strip()
    if upper in _NUMERIC_DUCKDB:
        return "numeric"
    if upper in _STRING_DUCKDB:
        return "string"
    if upper in _BOOL_DUCKDB:
        return "bool"
    return "unknown"


def _dtype_compatible(hf_dtype: str | dict, duckdb_raw: str) -> bool:
    """Return True if the DuckDB raw type is compatible with the HF dtype."""
    hf_fam = _hf_dtype_family(hf_dtype)
    db_fam = _duckdb_family(duckdb_raw)
    if hf_fam == "class_label":
        return db_fam in ("numeric", "string")
    if hf_fam == "unknown" or db_fam == "unknown":
        return True
    return hf_fam == db_fam
