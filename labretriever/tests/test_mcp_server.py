"""Tests for scaffold_readme and audit_collection MCP tools."""

from __future__ import annotations

import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

from labretriever.mcp_server._repo_server import (
    _audit_collection_impl,
    _scaffold_readme_impl,
)


def _make_file(path: str, size: int = 1000) -> dict:
    return {"path": path, "size": size, "is_lfs": False}


def _make_structure(files: list[dict]) -> dict:
    return {
        "repo_id": "test/repo",
        "files": files,
        "partitions": {},
        "total_files": len(files),
        "last_modified": None,
    }


class TestScaffoldReadme:
    REPO_ID = "test/repo"

    # ------------------------------------------------------------------
    # helpers that patch the two I/O points
    # ------------------------------------------------------------------

    def _run(
        self,
        files: list[dict],
        parquet_schema: list[tuple[str, str]] | None = None,
        parquet_data: dict[str, list] | None = None,
    ) -> dict:
        """
        Run _scaffold_readme_impl with mocked HF and DuckDB calls.

        *parquet_schema* is a list of (column, duckdb_type) pairs that DESCRIBE returns.
        *parquet_data* maps column name to list of values used to build a temporary
        parquet so DuckDB cardinality queries work.

        """
        structure = _make_structure(files)

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = structure

            conn = MagicMock()
            mock_duckdb.connect.return_value = conn

            if parquet_schema is not None:
                # DESCRIBE returns list of (col, type, ...) tuples
                describe_rows = [
                    (col, typ, "YES", None, None, None) for col, typ in parquet_schema
                ]
                # cardinality query: return (n_rows=1000, n_distinct=2) by default
                # so numeric ints with low cardinality become class_label
                count_row = (1000, 2)
                conn.execute.return_value.fetchall.return_value = describe_rows
                conn.execute.return_value.fetchone.return_value = count_row

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        return result

    # ------------------------------------------------------------------
    # tests
    # ------------------------------------------------------------------

    def test_single_flat_parquet(self):
        """Single flat parquet produces one config with correct features."""
        files = [_make_file("metadata.parquet")]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            describe_rows = [
                ("gene_id", "VARCHAR", "YES", None, None, None),
                ("score", "DOUBLE", "YES", None, None, None),
            ]
            conn.execute.return_value.fetchall.return_value = describe_rows
            # gene_id: 1000 rows, 500 distinct -> not categorical
            conn.execute.return_value.fetchone.side_effect = [
                (1000, 500),  # gene_id cardinality
                (1000, 500),  # score cardinality (not reached for DOUBLE but safe)
            ]

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        assert "readme" in result
        readme = result["readme"]
        assert "config_name: metadata" in readme
        assert "name: gene_id" in readme
        assert "dtype: string" in readme
        assert "name: score" in readme
        assert "dtype: float64" in readme
        assert "description: ''" in readme
        # path should be exact, not a glob
        assert "path: metadata.parquet" in readme

    def test_partitioned_parquet(self):
        """Partitioned parquet directory uses glob path and appends partition col."""
        files = [
            _make_file("counts/batch=A/0.parquet", size=2000),
            _make_file("counts/batch=B/0.parquet", size=1000),
        ]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            describe_rows = [("value", "DOUBLE", "YES", None, None, None)]
            conn.execute.return_value.fetchall.return_value = describe_rows
            conn.execute.return_value.fetchone.return_value = (1000, 500)

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        assert "readme" in result
        readme = result["readme"]
        assert "path: counts/**/*.parquet" in readme
        # partition column appended
        assert "name: batch" in readme

    def test_csv_schema_read(self, tmp_path):
        """CSV file has its schema read via pandas."""
        csv_content = "gene,count,condition\n" + "\n".join(
            [f"g{i},{i},cond_a" for i in range(200)]
        )
        files = [_make_file("data.csv")]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value=str(tmp_path / "data.csv"),
            ),
        ):
            (tmp_path / "data.csv").write_text(csv_content)
            MockFetcher.return_value.fetch.return_value = _make_structure(files)

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        assert "readme" in result
        readme = result["readme"]
        assert "name: gene" in readme
        assert "name: count" in readme
        # condition has 1 unique value in 200 rows -> class_label
        assert "name: condition" in readme
        assert "class_label" in readme

    def test_unknown_extension_returns_needs_input(self):
        """File with unknown extension returns needs_input before schema read."""
        files = [_make_file("data.hdf5"), _make_file("meta.parquet")]

        with patch(
            "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
        ) as MockFetcher:
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        assert "needs_input" in result
        questions = result["needs_input"]
        assert len(questions) == 1
        assert questions[0]["file"] == "data.hdf5"
        assert ".hdf5" in questions[0]["question"]

    def test_ambiguous_dtype_returns_needs_input(self):
        """Column with unrecognized DuckDB dtype surfaces in needs_input."""
        files = [_make_file("data.parquet")]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            describe_rows = [("vec", "FLOAT[3]", "YES", None, None, None)]
            conn.execute.return_value.fetchall.return_value = describe_rows
            conn.execute.return_value.fetchone.return_value = (1000, 500)

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        assert "needs_input" in result
        ni = result["needs_input"]
        assert len(ni) == 1
        assert "vec" in ni[0]["question"]
        assert "FLOAT[3]" in ni[0]["question"]

    def test_obvious_categorical_produces_class_label(self):
        """Integer column with few distinct values gets class_label dtype."""
        files = [_make_file("data.parquet")]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            describe_rows = [("treatment", "INTEGER", "YES", None, None, None)]
            conn.execute.return_value.fetchall.return_value = describe_rows
            # 1000 rows, 3 distinct values -> categorical
            conn.execute.return_value.fetchone.return_value = (1000, 3)

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        assert "readme" in result
        readme = result["readme"]
        assert "class_label" in readme
        assert "names: []" in readme

    def test_skip_extensions_ignored(self):
        """README.md, scripts, and config files are silently skipped."""
        files = [
            _make_file("README.md"),
            _make_file("scripts/process.py"),
            _make_file("data.parquet"),
        ]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            describe_rows = [("id", "VARCHAR", "YES", None, None, None)]
            conn.execute.return_value.fetchall.return_value = describe_rows
            conn.execute.return_value.fetchone.return_value = (1000, 500)

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        # No needs_input from .md or .py
        assert "readme" in result

    def test_empty_descriptions(self):
        """All description fields in the skeleton are empty strings."""
        files = [_make_file("table.parquet")]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            describe_rows = [
                ("gene_id", "VARCHAR", "YES", None, None, None),
                ("value", "DOUBLE", "YES", None, None, None),
            ]
            conn.execute.return_value.fetchall.return_value = describe_rows
            conn.execute.return_value.fetchone.return_value = (1000, 500)

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        readme = result["readme"]

        # Every description field must be empty string, never invented text
        for line in readme.splitlines():
            if "description:" in line:
                assert line.strip() in ("description: ''", "description: ''"), line

    def test_no_brentlab_extensions(self):
        """BrentLab-specific keys must not appear in the skeleton."""
        files = [_make_file("table.parquet")]

        with (
            patch(
                "labretriever.mcp_server._repo_server.HfRepoStructureFetcher"
            ) as MockFetcher,
            patch(
                "labretriever.mcp_server._repo_server.hf_hub_download",
                return_value="/tmp/fake.parquet",
            ),
            patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb,
        ):
            MockFetcher.return_value.fetch.return_value = _make_structure(files)
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            describe_rows = [("id", "VARCHAR", "YES", None, None, None)]
            conn.execute.return_value.fetchall.return_value = describe_rows
            conn.execute.return_value.fetchone.return_value = (1000, 500)

            result = _scaffold_readme_impl(self.REPO_ID, token=None)

        readme = result["readme"]
        for forbidden in ("dataset_type:", "annotated_features:", "genome_map:"):
            assert (
                forbidden not in readme
            ), f"BrentLab extension '{forbidden}' found in skeleton"


# ---------------------------------------------------------------------------
# Helpers shared by TestAuditCollection
# ---------------------------------------------------------------------------

_MINIMAL_README = textwrap.dedent(
    """\
    ---
    configs:
    - config_name: {config_name}
      description: {description}
      dataset_type: annotated_features
      data_files:
      - split: train
        path: {data_path}
      dataset_info:
        features:
    {features}
    ---
    # test
    """
)


def _feature_block(features: list[dict]) -> str:
    lines = []
    for f in features:
        lines.append(f"    - name: {f['name']}")
        lines.append(f"      dtype: {f['dtype']}")
        lines.append(f"      description: '{f.get('description', '')}'")
        if f.get("role"):
            lines.append(f"      role: {f['role']}")
    return "\n".join(lines)


def _write_readme(
    path: Path,
    config_name: str = "data",
    description: str = "A dataset",
    data_path: str = "data.parquet",
    features: list[dict] | None = None,
) -> None:
    if features is None:
        features = [
            {
                "name": "gene_id",
                "dtype": "string",
                "description": "A gene identifier",
                "role": "target_identifier",
            }
        ]
    feat_block = _feature_block(features)
    path.write_text(
        _MINIMAL_README.format(
            config_name=config_name,
            description=description,
            data_path=data_path,
            features=feat_block,
        ),
        encoding="utf-8",
    )


class TestAuditCollection:

    # ------------------------------------------------------------------
    # local-directory source
    # ------------------------------------------------------------------

    def test_invalid_source_returns_error(self):
        """Unrecognized source returns an error key."""
        result = _audit_collection_impl("not_a_dir_or_url", None, None)
        assert "error" in result

    def test_empty_directory_returns_empty_repos(self, tmp_path):
        """Directory with no README sub-dirs produces empty repos dict."""
        result = _audit_collection_impl(str(tmp_path), None, None)
        assert result["repos"] == {}
        assert result["schema_checks_performed"] is True

    def test_unparseable_readme_gives_parse_failure_finding(self, tmp_path):
        """Sub-dir with an invalid README gives a parse_failure finding."""
        repo_dir = tmp_path / "bad_repo"
        repo_dir.mkdir()
        (repo_dir / "README.md").write_text("# just markdown, no yaml front-matter")

        result = _audit_collection_impl(str(tmp_path), None, None)
        assert "bad_repo" in result["repos"]
        checks = [f["check"] for f in result["repos"]["bad_repo"]["findings"]]
        assert "parse_failure" in checks

    def test_missing_doi_and_citation_flagged_as_info(self, tmp_path):
        """Cards missing doi/citation get info-level findings."""
        repo_dir = tmp_path / "my_repo"
        repo_dir.mkdir()
        _write_readme(repo_dir / "README.md")

        result = _audit_collection_impl(str(tmp_path), None, None)
        findings = result["repos"]["my_repo"]["findings"]
        checks = {f["check"] for f in findings}
        assert "missing_doi" in checks
        assert "missing_citation" in checks
        severities = {f["check"]: f["severity"] for f in findings}
        assert severities["missing_doi"] == "info"
        assert severities["missing_citation"] == "info"

    def test_empty_description_flagged_as_warning(self, tmp_path):
        """Config with empty description produces a missing_description warning."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        _write_readme(repo_dir / "README.md", description="")

        result = _audit_collection_impl(str(tmp_path), None, None)
        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "missing_description" in checks

    def test_empty_feature_description_flagged(self, tmp_path):
        """Feature with empty description produces missing_feature_descriptions."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        feats = [
            {
                "name": "gene_id",
                "dtype": "string",
                "description": "",
                "role": "target_identifier",
            }
        ]
        _write_readme(repo_dir / "README.md", features=feats)

        result = _audit_collection_impl(str(tmp_path), None, None)
        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "missing_feature_descriptions" in checks

    def test_missing_role_flagged_as_warning(self, tmp_path):
        """Feature with no role field produces missing_role warning."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        feats = [
            {"name": "gene_id", "dtype": "string", "description": "A gene identifier"}
        ]
        _write_readme(repo_dir / "README.md", features=feats)

        result = _audit_collection_impl(str(tmp_path), None, None)
        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "missing_role" in checks

    def test_class_label_empty_names_flagged(self, tmp_path):
        """Feature with class_label and empty names list is flagged."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        # Write README manually for the class_label dtype
        readme_text = textwrap.dedent(
            """\
            ---
            configs:
            - config_name: data
              description: A dataset
              dataset_type: annotated_features
              data_files:
              - split: train
                path: data.parquet
              dataset_info:
                features:
                - name: condition
                  dtype:
                    class_label:
                      names: []
                  description: The condition
                  role: experimental_condition
            ---
            # test
            """
        )
        (repo_dir / "README.md").write_text(readme_text)

        result = _audit_collection_impl(str(tmp_path), None, None)
        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "class_label_empty_names" in checks

    def test_undocumented_column_detected(self, tmp_path):
        """Column in parquet file that is not in features triggers error."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        feats = [
            {
                "name": "gene_id",
                "dtype": "string",
                "description": "A gene id",
                "role": "target_identifier",
            }
        ]
        _write_readme(repo_dir / "README.md", data_path="data.parquet", features=feats)

        with patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb:
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            # File has gene_id AND an extra undocumented column
            conn.execute.return_value.fetchall.return_value = [
                ("gene_id", "VARCHAR", "YES", None, None, None),
                ("secret_col", "DOUBLE", "YES", None, None, None),
            ]
            # Create a dummy parquet file so glob finds it
            parquet = repo_dir / "data.parquet"
            parquet.write_bytes(b"")
            result = _audit_collection_impl(str(tmp_path), None, None)

        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "undocumented_column" in checks
        fields = [
            f["field"]
            for f in result["repos"]["repo"]["findings"]
            if f["check"] == "undocumented_column"
        ]
        assert "secret_col" in fields

    def test_phantom_column_detected(self, tmp_path):
        """Feature in DataCard not found in file triggers phantom_column warning."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        feats = [
            {
                "name": "gene_id",
                "dtype": "string",
                "description": "A gene id",
                "role": "target_identifier",
            },
            {
                "name": "ghost_col",
                "dtype": "float64",
                "description": "Does not exist in file",
                "role": "quantitative_measure",
            },
        ]
        _write_readme(repo_dir / "README.md", data_path="data.parquet", features=feats)

        with patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb:
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            # File only has gene_id
            conn.execute.return_value.fetchall.return_value = [
                ("gene_id", "VARCHAR", "YES", None, None, None),
            ]
            parquet = repo_dir / "data.parquet"
            parquet.write_bytes(b"")
            result = _audit_collection_impl(str(tmp_path), None, None)

        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "phantom_column" in checks

    def test_dtype_mismatch_detected(self, tmp_path):
        """VARCHAR column declared as float64 triggers dtype_mismatch."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        feats = [
            {
                "name": "score",
                "dtype": "float64",
                "description": "A score",
                "role": "quantitative_measure",
            }
        ]
        _write_readme(repo_dir / "README.md", data_path="data.parquet", features=feats)

        with patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb:
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            # File has score as VARCHAR -- incompatible with float64
            conn.execute.return_value.fetchall.return_value = [
                ("score", "VARCHAR", "YES", None, None, None),
            ]
            parquet = repo_dir / "data.parquet"
            parquet.write_bytes(b"")
            result = _audit_collection_impl(str(tmp_path), None, None)

        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "dtype_mismatch" in checks

    def test_compatible_dtype_not_flagged(self, tmp_path):
        """DOUBLE column declared as float64 is compatible and not flagged."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        feats = [
            {
                "name": "score",
                "dtype": "float64",
                "description": "A score",
                "role": "quantitative_measure",
            }
        ]
        _write_readme(repo_dir / "README.md", data_path="data.parquet", features=feats)

        with patch("labretriever.mcp_server._repo_server.duckdb") as mock_duckdb:
            conn = MagicMock()
            mock_duckdb.connect.return_value = conn
            conn.execute.return_value.fetchall.return_value = [
                ("score", "DOUBLE", "YES", None, None, None),
            ]
            parquet = repo_dir / "data.parquet"
            parquet.write_bytes(b"")
            result = _audit_collection_impl(str(tmp_path), None, None)

        checks = [f["check"] for f in result["repos"]["repo"]["findings"]]
        assert "dtype_mismatch" not in checks

    def test_same_repo_consolidation_generated(self, tmp_path):
        """Fields shared across >= 2 configs in one repo get a generated_yaml
        suggestion."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        readme_text = textwrap.dedent(
            """\
            ---
            configs:
            - config_name: config_a
              description: Config A
              dataset_type: annotated_features
              data_files:
              - split: train
                path: a.parquet
              dataset_info:
                features:
                - name: gene_id
                  dtype: string
                  description: Gene identifier
                  role: target_identifier
            - config_name: config_b
              description: Config B
              dataset_type: annotated_features
              data_files:
              - split: train
                path: b.parquet
              dataset_info:
                features:
                - name: gene_id
                  dtype: string
                  description: Gene identifier
                  role: target_identifier
                - name: score
                  dtype: float64
                  description: A score
                  role: quantitative_measure
            ---
            # test
            """
        )
        (repo_dir / "README.md").write_text(readme_text)

        result = _audit_collection_impl(str(tmp_path), None, None)
        same_repo = [
            s
            for s in result["collection_suggestions"]
            if s["scope"] == "same_repo" and s["field"] == "gene_id"
        ]
        assert len(same_repo) == 1
        s = same_repo[0]
        assert s["generated_yaml"] is not None
        assert "gene_id" in s["generated_yaml"]
        assert "applies_to" in s["generated_yaml"]
        assert set(s["configs"]) == {"config_a", "config_b"}

    def test_findings_include_doc_links(self, tmp_path):
        """All findings that have a known check include a non-null doc_link."""
        repo_dir = tmp_path / "repo"
        repo_dir.mkdir()
        feats = [{"name": "gene_id", "dtype": "string", "description": ""}]
        _write_readme(repo_dir / "README.md", features=feats)

        result = _audit_collection_impl(str(tmp_path), None, None)
        for finding in result["repos"]["repo"]["findings"]:
            if finding["check"] != "parse_failure":
                assert (
                    finding.get("doc_link") is not None
                ), f"check '{finding['check']}' has no doc_link"

    def test_schema_checks_false_for_hf_url(self):
        """HF collection URL source sets schema_checks_performed to False."""
        mock_item = MagicMock()
        mock_item.item_type = "dataset"
        mock_item.item_id = "org/repo"
        mock_collection = MagicMock()
        mock_collection.items = [mock_item]

        with (
            patch(
                "labretriever.mcp_server._repo_server.get_collection",
                return_value=mock_collection,
            ),
            patch(
                "labretriever.mcp_server._repo_server.HfDataCardFetcher"
            ) as MockFetcher,
        ):
            MockFetcher.return_value.fetch.return_value = {}
            result = _audit_collection_impl(
                "https://huggingface.co/collections/org/slug-abc123",
                None,
                None,
            )

        assert result["schema_checks_performed"] is False
