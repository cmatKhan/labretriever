"""
Tests for the SQL-first VirtualDB interface.

Uses local Parquet fixtures and monkeypatches ``_resolve_parquet_files``
and ``_cached_datacard`` so no network access is needed.

"""

from pathlib import Path
from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest
import yaml  # type: ignore

from labretriever.datacard import DatasetSchema
from labretriever.models import DatasetType, FeatureInfo, MetadataConfig
from labretriever.virtual_db import ColumnMeta, VirtualDB

# ------------------------------------------------------------------
# Fixtures
# ------------------------------------------------------------------


def _write_parquet(path: Path, df: pd.DataFrame) -> str:
    """Write a DataFrame to a Parquet file using DuckDB."""
    conn = duckdb.connect(":memory:")
    conn.execute(f"COPY (SELECT * FROM df) TO '{path}' (FORMAT PARQUET)")
    conn.close()
    return str(path)


@pytest.fixture()
def parquet_dir(tmp_path):
    """
    Create Parquet files for two primary datasets and one comparative.

    harbison has a ``condition`` column (like the real dataset) rather
    than ``carbon_source`` / ``temperature_celsius`` as raw columns.
    Those are derived from DataCard field definitions via config
    property mappings.

    kemmeren has no ``condition`` column; carbon_source and
    temperature_celsius come from config-level (path-only) mappings
    that resolve to constants from the DataCard.

    Returns dict mapping (repo_id, config_name) -> [parquet_path].

    """
    # harbison: 4 samples; samples 1-3 have 2 target measurements each,
    # sample 4 has 2 targets but condition "Unknown" has no definition
    # for carbon_source (tests missing_value_labels fallback)
    harbison_df = pd.DataFrame(
        {
            "sample_id": [1, 1, 2, 2, 3, 3, 4, 4],
            "regulator_locus_tag": [
                "YBR049C",
                "YBR049C",
                "YDR463W",
                "YDR463W",
                "YBR049C",
                "YBR049C",
                "YDR463W",
                "YDR463W",
            ],
            "regulator_symbol": [
                "REB1",
                "REB1",
                "STP1",
                "STP1",
                "REB1",
                "REB1",
                "STP1",
                "STP1",
            ],
            "condition": [
                "YPD",
                "YPD",
                "Galactose",
                "Galactose",
                "Acid",
                "Acid",
                "Unknown",
                "Unknown",
            ],
            "target_locus_tag": [
                "YAL001C",
                "YAL002W",
                "YAL001C",
                "YAL003W",
                "YAL002W",
                "YAL003W",
                "YAL001C",
                "YAL002W",
            ],
            "effect": [1.5, 0.8, 2.1, 0.3, 1.2, 0.9, 0.6, 1.0],
            "pvalue": [0.01, 0.4, 0.001, 0.9, 0.05, 0.3, 0.2, 0.7],
        }
    )
    # kemmeren: 2 samples, each with 2 targets = 4 rows
    # No condition column; carbon_source comes from path-only mapping
    kemmeren_df = pd.DataFrame(
        {
            "sample_id": [10, 10, 11, 11],
            "regulator_locus_tag": [
                "YBR049C",
                "YBR049C",
                "YDR463W",
                "YDR463W",
            ],
            "regulator_symbol": [
                "REB1",
                "REB1",
                "STP1",
                "STP1",
            ],
            "target_locus_tag": [
                "YAL001C",
                "YAL002W",
                "YAL001C",
                "YAL003W",
            ],
            "effect": [1.1, 0.7, 1.8, 0.5],
            "pvalue": [0.02, 0.5, 0.003, 0.7],
        }
    )
    dto_df = pd.DataFrame(
        {
            "binding_id": [
                "BrentLab/harbison;harbison_2004;1",
                "BrentLab/harbison;harbison_2004;2",
                "BrentLab/harbison;harbison_2004;3",
            ],
            "perturbation_id": [
                "BrentLab/kemmeren;kemmeren_2014;10",
                "BrentLab/kemmeren;kemmeren_2014;11",
                "BrentLab/kemmeren;kemmeren_2014;10",
            ],
            "dto_empirical_pvalue": [0.001, 0.05, 0.8],
            "dto_fdr": [0.01, 0.1, 0.9],
        }
    )

    files = {}
    h_path = tmp_path / "harbison.parquet"
    files[("BrentLab/harbison", "harbison_2004")] = [
        _write_parquet(h_path, harbison_df)
    ]

    k_path = tmp_path / "kemmeren.parquet"
    files[("BrentLab/kemmeren", "kemmeren_2014")] = [
        _write_parquet(k_path, kemmeren_df)
    ]

    d_path = tmp_path / "dto.parquet"
    files[("BrentLab/comp", "dto")] = [_write_parquet(d_path, dto_df)]

    return files


@pytest.fixture()
def config_path(tmp_path):
    """Create a YAML config file for the test datasets."""
    config = {
        "factor_aliases": {
            "carbon_source": {
                "glucose": ["D-glucose", "dextrose"],
                "galactose": ["D-galactose"],
            }
        },
        "missing_value_labels": {"carbon_source": "unspecified"},
        "repositories": {
            "BrentLab/harbison": {
                "dataset": {
                    "harbison_2004": {
                        "db_name": "harbison",
                        "sample_id": {"field": "sample_id"},
                        "regulator_locus_tag": {
                            "field": "regulator_locus_tag",
                        },
                        "regulator_symbol": {
                            "field": "regulator_symbol",
                        },
                        # field+path: derive from condition definitions
                        "carbon_source": {
                            "field": "condition",
                            "path": "media.carbon_source.compound",
                        },
                        "temperature_celsius": {
                            "field": "condition",
                            "path": "temperature_celsius",
                            "dtype": "numeric",
                        },
                        # field-only rename
                        "environmental_condition": {
                            "field": "condition",
                        },
                    }
                }
            },
            "BrentLab/kemmeren": {
                # repo-level path-only mappings (constants)
                # Paths include experimental_conditions prefix
                # to match real datacard model_extra structure
                "carbon_source": {
                    "path": ("experimental_conditions" ".media.carbon_source.compound"),
                },
                "temperature_celsius": {
                    "path": ("experimental_conditions" ".temperature_celsius"),
                    "dtype": "numeric",
                },
                "dataset": {
                    "kemmeren_2014": {
                        "db_name": "kemmeren",
                        "sample_id": {"field": "sample_id"},
                        "regulator_locus_tag": {
                            "field": "regulator_locus_tag",
                        },
                        "regulator_symbol": {
                            "field": "regulator_symbol",
                        },
                    }
                },
            },
            "BrentLab/comp": {
                "dataset": {
                    "dto": {
                        "dto_pvalue": {"field": "dto_empirical_pvalue"},
                        "dto_fdr": {"field": "dto_fdr"},
                        "links": {
                            "binding_id": [
                                [
                                    "BrentLab/harbison",
                                    "harbison_2004",
                                ],
                            ],
                            "perturbation_id": [
                                [
                                    "BrentLab/kemmeren",
                                    "kemmeren_2014",
                                ],
                            ],
                        },
                    }
                }
            },
        },
    }
    p = tmp_path / "config.yaml"
    with open(p, "w") as f:
        yaml.dump(config, f)
    return p


# metadata_fields per dataset (mirrors what the DataCard would return)
METADATA_FIELDS = {
    "harbison_2004": [
        "regulator_locus_tag",
        "regulator_symbol",
        "condition",
    ],
    "kemmeren_2014": [
        "regulator_locus_tag",
        "regulator_symbol",
    ],
}

# Field definitions from DataCard (condition field for harbison)
HARBISON_CONDITION_DEFS = {
    "YPD": {
        "temperature_celsius": 30,
        "media": {
            "carbon_source": [
                {"compound": "D-glucose"},
            ],
        },
    },
    "Galactose": {
        "temperature_celsius": 30,
        "media": {
            "carbon_source": [
                {"compound": "D-galactose"},
            ],
        },
    },
    "Acid": {
        "temperature_celsius": 30,
        "media": {
            "carbon_source": [
                {"compound": "D-glucose"},
            ],
        },
    },
}

# Experimental conditions from DataCard (kemmeren -- config-level)
KEMMEREN_EXP_CONDITIONS = {
    "temperature_celsius": 30,
    "media": {
        "carbon_source": [
            {"compound": "D-glucose"},
        ],
    },
}


def _make_mock_datacard(repo_id):
    """Create a mock DataCard for testing."""
    card = MagicMock()

    if repo_id == "BrentLab/harbison":
        config_mock = MagicMock()
        config_mock.metadata_fields = METADATA_FIELDS["harbison_2004"]
        card.get_config.return_value = config_mock
        card.get_field_definitions.return_value = HARBISON_CONDITION_DEFS
        card.get_experimental_conditions.return_value = {}
        card.get_metadata_fields.return_value = METADATA_FIELDS["harbison_2004"]
        card.get_metadata_config_name.return_value = None
        card.get_features.return_value = [
            FeatureInfo(
                name="condition",
                dtype="string",
                description="Experimental condition identifier",
                role="experimental_condition",
                definitions={
                    lv: {"description": f"{lv} condition"}
                    for lv in HARBISON_CONDITION_DEFS
                },
            ),
            FeatureInfo(
                name="regulator_locus_tag",
                dtype="string",
                description="Regulator locus tag",
                role="regulator_identifier",
            ),
            FeatureInfo(
                name="regulator_symbol",
                dtype="string",
                description="Regulator gene symbol",
            ),
        ]
        # Harbison: embedded metadata, condition is data col used for
        # derived properties; metadata_cols are the three metadata fields
        harbison_meta_cols = set(METADATA_FIELDS["harbison_2004"])
        harbison_data_cols = {
            "sample_id",
            "condition",
            "target_locus_tag",
            "effect",
            "pvalue",
        } - harbison_meta_cols
        card.get_data_col_names.return_value = {
            "sample_id",
            "regulator_locus_tag",
            "regulator_symbol",
            "condition",
            "target_locus_tag",
            "effect",
            "pvalue",
        }
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns=harbison_data_cols
            | {
                "sample_id",
                "condition",
                "target_locus_tag",
                "effect",
                "pvalue",
            },
            metadata_columns=harbison_meta_cols,
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )
    elif repo_id == "BrentLab/kemmeren":
        config_mock = MagicMock()
        config_mock.metadata_fields = METADATA_FIELDS["kemmeren_2014"]
        config_mock.model_extra = {}
        card.get_config.return_value = config_mock
        card.get_field_definitions.return_value = {}
        card.extract_metadata_schema.return_value = {"condition_fields": []}
        dataset_card_mock = MagicMock()
        dataset_card_mock.model_extra = {
            "experimental_conditions": KEMMEREN_EXP_CONDITIONS,
        }
        card.dataset_card = dataset_card_mock
        card.get_metadata_fields.return_value = METADATA_FIELDS["kemmeren_2014"]
        card.get_metadata_config_name.return_value = None
        card.get_features.return_value = [
            FeatureInfo(
                name="regulator_locus_tag",
                dtype="string",
                description="Regulator locus tag",
                role="regulator_identifier",
            ),
            FeatureInfo(
                name="regulator_symbol",
                dtype="string",
                description="Regulator gene symbol",
            ),
        ]
        kemmeren_meta_cols = set(METADATA_FIELDS["kemmeren_2014"])
        card.get_data_col_names.return_value = {
            "sample_id",
            "regulator_locus_tag",
            "regulator_symbol",
            "target_locus_tag",
            "effect",
            "pvalue",
        }
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={
                "sample_id",
                "target_locus_tag",
                "effect",
                "pvalue",
            },
            metadata_columns=kemmeren_meta_cols,
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )
    else:
        config_mock = MagicMock()
        config_mock.metadata_fields = None
        config_mock.dataset_type = DatasetType.COMPARATIVE
        card.get_config.return_value = config_mock
        card.get_field_definitions.return_value = {}
        card.get_experimental_conditions.return_value = {}
        card.get_metadata_fields.return_value = None
        card.get_metadata_config_name.return_value = None
        card.get_data_col_names.return_value = set()
        card.get_dataset_schema.return_value = None
        card.get_features.return_value = []

    return card


@pytest.fixture()
def vdb(config_path, parquet_dir, monkeypatch):
    """Return a VirtualDB with _resolve_parquet_files and _cached_datacard monkeypatched
    for local testing."""
    import labretriever.virtual_db as vdb_module

    def _fake_resolve(self, repo_id, config_name):
        return parquet_dir.get((repo_id, config_name), [])

    monkeypatch.setattr(VirtualDB, "_resolve_parquet_files", _fake_resolve)
    monkeypatch.setattr(
        vdb_module,
        "_cached_datacard",
        lambda repo_id, token=None: _make_mock_datacard(repo_id),
    )
    return VirtualDB(config_path)


# ------------------------------------------------------------------
# Tests: Initialisation and config
# ------------------------------------------------------------------


class TestVirtualDBConfig:
    """Tests for VirtualDB configuration loading."""

    def test_init_loads_config(self, config_path, monkeypatch):
        """Test that config loads without error."""
        monkeypatch.setattr(VirtualDB, "_load_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_validate_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_update_cache", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_register_all_views", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_build_column_metadata", lambda self: None)
        v = VirtualDB(config_path)
        assert v.config is not None
        assert v.token is None

    def test_init_with_token(self, config_path, monkeypatch):
        """Test token is stored."""
        monkeypatch.setattr(VirtualDB, "_load_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_validate_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_update_cache", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_register_all_views", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_build_column_metadata", lambda self: None)
        v = VirtualDB(config_path, token="tok123")
        assert v.token == "tok123"

    def test_init_missing_file(self):
        """Test FileNotFoundError for missing config."""
        with pytest.raises(FileNotFoundError):
            VirtualDB("/nonexistent/path.yaml")

    def test_repr(self, vdb):
        """Test repr shows repo, dataset, and view counts."""
        r = repr(vdb)
        assert "VirtualDB" in r
        assert "views)" in r

    def test_db_name_map(self, config_path, monkeypatch):
        """Test that db_name_map resolves db_name correctly."""
        monkeypatch.setattr(VirtualDB, "_load_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_validate_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_update_cache", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_register_all_views", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_build_column_metadata", lambda self: None)
        v = VirtualDB(config_path)
        assert "harbison" in v.db_name_map
        assert "kemmeren" in v.db_name_map
        assert "dto" in v.db_name_map
        assert v.db_name_map["harbison"] == (
            "BrentLab/harbison",
            "harbison_2004",
        )


    def test_get_dataset_description_known(self, vdb):
        """get_dataset_description returns the DataCard config description."""
        # The harbison mock card returns a MagicMock config; set description.
        card = vdb.datacards["BrentLab/harbison"]
        card.get_config.return_value.description = "Harbison 2004 ChIP-chip study"
        desc = vdb.get_dataset_description("harbison")
        assert desc == "Harbison 2004 ChIP-chip study"

    def test_get_dataset_description_unknown(self, vdb):
        """get_dataset_description returns None for an unknown db_name."""
        assert vdb.get_dataset_description("nonexistent") is None

    def test_get_dataset_description_no_description(self, vdb):
        """get_dataset_description returns None when the config description is None."""
        card = vdb.datacards["BrentLab/harbison"]
        card.get_config.return_value.description = None
        assert vdb.get_dataset_description("harbison") is None


# ------------------------------------------------------------------
# Tests: Tags
# ------------------------------------------------------------------


class TestTags:
    """Tests for get_tags() hierarchical merging."""

    def _make_config(self, yaml_str: str) -> MetadataConfig:
        import yaml as _yaml

        return MetadataConfig.model_validate(_yaml.safe_load(yaml_str))

    def test_repo_level_tags_only(self):
        """Repo-level tags propagate when dataset has none."""
        config = self._make_config(
            """
            repositories:
              BrentLab/harbison:
                tags:
                  assay: binding
                  organism: yeast
                dataset:
                  harbison_2004:
                    sample_id:
                      field: sample_id
            """
        )
        tags = config.get_tags("BrentLab/harbison", "harbison_2004")
        assert tags == {"assay": "binding", "organism": "yeast"}

    def test_dataset_level_tags_only(self):
        """Dataset-level tags are returned when repo has none."""
        config = self._make_config(
            """
            repositories:
              BrentLab/harbison:
                dataset:
                  harbison_2004:
                    sample_id:
                      field: sample_id
                    tags:
                      assay: chip-chip
            """
        )
        tags = config.get_tags("BrentLab/harbison", "harbison_2004")
        assert tags == {"assay": "chip-chip"}

    def test_dataset_overrides_repo_tags(self):
        """Dataset-level tags override repo-level for the same key."""
        config = self._make_config(
            """
            repositories:
              BrentLab/harbison:
                tags:
                  assay: binding
                  organism: yeast
                dataset:
                  harbison_2004:
                    sample_id:
                      field: sample_id
                    tags:
                      assay: chip-chip
            """
        )
        tags = config.get_tags("BrentLab/harbison", "harbison_2004")
        assert tags["assay"] == "chip-chip"
        assert tags["organism"] == "yeast"

    def test_no_tags(self):
        """Returns empty dict when neither level has tags."""
        config = self._make_config(
            """
            repositories:
              BrentLab/harbison:
                dataset:
                  harbison_2004:
                    sample_id:
                      field: sample_id
            """
        )
        tags = config.get_tags("BrentLab/harbison", "harbison_2004")
        assert tags == {}

    def test_unknown_repo_returns_empty(self):
        """Unknown repo_id returns empty dict."""
        config = self._make_config(
            """
            repositories:
              BrentLab/harbison:
                dataset:
                  harbison_2004:
                    sample_id:
                      field: sample_id
            """
        )
        assert config.get_tags("BrentLab/nonexistent", "harbison_2004") == {}

    def test_yaml_round_trip(self):
        """Tags parsed from YAML produce correct merged result."""
        config = self._make_config(
            """
            repositories:
              BrentLab/repo_a:
                tags:
                  type: primary
                  organism: yeast
                dataset:
                  dataset_a:
                    sample_id:
                      field: sample_id
                    tags:
                      type: binding
                      version: "2024"
              BrentLab/repo_b:
                tags:
                  type: perturbation
                dataset:
                  dataset_b:
                    sample_id:
                      field: sample_id
            """
        )
        tags_a = config.get_tags("BrentLab/repo_a", "dataset_a")
        assert tags_a == {"type": "binding", "organism": "yeast", "version": "2024"}

        tags_b = config.get_tags("BrentLab/repo_b", "dataset_b")
        assert tags_b == {"type": "perturbation"}

    def _make_vdb(self, yaml_str: str, tmp_path, monkeypatch) -> VirtualDB:
        monkeypatch.setattr(VirtualDB, "_load_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_validate_datacards", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_update_cache", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_register_all_views", lambda self: None)
        monkeypatch.setattr(VirtualDB, "_build_column_metadata", lambda self: None)
        p = tmp_path / "config.yaml"
        p.write_text(yaml_str)
        return VirtualDB(str(p))

    def test_vdb_get_tags_returns_merged(self, tmp_path, monkeypatch):
        """VirtualDB.get_tags() returns merged repo+dataset tags by db_name."""
        vdb = self._make_vdb(
            """
            repositories:
              BrentLab/harbison:
                tags:
                  assay: binding
                  organism: yeast
                dataset:
                  harbison_2004:
                    db_name: harbison
                    sample_id:
                      field: sample_id
                    tags:
                      assay: chip-chip
            """,
            tmp_path,
            monkeypatch,
        )
        tags = vdb.get_tags("harbison")
        assert tags == {"assay": "chip-chip", "organism": "yeast"}

    def test_vdb_get_tags_unknown_name_returns_empty(self, tmp_path, monkeypatch):
        """VirtualDB.get_tags() returns empty dict for unknown db_name."""
        vdb = self._make_vdb(
            """
            repositories:
              BrentLab/harbison:
                dataset:
                  harbison_2004:
                    db_name: harbison
                    sample_id:
                      field: sample_id
            """,
            tmp_path,
            monkeypatch,
        )
        assert vdb.get_tags("nonexistent") == {}

    def test_vdb_get_tags_no_views_needed(self, tmp_path, monkeypatch):
        """VirtualDB.get_tags() returns correct tags from config."""
        vdb = self._make_vdb(
            """
            repositories:
              BrentLab/harbison:
                tags:
                  assay: binding
                dataset:
                  harbison_2004:
                    db_name: harbison
                    sample_id:
                      field: sample_id
            """,
            tmp_path,
            monkeypatch,
        )
        tags = vdb.get_tags("harbison")
        assert tags == {"assay": "binding"}

    def test_vdb_get_datasets(self, tmp_path, monkeypatch):
        """VirtualDB.get_datasets() returns sorted db_names from config."""
        vdb = self._make_vdb(
            """
            repositories:
              BrentLab/harbison:
                dataset:
                  harbison_2004:
                    db_name: harbison
                    sample_id:
                      field: sample_id
              BrentLab/kemmeren:
                dataset:
                  kemmeren_2014:
                    db_name: kemmeren
                    sample_id:
                      field: sample_id
            """,
            tmp_path,
            monkeypatch,
        )
        assert vdb.get_datasets() == ["harbison", "kemmeren"]


# ------------------------------------------------------------------
# Tests: View registration
# ------------------------------------------------------------------


class TestViewRegistration:
    """Tests for view creation."""

    def test_raw_views_created(self, vdb):
        """Test that raw per-dataset views exist."""
        views = vdb.tables()
        assert "harbison" in views
        assert "kemmeren" in views
        # Comparative datasets only get _expanded, not a bare view
        assert "dto" not in views
        assert "dto_expanded" in views

    def test_raw_view_has_all_rows(self, vdb):
        """Test raw view returns measurement-level data."""
        df = vdb.query("SELECT COUNT(*) AS n FROM harbison")
        # 4 samples x 2 targets each = 8 rows
        assert df["n"].iloc[0] == 8

    def test_raw_view_has_measurement_columns(self, vdb):
        """Test raw view includes measurement columns."""
        fields = vdb.get_fields("harbison")
        assert "target_locus_tag" in fields
        assert "effect" in fields
        assert "pvalue" in fields

    def test_raw_view_has_condition_column(self, vdb):
        """Test harbison raw view has condition and derived columns."""
        fields = vdb.get_fields("harbison")
        assert "condition" in fields
        # Derived columns are available via join to _meta
        assert "carbon_source" in fields
        assert "temperature_celsius" in fields

    def test_meta_views_created(self, vdb):
        """Test that _meta views exist for primary datasets."""
        views = vdb.tables()
        assert "harbison_meta" in views
        assert "kemmeren_meta" in views
        # Comparative datasets should NOT have _meta views
        assert "dto_meta" not in views

    def test_meta_view_one_row_per_sample(self, vdb):
        """Test _meta view has one row per sample_id."""
        df = vdb.query("SELECT COUNT(*) AS n FROM harbison_meta")
        # 4 distinct samples
        assert df["n"].iloc[0] == 4

    def test_meta_view_excludes_measurement_columns(self, vdb):
        """Test _meta view has only metadata columns."""
        fields = vdb.get_fields("harbison_meta")
        assert "sample_id" in fields
        assert "regulator_locus_tag" in fields
        # Measurement columns should NOT be in _meta
        assert "target_locus_tag" not in fields
        assert "effect" not in fields
        assert "pvalue" not in fields

    def test_meta_view_has_derived_carbon_source(self, vdb):
        """Test harbison_meta has carbon_source from field+path."""
        fields = vdb.get_fields("harbison_meta")
        assert "carbon_source" in fields
        df = vdb.query(
            "SELECT sample_id, carbon_source " "FROM harbison_meta ORDER BY sample_id"
        )
        values = dict(zip(df["sample_id"], df["carbon_source"]))
        # YPD -> D-glucose -> glucose (aliased)
        assert values[1] == "glucose"
        # Galactose -> D-galactose -> galactose (aliased)
        assert values[2] == "galactose"
        # Acid -> D-glucose -> glucose (aliased)
        assert values[3] == "glucose"
        # Unknown -> no definition -> missing_value_labels fallback
        assert values[4] == "unspecified"

    def test_meta_view_has_derived_temperature(self, vdb):
        """Test harbison_meta has temperature_celsius from field+path."""
        fields = vdb.get_fields("harbison_meta")
        assert "temperature_celsius" in fields
        df = vdb.query(
            "SELECT DISTINCT temperature_celsius "
            "FROM harbison_meta "
            "WHERE temperature_celsius IS NOT NULL"
        )
        # Conditions with definitions have temperature_celsius=30;
        # "Unknown" has no definition so gets NULL
        assert len(df) == 1
        assert df["temperature_celsius"].iloc[0] == 30.0

    def test_meta_view_has_field_rename(self, vdb):
        """Test harbison_meta has environmental_condition alias."""
        fields = vdb.get_fields("harbison_meta")
        assert "environmental_condition" in fields
        df = vdb.query(
            "SELECT DISTINCT environmental_condition "
            "FROM harbison_meta ORDER BY environmental_condition"
        )
        values = sorted(df["environmental_condition"].tolist())
        assert values == ["Acid", "Galactose", "Unknown", "YPD"]

    def test_meta_view_path_only_constant(self, vdb):
        """Test kemmeren_meta has carbon_source from path-only."""
        fields = vdb.get_fields("kemmeren_meta")
        assert "carbon_source" in fields
        df = vdb.query("SELECT DISTINCT carbon_source FROM kemmeren_meta")
        # Constant resolved from experimental_conditions
        # D-glucose -> glucose (aliased)
        assert len(df) == 1
        assert df["carbon_source"].iloc[0] == "glucose"

    def test_meta_view_path_only_numeric(self, vdb):
        """Test kemmeren_meta has temperature_celsius as numeric."""
        df = vdb.query("SELECT DISTINCT temperature_celsius " "FROM kemmeren_meta")
        assert len(df) == 1
        assert df["temperature_celsius"].iloc[0] == 30.0

    def test_comparative_expanded_view(self, vdb):
        """Test that dto_expanded view is created."""
        views = vdb.tables()
        assert "dto_expanded" in views

    def test_expanded_view_has_parsed_columns(self, vdb):
        """Test that expanded view has _source and _id columns."""
        df = vdb.query("SELECT * FROM dto_expanded LIMIT 1")
        assert "binding_id_source" in df.columns
        assert "binding_id_id" in df.columns
        assert "perturbation_id_source" in df.columns
        assert "perturbation_id_id" in df.columns

    def test_expanded_view_source_aliased(self, vdb):
        """Test that _source columns use db_name aliases."""
        df = vdb.query("SELECT DISTINCT binding_id_source " "FROM dto_expanded")
        assert "harbison" in df["binding_id_source"].tolist()

    def test_expanded_view_perturbation_source_aliased(self, vdb):
        """Test that perturbation_id_source uses db_name alias."""
        df = vdb.query("SELECT DISTINCT perturbation_id_source " "FROM dto_expanded")
        assert "kemmeren" in df["perturbation_id_source"].tolist()

    def test_expanded_view_id_values(self, vdb):
        """Test that _id columns contain the sample_id component."""
        df = vdb.query(
            "SELECT DISTINCT binding_id_id " "FROM dto_expanded ORDER BY binding_id_id"
        )
        assert set(df["binding_id_id"]) == {"1", "2", "3"}


# ------------------------------------------------------------------
# Tests: Factor aliases in _meta views
# ------------------------------------------------------------------


class TestFactorAliases:
    """Tests that factor aliases are applied in _meta views."""

    def test_alias_applied_in_meta(self, vdb):
        """Test that aliases are applied at _meta level too."""
        df = vdb.query(
            "SELECT DISTINCT carbon_source " "FROM harbison_meta ORDER BY carbon_source"
        )
        values = df["carbon_source"].tolist()
        assert "glucose" in values
        assert "galactose" in values
        assert "D-glucose" not in values


# ------------------------------------------------------------------
# Tests: query() public API
# ------------------------------------------------------------------


class TestQuery:
    """Tests for the query() method."""

    def test_raw_sql(self, vdb):
        """Test basic SQL execution."""
        df = vdb.query("SELECT * FROM harbison WHERE sample_id = 1")
        # 2 rows: sample 1 has two target measurements
        assert len(df) == 2
        assert all(df["sample_id"] == 1)

    def test_parameterized_query(self, vdb):
        """Test query with named parameters."""
        df = vdb.query(
            "SELECT * FROM harbison WHERE sample_id = $sid",
            sid=1,
        )
        # 2 rows: sample 1 has two target measurements
        assert len(df) == 2
        assert all(df["sample_id"] == 1)

    def test_query_returns_dataframe(self, vdb):
        """Test that query always returns a DataFrame."""
        df = vdb.query("SELECT 1 AS x")
        assert isinstance(df, pd.DataFrame)


# ------------------------------------------------------------------
# Tests: prepare() and prepared queries
# ------------------------------------------------------------------


class TestPrepare:
    """Tests for the prepare() method."""

    def test_prepare_and_query(self, vdb):
        """Test registering and using a prepared query."""
        vdb.prepare(
            "by_condition",
            "SELECT * FROM harbison " "WHERE condition = $cond",
        )
        df = vdb.query("by_condition", cond="YPD")
        # 2 rows: sample 1 with YPD has 2 targets
        assert len(df) == 2
        assert all(df["condition"] == "YPD")

    def test_prepare_name_collision_with_view(self, vdb):
        """Test that prepare rejects names colliding with views."""
        with pytest.raises(ValueError, match="collides with"):
            vdb.prepare("harbison", "SELECT 1")

    def test_prepare_overwrite(self, vdb):
        """Test that re-preparing the same name overwrites."""
        vdb.prepare("q1", "SELECT 1 AS x")
        vdb.prepare("q1", "SELECT 2 AS x")
        df = vdb.query("q1")
        assert df["x"].iloc[0] == 2


# ------------------------------------------------------------------
# Tests: tables() and describe()
# ------------------------------------------------------------------


class TestDiscovery:
    """Tests for tables(), describe(), get_fields()."""

    def test_tables_sorted(self, vdb):
        """Test that tables() returns sorted view names."""
        views = vdb.tables()
        assert views == sorted(views)

    def test_describe_single(self, vdb):
        """Test describe for a single view."""
        df = vdb.describe("harbison")
        assert "column_name" in df.columns
        assert "column_type" in df.columns
        assert "table" in df.columns
        assert all(df["table"] == "harbison")
        col_names = df["column_name"].tolist()
        assert "sample_id" in col_names
        assert "condition" in col_names

    def test_describe_all(self, vdb):
        """Test describe for all views."""
        df = vdb.describe()
        tables = df["table"].unique().tolist()
        assert "harbison" in tables
        assert "kemmeren" in tables

    def test_get_fields_single(self, vdb):
        """Test get_fields for a specific view."""
        fields = vdb.get_fields("harbison")
        assert "sample_id" in fields
        assert "condition" in fields
        assert fields == sorted(fields)

    def test_get_fields_all(self, vdb):
        """Test get_fields across all views."""
        fields = vdb.get_fields()
        assert "sample_id" in fields
        # comparative fields
        assert "dto_empirical_pvalue" in fields

    def test_get_common_fields(self, vdb):
        """Test common fields across primary _meta views."""
        common = vdb.get_common_fields()
        # Both harbison_meta and kemmeren_meta share these
        assert "sample_id" in common
        assert "carbon_source" in common
        assert "temperature_celsius" in common
        assert "regulator_locus_tag" in common


# ------------------------------------------------------------------
# Tests: get_nested_value helper
# ------------------------------------------------------------------


class TestGetNestedValue:
    """Tests for the get_nested_value module-level helper."""

    def test_simple_path(self):
        from labretriever.virtual_db import get_nested_value

        data = {"media": {"name": "YPD"}}
        assert get_nested_value(data, "media.name") == "YPD"

    def test_list_extraction(self):
        from labretriever.virtual_db import get_nested_value

        data = {
            "media": {
                "carbon_source": [
                    {"compound": "D-glucose"},
                ],
            },
        }
        result = get_nested_value(data, "media.carbon_source.compound")
        assert result == ["D-glucose"]

    def test_missing_key(self):
        from labretriever.virtual_db import get_nested_value

        assert get_nested_value({"a": 1}, "b") is None

    def test_deep_missing(self):
        from labretriever.virtual_db import get_nested_value

        assert get_nested_value({"a": {"b": 1}}, "a.c") is None

    def test_non_dict_input(self):
        from labretriever.virtual_db import get_nested_value

        assert get_nested_value("not a dict", "a.b") is None  # type: ignore


# ------------------------------------------------------------------
# Tests: edge cases
# ------------------------------------------------------------------


class TestEdgeCases:
    """Edge case and error handling tests."""

    def test_no_parquet_files(self, tmp_path, monkeypatch):
        """Test graceful handling when no parquet files are found."""
        import labretriever.virtual_db as vdb_module

        config = {
            "repositories": {
                "BrentLab/empty": {
                    "dataset": {
                        "empty_data": {
                            "sample_id": {"field": "sample_id"},
                        }
                    }
                }
            }
        }
        p = tmp_path / "config.yaml"
        with open(p, "w") as f:
            yaml.dump(config, f)

        def _fake_resolve(self, repo_id, config_name):
            return []

        monkeypatch.setattr(VirtualDB, "_resolve_parquet_files", _fake_resolve)
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: _make_mock_datacard(repo_id),
        )

        # Should not raise; just have no views
        v = VirtualDB(p)
        views = v.tables()
        assert "empty_data" not in views

    def test_links_with_non_comparative_dataset_type_raises(
        self, tmp_path, monkeypatch
    ):
        """Dataset with 'links' but datacard dataset_type != comparative raises
        ValueError."""
        import labretriever.virtual_db as vdb_module

        config = {
            "repositories": {
                "BrentLab/harbison": {
                    "dataset": {
                        "harbison_2004": {
                            "sample_id": {"field": "sample_id"},
                            "links": {
                                "sample_id": [["BrentLab/primary", "primary_data"]]
                            },
                        }
                    }
                }
            }
        }
        p = tmp_path / "config.yaml"
        with open(p, "w") as f:
            yaml.dump(config, f)

        non_comparative_card = _make_mock_datacard("BrentLab/harbison")
        cfg_mock = MagicMock()
        cfg_mock.dataset_type = DatasetType.ANNOTATED_FEATURES
        non_comparative_card.get_config.return_value = cfg_mock

        monkeypatch.setattr(VirtualDB, "_resolve_parquet_files", lambda *a: [])
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: non_comparative_card,
        )

        with pytest.raises(ValueError, match="comparative"):
            VirtualDB(p)


# ------------------------------------------------------------------
# Tests: dynamic sample_id column
# ------------------------------------------------------------------


class TestDynamicSampleId:
    """Tests that the sample identifier column is resolved from config."""

    def test_non_default_sample_id(self, tmp_path, monkeypatch):
        """Views work when sample_id maps to a non-default column."""
        import labretriever.virtual_db as vdb_module

        # Config uses experiment_id as the sample identifier
        config = {
            "repositories": {
                "TestOrg/custom_id": {
                    "dataset": {
                        "custom_data": {
                            "db_name": "custom",
                            "sample_id": {
                                "field": "experiment_id",
                            },
                            "regulator": {
                                "field": "regulator",
                            },
                        }
                    }
                }
            }
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        # Parquet uses experiment_id (not sample_id)
        df = pd.DataFrame(
            {
                "experiment_id": [100, 100, 200, 200],
                "regulator": ["TF1", "TF1", "TF2", "TF2"],
                "target": ["G1", "G2", "G1", "G2"],
                "score": [1.5, 0.8, 2.1, 0.3],
            }
        )
        parquet_path = tmp_path / "custom.parquet"
        files = {
            ("TestOrg/custom_id", "custom_data"): [_write_parquet(parquet_path, df)],
        }

        # Mock datacard
        mock_card = MagicMock()
        mock_card.get_metadata_fields.return_value = [
            "regulator",
        ]
        mock_card.get_field_definitions.return_value = {}
        mock_card.get_experimental_conditions.return_value = {}
        mock_card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"experiment_id", "target", "score"},
            metadata_columns={"regulator"},
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )

        monkeypatch.setattr(
            VirtualDB,
            "_resolve_parquet_files",
            lambda self, repo_id, cn: files.get((repo_id, cn), []),
        )
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: mock_card,
        )

        v = VirtualDB(config_path)

        # Meta view should rename experiment_id -> sample_id
        meta_df = v.query("SELECT * FROM custom_meta")
        assert "sample_id" in meta_df.columns
        assert "experiment_id" not in meta_df.columns
        assert list(meta_df["sample_id"]) == [100, 200] or set(
            meta_df["sample_id"]
        ) == {100, 200}
        assert len(meta_df) == 2  # 2 distinct samples

        # Enriched raw view should also expose sample_id
        raw_df = v.query("SELECT * FROM custom")
        assert "sample_id" in raw_df.columns
        assert "experiment_id" not in raw_df.columns
        assert len(raw_df) == 4  # all rows

    def test_non_default_sample_id_with_collision(self, tmp_path, monkeypatch):
        """When parquet has both gm_id (sample) and sample_id (other col), gm_id is
        renamed to sample_id and sample_id is preserved as sample_id_orig."""
        import labretriever.virtual_db as vdb_module

        config = {
            "repositories": {
                "TestOrg/collision": {
                    "dataset": {
                        "collision_data": {
                            "db_name": "collision",
                            "sample_id": {"field": "gm_id"},
                            "regulator": {"field": "regulator"},
                        }
                    }
                }
            }
        }
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config, f)

        # Parquet has gm_id (the real sample id) AND a literal sample_id col
        df = pd.DataFrame(
            {
                "gm_id": [1, 1, 2, 2],
                "sample_id": [101, 101, 102, 102],  # some other field
                "regulator": ["TF1", "TF1", "TF2", "TF2"],
                "target": ["G1", "G2", "G1", "G2"],
                "score": [1.0, 2.0, 3.0, 4.0],
            }
        )
        parquet_path = tmp_path / "collision.parquet"
        files = {
            ("TestOrg/collision", "collision_data"): [_write_parquet(parquet_path, df)],
        }

        mock_card = MagicMock()
        mock_card.get_metadata_fields.return_value = ["regulator"]
        mock_card.get_field_definitions.return_value = {}
        mock_card.get_experimental_conditions.return_value = {}
        mock_card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"gm_id", "sample_id", "target", "score"},
            metadata_columns={"regulator"},
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )

        monkeypatch.setattr(
            VirtualDB,
            "_resolve_parquet_files",
            lambda self, repo_id, cn: files.get((repo_id, cn), []),
        )
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: mock_card,
        )

        v = VirtualDB(config_path)

        # Meta view: gm_id -> sample_id, original sample_id -> sample_id_orig
        meta_df = v.query("SELECT * FROM collision_meta")
        assert "sample_id" in meta_df.columns
        assert "sample_id_orig" in meta_df.columns
        assert "gm_id" not in meta_df.columns
        assert set(meta_df["sample_id"]) == {1, 2}
        assert set(meta_df["sample_id_orig"]) == {101, 102}

        # Raw view same behavior
        raw_df = v.query("SELECT * FROM collision")
        assert "sample_id" in raw_df.columns
        assert "sample_id_orig" in raw_df.columns
        assert "gm_id" not in raw_df.columns
        assert len(raw_df) == 4

    def test_get_sample_id_field_dataset_level(self):
        """Dataset-level sample_id takes precedence."""
        config = MetadataConfig.model_validate(
            {
                "repositories": {
                    "Org/repo": {
                        "dataset": {
                            "ds": {
                                "sample_id": {
                                    "field": "my_id",
                                },
                            }
                        }
                    }
                }
            }
        )
        assert config.get_sample_id_field("Org/repo", "ds") == "my_id"

    def test_get_sample_id_field_repo_level(self):
        """Repo-level sample_id used when dataset has none."""
        config = MetadataConfig.model_validate(
            {
                "repositories": {
                    "Org/repo": {
                        "sample_id": {"field": "repo_sid"},
                        "dataset": {"ds": {}},
                    }
                }
            }
        )
        assert config.get_sample_id_field("Org/repo", "ds") == "repo_sid"

    def test_get_sample_id_field_default(self):
        """Falls back to 'sample_id' when not configured."""
        config = MetadataConfig.model_validate(
            {"repositories": {"Org/repo": {"dataset": {"ds": {}}}}}
        )
        assert config.get_sample_id_field("Org/repo", "ds") == "sample_id"

    def test_get_sample_id_field_dataset_overrides_repo(self):
        """Dataset-level overrides repo-level."""
        config = MetadataConfig.model_validate(
            {
                "repositories": {
                    "Org/repo": {
                        "sample_id": {"field": "repo_id_col"},
                        "dataset": {
                            "ds": {
                                "sample_id": {
                                    "field": "ds_id_col",
                                },
                            }
                        },
                    }
                }
            }
        )
        assert config.get_sample_id_field("Org/repo", "ds") == "ds_id_col"


class TestExternalMetadata:
    """Tests for datasets with external metadata parquet files."""

    def test_external_metadata_join(self, tmp_path, monkeypatch):
        """Meta view JOINs data and metadata parquet when metadata is in a separate
        config."""
        import labretriever.virtual_db as vdb_module

        # Data parquet: measurements with sample_id but no
        # metadata columns like db_id or batch
        data_df = pd.DataFrame(
            {
                "sample_id": [1, 1, 2, 2],
                "target_locus_tag": [
                    "YAL001C",
                    "YAL002W",
                    "YAL001C",
                    "YAL002W",
                ],
                "effect": [1.5, 0.8, 2.1, 0.3],
            }
        )
        # Metadata parquet: sample-level metadata
        meta_df = pd.DataFrame(
            {
                "sample_id": [1, 2],
                "db_id": [101, 102],
                "regulator_locus_tag": ["YBR049C", "YDR463W"],
                "background_hops": [500, 600],
            }
        )

        data_path = _write_parquet(tmp_path / "data.parquet", data_df)
        meta_path = _write_parquet(tmp_path / "meta.parquet", meta_df)

        parquet_files = {
            ("TestOrg/repo", "chip_data"): [data_path],
            ("TestOrg/repo", "sample_metadata"): [meta_path],
        }

        config = {
            "repositories": {
                "TestOrg/repo": {
                    "sample_id": {"field": "sample_id"},
                    "dataset": {
                        "chip_data": {
                            "db_name": "chip",
                            "regulator_locus_tag": {
                                "field": "regulator_locus_tag",
                            },
                        }
                    },
                }
            }
        }
        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        # Mock DataCard: external metadata via applies_to
        card = MagicMock()
        config_mock = MagicMock()
        config_mock.metadata_fields = None  # no embedded
        card.get_config.return_value = config_mock
        card.get_metadata_fields.return_value = [
            "sample_id",
            "db_id",
            "regulator_locus_tag",
            "background_hops",
        ]
        card.get_metadata_config_name.return_value = "sample_metadata"
        # Data parquet columns (from chip_data features)
        card.get_data_col_names.return_value = {
            "sample_id",
            "target_locus_tag",
            "effect",
        }
        card.get_field_definitions.return_value = {}
        card.get_experimental_conditions.return_value = {}
        # External metadata schema: data cols in data parquet,
        # metadata cols in metadata parquet, joined on sample_id
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"sample_id", "target_locus_tag", "effect"},
            metadata_columns={
                "sample_id",
                "db_id",
                "regulator_locus_tag",
                "background_hops",
            },
            join_columns={"sample_id"},
            metadata_source="external",
            external_metadata_config="sample_metadata",
            is_partitioned=False,
        )

        monkeypatch.setattr(
            VirtualDB,
            "_resolve_parquet_files",
            lambda self, repo_id, cfg: parquet_files.get((repo_id, cfg), []),
        )
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: card,
        )

        v = VirtualDB(config_file)
        tables = v.tables()
        assert "chip" in tables
        assert "chip_meta" in tables

        # Meta view should have columns from both parquets
        meta_result = v.query("SELECT * FROM chip_meta ORDER BY sample_id")
        meta_cols = set(meta_result.columns)
        assert "sample_id" in meta_cols
        assert "db_id" in meta_cols
        assert "regulator_locus_tag" in meta_cols
        assert "background_hops" in meta_cols

        # Verify data is correct (joined properly)
        assert len(meta_result) == 2
        row1 = meta_result[meta_result["sample_id"] == 1].iloc[0]
        assert row1["db_id"] == 101
        assert row1["regulator_locus_tag"] == "YBR049C"

        # Enriched raw view should also work
        raw_result = v.query("SELECT * FROM chip ORDER BY sample_id")
        assert "db_id" in raw_result.columns
        assert len(raw_result) == 4  # 4 data rows

    def test_spaced_alias_in_join_context(self, tmp_path, monkeypatch):
        """Field alias with spaces must be qualified (m.col) in a JOIN, not bare."""
        import labretriever.virtual_db as vdb_module

        data_df = pd.DataFrame(
            {"sample_id": [1, 1, 2, 2], "effect": [1.5, 0.8, 2.1, 0.3]}
        )
        meta_df = pd.DataFrame(
            {
                "sample_id": [1, 2],
                "regulator_locus_tag": ["YBR049C", "YDR463W"],
            }
        )
        data_path = _write_parquet(tmp_path / "data.parquet", data_df)
        meta_path = _write_parquet(tmp_path / "meta.parquet", meta_df)
        parquet_files = {
            ("TestOrg/repo", "chip_data"): [data_path],
            ("TestOrg/repo", "sample_metadata"): [meta_path],
        }

        config = {
            "repositories": {
                "TestOrg/repo": {
                    "sample_id": {"field": "sample_id"},
                    "dataset": {
                        "chip_data": {
                            "db_name": "chip",
                            # spaced alias mapped to a metadata column
                            "Regulator locus tag": {
                                "field": "regulator_locus_tag"
                            },
                        }
                    },
                }
            }
        }
        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        card = MagicMock()
        config_mock = MagicMock()
        config_mock.metadata_fields = None
        card.get_config.return_value = config_mock
        card.get_metadata_fields.return_value = ["sample_id", "regulator_locus_tag"]
        card.get_metadata_config_name.return_value = "sample_metadata"
        card.get_data_col_names.return_value = {"sample_id", "effect"}
        card.get_field_definitions.return_value = {}
        card.get_experimental_conditions.return_value = {}
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"sample_id", "effect"},
            metadata_columns={"sample_id", "regulator_locus_tag"},
            join_columns={"sample_id"},
            metadata_source="external",
            external_metadata_config="sample_metadata",
            is_partitioned=False,
        )
        card.get_features.return_value = [
            FeatureInfo(name="sample_id", dtype="int64", description="id"),
            FeatureInfo(
                name="regulator_locus_tag", dtype="string", description="reg"
            ),
        ]

        monkeypatch.setattr(
            VirtualDB,
            "_resolve_parquet_files",
            lambda self, repo_id, cfg: parquet_files.get((repo_id, cfg), []),
        )
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: card,
        )

        v = VirtualDB(config_file)
        df = v.query("SELECT * FROM chip_meta ORDER BY sample_id")
        assert "Regulator locus tag" in df.columns
        assert list(df["Regulator locus tag"]) == ["YBR049C", "YDR463W"]


# ------------------------------------------------------------------
# Tests: dtype='factor' (DuckDB ENUM)
# ------------------------------------------------------------------


class TestFactorDtype:
    """Tests for PropertyMapping dtype='factor' and DuckDB ENUM columns."""

    def _make_vdb_with_factor(self, tmp_path, monkeypatch, feature_dtype):
        """
        Helper: build a VirtualDB with one dataset whose 'category' field
        has a PropertyMapping with dtype='factor'. ``feature_dtype`` is
        passed as the FeatureInfo.dtype for the 'category' field in the
        mock DataCard.
        """
        import labretriever.virtual_db as vdb_module

        df = pd.DataFrame(
            {
                "sample_id": [1, 1, 2, 2],
                "category": ["A", "B", "A", "C"],
                "value": [1.0, 2.0, 3.0, 4.0],
            }
        )
        parquet_path = tmp_path / "data.parquet"
        files = {("TestOrg/ds", "cfg"): [_write_parquet(parquet_path, df)]}

        config = {
            "repositories": {
                "TestOrg/ds": {
                    "dataset": {
                        "cfg": {
                            "db_name": "ds",
                            "sample_id": {"field": "sample_id"},
                            "category": {
                                "field": "category",
                                "dtype": "factor",
                            },
                        }
                    }
                }
            }
        }
        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        card = MagicMock()
        card.get_metadata_fields.return_value = ["sample_id", "category"]
        card.get_field_definitions.return_value = {}
        card.get_experimental_conditions.return_value = {}
        card.get_metadata_config_name.return_value = None
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"sample_id", "category", "value"},
            metadata_columns={"sample_id", "category"},
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )
        feature_list = [
            FeatureInfo(
                name="category",
                dtype=feature_dtype,
                description="A categorical field",
            ),
            FeatureInfo(
                name="sample_id",
                dtype="int64",
                description="Sample identifier",
            ),
        ]
        card.get_features.return_value = feature_list

        monkeypatch.setattr(
            VirtualDB,
            "_resolve_parquet_files",
            lambda self, repo_id, cn: files.get((repo_id, cn), []),
        )
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: card,
        )
        return VirtualDB(config_file)

    def test_factor_dtype_creates_enum_column(self, tmp_path, monkeypatch):
        """Dtype='factor' casts the column to a DuckDB ENUM in the _meta view."""
        v = self._make_vdb_with_factor(
            tmp_path,
            monkeypatch,
            feature_dtype={"class_label": {"names": ["A", "B", "C"]}},
        )
        df = v.query("SELECT * FROM ds_meta ORDER BY sample_id")
        assert "category" in df.columns
        # Values should be preserved
        assert set(df["category"].dropna()) == {"A", "B", "C"}

    def test_factor_dtype_enum_type_registered(self, tmp_path, monkeypatch):
        """The DuckDB ENUM type is registered and can be queried."""
        v = self._make_vdb_with_factor(
            tmp_path,
            monkeypatch,
            feature_dtype={"class_label": {"names": ["A", "B", "C"]}},
        )
        # Trigger view registration
        v.tables()
        # The ENUM type should be registered in DuckDB
        types_df = v._conn.execute(
            "SELECT type_name FROM duckdb_types() WHERE logical_type = 'ENUM'"
        ).fetchdf()
        assert "_enum_category" in types_df["type_name"].tolist()

    def test_factor_dtype_missing_class_label_raises(self, tmp_path, monkeypatch):
        """ValueError is raised when the DataCard field has no class_label dtype."""
        with pytest.raises(ValueError, match="class_label"):
            v = self._make_vdb_with_factor(
                tmp_path,
                monkeypatch,
                feature_dtype="string",  # not a class_label dict
            )
            v.tables()  # triggers view registration

    def test_factor_dtype_no_names_raises(self, tmp_path, monkeypatch):
        """ValueError is raised when class_label has no 'names' key."""
        with pytest.raises(ValueError, match="names"):
            v = self._make_vdb_with_factor(
                tmp_path,
                monkeypatch,
                feature_dtype={"class_label": {}},  # no names
            )
            v.tables()

    def test_factor_dtype_model_validator_requires_field(self):
        """PropertyMapping with dtype='factor' and no field raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="factor"):
            from labretriever.models import PropertyMapping

            PropertyMapping.model_validate({"path": "some.path", "dtype": "factor"})

    def test_factor_dtype_model_validator_rejects_expression(self):
        """PropertyMapping with dtype='factor' and expression raises ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            from labretriever.models import PropertyMapping

            PropertyMapping.model_validate({"expression": "col > 0", "dtype": "factor"})

    def test_factor_dtype_inplace_renames_raw_to_orig(self, tmp_path, monkeypatch):
        """
        When dtype='factor' maps a field to the same output name (e.g.
        category: {field: category, dtype: factor}), the raw column is
        renamed to <col>_orig in the _meta view, and the ENUM-cast column
        keeps the original name.
        """
        v = self._make_vdb_with_factor(
            tmp_path,
            monkeypatch,
            feature_dtype={"class_label": {"names": ["A", "B", "C"]}},
        )
        df = v.query("SELECT * FROM ds_meta ORDER BY sample_id")
        # ENUM-cast column keeps the original name
        assert "category" in df.columns
        # Raw numeric/string original is preserved under _orig alias
        assert "category_orig" in df.columns
        # The _orig column should hold the raw values
        assert set(df["category_orig"].dropna()) == {"A", "B", "C"}

    def test_factor_dtype_orig_suffix_avoids_collision(self, tmp_path, monkeypatch):
        """When <col>_orig already exists in the parquet, the rename uses <col>_orig_1
        instead."""
        import labretriever.virtual_db as vdb_module

        df = pd.DataFrame(
            {
                "sample_id": [1, 2],
                "category": ["A", "B"],
                "category_orig": ["x", "y"],  # pre-existing _orig column
                "value": [1.0, 2.0],
            }
        )
        parquet_path = tmp_path / "data2.parquet"
        files = {("TestOrg/ds2", "cfg2"): [_write_parquet(parquet_path, df)]}

        config = {
            "repositories": {
                "TestOrg/ds2": {
                    "dataset": {
                        "cfg2": {
                            "db_name": "ds2",
                            "sample_id": {"field": "sample_id"},
                            "category": {
                                "field": "category",
                                "dtype": "factor",
                            },
                        }
                    }
                }
            }
        }
        config_file = tmp_path / "config2.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        card = MagicMock()
        card.get_metadata_fields.return_value = [
            "sample_id",
            "category",
            "category_orig",
        ]
        card.get_field_definitions.return_value = {}
        card.get_experimental_conditions.return_value = {}
        card.get_metadata_config_name.return_value = None
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"sample_id", "category", "category_orig", "value"},
            metadata_columns={"sample_id", "category", "category_orig"},
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )
        card.get_features.return_value = [
            FeatureInfo(
                name="category",
                dtype={"class_label": {"names": ["A", "B"]}},
                description="categorical",
            ),
            FeatureInfo(
                name="sample_id",
                dtype="int64",
                description="id",
            ),
        ]

        monkeypatch.setattr(
            VirtualDB,
            "_resolve_parquet_files",
            lambda self, repo_id, cn: files.get((repo_id, cn), []),
        )
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: card,
        )
        v = VirtualDB(config_file)

        result = v.query("SELECT * FROM ds2_meta ORDER BY sample_id")
        # Should use _orig_1 because _orig is taken
        assert "category_orig_1" in result.columns
        assert "category" in result.columns


class TestSpacedAliases:
    """Column alias keys containing spaces must produce valid SQL (issue #1)."""

    def _make_vdb(self, tmp_path, monkeypatch, config_extra: dict):
        """Helper: VirtualDB with a single dataset and extra property mappings."""
        import labretriever.virtual_db as vdb_module

        df = pd.DataFrame(
            {
                "sample_id": [1, 2],
                "regulator": ["GAL4", "MSN2"],
                "condition": ["YPD", "Heat"],
            }
        )
        parquet_path = tmp_path / "data.parquet"
        files = {("TestOrg/ds", "cfg"): [_write_parquet(parquet_path, df)]}

        config: dict = {
            "repositories": {
                "TestOrg/ds": {
                    "dataset": {
                        "cfg": {
                            "db_name": "ds",
                            "sample_id": {"field": "sample_id"},
                            **config_extra,
                        }
                    }
                }
            }
        }
        config_file = tmp_path / "config.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config, f)

        card = MagicMock()
        card.get_metadata_fields.return_value = ["sample_id", "regulator", "condition"]
        card.get_field_definitions.return_value = {}
        card.get_experimental_conditions.return_value = {}
        card.get_metadata_config_name.return_value = None
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"sample_id", "regulator", "condition"},
            metadata_columns={"sample_id", "regulator", "condition"},
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )
        card.get_features.return_value = [
            FeatureInfo(name="sample_id", dtype="int64", description="id"),
            FeatureInfo(name="regulator", dtype="string", description="reg"),
            FeatureInfo(name="condition", dtype="string", description="cond"),
        ]

        monkeypatch.setattr(
            VirtualDB,
            "_resolve_parquet_files",
            lambda self, repo_id, cn: files.get((repo_id, cn), []),
        )
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: card,
        )
        return VirtualDB(config_file)

    def test_field_alias_with_spaces(self, tmp_path, monkeypatch):
        """A field alias key containing spaces should create a valid meta view."""
        v = self._make_vdb(
            tmp_path,
            monkeypatch,
            config_extra={"Regulator locus tag": {"field": "regulator"}},
        )
        df = v.query('SELECT * FROM ds_meta ORDER BY sample_id')
        assert "Regulator locus tag" in df.columns
        assert list(df["Regulator locus tag"]) == ["GAL4", "MSN2"]

    def test_expression_alias_with_spaces(self, tmp_path, monkeypatch):
        """An expression mapping with a space-containing key should work."""
        v = self._make_vdb(
            tmp_path,
            monkeypatch,
            config_extra={
                "Growth condition": {"expression": "UPPER(condition)"},
            },
        )
        df = v.query('SELECT * FROM ds_meta ORDER BY sample_id')
        assert "Growth condition" in df.columns
        assert set(df["Growth condition"]) == {"HEAT", "YPD"}


# ------------------------------------------------------------------
# Tests: get_condition_field_info
# ------------------------------------------------------------------


class TestGetConditionFieldInfo:
    """Tests for VirtualDB.get_condition_field_info()."""

    def test_harbison_returns_linked_group(self, vdb):
        """Harbison has two field+path mappings from 'condition' — returns one group."""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            info = vdb.get_condition_field_info("harbison")
        assert info is not None
        assert "condition" in info
        group = info["condition"]
        assert set(group["property_cols"]) == {"carbon_source", "temperature_celsius"}

    def test_harbison_level_descriptions_present(self, vdb):
        """Level descriptions are populated from the mock DataCard."""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            info = vdb.get_condition_field_info("harbison")
        assert info is not None
        descs = info["condition"]["level_descriptions"]
        # The mock DataCard returns HARBISON_CONDITION_DEFS which have no
        # "description" key, so all levels fall back to the default string.
        for level in ("YPD", "Galactose", "Acid"):
            assert level in descs
            assert descs[level] == "Description unavailable"

    def test_harbison_level_descriptions_with_description_key(self, vdb, monkeypatch):
        """When a definition dict contains 'description', it is used verbatim."""
        defs_with_desc = {
            "YPD": {"description": "Rich media baseline", "temperature_celsius": 30},
            "Galactose": {"description": "Galactose carbon source"},
        }
        # Patch the mock DataCard's get_field_definitions for harbison
        card = vdb.datacards["BrentLab/harbison"]
        card.get_field_definitions.return_value = defs_with_desc

        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            info = vdb.get_condition_field_info("harbison")
        assert info is not None
        descs = info["condition"]["level_descriptions"]
        assert descs["YPD"] == "Rich media baseline"
        assert descs["Galactose"] == "Galactose carbon source"

    def test_kemmeren_returns_none(self, vdb):
        """Kemmeren only has path-only (constant) mappings — no linked group."""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            info = vdb.get_condition_field_info("kemmeren")
        assert info is None

    def test_unknown_db_name_returns_none(self, vdb):
        """An unrecognised dataset name returns None without raising."""
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            info = vdb.get_condition_field_info("nonexistent_dataset")
        assert info is None

    def test_datacard_missing_returns_group_without_descriptions(
        self, vdb, monkeypatch
    ):
        """When the DataCard is unavailable, property_cols are still returned."""
        # Remove the datacard so the method cannot fetch definitions.
        vdb.datacards.pop("BrentLab/harbison", None)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            info = vdb.get_condition_field_info("harbison")
        assert info is not None
        group = info["condition"]
        assert set(group["property_cols"]) == {"carbon_source", "temperature_celsius"}
        assert group["level_descriptions"] == {}

    def test_no_type_a_alias_returns_none(self, tmp_path, monkeypatch):
        """A dataset with no Type-A (field-only) alias mapping has no group."""
        import labretriever.virtual_db as vdb_module
        import yaml as _yaml

        config = {
            "repositories": {
                "BrentLab/test": {
                    "dataset": {
                        "test_ds": {
                            "db_name": "test_ds",
                            "sample_id": {"field": "sample_id"},
                            # Only one field+path mapping
                            "carbon_source": {
                                "field": "condition",
                                "path": "media.carbon_source.compound",
                            },
                        }
                    }
                }
            }
        }
        config_path = tmp_path / "cfg.yaml"
        with open(config_path, "w") as f:
            _yaml.dump(config, f)

        df = pd.DataFrame(
            {
                "sample_id": [1],
                "condition": ["YPD"],
                "target_locus_tag": ["YAL001C"],
                "effect": [1.0],
                "pvalue": [0.05],
            }
        )
        pq_path = tmp_path / "test_ds.parquet"
        _write_parquet(pq_path, df)

        card = MagicMock()
        card.get_config.return_value = MagicMock(
            metadata_fields=["sample_id", "condition"]
        )
        card.get_field_definitions.return_value = {}
        card.get_experimental_conditions.return_value = {}
        card.get_metadata_fields.return_value = ["sample_id", "condition"]
        card.get_metadata_config_name.return_value = None
        card.get_data_col_names.return_value = {
            "sample_id", "condition", "target_locus_tag", "effect", "pvalue"
        }
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"sample_id", "target_locus_tag", "effect", "pvalue"},
            metadata_columns={"sample_id", "condition"},
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )
        card.get_features.return_value = [
            FeatureInfo(name="sample_id", dtype="int64", description="id"),
            FeatureInfo(
                name="condition",
                dtype="string",
                description="Experimental condition",
                role="experimental_condition",
            ),
        ]

        def _fake_resolve(self, repo_id, config_name):
            return [str(pq_path)] if repo_id == "BrentLab/test" else []

        monkeypatch.setattr(VirtualDB, "_resolve_parquet_files", _fake_resolve)
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: card,
        )

        v = VirtualDB(config_path)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            assert v.get_condition_field_info("test_ds") is None

    def test_type_a_alias_with_type_c_only_returns_group(self, tmp_path, monkeypatch):
        """
        A dataset whose only derived columns are Type-C (repo-level, path-only) plus
        a Type-A condition alias (field-only) forms a valid group.

        This mirrors the chec_m2025 profile: Temperature is Type-C at the repo level,
        Experimental condition is Type-A at the dataset level.
        """
        import labretriever.virtual_db as vdb_module
        import yaml as _yaml

        config = {
            "repositories": {
                "BrentLab/test2": {
                    # Type-C: repo-level path-only mapping
                    "temperature_celsius": {
                        "path": "experimental_conditions.temperature_celsius",
                        "dtype": "string",
                    },
                    "dataset": {
                        "test_ds2": {
                            "db_name": "test_ds2",
                            "sample_id": {"field": "sample_id"},
                            "regulator_locus_tag": {"field": "regulator_locus_tag"},
                            # Type-A: condition alias (prop_col != pm.field)
                            "experimental_condition": {"field": "condition"},
                        }
                    },
                }
            }
        }
        config_path = tmp_path / "cfg2.yaml"
        with open(config_path, "w") as f:
            _yaml.dump(config, f)

        df = pd.DataFrame(
            {
                "sample_id": [1, 2],
                "condition": ["cond_A", "cond_B"],
                "regulator_locus_tag": ["YAL001C", "YAL002C"],
                "target_locus_tag": ["YBL001C", "YBL002C"],
                "effect": [1.0, -1.0],
                "pvalue": [0.01, 0.05],
            }
        )
        pq_path = tmp_path / "test_ds2.parquet"
        _write_parquet(pq_path, df)

        card = MagicMock()
        card.get_config.return_value = MagicMock(
            metadata_fields=["sample_id", "condition", "regulator_locus_tag"]
        )
        card.get_field_definitions.return_value = {
            "cond_A": {"description": "Condition A description"},
            "cond_B": {},
        }
        card.get_experimental_conditions.return_value = {}
        card.get_metadata_fields.return_value = [
            "sample_id", "condition", "regulator_locus_tag"
        ]
        card.get_metadata_config_name.return_value = None
        card.get_data_col_names.return_value = {
            "sample_id", "condition", "regulator_locus_tag",
            "target_locus_tag", "effect", "pvalue",
        }
        card.get_dataset_schema.return_value = DatasetSchema(
            data_columns={"sample_id", "target_locus_tag", "effect", "pvalue"},
            metadata_columns={"sample_id", "condition", "regulator_locus_tag"},
            join_columns=set(),
            metadata_source="embedded",
            external_metadata_config=None,
            is_partitioned=False,
        )
        card.get_features.return_value = [
            FeatureInfo(name="sample_id", dtype="int64", description="id"),
            FeatureInfo(
                name="condition",
                dtype="string",
                description="Experimental condition",
                role="experimental_condition",
                definitions={
                    "cond_A": {"description": "Condition A description"},
                    "cond_B": {},
                },
            ),
            FeatureInfo(
                name="regulator_locus_tag",
                dtype="string",
                description="Regulator locus tag",
                role="regulator_identifier",
            ),
        ]

        def _fake_resolve(self, repo_id, config_name):
            return [str(pq_path)] if repo_id == "BrentLab/test2" else []

        monkeypatch.setattr(VirtualDB, "_resolve_parquet_files", _fake_resolve)
        monkeypatch.setattr(
            vdb_module,
            "_cached_datacard",
            lambda repo_id, token=None: card,
        )

        v = VirtualDB(config_path)
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            info = v.get_condition_field_info("test_ds2")
        assert info is not None
        assert "condition" in info
        group = info["condition"]
        assert group["property_cols"] == ["temperature_celsius"]
        assert group["level_descriptions"]["cond_A"] == "Condition A description"
        assert group["level_descriptions"]["cond_B"] == "Description unavailable"


# ------------------------------------------------------------------
# Tests: get_column_metadata
# ------------------------------------------------------------------


class TestGetColumnMetadata:
    """Tests for VirtualDB.get_column_metadata()."""

    def test_unknown_db_name_returns_none(self, vdb):
        """Returns None for an unrecognised dataset name."""
        assert vdb.get_column_metadata("nonexistent") is None

    def test_returns_dict_for_harbison(self, vdb):
        """Returns a dict of ColumnMeta for a known primary dataset."""
        meta = vdb.get_column_metadata("harbison")
        assert isinstance(meta, dict)
        assert len(meta) > 0

    def test_condition_col_has_experimental_condition_role(self, vdb):
        """'condition' feature has role=experimental_condition."""
        meta = vdb.get_column_metadata("harbison")
        assert meta is not None
        assert "condition" in meta
        assert meta["condition"].role == "experimental_condition"

    def test_condition_col_has_level_definitions(self, vdb):
        """'condition' feature has level_definitions from DataCard definitions."""
        meta = vdb.get_column_metadata("harbison")
        assert meta is not None
        cond = meta["condition"]
        assert cond.level_definitions is not None
        # The mock DataCard definitions have a "description" key
        for level in ("YPD", "Galactose", "Acid"):
            assert level in cond.level_definitions
            assert cond.level_definitions[level] == f"{level} condition"

    def test_condition_col_description(self, vdb):
        """'condition' feature description is populated from FeatureInfo."""
        meta = vdb.get_column_metadata("harbison")
        assert meta is not None
        assert meta["condition"].description == "Experimental condition identifier"

    def test_non_condition_col_has_no_level_definitions(self, vdb):
        """A column without role=experimental_condition has level_definitions=None."""
        meta = vdb.get_column_metadata("harbison")
        assert meta is not None
        assert "regulator_locus_tag" in meta
        reg = meta["regulator_locus_tag"]
        assert reg.level_definitions is None
        assert reg.role == "regulator_identifier"

    def test_type_a_renamed_col_inherits_metadata(self, vdb):
        """A Type-A rename (environmental_condition -> condition) propagates ColumnMeta."""
        meta = vdb.get_column_metadata("harbison")
        assert meta is not None
        # 'environmental_condition' maps field=condition, so should inherit
        # condition's ColumnMeta
        assert "environmental_condition" in meta
        ec = meta["environmental_condition"]
        assert ec.role == "experimental_condition"
        assert ec.level_definitions is not None

    def test_kemmeren_no_condition_cols(self, vdb):
        """Kemmeren has no experimental_condition features — no level_definitions."""
        meta = vdb.get_column_metadata("kemmeren")
        assert meta is not None
        for col_meta in meta.values():
            assert col_meta.level_definitions is None

    def test_comparative_dataset_not_in_metadata(self, vdb):
        """Comparative datasets (dto) are excluded from column metadata."""
        assert vdb.get_column_metadata("dto") is None

    def test_condition_col_no_definitions_has_none_level_defs(self, vdb):
        """A condition col with no definitions dict has level_definitions=None."""
        # Override harbison features to return a condition col without definitions
        card = vdb.datacards["BrentLab/harbison"]
        card.get_features.return_value = [
            FeatureInfo(
                name="condition",
                dtype="string",
                description="Experimental condition identifier",
                role="experimental_condition",
                definitions=None,
            ),
        ]
        # Re-run _build_column_metadata to pick up the change
        vdb._build_column_metadata()
        meta = vdb.get_column_metadata("harbison")
        assert meta is not None
        assert meta["condition"].level_definitions is None
