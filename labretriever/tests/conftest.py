import pickle
from pathlib import Path
from unittest.mock import patch

import pytest


@pytest.fixture
def mock_cache_info():
    """Load real cache data from pickle file."""
    cache_file = Path(__file__).parent / "data" / "cache_info.pkl"

    if not cache_file.exists():
        pytest.skip(
            "test_cache_data.pkl not found. Run cache data generation script first."
        )

    with open(cache_file, "rb") as f:
        return pickle.load(f)


@pytest.fixture
def mock_scan_cache_dir(mock_cache_info):
    """Mock scan_cache_dir to return our pickled cache data."""
    with patch("huggingface_hub.scan_cache_dir", return_value=mock_cache_info):
        yield mock_cache_info


# ============================================================================
# Datainfo Fixtures (merged from tests/datainfo/conftest.py)
# ============================================================================


@pytest.fixture
def sample_dataset_card_data():
    """Sample dataset card data for testing."""
    return {
        "license": "mit",
        "language": ["en"],
        "tags": ["biology", "genomics", "yeast"],
        "pretty_name": "Test Genomics Dataset",
        "size_categories": ["100K<n<1M"],
        "configs": [
            {
                "config_name": "genomic_features",
                "description": "Gene annotations and regulatory features",
                "dataset_type": "genomic_features",
                "default": True,
                "data_files": [{"split": "train", "path": "features.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "gene_id",
                            "dtype": "string",
                            "description": "Systematic gene identifier",
                        },
                        {
                            "name": "gene_symbol",
                            "dtype": "string",
                            "description": "Standard gene symbol",
                        },
                        {
                            "name": "chromosome",
                            "dtype": "string",
                            "description": "Chromosome identifier",
                        },
                        {
                            "name": "start",
                            "dtype": "int64",
                            "description": "Gene start position",
                        },
                        {
                            "name": "end",
                            "dtype": "int64",
                            "description": "Gene end position",
                        },
                    ]
                },
            },
            {
                "config_name": "binding_data",
                "description": "Transcription factor binding measurements",
                "dataset_type": "annotated_features",
                "metadata_fields": ["regulator_symbol", "experimental_condition"],
                "data_files": [{"split": "train", "path": "binding/*.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "regulator_symbol",
                            "dtype": "string",
                            "description": "Transcription factor name",
                        },
                        {
                            "name": "target_gene",
                            "dtype": "string",
                            "description": "Target gene identifier",
                        },
                        {
                            "name": "experimental_condition",
                            "dtype": "string",
                            "description": "Experimental treatment condition",
                        },
                        {
                            "name": "binding_score",
                            "dtype": "float64",
                            "description": "Quantitative binding measurement",
                        },
                    ]
                },
            },
            {
                "config_name": "genome_map_data",
                "description": "Genome-wide signal tracks",
                "dataset_type": "genome_map",
                "data_files": [
                    {
                        "split": "train",
                        "path": "tracks/regulator=*/experiment=*/*.parquet",
                    }
                ],
                "dataset_info": {
                    "features": [
                        {
                            "name": "chr",
                            "dtype": "string",
                            "description": "Chromosome identifier",
                        },
                        {
                            "name": "pos",
                            "dtype": "int32",
                            "description": "Genomic position",
                        },
                        {
                            "name": "signal",
                            "dtype": "float32",
                            "description": "Signal intensity",
                        },
                    ],
                    "partitioning": {
                        "enabled": True,
                        "partition_by": ["regulator", "experiment"],
                    },
                },
            },
            {
                "config_name": "experiment_metadata",
                "description": "Experimental conditions and sample information",
                "dataset_type": "metadata",
                "applies_to": ["binding_data"],
                "data_files": [{"split": "train", "path": "metadata.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "sample_id",
                            "dtype": "string",
                            "description": "Unique sample identifier",
                        },
                        {
                            "name": "experimental_condition",
                            "dtype": "string",
                            "description": "Experimental treatment or condition",
                        },
                        {
                            "name": "publication_doi",
                            "dtype": "string",
                            "description": "DOI of associated publication",
                        },
                    ]
                },
            },
        ],
    }


@pytest.fixture
def minimal_dataset_card_data():
    """Minimal valid dataset card data."""
    return {
        "configs": [
            {
                "config_name": "test_config",
                "description": "Test configuration",
                "dataset_type": "genomic_features",
                "data_files": [{"split": "train", "path": "test.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "test_field",
                            "dtype": "string",
                            "description": "Test field",
                        }
                    ]
                },
            }
        ]
    }


@pytest.fixture
def invalid_dataset_card_data():
    """Invalid dataset card data for testing validation errors."""
    return {
        "configs": [
            {
                "config_name": "invalid_config",
                "description": "Invalid configuration",
                # Missing required dataset_type field
                "data_files": [{"split": "train", "path": "test.parquet"}],
                "dataset_info": {"features": []},  # Empty features list
            }
        ]
    }


@pytest.fixture
def sample_repo_structure():
    """Sample repository structure data."""
    return {
        "repo_id": "test/dataset",
        "files": [
            {"path": "features.parquet", "size": 2048000, "is_lfs": True},
            {"path": "binding/part1.parquet", "size": 1024000, "is_lfs": True},
            {
                "path": "tracks/regulator=TF1/experiment=exp1/data.parquet",
                "size": 5120000,
                "is_lfs": True,
            },
            {
                "path": "tracks/regulator=TF1/experiment=exp2/data.parquet",
                "size": 4096000,
                "is_lfs": True,
            },
            {
                "path": "tracks/regulator=TF2/experiment=exp1/data.parquet",
                "size": 3072000,
                "is_lfs": True,
            },
        ],
        "partitions": {"regulator": {"TF1", "TF2"}, "experiment": {"exp1", "exp2"}},
        "total_files": 5,
        "last_modified": "2023-12-01T10:30:00Z",
    }


@pytest.fixture
def sample_size_info():
    """Sample size information data."""
    return {
        "dataset": "test/dataset",
        "num_bytes": 15360000,
        "num_rows": 150000,
        "download_size": 12288000,
        "dataset_size": 15360000,
    }


@pytest.fixture
def mock_hf_card_fetcher():
    """Mock HfDataCardFetcher instance."""
    from unittest.mock import Mock

    mock_fetcher = Mock()
    mock_fetcher.fetch.return_value = {}
    return mock_fetcher


@pytest.fixture
def mock_hf_structure_fetcher():
    """Mock HfRepoStructureFetcher instance."""
    from unittest.mock import Mock

    mock_fetcher = Mock()
    mock_fetcher.fetch.return_value = {}
    mock_fetcher.get_partition_values.return_value = []
    mock_fetcher.get_dataset_files.return_value = []
    return mock_fetcher


@pytest.fixture
def mock_hf_size_fetcher():
    """Mock HfSizeInfoFetcher instance."""
    from unittest.mock import Mock

    mock_fetcher = Mock()
    mock_fetcher.fetch.return_value = {}
    return mock_fetcher


@pytest.fixture
def test_repo_id():
    """Standard test repository ID."""
    return "test/genomics-dataset"


@pytest.fixture
def test_token():
    """Test HuggingFace token."""
    return "test_hf_token_12345"


@pytest.fixture
def sample_feature_info():
    """Sample feature information for testing."""
    return {
        "name": "gene_symbol",
        "dtype": "string",
        "description": "Standard gene symbol (e.g., HO, GAL1)",
    }


@pytest.fixture
def sample_partitioning_info():
    """Sample partitioning information."""
    return {
        "enabled": True,
        "partition_by": ["regulator", "condition"],
        "path_template": "data/regulator={regulator}/condition={condition}/*.parquet",
    }


@pytest.fixture
def sample_data_file_info():
    """Sample data file information."""
    return {"split": "train", "path": "genomic_features.parquet"}


# ============================================================================
# Filter Resolver Fixtures
# ============================================================================


@pytest.fixture
def write_config(tmp_path):
    """
    Helper to write config dict to temp file.

    :param tmp_path: pytest tmp_path fixture
    :return: Function that writes config dict and returns Path

    """
    import yaml  # type: ignore[import-untyped]

    def _write(config_dict):
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(config_dict, f)
        return config_path

    return _write


# ============================================================================
# Datacard Fixtures (from huggingface_collection_datacards.txt)
# ============================================================================


# Datacard fixtures copied directly from huggingface_collection_datacards.txt
# These contain the complete YAML metadata for each repository


@pytest.fixture
def hackett_2020_datacard():
    """Complete hackett_2020 datacard YAML from huggingface_collection_datacards.txt."""
    return {
        "license": "mit",
        "language": ["en"],
        "tags": [
            "genomics",
            "yeast",
            "transcription",
            "perturbation",
            "response",
            "overexpression",
        ],
        "pretty_name": "Hackett, 2020 Overexpression",
        "size_categories": ["1M<n<10M"],
        "experimental_conditions": {
            "temperature_celsius": 30,
            "cultivation_method": "chemostat",
            "media": {
                "name": "minimal",
                "carbon_source": [
                    {"compound": "D-glucose", "concentration_percent": 1}
                ],
            },
        },
        "configs": [
            {
                "config_name": "hackett_2020",
                "description": "TF overexpression data from Hackett 2020",
                "default": True,
                "dataset_type": "annotated_features",
                "metadata_fields": [
                    "sample_id",
                    "regulator_locus_tag",
                    "regulator_symbol",
                    "time",
                    "mechanism",
                    "restriction",
                    "date",
                    "strain",
                ],
                "data_files": [{"split": "train", "path": "hackett_2020.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "sample_id",
                            "dtype": "integer",
                            "description": "unique identifier for a "
                            "specific sample. The "
                            "sample ID identifies a "
                            "unique "
                            "(regulator_locus_tag, "
                            "time, mechanism, "
                            "restriction, date, "
                            "strain) tuple.",
                        },
                        {
                            "name": "db_id",
                            "dtype": "integer",
                            "description": "an old unique identifer, "
                            "for use internally only. "
                            "Deprecated and will be "
                            "removed eventually. Do "
                            "not use in analysis. "
                            "db_id = 0, for GEV and "
                            "Z3EV, means that those "
                            "samples are not included "
                            "in the original DB.",
                        },
                        {
                            "name": "regulator_locus_tag",
                            "dtype": "string",
                            "description": "induced transcriptional "
                            "regulator systematic ID. "
                            "See "
                            "hf/BrentLab/yeast_genome_resources",
                            "role": "regulator_identifier",
                        },
                        {
                            "name": "regulator_symbol",
                            "dtype": "string",
                            "description": "induced transcriptional "
                            "regulator common name. If "
                            "no common name exists, "
                            "then the "
                            "`regulator_locus_tag` is "
                            "used.",
                            "role": "regulator_identifier",
                        },
                        {
                            "name": "target_locus_tag",
                            "dtype": "string",
                            "description": "The systematic ID of the "
                            "feature to which the "
                            "effect/pvalue is "
                            "assigned. See "
                            "hf/BrentLab/yeast_genome_resources",
                            "role": "target_identifier",
                        },
                        {
                            "name": "target_symbol",
                            "dtype": "string",
                            "description": "The common name of the "
                            "feature to which the "
                            "effect/pvalue is "
                            "assigned. If there is no "
                            "common name, the "
                            "`target_locus_tag` is "
                            "used.",
                            "role": "target_identifier",
                        },
                        {
                            "name": "time",
                            "dtype": "float",
                            "description": "time point (minutes)",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "mechanism",
                            "dtype": {"class_label": {"names": ["GEV", "ZEV"]}},
                            "description": "Synthetic TF induction "
                            "system (GEV or ZEV)",
                            "role": "experimental_condition",
                            "definitions": {
                                "GEV": {
                                    "perturbation_method": {
                                        "type": "inducible_overexpression",
                                        "system": "GEV",
                                        "inducer": "beta-estradiol",
                                        "description": "Galactose-inducible "
                                        "estrogen "
                                        "receptor-VP16 "
                                        "fusion "
                                        "system",
                                    }
                                },
                                "ZEV": {
                                    "perturbation_method": {
                                        "type": "inducible_overexpression",
                                        "system": "ZEV",
                                        "inducer": "beta-estradiol",
                                        "description": "Z3 "
                                        "(synthetic "
                                        "zinc "
                                        "finger)-estrogen "
                                        "receptor-VP16 "
                                        "fusion "
                                        "system",
                                    }
                                },
                            },
                        },
                        {
                            "name": "restriction",
                            "dtype": {"class_label": {"names": ["M", "N", "P"]}},
                            "description": "nutrient limitation, one "
                            "of P (phosphate "
                            "limitation (20 mg/l).), N "
                            "(Nitrogen‐limited "
                            "cultures were maintained "
                            "at 40 mg/l ammonium "
                            "sulfate) or M (Not "
                            "defined in the paper or "
                            "on the Calico website)",
                            "role": "experimental_condition",
                            "definitions": {
                                "P": {
                                    "media": {
                                        "nitrogen_source": [
                                            {
                                                "compound": "ammonium_sulfate",
                                                "concentration_percent": 0.5,
                                            }
                                        ],
                                        "phosphate_source": [
                                            {
                                                "compound": (
                                                    "potassium_phosphate_monobasic"
                                                ),
                                                "concentration_percent": 0.002,
                                            }
                                        ],
                                    }
                                },
                                "N": {
                                    "media": {
                                        "nitrogen_source": [
                                            {
                                                "compound": "ammonium_sulfate",
                                                "concentration_percent": 0.004,
                                            }
                                        ]
                                    }
                                },
                                "M": {
                                    "description": "Not "
                                    "defined "
                                    "in "
                                    "the "
                                    "paper "
                                    "or "
                                    "on "
                                    "the "
                                    "Calico "
                                    "website"
                                },
                            },
                        },
                        {
                            "name": "date",
                            "dtype": "string",
                            "description": "date performed",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "strain",
                            "dtype": "string",
                            "description": "strain name",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "green_median",
                            "dtype": "float",
                            "description": "median of green "
                            "(reference) channel "
                            "fluorescence",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "red_median",
                            "dtype": "float",
                            "description": "median of red "
                            "(experimental) channel "
                            "fluorescence",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "log2_ratio",
                            "dtype": "float",
                            "description": "log2(red / green) "
                            "subtracting value at time "
                            "zero",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "log2_cleaned_ratio",
                            "dtype": "float",
                            "description": "Non-specific stress "
                            "response and prominent "
                            "outliers removed",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "log2_noise_model",
                            "dtype": "float",
                            "description": "estimated noise standard " "deviation",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "log2_cleaned_ratio_zth2d",
                            "dtype": "float",
                            "description": "cleaned timecourses "
                            "hard-thresholded based on "
                            "multiple observations (or "
                            "last observation) passing "
                            "the noise model",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "log2_selected_timecourses",
                            "dtype": "float",
                            "description": "cleaned timecourses "
                            "hard-thresholded based on "
                            "single observations "
                            "passing noise model and "
                            "impulse evaluation of "
                            "biological feasibility",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "log2_shrunken_timecourses",
                            "dtype": "float",
                            "description": "selected timecourses with "
                            "observation-level "
                            "shrinkage based on local "
                            "FDR (false discovery "
                            "rate). Most users of the "
                            "data will want to use "
                            "this column.",
                            "role": "quantitative_measure",
                        },
                    ]
                },
            }
        ],
    }


@pytest.fixture
def harbison_2004_datacard():
    """Complete harbison_2004 datacard YAML from
    huggingface_collection_datacards.txt."""
    return {
        "license": "mit",
        "language": ["en"],
        "tags": ["genomics", "yeast", "transcription", "binding"],
        "pretty_name": "Harbison, 2004 ChIP-chip",
        "size_categories": ["1M<n<10M"],
        "strain_information": {"background": "W303", "base_strain": "Z1256"},
        "configs": [
            {
                "config_name": "harbison_2004",
                "description": "ChIP-chip transcription factor binding data with "
                "environmental conditions",
                "dataset_type": "annotated_features",
                "default": True,
                "metadata_fields": [
                    "regulator_locus_tag",
                    "regulator_symbol",
                    "condition",
                ],
                "data_files": [{"split": "train", "path": "harbison_2004.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "condition",
                            "dtype": {
                                "class_label": {
                                    "names": [
                                        "YPD",
                                        "SM",
                                        "RAPA",
                                        "H2O2Hi",
                                        "H2O2Lo",
                                        "Acid",
                                        "Alpha",
                                        "BUT14",
                                        "BUT90",
                                        "Thi-",
                                        "GAL",
                                        "HEAT",
                                        "Pi-",
                                        "RAFF",
                                    ]
                                }
                            },
                            "description": "Environmental condition "
                            "of the experiment. Nearly "
                            "all of the 204 regulators "
                            "have a YPD condition, and "
                            "some have others in "
                            "addition.",
                            "role": "experimental_condition",
                            "definitions": {
                                "YPD": {
                                    "description": "Rich "
                                    "media "
                                    "baseline "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                    },
                                },
                                "SM": {
                                    "description": "Amino "
                                    "acid "
                                    "starvation "
                                    "stress "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.6},
                                    "media": {
                                        "name": "synthetic_complete",
                                        "carbon_source": "unspecified",
                                        "nitrogen_source": "unspecified",
                                    },
                                    "chemical_treatment": {
                                        "compound": "sulfometuron_methyl",
                                        "concentration_percent": 0.02,
                                        "duration_hours": 2,
                                    },
                                },
                                "RAPA": {
                                    "description": "Nutrient "
                                    "deprivation "
                                    "via "
                                    "TOR "
                                    "inhibition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                    },
                                    "chemical_treatment": {
                                        "compound": "rapamycin",
                                        "concentration_percent": 9.142e-06,
                                        "duration_minutes": 20,
                                    },
                                },
                                "H2O2Hi": {
                                    "description": "High "
                                    "oxidative "
                                    "stress "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.5},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                    },
                                    "chemical_treatment": {
                                        "compound": "hydrogen_peroxide",
                                        "concentration_percent": 0.0136,
                                        "duration_minutes": 30,
                                    },
                                },
                                "H2O2Lo": {
                                    "description": "Moderate "
                                    "oxidative "
                                    "stress "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.5},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                    },
                                    "chemical_treatment": {
                                        "compound": "hydrogen_peroxide",
                                        "concentration_percent": 0.00136,
                                        "duration_minutes": 20,
                                    },
                                },
                                "Acid": {
                                    "description": "Acidic "
                                    "pH "
                                    "stress "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.5},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                    },
                                    "chemical_treatment": {
                                        "compound": "succinic_acid",
                                        "concentration_percent": 0.59,
                                        "target_pH": 4.0,
                                        "duration_minutes": 30,
                                    },
                                },
                                "Alpha": {
                                    "description": "Mating "
                                    "pheromone "
                                    "induction "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                    },
                                    "chemical_treatment": {
                                        "compound": "alpha_factor_pheromone",
                                        "concentration_percent": 0.5,
                                        "duration_minutes": 30,
                                    },
                                },
                                "BUT14": {
                                    "description": "Long-term "
                                    "filamentation "
                                    "induction "
                                    "with "
                                    "butanol",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                        "additives": [
                                            {
                                                "compound": "butanol",
                                                "concentration_percent": 1,
                                            }
                                        ],
                                    },
                                    "incubation_duration_hours": 14,
                                },
                                "BUT90": {
                                    "description": "Short-term "
                                    "filamentation "
                                    "induction "
                                    "with "
                                    "butanol",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                        "additives": [
                                            {
                                                "compound": "butanol",
                                                "concentration_percent": 1,
                                            }
                                        ],
                                    },
                                    "incubation_duration_minutes": 90,
                                },
                                "Thi-": {
                                    "description": "Vitamin "
                                    "B1 "
                                    "deprivation "
                                    "stress "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "synthetic_complete_minus_thiamine",
                                        "carbon_source": "unspecified",
                                        "nitrogen_source": "unspecified",
                                    },
                                },
                                "GAL": {
                                    "description": "Galactose-based "
                                    "growth "
                                    "medium "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "yeast_extract_peptone",
                                        "carbon_source": [
                                            {
                                                "compound": "D-galactose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": "unspecified",
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": "unspecified",
                                            },
                                        ],
                                    },
                                },
                                "HEAT": {
                                    "description": "Heat "
                                    "shock "
                                    "stress "
                                    "condition",
                                    "initial_temperature_celsius": 30,
                                    "temperature_shift_celsius": 37,
                                    "temperature_shift_duration_minutes": 45,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.5},
                                    "media": {
                                        "name": "YPD",
                                        "carbon_source": [
                                            {
                                                "compound": "D-glucose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": 1,
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": 2,
                                            },
                                        ],
                                    },
                                },
                                "Pi-": {
                                    "description": "Phosphate "
                                    "deprivation "
                                    "stress "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "synthetic_complete_minus_phosphate",
                                        "carbon_source": "unspecified",
                                        "nitrogen_source": "unspecified",
                                    },
                                },
                                "RAFF": {
                                    "description": "Raffinose-based "
                                    "growth "
                                    "medium "
                                    "condition",
                                    "temperature_celsius": 30,
                                    "cultivation_method": "unspecified",
                                    "growth_phase_at_harvest": {"od600": 0.8},
                                    "media": {
                                        "name": "yeast_extract_peptone",
                                        "carbon_source": [
                                            {
                                                "compound": "D-raffinose",
                                                "concentration_percent": 2,
                                            }
                                        ],
                                        "nitrogen_source": [
                                            {
                                                "compound": "yeast_extract",
                                                "concentration_percent": "unspecified",
                                            },
                                            {
                                                "compound": "peptone",
                                                "concentration_percent": "unspecified",
                                            },
                                        ],
                                    },
                                },
                            },
                        },
                        {
                            "name": "regulator_locus_tag",
                            "dtype": "string",
                            "description": "Systematic gene name (ORF "
                            "identifier) of the ChIPd "
                            "transcription factor",
                            "role": "regulator_identifier",
                        },
                        {
                            "name": "regulator_symbol",
                            "dtype": "string",
                            "description": "Standard gene symbol of "
                            "the ChIPd transcription "
                            "factor",
                            "role": "regulator_identifier",
                        },
                        {
                            "name": "target_locus_tag",
                            "dtype": "string",
                            "description": "Systematic gene name (ORF "
                            "identifier) of the target "
                            "gene measured",
                            "role": "target_identifier",
                        },
                        {
                            "name": "target_symbol",
                            "dtype": "string",
                            "description": "Standard gene symbol of "
                            "the target gene measured",
                            "role": "target_identifier",
                        },
                        {
                            "name": "effect",
                            "dtype": "float64",
                            "description": "The chip channel ratio " "(effect size)",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "pvalue",
                            "dtype": "float64",
                            "description": "pvalue of the chip "
                            "channel ratio (effect)",
                            "role": "quantitative_measure",
                        },
                    ]
                },
            }
        ],
    }


@pytest.fixture
def kemmeren_2014_datacard():
    """Complete kemmeren_2014 datacard YAML from
    huggingface_collection_datacards.txt."""
    return {
        "license": "mit",
        "language": ["en"],
        "tags": [
            "genomics",
            "yeast",
            "transcription",
            "perturbation",
            "response",
            "knockout",
            "TFKO",
        ],
        "pretty_name": "Kemmeren, 2014 Overexpression",
        "size_categories": ["1M<n<10M"],
        "experimental_conditions": {
            "temperature_celsius": 30,
            "cultivation_method": "plate",
            "growth_phase_at_harvest": {
                "phase": "mid_log_phase",
                "od600": 0.6,
                "od600_tolerance": 0.1,
            },
            "media": {
                "name": "synthetic_complete",
                "carbon_source": [
                    {"compound": "D-glucose", "concentration_percent": 2}
                ],
                "nitrogen_source": [
                    {
                        "compound": "yeast_nitrogen_base",
                        "concentration_percent": 0.671,
                        "specifications": [
                            "without_amino_acids",
                            "without_carbohydrate",
                            "with_ammonium_sulfate",
                        ],
                    },
                    {
                        "compound": "amino_acid_dropout_mix",
                        "concentration_percent": 0.2,
                    },
                ],
            },
        },
        "configs": [
            {
                "config_name": "kemmeren_2014",
                "description": "Transcriptional regulator overexpression perturbation "
                "data with differential expression measurements",
                "dataset_type": "annotated_features",
                "default": True,
                "metadata_fields": ["regulator_locus_tag", "regulator_symbol"],
                "data_files": [{"split": "train", "path": "kemmeren_2014.parquet"}],
                "dataset_info": {
                    "features": [
                        {
                            "name": "sample_id",
                            "dtype": "integer",
                            "description": "unique identifier for a "
                            "specific sample. The "
                            "sample ID identifies a "
                            "unique regulator.",
                        },
                        {
                            "name": "db_id",
                            "dtype": "integer",
                            "description": "an old unique identifer, "
                            "for use internally only. "
                            "Deprecated and will be "
                            "removed eventually. Do "
                            "not use in analysis. "
                            "db_id = 0 for loci that "
                            "were originally parsed "
                            "incorrectly.",
                        },
                        {
                            "name": "regulator_locus_tag",
                            "dtype": "string",
                            "description": "induced transcriptional "
                            "regulator systematic ID. "
                            "See "
                            "hf/BrentLab/yeast_genome_resources",
                            "role": "regulator_identifier",
                        },
                        {
                            "name": "regulator_symbol",
                            "dtype": "string",
                            "description": "induced transcriptional "
                            "regulator common name. If "
                            "no common name exists, "
                            "then the "
                            "`regulator_locus_tag` is "
                            "used.",
                            "role": "regulator_identifier",
                        },
                        {
                            "name": "reporterId",
                            "dtype": "string",
                            "description": "probe ID as reported from "
                            "the original data",
                        },
                        {
                            "name": "target_locus_tag",
                            "dtype": "string",
                            "description": "The systematic ID of the "
                            "feature to which the "
                            "effect/pvalue is "
                            "assigned. See "
                            "hf/BrentLab/yeast_genome_resources",
                            "role": "target_identifier",
                        },
                        {
                            "name": "target_symbol",
                            "dtype": "string",
                            "description": "The common name of the "
                            "feature to which the "
                            "effect/pvalue is "
                            "assigned. If there is no "
                            "common name, the "
                            "`target_locus_tag` is "
                            "used.",
                            "role": "target_identifier",
                        },
                        {
                            "name": "M",
                            "dtype": "float64",
                            "description": "log₂ fold change (mutant " "vs wildtype)",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "Madj",
                            "dtype": "float64",
                            "description": "M value with the cell "
                            "cycle signal removed (see "
                            "paper cited in the "
                            "introduction above)",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "A",
                            "dtype": "float64",
                            "description": "average log2 intensity of "
                            "the two channels, a proxy "
                            "for expression level "
                            "(This is a guess based on "
                            "microarray convention -- "
                            "not specified on holstege "
                            "site)",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "pval",
                            "dtype": "float64",
                            "description": "significance of the "
                            "modeled effect (M), from "
                            "limma",
                            "role": "quantitative_measure",
                        },
                        {
                            "name": "variable_in_wt",
                            "dtype": "string",
                            "description": "True if the given locus "
                            "is variable in the WT "
                            "condition. Recommended to "
                            "remove these from "
                            "analysis. False "
                            "otherwise. See Holstege "
                            "website for more "
                            "information",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "multiple_probes",
                            "dtype": "string",
                            "description": "True if there is more "
                            "than one probe associated "
                            "with the same genomic "
                            "locus. False otherwise",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "kemmeren_regulator",
                            "dtype": "string",
                            "description": "True if the regulator is "
                            "one of the regulators "
                            "studied in the original "
                            "Kemmeren et al. (2014) "
                            "global regulator study. "
                            "False otherwise",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "regulator_desc",
                            "dtype": "string",
                            "description": "functional description of "
                            "the induced regulator "
                            "from the original paper "
                            "supplement",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "functional_category",
                            "dtype": "string",
                            "description": "functional classification "
                            "of the regulator from the "
                            "original paper supplement",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "slides",
                            "dtype": "string",
                            "description": "identifier(s) for the "
                            "microarray slide(s) used "
                            "in this experiment",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "mating_type",
                            "dtype": "string",
                            "description": "mating type of the strain "
                            "background used in the "
                            "experiment",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "source_of_deletion_mutants",
                            "dtype": "string",
                            "description": "origin of the strain",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "primary_hybsets",
                            "dtype": "string",
                            "description": "identifier for the "
                            "primary hybridization set "
                            "to which this sample "
                            "belongs",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "responsive_non_responsive",
                            "dtype": "string",
                            "description": "classification of the "
                            "regulator as responsive "
                            "or not to the deletion "
                            "from the original paper "
                            "supplement",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "nr_sign_changes",
                            "dtype": "integer",
                            "description": "number of significant "
                            "changes in expression "
                            "detected for the "
                            "regulator locus tag "
                            "(abs(M) > log2(1.7) & "
                            "pval < 0.05). Note that "
                            "there is a slight "
                            "difference when "
                            "calculating from the data "
                            "provided here, I believe "
                            "due to a difference in "
                            "the way the targets are "
                            "parsed and filtered (some "
                            "ORFs that have since been "
                            "removed from the "
                            "annotations are removed). "
                            "I didn't investigate this "
                            "closely, though.",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "profile_first_published",
                            "dtype": "string",
                            "description": "citation or reference "
                            "indicating where this "
                            "expression profile was "
                            "first published",
                            "role": "experimental_condition",
                        },
                        {
                            "name": "chase_notes",
                            "dtype": "string",
                            "description": "notes added during data "
                            "curation and parsing",
                        },
                    ]
                },
            }
        ],
    }
