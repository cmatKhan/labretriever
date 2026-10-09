"""
Field+path property mappings follow the documented precedence.

A key resolves field-level over config-level over top-level
(``docs/huggingface_datacard.md``). A field+path mapping reads a key from each level's
condition definition; a level that omits the key inherits the config-level, else
top-level, ``experimental_conditions`` value.

The tests build the SQL expression with ``VirtualDB`` and evaluate it in DuckDB over a
column of condition levels, so they check the values a ``_meta`` view would hold. The
first group pins behavior that must not change.

"""

from __future__ import annotations

import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import duckdb
import pytest

from labretriever.errors import DataCardError
from labretriever.virtual_db import VirtualDB

ALIASES = {
    "carbon_source": {
        "glucose": ["D-glucose"],
        "galactose": ["D-galactose"],
        "raffinose": ["D-raffinose"],
    }
}


def _vdb(
    aliases: dict[str, Any] | None = None,
    missing: dict[str, str] | None = None,
    external: dict[str, str] | None = None,
    db_name_map: dict[str, tuple[str, str]] | None = None,
) -> VirtualDB:
    """A VirtualDB with just the state the property resolvers read."""
    v = VirtualDB.__new__(VirtualDB)
    v.config = SimpleNamespace(
        factor_aliases=aliases or {}, missing_value_labels=missing or {}
    )
    v.db_name_map = db_name_map or {}
    v._external_meta_configs = external or {}
    return v


def _card(
    defs: dict[str, dict[str, Any]] | None,
    conditions: dict[str, Any] | None = None,
    defs_by_config: dict[str, dict[str, Any]] | None = None,
) -> MagicMock:
    """A mock DataCard whose condition field has ``defs`` in every config."""
    card = MagicMock()

    def get_field_definitions(config_name: str, field: str) -> dict[str, Any]:
        if defs_by_config is not None:
            if config_name not in defs_by_config:
                raise DataCardError(f"Field '{field}' not found in '{config_name}'")
            return defs_by_config[config_name]
        assert defs is not None
        return defs

    card.get_field_definitions.side_effect = get_field_definitions
    card.get_experimental_conditions.return_value = conditions or {}
    return card


def _evaluate(expr: str | None, levels: list[str | None]) -> list[Any]:
    """Run the expression over a ``condition`` column; return one value per level."""
    assert expr is not None
    rows = ", ".join(
        f"({i}, NULL)" if lv is None else f"({i}, '{lv}')"
        for i, lv in enumerate(levels)
    )
    sql = (
        f"SELECT {expr} FROM (SELECT * FROM (VALUES {rows}) AS t(idx, condition)) "
        "ORDER BY idx"
    )
    return [r[0] for r in duckdb.connect().execute(sql).fetchall()]


def _field_path(
    v: VirtualDB,
    card: MagicMock,
    path: str,
    key: str = "temperature_celsius",
    dtype: str | None = None,
    **kwargs: Any,
) -> str | None:
    return v._build_field_path_expr(
        key, "condition", path, dtype, "data_cfg", card, **kwargs
    )


# ----------------------------------------------------------------------
# Behavior that must not change
# ----------------------------------------------------------------------


class TestUnchangedBehavior:
    def test_levels_that_agree_without_a_default_are_a_constant(self):
        v = _vdb()
        defs = {"A": {"temperature_celsius": 30}, "B": {"temperature_celsius": 30}}
        expr = _field_path(v, _card(defs), "temperature_celsius")
        assert expr == "'30' AS \"temperature_celsius\""
        # an undefined level and a NULL level are the constant too
        assert _evaluate(expr, ["A", "B", "other", None]) == ["30"] * 4

    def test_numeric_dtype_casts(self):
        v = _vdb()
        defs = {"A": {"temperature_celsius": 30}, "B": {"temperature_celsius": 37}}
        expr = _field_path(v, _card(defs), "temperature_celsius", dtype="numeric")
        assert expr is not None and expr.startswith("CAST(CASE")
        assert _evaluate(expr, ["A", "B"]) == [30.0, 37.0]

    def test_varying_levels_with_missing_label_for_the_rest(self):
        v = _vdb(aliases=ALIASES, missing={"carbon_source": "unspecified"})
        defs = {
            "YPD": {"media": {"carbon_source": [{"compound": "D-glucose"}]}},
            "GAL": {"media": {"carbon_source": [{"compound": "D-galactose"}]}},
            "SM": {"description": "no carbon source stated"},
        }
        expr = _field_path(
            v, _card(defs), "media.carbon_source.compound", key="carbon_source"
        )
        out = _evaluate(expr, ["YPD", "GAL", "SM", "undefined", None])
        assert out == [
            "glucose",
            "galactose",
            "unspecified",
            "unspecified",
            "unspecified",
        ]

    def test_varying_levels_without_a_missing_label_give_null(self):
        v = _vdb(aliases=ALIASES)
        defs = {
            "YPD": {"media": {"carbon_source": [{"compound": "D-glucose"}]}},
            "GAL": {"media": {"carbon_source": [{"compound": "D-galactose"}]}},
            "SM": {},
        }
        expr = _field_path(
            v, _card(defs), "media.carbon_source.compound", key="carbon_source"
        )
        assert _evaluate(expr, ["YPD", "GAL", "SM"]) == ["glucose", "galactose", None]

    def test_single_compound_list_is_aliased(self):
        v = _vdb(aliases=ALIASES)
        defs = {
            "A": {"media": {"carbon_source": [{"compound": "D-glucose"}]}},
            "B": {"media": {"carbon_source": [{"compound": "D-galactose"}]}},
        }
        expr = _field_path(
            v, _card(defs), "media.carbon_source.compound", key="carbon_source"
        )
        assert _evaluate(expr, ["A", "B"]) == ["glucose", "galactose"]

    def test_no_definitions_gives_no_expression(self):
        v = _vdb()
        assert _field_path(v, _card({}), "temperature_celsius") is None

    def test_field_declared_nowhere_warns_and_gives_none(self, caplog):
        v = _vdb()
        card = _card(None, defs_by_config={})
        with caplog.at_level(logging.WARNING):
            assert _field_path(v, card, "temperature_celsius") is None
        assert "Could not get definitions for field 'condition'" in caplog.text


# ----------------------------------------------------------------------
# Precedence: a level that omits the key inherits the default
# ----------------------------------------------------------------------


class TestDefaultsApply:
    def test_level_without_the_key_inherits_the_default(self):
        """Harbison: only the heat shock states a temperature; the rest are 30."""
        v = _vdb()
        defs = {
            "YPD": {"media": {"name": "ypd"}},
            "SM": {"description": "starvation"},
            "HEAT": {"temperature_celsius": 37},
        }
        expr = _field_path(
            v, _card(defs, {"temperature_celsius": 30}), "temperature_celsius"
        )
        assert expr is not None and "CASE" in expr  # not a constant
        assert _evaluate(expr, ["YPD", "SM", "HEAT"]) == ["30", "30", "37"]

    def test_levels_with_no_definition_and_null_take_the_default(self):
        v = _vdb(missing={"temperature_celsius": "unspecified"})
        defs = {"HEAT": {"temperature_celsius": 37}, "YPD": {}}
        expr = _field_path(
            v, _card(defs, {"temperature_celsius": 30}), "temperature_celsius"
        )
        assert _evaluate(expr, ["HEAT", "YPD", "never defined", None]) == [
            "37",
            "30",
            "30",
            "30",
        ]

    def test_default_beats_the_missing_label(self):
        v = _vdb(missing={"temperature_celsius": "unspecified"})
        defs = {"A": {"temperature_celsius": 37}, "B": {}}
        expr = _field_path(
            v, _card(defs, {"temperature_celsius": 30}), "temperature_celsius"
        )
        assert _evaluate(expr, ["A", "B"]) == ["37", "30"]

    def test_without_a_default_an_omitting_level_is_missing_not_constant(self):
        """Previously one level stating the key made it the value of every row."""
        v = _vdb(missing={"temperature_celsius": "unspecified"})
        defs = {"HEAT": {"temperature_celsius": 37}, "YPD": {}}
        expr = _field_path(v, _card(defs), "temperature_celsius")
        assert _evaluate(expr, ["HEAT", "YPD"]) == ["37", "unspecified"]

    def test_default_equal_to_the_only_value_is_still_a_constant(self):
        v = _vdb()
        defs = {"A": {"temperature_celsius": 30}, "B": {}}
        expr = _field_path(
            v, _card(defs, {"temperature_celsius": 30}), "temperature_celsius"
        )
        assert expr == "'30' AS \"temperature_celsius\""

    def test_all_levels_state_the_key_but_the_default_differs(self):
        """A value with no definition takes the default, not the levels' value."""
        v = _vdb()
        defs = {"A": {"temperature_celsius": 37}, "B": {"temperature_celsius": 37}}
        expr = _field_path(
            v, _card(defs, {"temperature_celsius": 30}), "temperature_celsius"
        )
        assert _evaluate(expr, ["A", "B", "other"]) == ["37", "37", "30"]

    def test_numeric_default(self):
        v = _vdb()
        defs = {"HEAT": {"temperature_celsius": 37}, "YPD": {}}
        expr = _field_path(
            v,
            _card(defs, {"temperature_celsius": 30}),
            "temperature_celsius",
            dtype="numeric",
        )
        assert _evaluate(expr, ["HEAT", "YPD"]) == [37.0, 30.0]

    def test_list_valued_default_is_aliased(self):
        v = _vdb(aliases=ALIASES)
        defs = {
            "galactose": {
                "media": {"carbon_source": [{"compound": "D-raffinose"}]},
            },
            "standard": {"description": "inherits the media"},
        }
        conditions = {"media": {"carbon_source": [{"compound": "D-glucose"}]}}
        expr = _field_path(
            v,
            _card(defs, conditions),
            "media.carbon_source.compound",
            key="carbon_source",
        )
        assert _evaluate(expr, ["galactose", "standard"]) == ["raffinose", "glucose"]

    def test_chec_seq_like_conditions(self):
        """Two conditions state a carbon source and one states a temperature."""
        v = _vdb(aliases=ALIASES, missing={"carbon_source": "unspecified"})
        defs = {
            "standard": {"temperature_celsius": 30},
            "30": {"temperature_celsius": 30},
            "37": {"final_temperature_celsius": 37, "temperature_celsius": 37},
            "galactose": {
                "temperature_celsius": 30,
                "media": {
                    "carbon_source": [
                        {"compound": "D-raffinose"},
                        {"compound": "D-galactose"},
                    ]
                },
            },
            "raffinose": {
                "temperature_celsius": 30,
                "media": {"carbon_source": [{"compound": "D-raffinose"}]},
            },
        }
        conditions = {
            "temperature_celsius": 30,
            "media": {"carbon_source": [{"compound": "D-glucose"}]},
        }
        card = _card(defs, conditions)
        levels = ["standard", "30", "37", "galactose", "raffinose"]
        carbon = _field_path(
            v, card, "media.carbon_source.compound", key="carbon_source"
        )
        assert _evaluate(carbon, levels) == [
            "glucose",
            "glucose",
            "glucose",
            "raffinose, galactose",
            "raffinose",
        ]
        temperature = _field_path(v, card, "temperature_celsius")
        assert _evaluate(temperature, levels) == ["30", "30", "37", "30", "30"]


# ----------------------------------------------------------------------
# Definitions held by an external metadata config
# ----------------------------------------------------------------------


class TestExternalMetadataConfig:
    DEFS = {"A": {"temperature_celsius": 37}, "B": {}}

    def test_definitions_found_through_the_metadata_config(self):
        v = _vdb(
            external={"ds": "meta_cfg"},
            db_name_map={"ds": ("org/repo", "data_cfg")},
        )
        card = _card(
            None,
            {"temperature_celsius": 30},
            defs_by_config={"meta_cfg": self.DEFS},
        )
        expr = _field_path(v, card, "temperature_celsius", db_name="ds")
        assert _evaluate(expr, ["A", "B"]) == ["37", "30"]

    def test_metadata_config_found_from_the_repo_when_db_name_is_not_given(self):
        v = _vdb(
            external={"ds": "meta_cfg"},
            db_name_map={"ds": ("org/repo", "data_cfg")},
        )
        card = _card(
            None,
            {"temperature_celsius": 30},
            defs_by_config={"meta_cfg": self.DEFS},
        )
        expr = _field_path(v, card, "temperature_celsius", repo_id="org/repo")
        assert _evaluate(expr, ["A", "B"]) == ["37", "30"]

    def test_without_the_dataset_the_metadata_config_is_not_consulted(self):
        v = _vdb(external={"ds": "meta_cfg"})
        card = _card(None, defs_by_config={"meta_cfg": self.DEFS})
        assert _field_path(v, card, "temperature_celsius") is None

    def test_the_data_config_wins_when_it_declares_the_field(self):
        v = _vdb(
            external={"ds": "meta_cfg"},
            db_name_map={"ds": ("org/repo", "data_cfg")},
        )
        card = _card(
            None,
            defs_by_config={
                "data_cfg": {"A": {"temperature_celsius": 25}, "B": {}},
                "meta_cfg": self.DEFS,
            },
        )
        expr = _field_path(v, card, "temperature_celsius", db_name="ds")
        assert _evaluate(expr, ["A", "B"]) == ["25", None]


# ----------------------------------------------------------------------
# Path-only mappings share the value formatting
# ----------------------------------------------------------------------


class TestPathOnlyFormatting:
    @staticmethod
    def _card_with(conditions: dict[str, Any]) -> MagicMock:
        card = MagicMock()
        card.dataset_card.model_dump.return_value = {
            "experimental_conditions": conditions
        }
        card.get_config.return_value.model_dump.return_value = {}
        return card

    def test_single_compound_is_aliased(self):
        v = _vdb(aliases=ALIASES)
        card = self._card_with(
            {"media": {"carbon_source": [{"compound": "D-glucose"}]}}
        )
        expr = v._build_path_only_expr(
            "carbon_source",
            "experimental_conditions.media.carbon_source.compound",
            None,
            "cfg",
            card,
        )
        assert expr == "'glucose' AS \"carbon_source\""

    def test_each_compound_of_a_list_is_aliased(self):
        v = _vdb(aliases=ALIASES)
        card = self._card_with(
            {
                "media": {
                    "carbon_source": [
                        {"compound": "D-raffinose"},
                        {"compound": "D-galactose"},
                    ]
                }
            }
        )
        expr = v._build_path_only_expr(
            "carbon_source",
            "experimental_conditions.media.carbon_source.compound",
            None,
            "cfg",
            card,
        )
        assert expr == "'raffinose, galactose' AS \"carbon_source\""

    def test_scalar_value(self):
        v = _vdb()
        card = self._card_with({"temperature_celsius": 30})
        expr = v._build_path_only_expr(
            "temperature_celsius",
            "experimental_conditions.temperature_celsius",
            "numeric",
            "cfg",
            card,
        )
        assert expr == "CAST('30' AS DOUBLE) AS \"temperature_celsius\""


@pytest.mark.parametrize(
    "raw, expected",
    [
        (["D-glucose"], "glucose"),
        ("D-glucose", "glucose"),
        (["D-raffinose", "D-galactose"], "raffinose, galactose"),
        (["D-raffinose", "unlisted"], "raffinose, unlisted"),
        (30, "30"),
    ],
)
def test_format_condition_value(raw, expected):
    assert (
        _vdb(aliases=ALIASES)._format_condition_value("carbon_source", raw) == expected
    )
