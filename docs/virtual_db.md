# VirtualDB

VirtualDB provides a SQL query interface across heterogeneous HuggingFace
datasets using an in-memory DuckDB database. Each dataset defines experimental
conditions in its own way, with properties stored at different hierarchy levels
(repository, dataset, or field) and using different naming conventions.
VirtualDB uses an external YAML configuration to map these varying structures
to a common schema, normalize factor level names (e.g., "D-glucose",
"dextrose", "glu" all become "glucose"), and enable cross-dataset queries with
standardized field names and values.

For primary datasets, VirtualDB creates:

- **`<db_name>_meta`** -- one row per sample with derived metadata columns
- **`<db_name>`** -- full measurement-level data joined to the metadata view

For comparative analysis datasets, VirtualDB creates:

- **`<db_name>_expanded`** -- the raw data with composite ID fields parsed
  into `<link_field>_source` (aliased to configured `db_name`) and
  `<link_field>_id` (sample_id) columns

See the [configuration guide](virtual_db_configuration.md) for setup details
and the [tutorial](tutorials/virtual_db_tutorial.ipynb) for usage examples.

## Advanced Usage

The underlying DuckDB connection is available as `vdb._conn`. You can use
`_conn` to execute any SQL on the database, eg creating more views, or
creating a table in memory.

Custom **views** created this way appear in `tables()`, `describe()`, and
`get_fields()` automatically because those methods query DuckDB's
`information_schema`. Custom **tables** do not appear in `tables()` (which
only lists views), but are fully queryable via `vdb.query()`.

Example -- create a materialized analysis table::

    # Create a persistent in-memory table from a complex query.
    # This example selects one "best" Hackett-2020 sample per regulator
    # using a priority system: ZEV+P > GEV+P > GEV+M.
    vdb._conn.execute("""
        CREATE OR REPLACE TABLE hackett_analysis_set AS
        WITH regulator_tiers AS (
            SELECT
                regulator_locus_tag,
                CASE
                    WHEN BOOL_OR(mechanism = 'ZEV' AND restriction = 'P') THEN 1
                    WHEN BOOL_OR(mechanism = 'GEV' AND restriction = 'P') THEN 2
                    ELSE 3
                END AS tier
            FROM hackett_meta
            WHERE regulator_locus_tag NOT IN ('Z3EV', 'GEV')
            GROUP BY regulator_locus_tag
        ),
        tier_filter AS (
            SELECT
                h.sample_id, h.regulator_locus_tag, h.regulator_symbol,
                h.mechanism, h.restriction, h.date, h.strain, t.tier
            FROM hackett_meta h
            JOIN regulator_tiers t USING (regulator_locus_tag)
            WHERE
                (t.tier = 1 AND h.mechanism = 'ZEV' AND h.restriction = 'P')
                OR (t.tier = 2 AND h.mechanism = 'GEV' AND h.restriction = 'P')
                OR (t.tier = 3 AND h.mechanism = 'GEV' AND h.restriction = 'M')
        )
        SELECT DISTINCT
            sample_id, regulator_locus_tag, regulator_symbol,
            mechanism, restriction, date, strain
        FROM tier_filter
        WHERE regulator_symbol NOT IN ('GCN4', 'RDS2', 'SWI1', 'MAC1')
        ORDER BY regulator_locus_tag, sample_id
    """)

    df = vdb.query("SELECT * FROM hackett_analysis_set")

Tables and views created this way are in-memory only and do not persist across
VirtualDB instances. They exist for the lifetime of the DuckDB connection.

## API Reference

::: labretriever.virtual_db.VirtualDB
    options:
      show_root_heading: true
      show_source: true
