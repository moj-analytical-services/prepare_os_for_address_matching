"""Inspect flatfile output and show the UPRN with most variants."""

from __future__ import annotations

from pathlib import Path

import duckdb

from ngd_pipeline.settings import load_settings

CONFIG_PATH = Path("config.yaml")


settings = load_settings(CONFIG_PATH)
output_dir = settings.paths.output_dir
pattern = output_dir / "ngd_for_uk_address_matcher*.parquet"
output_files = list(output_dir.glob(pattern.name))

if not output_files:
    raise SystemExit(f"No flatfile outputs found in {output_dir}")

con = duckdb.connect()
files_sql = pattern.as_posix()
files_sql
con.read_parquet(files_sql).filter("filename != 'add_gb_royalmailaddress.parquet'").limit(5).show(
    max_width=10000
)
con.read_parquet(files_sql).filter("uprn = 100021885286").show(max_width=10000)

top = con.execute(
    f"""
    WITH data AS (
        SELECT * FROM read_parquet('{files_sql}')
    )
    SELECT uprn, COUNT(*) AS variant_count
    FROM data
    GROUP BY uprn
    ORDER BY variant_count DESC, uprn ASC
    LIMIT 1 offset 10
    """
).fetchone()

if not top:
    raise SystemExit("No records found in flatfile outputs.")

uprn, variant_count = top

con.sql(
    f"""
    SELECT full_address_with_postcode, language, filename, feature_type, address_status, *
    FROM read_parquet('{files_sql}')
    WHERE uprn = {uprn}
    ORDER BY full_address_with_postcode, language, filename
    """
).show(max_width=10000)
