"""Transform NGD data to flatfile module.

Transforms the extracted parquet files into a single flatfile suitable for
UK address matching. This includes:
- Processing core feature types (Built Address, Historic Address, etc.)
- Processing alternate address records
- Processing Royal Mail addresses
- Handling Welsh language variants
- Deduplication with priority rules
"""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

import duckdb

from ngd_pipeline.settings import Settings, create_duckdb_connection

logger = logging.getLogger(__name__)


class ToFlatfileError(Exception):
    """Error during flatfile transformation."""


# Mapping of file stems to feature types
FEATURE_TYPE_BY_STEM = {
    "add_gb_builtaddress": "Built Address",
    "add_gb_builtaddress_altadd": "Built Address",
    "add_gb_historicaddress": "Historic Address",
    "add_gb_historicaddress_altadd": "Historic Address",
    "add_gb_nonaddressableobject": "Non-Addressable Object",
    "add_gb_nonaddressableobject_altadd": "Non-Addressable Object",
    "add_gb_prebuildaddress": "Pre-Build Address",
    "add_gb_prebuildaddress_altadd": "Pre-Build Address",
    "add_gb_royalmailaddress": "Royal Mail Address",
}

# Core feature stems (contain fulladdress and classification fields)
CORE_FEATURE_STEMS = {
    "add_gb_builtaddress",
    "add_gb_historicaddress",
    "add_gb_nonaddressableobject",
    "add_gb_prebuildaddress",
}

# Alternate address stems (no classification fields)
ALTADD_STEMS = {
    "add_gb_builtaddress_altadd",
    "add_gb_historicaddress_altadd",
    "add_gb_nonaddressableobject_altadd",
    "add_gb_prebuildaddress_altadd",
}


def _create_core_feature_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
) -> None:
    """Create view for core feature types (Built, Historic, Pre-Build, Non-Addressable).

    These tables have fulladdress, classification fields, and Welsh language columns.
    Produces both English and Welsh (where available) address records.
    """
    sql = f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        WITH src AS (
            SELECT * FROM read_parquet('{parquet_path.as_posix()}')
        )
        -- English track
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            CAST(fulladdress AS VARCHAR) AS full_address_with_postcode,
            '{parquet_path.name}' AS filename,
            CAST(description AS VARCHAR) AS feature_type,
            CAST(addressstatus AS VARCHAR) AS address_status,
            CAST(buildstatus AS VARCHAR) AS build_status,
            CAST(postcodesource AS VARCHAR) AS postcodesource,
            CAST(classificationcode AS VARCHAR) AS classification_code,
            CAST(NULL AS VARCHAR) AS matched_address_feature_type,
            'eng' AS language
        FROM src
        UNION ALL
        -- Welsh (if present) track
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            CAST(
              COALESCE(
                alternatelanguagefulladdress,
                TRIM(BOTH ', ' FROM
                  COALESCE(alternatelanguagesubname || ', ', '') ||
                  COALESCE(alternatelanguagename || ', ', '') ||
                  COALESCE(alternatelanguagenumber || ', ', '') ||
                  COALESCE(alternatelanguagestreetname || ', ', '') ||
                  COALESCE(alternatelanguagelocality || ', ', '') ||
                  COALESCE(alternatelanguagetownname || ', ', '') ||
                  COALESCE(alternatelanguageislandname || ', ', '') ||
                  COALESCE(postcode, '')
                )
              ) AS VARCHAR
            ) AS full_address_with_postcode,
            '{parquet_path.name}' AS filename,
            CAST(description AS VARCHAR) AS feature_type,
            CAST(addressstatus AS VARCHAR) AS address_status,
            CAST(buildstatus AS VARCHAR) AS build_status,
            CAST(postcodesource AS VARCHAR) AS postcodesource,
            CAST(classificationcode AS VARCHAR) AS classification_code,
            CAST(NULL AS VARCHAR) AS matched_address_feature_type,
            'cym' AS language
        FROM src
        WHERE lower(coalesce(alternatelanguage,'')) IN ('wel','cym','welsh','cymraeg')
          AND (
                alternatelanguagefulladdress IS NOT NULL
             OR alternatelanguagesubname IS NOT NULL
             OR alternatelanguagename IS NOT NULL
             OR alternatelanguagenumber IS NOT NULL
             OR alternatelanguagestreetname IS NOT NULL
             OR alternatelanguagelocality IS NOT NULL
             OR alternatelanguagetownname IS NOT NULL
             OR alternatelanguageislandname IS NOT NULL
          );
    """
    con.execute(sql)


def _create_altadd_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
    feature_type: str,
) -> None:
    """Create view for alternate address records.

    These tables have fewer fields - no classification columns.
    """
    sql = f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            CAST(fulladdress AS VARCHAR) AS full_address_with_postcode,
            '{parquet_path.name}' AS filename,
            '{feature_type}' AS feature_type,
            CAST(addressstatus AS VARCHAR) AS address_status,
            CAST(NULL AS VARCHAR) AS build_status,
            CAST(NULL AS VARCHAR) AS postcodesource,
            CAST(NULL AS VARCHAR) AS classification_code,
            CAST(NULL AS VARCHAR) AS matched_address_feature_type,
            CASE
              WHEN lower(coalesce(language,'')) IN ('cym','wel','welsh','cymraeg') THEN 'cym'
              ELSE 'eng'
            END AS language
        FROM read_parquet('{parquet_path.as_posix()}');
    """
    con.execute(sql)


def _create_royal_mail_view(
    con: duckdb.DuckDBPyConnection,
    view_name: str,
    parquet_path: Path,
) -> None:
    """Create view for Royal Mail Address records.

    Builds address from component fields. Produces both English and Welsh variants.
    Excludes records where matchedaddressfeaturetype is 'Non-Addressable Object'.
    """
    sql = f"""
        CREATE OR REPLACE TEMP VIEW {view_name} AS
        WITH src AS (
            SELECT * FROM read_parquet('{parquet_path.as_posix()}')
            WHERE matchedaddressfeaturetype != 'Non-Addressable Object'
        )
        -- English
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            TRIM(BOTH ', ' FROM
                COALESCE(organisationname || ', ', '') ||
                COALESCE(departmentname   || ', ', '') ||
                COALESCE(subbuildingname  || ', ', '') ||
                COALESCE(buildingname     || ', ', '') ||
                COALESCE(CAST(buildingnumber AS VARCHAR) || ', ', '') ||
                COALESCE(dependentthoroughfare || ', ', '') ||
                COALESCE(thoroughfare     || ', ', '') ||
                COALESCE(doubledependentlocality || ', ', '') ||
                COALESCE(dependentlocality || ', ', '') ||
                COALESCE(posttown || ', ', '') ||
                COALESCE(postcode, '')
            ) AS full_address_with_postcode,
            '{parquet_path.name}' AS filename,
            'Royal Mail Address' AS feature_type,
            CAST(NULL AS VARCHAR) AS address_status,
            CAST(NULL AS VARCHAR) AS build_status,
            CAST(NULL AS VARCHAR) AS postcodesource,
            CAST(NULL AS VARCHAR) AS classification_code,
            CAST(matchedaddressfeaturetype AS VARCHAR) AS matched_address_feature_type,
            'eng' AS language
        FROM src
        UNION ALL
        -- Welsh
        SELECT
            CAST(uprn AS BIGINT) AS uprn,
            TRIM(BOTH ', ' FROM
                COALESCE(organisationname || ', ', '') ||
                COALESCE(departmentname   || ', ', '') ||
                COALESCE(subbuildingname  || ', ', '') ||
                COALESCE(buildingname     || ', ', '') ||
                COALESCE(CAST(buildingnumber AS VARCHAR) || ', ', '') ||
                COALESCE(welshdependentthoroughfare || ', ', '') ||
                COALESCE(welshthoroughfare     || ', ', '') ||
                COALESCE(welshdoubledependentlocality || ', ', '') ||
                COALESCE(welshdependentlocality || ', ', '') ||
                COALESCE(welshposttown || ', ', '') ||
                COALESCE(postcode, '')
            ) AS full_address_with_postcode,
            '{parquet_path.name}' AS filename,
            'Royal Mail Address' AS feature_type,
            CAST(NULL AS VARCHAR) AS address_status,
            CAST(NULL AS VARCHAR) AS build_status,
            CAST(NULL AS VARCHAR) AS postcodesource,
            CAST(NULL AS VARCHAR) AS classification_code,
            CAST(matchedaddressfeaturetype AS VARCHAR) AS matched_address_feature_type,
            'cym' AS language
        FROM src
        WHERE welshdependentthoroughfare IS NOT NULL
           OR welshthoroughfare IS NOT NULL
           OR welshdoubledependentlocality IS NOT NULL
           OR welshdependentlocality IS NOT NULL
           OR welshposttown IS NOT NULL;
    """
    con.execute(sql)


def _create_dedup_view(con: duckdb.DuckDBPyConnection) -> None:
    """Create deduplicated view of all addresses.

    Priority rules for deduplication:
    - Feature type: Built Address -> Pre-Build -> Royal Mail -> Historic -> Non-Addressable
    - Address status: Approved -> Provisional -> Alternative -> Historical
    - Build status: Built Complete -> Under Construction -> Prebuild -> Historic -> Demolished

    Excludes Non-Addressable Objects from output.
    """
    dedup_sql = """
        CREATE OR REPLACE TEMP VIEW all_full_addresses_dedup AS
        WITH ranked AS (
          SELECT
            *,
            CASE feature_type
              WHEN 'Built Address' THEN 1
              WHEN 'Pre-Build Address' THEN 2
              WHEN 'Royal Mail Address' THEN 3
              WHEN 'Historic Address' THEN 4
              WHEN 'Non-Addressable Object' THEN 5
              ELSE 9
            END AS feature_type_rank,
            CASE
              WHEN lower(coalesce(address_status, '')) = 'approved' THEN 1
              WHEN lower(coalesce(address_status, '')) = 'provisional' THEN 2
              WHEN lower(coalesce(address_status, '')) = 'alternative' THEN 3
              WHEN lower(coalesce(address_status, '')) = 'historical' THEN 9
              ELSE 5
            END AS address_status_rank,
            CASE
              WHEN lower(coalesce(build_status, '')) = 'built complete' THEN 1
              WHEN lower(coalesce(build_status, '')) = 'under construction' THEN 2
              WHEN lower(coalesce(build_status, '')) = 'prebuild' THEN 3
              WHEN lower(coalesce(build_status, '')) = 'historic' THEN 8
              WHEN lower(coalesce(build_status, '')) = 'demolished' THEN 9
              ELSE 5
            END AS build_status_rank,
            ROW_NUMBER() OVER (
              PARTITION BY uprn, full_address_with_postcode
              ORDER BY
                feature_type_rank,
                address_status_rank,
                build_status_rank
            ) AS rn
          FROM all_full_addresses
          WHERE feature_type != 'Non-Addressable Object'
        )
        SELECT
          uprn,
          full_address_with_postcode,
          filename,
          feature_type,
          address_status,
          build_status,
          postcodesource,
          classification_code,
          matched_address_feature_type,
          language
        FROM ranked
        WHERE rn = 1;
    """
    con.execute(dedup_sql)


def _hash_partition_predicate(num_chunks: int, chunk_index: int) -> str:
    """Build a hash partition predicate for UPRN.

    Args:
        num_chunks: Total number of chunks.
        chunk_index: Zero-based chunk index.

    Returns:
        SQL predicate string for the partition.
    """
    return f"abs(hash(uprn)) % {num_chunks} = {chunk_index}"


def run_flatfile_step(settings: Settings, force: bool = False) -> list[Path]:
    """Run the flatfile step of the pipeline.

    Transforms extracted parquet files into the final flatfile format
    suitable for UK address matching.

    Args:
        settings: Application settings.
        force: Force recreation even if output exists.

    Returns:
        List of output parquet file paths.

    Raises:
        ToFlatfileError: If transformation fails.
    """
    t0 = perf_counter()

    parquet_dir = settings.paths.extracted_dir / "parquet"
    output_dir = settings.paths.output_dir
    num_chunks = settings.processing.num_chunks

    # Check for existing output
    output_pattern = "ngd_for_uk_address_matcher.chunk_*.parquet"
    existing_outputs = list(output_dir.glob(output_pattern)) if output_dir.exists() else []

    if existing_outputs and not force:
        logger.info(
            "Output files already exist (%d files). Use --force to regenerate.",
            len(existing_outputs),
        )
        return existing_outputs

    # Clear existing outputs on force
    if existing_outputs and force:
        for f in existing_outputs:
            f.unlink()
            logger.debug("Removed existing output: %s", f.name)

    # Check parquet directory exists
    if not parquet_dir.exists():
        raise ToFlatfileError(
            f"Parquet directory not found: {parquet_dir}. Run --step extract first."
        )

    parquet_files = list(parquet_dir.glob("*.parquet"))
    if not parquet_files:
        raise ToFlatfileError(f"No parquet files found in {parquet_dir}. Run --step extract first.")

    logger.info("Processing %d parquet files from %s", len(parquet_files), parquet_dir)

    # Create DuckDB connection
    con = create_duckdb_connection(settings)

    # Set temp directory for spill
    output_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = output_dir / "duckdb_tmp"
    temp_dir.mkdir(exist_ok=True)
    con.execute(f"PRAGMA temp_directory='{temp_dir.as_posix()}'")

    # Create views for each parquet file
    created_views: list[str] = []

    for path in sorted(parquet_files):
        stem = path.stem.lower()
        view_name = f"addr_{stem.replace('-', '_')}"

        if stem in CORE_FEATURE_STEMS:
            logger.debug("Creating core feature view for %s", path.name)
            _create_core_feature_view(con, view_name, path)
            created_views.append(view_name)

        elif stem in ALTADD_STEMS:
            feature_type = FEATURE_TYPE_BY_STEM.get(stem, "Alternate Address")
            logger.debug("Creating alternate address view for %s", path.name)
            _create_altadd_view(con, view_name, path, feature_type)
            created_views.append(view_name)

        elif stem == "add_gb_royalmailaddress":
            logger.debug("Creating Royal Mail view for %s", path.name)
            _create_royal_mail_view(con, view_name, path)
            created_views.append(view_name)

        else:
            logger.debug("Skipping %s (not a recognized address file)", path.name)
            continue

    if not created_views:
        raise ToFlatfileError("No valid address parquet files found to process.")

    logger.info("Created %d address views", len(created_views))

    # Union all views into a single table
    union_sql = " \nUNION ALL\n".join(f"SELECT * FROM {v}" for v in created_views)
    logger.info("Creating union table of all addresses...")
    con.execute(f"""
        CREATE OR REPLACE TABLE all_full_addresses AS
        {union_sql};
    """)

    # Create deduplicated view
    logger.info("Creating deduplicated view...")
    _create_dedup_view(con)

    # Get count
    count_result = con.execute("SELECT COUNT(*) FROM all_full_addresses_dedup").fetchone()
    total_count = count_result[0] if count_result else 0
    logger.info("Total addresses after deduplication: %d", total_count)

    # Export to parquet file(s)
    output_files: list[Path] = []

    if num_chunks <= 1:
        # Single file output
        output_path = output_dir / "ngd_for_uk_address_matcher.chunk_001_of_001.parquet"
        logger.info("Exporting to %s...", output_path.name)

        if output_path.exists():
            output_path.unlink()

        con.execute(f"""
            COPY (
                SELECT * FROM all_full_addresses_dedup
            ) TO '{output_path.as_posix()}' (FORMAT 'PARQUET');
        """)
        output_files.append(output_path)

    else:
        # Multi-chunk output
        logger.info("Splitting output into %d chunks...", num_chunks)

        for i in range(num_chunks):
            chunk_name = f"ngd_for_uk_address_matcher.chunk_{i + 1:03d}_of_{num_chunks:03d}.parquet"
            output_path = output_dir / chunk_name

            logger.info("Exporting chunk %d/%d: %s", i + 1, num_chunks, chunk_name)

            if output_path.exists():
                output_path.unlink()

            con.execute(f"""
                COPY (
                    SELECT * FROM all_full_addresses_dedup
                    WHERE {_hash_partition_predicate(num_chunks, i)}
                ) TO '{output_path.as_posix()}' (FORMAT 'PARQUET');
            """)
            output_files.append(output_path)

    # Cleanup temp directory
    try:
        import shutil

        shutil.rmtree(temp_dir)
    except Exception as e:
        logger.warning("Failed to remove temp directory %s: %s", temp_dir, e)

    con.close()

    elapsed = perf_counter() - t0
    logger.info(
        "Flatfile step completed in %.2f seconds. Output: %d file(s)", elapsed, len(output_files)
    )

    return output_files
