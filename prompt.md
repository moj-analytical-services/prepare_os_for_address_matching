# Prepare NGD (Ordnance Survey National Geographic Database) data for address matching

## Aim

Transform NGD data into a clean flatfile format suitable for use with [`uk_address_matcher`](https://github.com/moj-analytical-services/uk_address_matcher).

This repo will be laid out cleanly and professionally.

It will have the following steps:
- Download (download NGD data from from OS Data Hub)
- Extract (unzip download)
- Flatfile (Turn downloaded data into flatfile)

Each stage should be idemopotent - safe to rerun.

The final output is written as one or more parquet files:

Single chunk mode (num_chunks: 1): ngd_for_uk_address_matcher.chunk_001_of_001.parquet
Multi-chunk mode (num_chunks: N): ngd_for_uk_address_matcher.chunk_001_of_00N.parquet, chunk_002_of_00N.parquet, etc.


## Code reference

In old_code/ I have some sccripts I wrote to accomplish the task, They contain the correct logic but are not laid out nicely

## Companion repo:

This repo is a companion to:
https://github.com/moj-analytical-services/prepare_addressbase_for_address_matching

Which i have cloned to In prepare_addressbase_for_address_matching/ so you can look at the code.

Which prepares AddressBase Premium files for address matching.

The structure of the companium repo is - you shoudld use something similar.

Directory structure:
└── prepare_addressbase_for_address_matching/
    ├── README.md
    ├── AGENTS.md
    ├── config.yaml
    ├── pyproject.toml
    ├── script.py
    ├── try_view.py
    ├── .env.example
    ├── schemas/
    │   └── summary_uml.uml
    ├── scripts/
    │   ├── downloadable_files.py
    │   └── os_docs_to_md.py
    ├── src/
    │   └── abp_pipeline/
    │       ├── __init__.py
    │       ├── extract.py
    │       ├── inspect_results.py
    │       ├── os_downloads.py
    │       ├── pipeline.py
    │       ├── settings.py
    │       ├── split_raw.py
    │       ├── to_flatfile.py
    │       ├── schemas/
    │       │   └── abp_schema.yaml
    │       └── transform/
    │           ├── __init__.py
    │           ├── common.py
    │           ├── runner.py
    │           └── stages/
    │               ├── __init__.py
    │               ├── business.py
    │               ├── combine.py
    │               ├── lpi.py
    │               ├── misc.py
    │               └── postal.py
    ├── tests/
    │   ├── test_chunking.py
    │   ├── test_smoke.py
    │   └── data/
    │       └── sample_abp_lines.csv
    └── .github/
        └── workflows/
            └── tests.yml


## Downloading the datapacakge files

Notes: OS Data Hub datapackage id=16331 (Example)

Base: https://api.os.uk/downloads/v1
Auth: send header key: OS_PROJECT_API_KEY

1) List versions for a datapackage
GET /dataPackages/16331/versions
Pick the version ID from the response (field: id).

2) List files available for download
GET /dataPackages/16331/versions/{version_id}
Read downloads[] for fileName, size, md5, url.

3) Download data for a specific version
GET /dataPackages/16331/versions/{version_id}
From downloads[] pick a fileName and url, then download:
- Use the provided url and append ?key=YOUR_API_KEY (or add key as query param)
- Example pattern:
  https://api.os.uk/downloads/v1/dataPackages/16331/versions/{version_id}
  -> downloads[].url + ?key=YOUR_API_KEY
