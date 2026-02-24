#!/usr/bin/env bash
set -euo pipefail

uv sync --all-groups
uv run ruff check ukam_os_builder/ tests/
uv run pytest
uv build