#!/usr/bin/env bash
set -euo pipefail

uv sync --all-groups
uv run ruff check src/ tests/
uv run pytest
uv build