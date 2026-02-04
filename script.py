"""NGD Pipeline - Notebook-friendly script.

Downloads, processes and transforms NGD (National Geographic Database) data
for UK address matching.

Modify the settings below to configure the pipeline.
"""

from __future__ import annotations

import logging
from pathlib import Path

from ngd_pipeline.pipeline import run
from ngd_pipeline.settings import load_settings

# ============================================================================
# CONFIGURATION - Modify these settings as needed
# ============================================================================

# Path to config.yaml file
CONFIG_PATH = Path("config.yaml")

# Pipeline step(s) to run. Can be:
# - A single step: "download", "extract", "flatfile", or "all"
# - A list of steps to run in sequence: ["extract", "flatfile"]
STEP: str | list[str] = ["download", "extract", "flatfile"]

# Force re-run even if outputs exist
FORCE = True

# List available downloads without downloading (only for step="download")
LIST_ONLY = False

# Enable verbose/debug logging
VERBOSE = True

# ============================================================================

# Configure logging
logging.basicConfig(
    level=logging.DEBUG if VERBOSE else logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    """Run the NGD pipeline with configured settings."""
    # Load settings
    settings = load_settings(CONFIG_PATH)

    logger.info("Loaded config from %s", CONFIG_PATH)

    # Normalize step(s) to a list
    steps = [STEP] if isinstance(STEP, str) else STEP

    # Run each step in sequence
    for step in steps:
        run(
            step=step,
            settings=settings,
            force=FORCE,
            list_only=LIST_ONLY,
        )

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    main()
