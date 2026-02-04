"""Pipeline orchestrator module.

Coordinates the execution of pipeline stages with idempotency support.
"""

from __future__ import annotations

import logging
from pathlib import Path
from time import perf_counter

from ngd_pipeline.extract import run_extract_step
from ngd_pipeline.os_downloads import run_download_step
from ngd_pipeline.settings import Settings
from ngd_pipeline.to_flatfile import run_flatfile_step

logger = logging.getLogger(__name__)


class PipelineError(Exception):
    """Error during pipeline execution."""


# Glob patterns for files to clean per directory (relative to the directory)
# These are deliberately restrictive to avoid accidental deletion
_CLEAN_PATTERNS: dict[str, list[str]] = {
    # CSV files in extraction subdirectories
    "extracted_dir": ["*/*.csv", "parquet/*.parquet"],
    # Only parquet files directly in output (not recursive)
    "output_dir": ["*.parquet"],
}


def _clean_directory(directory: Path, patterns: list[str]) -> int:
    """Remove files matching specific glob patterns from a directory.

    Args:
        directory: Directory to clean.
        patterns: List of glob patterns to match.

    Returns:
        Number of files deleted.
    """
    if not directory.exists():
        return 0

    deleted = 0
    for pattern in patterns:
        for file_path in directory.glob(pattern):
            if file_path.is_file():
                file_path.unlink()
                deleted += 1
                logger.debug("Deleted: %s", file_path)

    return deleted


def _clean_outputs_for_step(step: str, settings: Settings) -> None:
    """Clean output directories for a specific step when force=True.

    Args:
        step: Pipeline step being run.
        settings: Application settings containing paths.
    """
    # Map steps to the directories they write to
    step_outputs: dict[str, list[str]] = {
        "extract": ["extracted_dir"],
        "flatfile": ["output_dir"],
        "all": ["extracted_dir", "output_dir"],
    }

    dirs_to_clean = step_outputs.get(step, [])
    if not dirs_to_clean:
        return

    total_deleted = 0
    for dir_name in dirs_to_clean:
        directory = getattr(settings.paths, dir_name)

        # Safety check: only clean directories under work_dir
        try:
            directory.relative_to(settings.paths.work_dir)
        except ValueError:
            logger.warning(
                "Refusing to clean %s - not under work_dir %s",
                directory,
                settings.paths.work_dir,
            )
            continue

        patterns = _CLEAN_PATTERNS.get(dir_name, [])
        if patterns:
            deleted = _clean_directory(directory, patterns)
            if deleted > 0:
                logger.info("Cleaned %d files from %s", deleted, directory)
            total_deleted += deleted

    if total_deleted > 0:
        logger.info("Total files cleaned: %d", total_deleted)


def _run_download(settings: Settings, force: bool, list_only: bool) -> None:
    """Run download step."""
    t0 = perf_counter()
    run_download_step(settings, force=force, list_only=list_only)
    logger.info("Download step completed in %.2f seconds", perf_counter() - t0)


def _run_extract(settings: Settings, force: bool) -> None:
    """Run extract step."""
    if force:
        _clean_outputs_for_step("extract", settings)
    t0 = perf_counter()
    run_extract_step(settings, force=force)
    logger.info("Extract step completed in %.2f seconds", perf_counter() - t0)


def _run_flatfile(settings: Settings, force: bool) -> None:
    """Run flatfile step."""
    if force:
        _clean_outputs_for_step("flatfile", settings)
    t0 = perf_counter()
    run_flatfile_step(settings, force=force)
    logger.info("Flatfile step completed in %.2f seconds", perf_counter() - t0)


def run(
    step: str,
    settings: Settings,
    force: bool = False,
    list_only: bool = False,
) -> None:
    """Run the specified pipeline step(s).

    Args:
        step: Pipeline step to run. One of: download, extract, flatfile, all.
        settings: Application settings.
        force: Force re-run even if outputs exist.
        list_only: For download step, just list available files.

    Raises:
        PipelineError: If an invalid step is specified.
    """
    valid_steps = {"download", "extract", "flatfile", "all"}

    if step not in valid_steps:
        raise PipelineError(
            f"Invalid step: {step}. Must be one of: {', '.join(sorted(valid_steps))}"
        )

    logger.info("Running pipeline step: %s (force=%s)", step, force)

    if step == "download":
        _run_download(settings, force, list_only)
    elif step == "extract":
        _run_extract(settings, force)
    elif step == "flatfile":
        _run_flatfile(settings, force)
    elif step == "all":
        if force:
            _clean_outputs_for_step("all", settings)
        _run_download(settings, force, list_only=False)
        _run_extract(settings, force)
        _run_flatfile(settings, force)

    logger.info("Pipeline step '%s' completed successfully", step)
