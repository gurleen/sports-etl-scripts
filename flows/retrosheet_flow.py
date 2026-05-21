"""Prefect flows for Retrosheet parsed CSV ingest."""

from __future__ import annotations

from typing import Any

from prefect import flow, get_run_logger, task
from prefect.artifacts import create_markdown_artifact

from etl_scripts.prefect_runtime import resolve_database_url_for_flow
from etl_scripts.retrosheet import (
    RETROSHEET_CSV_TABLES,
    RETROSHEET_CSV_ZIP_URL,
    download_retrosheet_csv_zip,
    extract_retrosheet_csvs,
    load_all_retrosheet_csvs,
    load_retrosheet_csv_file,
    retrosheet_table_metrics,
)


@task
def retrosheet_metrics_task(database_url: str) -> dict[str, dict[str, Any]]:
    return retrosheet_table_metrics(database_url)


@task
def retrosheet_download_zip_task(
    zip_url: str = RETROSHEET_CSV_ZIP_URL,
) -> str:
    """Return path to downloaded zip (stored under the flow run temp dir)."""
    import tempfile
    from pathlib import Path

    tmp = Path(tempfile.mkdtemp(prefix="retrosheet_prefect_"))
    path = download_retrosheet_csv_zip(tmp / "csvdownloads.zip", url=zip_url)
    return str(path)


@task
def retrosheet_extract_csvs_task(zip_path: str) -> dict[str, str]:
    import tempfile
    from pathlib import Path

    tmp = Path(tempfile.mkdtemp(prefix="retrosheet_csv_"))
    paths = extract_retrosheet_csvs(Path(zip_path), tmp)
    return {name: str(p) for name, p in paths.items()}


@task
def retrosheet_load_csv_task(
    csv_path: str,
    table_name: str,
    database_url: str,
) -> dict[str, Any]:
    from pathlib import Path

    return load_retrosheet_csv_file(Path(csv_path), table_name=table_name, database_url=database_url)


def _artifact_markdown(
    job: str,
    before: dict[str, dict[str, Any]],
    after: dict[str, dict[str, Any]],
    load_summary: dict[str, Any],
) -> str:
    lines = [
        f"# Retrosheet run: `{job}`",
        "",
        f"- Zip: `{load_summary.get('zip_url', RETROSHEET_CSV_ZIP_URL)}`",
        f"- Total rows loaded: **{load_summary.get('total_rows_loaded', 0)}**",
        "",
        "## Tables",
        "| Table | Before | After | Δ rows |",
        "|-------|--------|-------|--------|",
    ]
    for entry in load_summary.get("tables") or []:
        table = entry["table"]
        b = before.get(table, {})
        a = after.get(table, {})
        b_count = int(b.get("row_count") or 0)
        a_count = int(a.get("row_count") or 0)
        delta = a_count - b_count
        lines.append(f"| `{table}` | {b_count} | {a_count} | {delta:+d} |")
    return "\n".join(lines)


@flow(name="retrosheet-load-all", log_prints=True)
def retrosheet_load_all_flow(
    zip_url: str = RETROSHEET_CSV_ZIP_URL,
) -> dict[str, Any]:
    """Download Retrosheet master CSV zip and load all seven tables (full refresh)."""
    log = get_run_logger()
    database_url = resolve_database_url_for_flow()
    before = retrosheet_metrics_task(database_url)
    load_summary = load_all_retrosheet_csvs(database_url=database_url, zip_url=zip_url)
    after = retrosheet_metrics_task(database_url)
    create_markdown_artifact(
        key="retrosheet-run-summary",
        markdown=_artifact_markdown("load_all", before, after, load_summary),
    )
    log.info("Retrosheet load complete: %s", load_summary)
    return {"before": before, "after": after, "load": load_summary}


@flow(name="retrosheet-load-table", log_prints=True)
def retrosheet_load_table_flow(
    csv_file: str,
    zip_url: str = RETROSHEET_CSV_ZIP_URL,
) -> dict[str, Any]:
    """
    Load a single Retrosheet master CSV (e.g. ``gameinfo.csv``) from the official zip.

    ``csv_file`` must be one of the keys in ``RETROSHEET_CSV_TABLES``.
    """
    log = get_run_logger()
    if csv_file not in RETROSHEET_CSV_TABLES:
        raise ValueError(f"csv_file must be one of {sorted(RETROSHEET_CSV_TABLES)}; got {csv_file!r}")
    table_name = RETROSHEET_CSV_TABLES[csv_file]
    database_url = resolve_database_url_for_flow()
    before = retrosheet_metrics_task(database_url)
    zip_path = retrosheet_download_zip_task(zip_url)
    csv_paths = retrosheet_extract_csvs_task(zip_path)
    ingest = retrosheet_load_csv_task(csv_paths[csv_file], table_name, database_url)
    after = retrosheet_metrics_task(database_url)
    load_summary = {
        "zip_url": zip_url,
        "tables": [ingest],
        "total_rows_loaded": ingest["rows_loaded"],
    }
    create_markdown_artifact(
        key="retrosheet-run-summary",
        markdown=_artifact_markdown(f"load_table {csv_file}", before, after, load_summary),
    )
    log.info("Retrosheet table load: %s", ingest)
    return {"before": before, "after": after, "load": load_summary, "ingest": ingest}
