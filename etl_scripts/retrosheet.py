"""Retrosheet parsed CSV fetch and load (master files from csvdownloads.zip)."""

from __future__ import annotations

import csv
import os
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Mapping, Sequence
from urllib.request import Request, urlopen

from loguru import logger
import psycopg2
from psycopg2 import sql

from etl_scripts.statcast import get_database_url

RETROSHEET_CSV_ZIP_URL = "https://www.retrosheet.org/downloads/csvdownloads.zip"
RETROSHEET_USER_AGENT = (
    "Mozilla/5.0 (compatible; etl-scripts/1.0; +https://github.com/) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024
REQUEST_TIMEOUT_SEC = 600

# Retrosheet master CSV filename -> warehouse table (columns loaded as-is from the file).
RETROSHEET_CSV_TABLES: dict[str, str] = {
    "allplayers.csv": "retrosheet_allplayers",
    "gameinfo.csv": "retrosheet_gameinfo",
    "teamstats.csv": "retrosheet_teamstats",
    "batting.csv": "retrosheet_batting",
    "pitching.csv": "retrosheet_pitching",
    "fielding.csv": "retrosheet_fielding",
    "plays.csv": "retrosheet_plays",
}


def _read_csv_header(csv_path: Path) -> list[str]:
    with csv_path.open(newline="", encoding="utf-8") as f:
        row = next(csv.reader(f), None)
    if not row:
        raise ValueError(f"No header row in {csv_path}")
    return [c.strip() for c in row]


def _table_columns(conn: psycopg2.extensions.connection, table_name: str) -> list[str] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        rows = cur.fetchall()
    if not rows:
        return None
    return [r[0] for r in rows]


def _create_text_table(cur: psycopg2.extensions.cursor, table_name: str, columns: Sequence[str]) -> None:
    if not columns:
        raise ValueError(f"Cannot create {table_name} with no columns")
    col_defs = sql.SQL(", ").join(
        sql.SQL("{} TEXT").format(sql.Identifier(c)) for c in columns
    )
    stmt = sql.SQL("CREATE TABLE {} ({})").format(sql.Identifier(table_name), col_defs)
    cur.execute(stmt)


def _replace_table_schema(
    cur: psycopg2.extensions.cursor,
    table_name: str,
    columns: Sequence[str],
) -> None:
    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(sql.Identifier(table_name)))
    _create_text_table(cur, table_name, columns)


def _copy_csv(
    cur: psycopg2.extensions.cursor,
    conn: psycopg2.extensions.connection,
    *,
    table_name: str,
    columns: Sequence[str],
    csv_path: Path,
) -> int:
    fields = sql.SQL(", ").join(sql.Identifier(c) for c in columns)
    copy_stmt = sql.SQL(
        "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true)"
    ).format(sql.Identifier(table_name), fields)
    with csv_path.open(newline="", encoding="utf-8") as f:
        cur.copy_expert(copy_stmt.as_string(conn), f)
    return cur.rowcount


def load_retrosheet_csv_file(
    csv_path: Path,
    *,
    table_name: str,
    database_url: str | None = None,
) -> dict[str, Any]:
    """Load one Retrosheet CSV into ``table_name`` (full replace). Returns row count."""
    path = Path(csv_path)
    if not path.is_file():
        raise FileNotFoundError(path)

    columns = _read_csv_header(path)
    url = database_url or get_database_url()
    logger.info("Loading {} into {}", path.name, table_name)

    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            existing = _table_columns(conn, table_name)
            if existing != columns:
                if existing is not None:
                    logger.info(
                        "Recreating {} (schema changed: {} -> {} columns)",
                        table_name,
                        len(existing),
                        len(columns),
                    )
                else:
                    logger.info("Creating table {} ({} columns)", table_name, len(columns))
                _replace_table_schema(cur, table_name, columns)
            else:
                cur.execute(sql.SQL("TRUNCATE {}").format(sql.Identifier(table_name)))
            row_count = _copy_csv(cur, conn, table_name=table_name, columns=columns, csv_path=path)
        conn.commit()

    logger.info("Loaded {} into {} ({} rows)", path.name, table_name, row_count)
    return {"csv_file": path.name, "table": table_name, "rows_loaded": int(row_count), "columns": len(columns)}


def download_retrosheet_csv_zip(
    dest_path: Path | None = None,
    *,
    url: str = RETROSHEET_CSV_ZIP_URL,
) -> Path:
    """Stream-download the Retrosheet master CSV zip to ``dest_path`` (or a temp file)."""
    if dest_path is not None:
        target = Path(dest_path)
    else:
        fd, name = tempfile.mkstemp(suffix=".zip")
        os.close(fd)
        target = Path(name)
    target.parent.mkdir(parents=True, exist_ok=True)
    req = Request(url, headers={"User-Agent": RETROSHEET_USER_AGENT})
    logger.info("Downloading Retrosheet CSV zip from {}", url)
    with urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp, target.open("wb") as out:
        total = int(resp.headers.get("Content-Length") or 0)
        downloaded = 0
        while True:
            chunk = resp.read(DOWNLOAD_CHUNK_BYTES)
            if not chunk:
                break
            out.write(chunk)
            downloaded += len(chunk)
            if total:
                logger.debug("Downloaded {:.1f}%", 100.0 * downloaded / total)
    logger.info("Download complete: {} ({} bytes)", target, target.stat().st_size)
    return target


def extract_retrosheet_csvs(
    zip_path: Path,
    dest_dir: Path | None = None,
) -> dict[str, Path]:
    """Extract the seven master CSV files; returns map of csv filename -> path."""
    archive = Path(zip_path)
    if not archive.is_file():
        raise FileNotFoundError(archive)
    out_dir = Path(dest_dir) if dest_dir is not None else Path(tempfile.mkdtemp(prefix="retrosheet_csv_"))
    out_dir.mkdir(parents=True, exist_ok=True)
    extracted: dict[str, Path] = {}
    with zipfile.ZipFile(archive) as zf:
        names = {info.filename: info for info in zf.infolist() if not info.is_dir()}
        for csv_name in RETROSHEET_CSV_TABLES:
            member = next((n for n in names if n.endswith(csv_name) or n == csv_name), None)
            if member is None:
                raise FileNotFoundError(f"{csv_name} not found in {archive}")
            target = out_dir / csv_name
            with zf.open(member) as src, target.open("wb") as dst:
                dst.write(src.read())
            extracted[csv_name] = target
            logger.debug("Extracted {} -> {}", member, target)
    return extracted


def load_all_retrosheet_csvs(
    *,
    database_url: str | None = None,
    zip_url: str = RETROSHEET_CSV_ZIP_URL,
    keep_zip: bool = False,
) -> dict[str, Any]:
    """Download zip, extract master CSVs, and load each into its warehouse table."""
    url = database_url or get_database_url()
    with tempfile.TemporaryDirectory(prefix="retrosheet_etl_") as tmp:
        tmp_path = Path(tmp)
        zip_file = download_retrosheet_csv_zip(tmp_path / "csvdownloads.zip", url=zip_url)
        csv_paths = extract_retrosheet_csvs(zip_file, tmp_path / "csv")
        tables: list[dict[str, Any]] = []
        for csv_name, table_name in RETROSHEET_CSV_TABLES.items():
            summary = load_retrosheet_csv_file(
                csv_paths[csv_name],
                table_name=table_name,
                database_url=url,
            )
            tables.append(summary)
        if not keep_zip:
            zip_file.unlink(missing_ok=True)
    return {
        "zip_url": zip_url,
        "tables": tables,
        "total_rows_loaded": sum(int(t["rows_loaded"]) for t in tables),
    }


def retrosheet_table_metrics(
    database_url: str | None = None,
    *,
    tables: Mapping[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    """Row counts per Retrosheet table (empty dict if table missing)."""
    mapping = tables or RETROSHEET_CSV_TABLES
    url = database_url or get_database_url()
    metrics: dict[str, dict[str, Any]] = {}
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            for table_name in mapping.values():
                cur.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_name = %s
                    )
                    """,
                    (table_name,),
                )
                exists = bool(cur.fetchone()[0])
                if not exists:
                    metrics[table_name] = {"exists": False, "row_count": 0}
                    continue
                cur.execute(
                    sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
                )
                count = int(cur.fetchone()[0])
                metrics[table_name] = {"exists": True, "row_count": count}
    return metrics
