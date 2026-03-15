"""Data analysis helper utilities.

Functions here are either called by agent-generated Bash scripts, or imported
directly by other tool modules (e.g. csv_to_sqlite for the SQLite tool).
"""

from __future__ import annotations

import logging
import sqlite3
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DataFrame helpers
# ---------------------------------------------------------------------------

def load_dataframe(path: str | Path) -> pd.DataFrame:
    """Load a CSV or Excel file into a pandas DataFrame."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Data file not found: {path}")
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    return pd.read_csv(path)


def summarise_dataframe(df: pd.DataFrame) -> str:
    """Return a concise text summary of a DataFrame."""
    lines = [
        f"Shape: {df.shape[0]} rows × {df.shape[1]} columns",
        f"Columns: {', '.join(df.columns.tolist())}",
        "",
        "Data types:",
        df.dtypes.to_string(),
        "",
        "Numeric summary:",
        df.describe(include="number").to_string() if not df.select_dtypes("number").empty else "(no numeric columns)",
        "",
        f"Missing values:\n{df.isnull().sum().to_string()}",
    ]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CSV → SQLite import
# ---------------------------------------------------------------------------

def csv_to_sqlite(
    csv_path: str | Path,
    db_path: str | Path,
    table_name: str | None = None,
    if_exists: str = "replace",
) -> str:
    """Import a CSV file into a SQLite database table.

    Args:
        csv_path:   Path to the source CSV file.
        db_path:    Path to the destination SQLite database (created if needed).
        table_name: Table name to write into. Defaults to the CSV stem.
        if_exists:  How to behave if the table already exists:
                    'replace' (default), 'append', or 'fail'.

    Returns:
        A human-readable summary of the import.

    Raises:
        FileNotFoundError: If csv_path does not exist.
        ValueError: If if_exists is not a recognised option.
    """
    csv_path = Path(csv_path)
    db_path = Path(db_path)

    if not csv_path.exists():
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    if if_exists not in {"replace", "append", "fail"}:
        raise ValueError(f"if_exists must be 'replace', 'append', or 'fail', got {if_exists!r}")

    table_name = table_name or csv_path.stem

    df = pd.read_csv(csv_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    logger.info(
        "csv_to_sqlite.ok",
        extra={
            "csv": str(csv_path),
            "db": str(db_path),
            "table": table_name,
            "rows": row_count,
        },
    )
    return (
        f"Imported {len(df)} rows into '{table_name}' "
        f"in {db_path} ({row_count} rows in table after import)."
    )
